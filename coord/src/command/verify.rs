use anyhow::{Context, Result, bail};
use url::Url;
use std::fmt::Display;
use tracing::{info, warn};
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::Semaphore;
use clap::Parser;
use reqwest::Client;
use std::time::Duration;
use std::path::Path;
use futures::stream::{self, StreamExt};
use serde_json;

use common::constants::META_KEY_PREFIX;
use common::schemas::{BlobHead, ListResponse};
use common::file_utils::meta_key_for;

use crate::core::meta::{KvDb, Meta, TxState};

#[derive(Parser, Debug, Clone)]
pub struct VerifyArgs {
    /// RocksDB directory path
    #[arg(long)]
    index: String,

    /// List of node URLs
    #[arg(long, value_delimiter = ',')]
    nodes: Vec<String>,

    /// Fix inconsistencies
    #[arg(long, default_value_t = false)]
    fix: bool,

    /// Perform deep verification (slower)
    #[arg(long, default_value_t = false)]
    deep: bool,

    /// Maximum concurrent operations
    #[arg(long, default_value_t = 16)]
    concurrency: usize,

    /// Max concurrent ops per node
    #[arg(long, default_value_t = 4)]
    per_node: usize,

    /// Request timeout (seconds) for control-plane calls
    #[arg(long, default_value_t = 5)]
    http_timeout_secs: u64,
}

#[derive(Debug, Default, Clone)]
pub struct VerifyReport {
    pub scanned_keys: usize,
    pub under_replicated: usize,
    pub corrupted: usize,            // exists but size/etag mismatch
    pub unindexed: usize,            // file exists but no meta
    pub should_gc: usize,            // file exists + meta tombstoned
}

impl VerifyReport {
    fn merge_from(&mut self, other: &Self) {
        self.scanned_keys += other.scanned_keys;
        self.under_replicated += other.under_replicated;
        self.corrupted += other.corrupted;
        self.unindexed += other.unindexed;
        self.should_gc += other.should_gc;
    }
}

impl Display for VerifyReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Verify report:")?;
        writeln!(f, "  Scanned keys:     {}", self.scanned_keys)?;
        writeln!(f, "  Under-replicated: {}", self.under_replicated)?;
        writeln!(f, "  Corrupted:        {}", self.corrupted)?;
        writeln!(f, "  Unindexed:        {}", self.unindexed)?;
        writeln!(f, "  Should GC:        {}", self.should_gc)?;
        Ok(())
    }
}


pub async fn verify(args: VerifyArgs) -> anyhow::Result<()> {
    // Build node map: node_id -> internal_url
    let nodes = parse_nodes(&args.nodes)?;
    if nodes.is_empty() {
        bail!("--nodes must specify at least one node, e.g. nodeA=http://127.0.0.1:3001");
    }

    let db = KvDb::open(&Path::new(&args.index))
        .with_context(|| format!("opening RocksDB at {}", args.index))?;

    let http = Client::builder()
        .pool_idle_timeout(Duration::from_secs(30))
        .tcp_keepalive(Duration::from_secs(30))
        .build()?;

    // Per-node concurrency caps
    let per_node: Arc<HashMap<String, Arc<Semaphore>>> = Arc::new(
        nodes
            .keys()
            .map(|id| (id.clone(), Arc::new(Semaphore::new(args.per_node))))
            .collect(),
    );

    let mut report = VerifyReport::default();

    let db_part =
        walk_db(&db, &http, &nodes, args.deep, args.concurrency, args.http_timeout_secs, per_node.clone()).await?;
    report.merge_from(&db_part);

    let vol_part =
        walk_volumes(&db, &http, &nodes, args.fix, args.concurrency, args.http_timeout_secs).await?;
    report.merge_from(&vol_part);

    info!("{}", report);
    Ok(())
}

/// Verify all committed metas against volumes listed in meta.replicas.
/// Classifies missing replicas (under_replicated) and content mismatches (corrupted).
async fn walk_db(
    db: &KvDb,
    http: &Client,
    nodes: &HashMap<String, Url>,
    deep: bool,
    concurrency: usize,
    http_timeout_secs: u64,
    per_node: Arc<HashMap<String, Arc<Semaphore>>>,
) -> Result<VerifyReport> {
    let mut keys: Vec<(String, Meta)> = Vec::new();

    for kv in db.iter() {
        let (k, v) = kv?;
        if !k.starts_with(META_KEY_PREFIX.as_bytes()) { continue; }
        let key_enc = std::str::from_utf8(&k)?.strip_prefix(META_KEY_PREFIX).unwrap().to_string();
        let meta: Meta = serde_json::from_slice(&v)?;
        if !matches!(meta.state, TxState::Committed) { continue; }
        keys.push((key_enc, meta));
    }

    let total = keys.len();

    // Process keys with bounded concurrency
    let per_request_timeout = Duration::from_secs(http_timeout_secs);
    let results = stream::iter(keys)
        .map(|(key_enc, meta)| {
            let http = http.clone();
            let nodes = nodes.clone();
            let per_node = per_node.clone();

            async move {
                let mut local = VerifyReport::default();
                local.scanned_keys = 1;

                // Iterate expected replicas; resolve node_id -> Url
                let expected: Vec<&String> = meta.replicas.iter().collect();

                for node_id in expected {
                    let base = match nodes.get(node_id) {
                        Some(u) => u.clone(),
                        None => {
                            // If we don't know this node, treat as under-replicated
                            local.under_replicated += 1;
                            continue;
                        }
                    };

                    // Per-node slot
                    let Some(sem) = per_node.get(node_id) else { continue; };
                    let _permit = sem.acquire().await.ok();

                    // Build URL: {base}/admin/blob?key=...&deep=...
                    let mut url = base.clone();
                    url.set_path("/admin/blob");
                    url.query_pairs_mut()
                        .append_pair("key", &key_enc)
                        .append_pair("deep", if deep { "true" } else { "false" });

                    // Small, time-boxed retry for transient failures
                    let mut attempts = 0usize;
                    let head: Option<BlobHead> = loop {
                        attempts += 1;
                        match http.get(url.clone())
                            .timeout(per_request_timeout)
                            .send().await
                        {
                            Ok(resp) if resp.status().is_success() => match resp.json::<BlobHead>().await {
                                Ok(h) => break Some(h),
                                Err(e) => {
                                    warn!(%e, "bad JSON from {}", url);
                                    break None;
                                }
                            },
                            Ok(resp) => {
                                // 5xx retryable, 4xx treat as missing
                                if resp.status().is_server_error() && attempts < 3 {
                                    continue;
                                } else {
                                    warn!(status = %resp.status(), %url, "head non-success");
                                    break None;
                                }
                            }
                            Err(e) => {
                                if (e.is_connect() || e.is_timeout() || e.is_request()) && attempts < 3 {
                                    continue;
                                } else {
                                    warn!(error = %e, %url, "head transport error");
                                    break None;
                                }
                            }
                        }
                    };

                    match head {
                        None => {
                            // treat as missing copy
                            local.under_replicated += 1;
                        }
                        Some(h) if !h.exists => {
                            local.under_replicated += 1;
                        }
                        Some(h) => {
                            // size mismatch => corrupted
                            if h.size != meta.size {
                                local.corrupted += 1;
                                continue;
                            }
                            // deep=etag mismatch => corrupted
                            if deep {
                                if let Some(et) = h.etag.as_deref() {
                                    if !meta.etag_hex.is_empty() && et != meta.etag_hex {
                                        local.corrupted += 1;
                                    }
                                } else {
                                    // server didn't return etag in deep mode; warn but don't count as corrupted
                                    warn!(%url, "server returned etag but deep=false");
                                }
                            }
                        }
                    }
                }

                Ok::<VerifyReport, anyhow::Error>(local)
            }
        })
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await;

    let mut acc = VerifyReport::default();
    for r in results {
        match r {
            Ok(local) => acc.merge_from(&local),
            Err(e) => warn!(%e, "verify task failed"),
        }
    }

    info!("verify-db: scanned={} (of {}) under_repl={} corrupted={}",
        acc.scanned_keys, total, acc.under_replicated, acc.corrupted);

    Ok(acc)
}

/// Walk volumes (filesystem view) and find:
/// - files with no meta (unindexed)
/// - files that should be GC'ed (meta is tombstoned)
/// If --fix is set, **delete** tombstoned files (safe GC); we do NOT auto-create metas for unindexed (no resurrection in verify).
async fn walk_volumes(
    db: &KvDb,
    http: &Client,
    nodes: &HashMap<String, Url>,
    fix: bool,
    concurrency: usize,
    http_timeout_secs: u64,
) -> Result<VerifyReport> {
    let per_request_timeout = Duration::from_secs(http_timeout_secs);

    // Build list of (node_id, base_url)
    let vols: Vec<(String, Url)> = nodes.iter().map(|(id, u)| (id.clone(), u.clone())).collect();

    let results = stream::iter(vols)
        .map(|(node_id, base)| {
            let http = http.clone();
            let db = db.clone();

            async move {
                let mut local = VerifyReport::default();

                let mut after: Option<String> = None;
                loop {
                    let mut url = base.clone();
                    url.set_path("/admin/list");
                    {
                        let mut qp = url.query_pairs_mut();
                        qp.append_pair("limit", "1000");
                        if let Some(a) = &after {
                            qp.append_pair("after", a);
                        }
                    }

                    let page = match http.get(url.clone())
                        .timeout(per_request_timeout)
                        .send().await
                    {
                        Ok(r) if r.status().is_success() => r.json::<ListResponse>().await
                            .with_context(|| format!("decoding list page from {}", base)),
                        Ok(r) => {
                            warn!(status = %r.status(), node=%node_id, "list non-success");
                            break Ok::<VerifyReport, anyhow::Error>(local);
                        }
                        Err(e) => {
                            warn!(%e, node=%node_id, "list transport error");
                            break Ok(local);
                        }
                    }?;

                    for key_enc in page.keys {
                        let mk = meta_key_for(&key_enc);
                        match db.get::<Meta>(&mk) {
                            Ok(Some(meta)) => {
                                match meta.state {
                                    TxState::Tombstoned => {
                                        local.should_gc += 1;
                                        if fix {
                                            // Safe fix: issue internal delete to this volume (idempotent)
                                            let mut del = base.clone();
                                            del.set_path("/internal/delete");
                                            del.query_pairs_mut().append_pair("key", &key_enc);
                                            let _ = http.post(del).timeout(per_request_timeout).send().await;
                                        }
                                    }
                                    TxState::Committed => { /* ok */ }
                                    _ => { /* pending etc., ignore */ }
                                }
                            }
                            Ok(None) | Err(_) => {
                                // file exists on FS but not indexed
                                local.unindexed += 1;
                            }
                        }
                    }

                    after = page.next_after;
                    if after.is_none() {
                        break Ok(local);
                    }
                }
            }
        })
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await;

    let mut acc = VerifyReport::default();
    for r in results {
        match r {
            Ok(local) => acc.merge_from(&local),
            Err(e) => warn!(%e, "volume walk failed"),
        }
    }

    info!("verify-vol: unindexed={} should_gc={} (fix={})",
        acc.unindexed, acc.should_gc, fix);

    Ok(acc)
}

fn parse_nodes(items: &[String]) -> Result<HashMap<String, Url>> {
    let mut out = HashMap::new();
    for raw in items {
        // Accept "nodeA=http://127.0.0.1:3001" OR "http://127.0.0.1:3001"
        if let Some((id, url_str)) = raw.split_once('=') {
            let url = Url::parse(url_str)
                .with_context(|| format!("bad URL for node {}: {}", id, url_str))?;
            out.insert(id.to_string(), url);
        } else {
            let url = Url::parse(raw)
                .with_context(|| format!("bad URL: {}", raw))?;
            let id = derive_node_id(&url);
            out.insert(id, url);
        }
    }
    Ok(out)
}

fn derive_node_id(u: &Url) -> String {
    // host:port is good enough if user didn't provide an explicit id
    match (u.host_str(), u.port()) {
        (Some(h), Some(p)) => format!("{h}:{p}"),
        (Some(h), None) => h.to_string(),
        _ => u.as_str().to_string(),
    }
}