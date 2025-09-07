use anyhow::{Result, Context, bail};
use clap::Parser;
use futures::stream::{self, StreamExt};
use humantime;
use reqwest::{Client, Url};
use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::sync::Semaphore;
use tracing::{info, warn};

use common::constants::META_KEY_PREFIX;
use common::schemas::{ListResponse, SweepTmpResponse};
use common::time_utils::utc_now_ms;

use crate::command::common::{nodes_from_db, nodes_from_explicit};
use crate::core::meta::{KvDb, Meta, TxState};

#[derive(Parser, Debug, Clone)]
pub struct GcArgs {
    #[arg(long)]
    pub index: PathBuf,

    /// If omitted, use node registry (node:*). Otherwise, explicit volume URLs
    #[arg(long, value_delimiter = ',')]
    pub volumes: Option<Vec<String>>,

    /// Tombstone TTL (e.g., "7d", "12h")
    #[arg(long, default_value = "7d")]
    pub tombstone_ttl: String,

    /// Purge tombstone metas after deletes
    #[arg(long, default_value_t = false)]
    pub purge_tombstone_meta: bool,

    /// Sweep tmp files older than this age (e.g., "1h")
    #[arg(long, default_value = "1h")]
    pub sweep_tmp_age: String,

    /// Also delete extra copies not in meta.replicas
    #[arg(long, default_value_t = false)]
    pub delete_extraneous: bool,

    /// Also delete files without meta
    #[arg(long, default_value_t = false)]
    pub purge_orphans: bool,

    /// Delete tombstones on all nodes (not only those in meta.replicas)
    #[arg(long, default_value_t = false)]
    pub broadcast_deletes: bool,

    /// Force purge metas even if deletes fail
    #[arg(long, default_value_t = false)]
    pub force_purge: bool,

    /// Concurrency controls
    #[arg(long, default_value_t = 16)]
    pub concurrency: usize,

    #[arg(long, default_value_t = 2)]
    pub per_node: usize,

    /// Per-request timeout (seconds)
    #[arg(long, default_value_t = 5)]
    pub http_timeout_secs: u64,

    /// Dry run: report only
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
}

#[derive(Default)]
pub struct GcReport {
    pub tombstones_scanned: usize,
    pub tombstones_past_ttl: usize,
    pub tombstone_deletes_sent: usize,
    pub tombstone_metas_purged: usize,
    pub tmp_swept_volumes: usize,
    pub tmp_removed_total: u64,
    pub extraneous_deleted: usize,
    pub orphans_deleted: usize,
    pub orphans_found: usize,
    pub extraneous_found: usize,
    pub list_pages_scanned: usize,
    pub probe_errors: usize,
    pub sample_tombstones: Vec<String>,
    pub sample_orphans: Vec<String>,
    pub sample_extras: Vec<(String/*key*/, String/*node*/)>
}

impl Display for GcReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "GC report:")?;
        writeln!(f, "  Tombstones scanned  = {}", self.tombstones_scanned)?;
        writeln!(f, "  Tombstones past TTL = {}", self.tombstones_past_ttl)?;
        writeln!(f, "  Deletes sent        = {}", self.tombstone_deletes_sent)?;
        writeln!(f, "  Metas purged        = {}", self.tombstone_metas_purged)?;
        writeln!(f, "  Tmp swept volumes   = {}", self.tmp_swept_volumes)?;
        writeln!(f, "  Tmp removed total   = {}", self.tmp_removed_total)?;
        writeln!(f, "  Extraneous found    = {}", self.extraneous_found)?;
        writeln!(f, "  Extraneous deleted  = {}", self.extraneous_deleted)?;
        writeln!(f, "  Orphans found       = {}", self.orphans_found)?;
        writeln!(f, "  Orphans deleted     = {}", self.orphans_deleted)?;
        writeln!(f, "  List pages scanned  = {}", self.list_pages_scanned)?;
        writeln!(f, "  Probe errors        = {}", self.probe_errors)?;
        Ok(())
    }
}

pub async fn gc(args: GcArgs) -> Result<()> {
    let http = Client::builder()
        .pool_idle_timeout(Duration::from_secs(30))
        .tcp_keepalive(Duration::from_secs(30))
        .build()?;
    let db = KvDb::open(&args.index)?;

    // Resolve nodes
    let nodes = if let Some(vs) = args.volumes.clone() {
        nodes_from_explicit(&vs)
    } else {
        nodes_from_db(&db, true /* include suspect */)?
    };
    if nodes.is_empty() { bail!("gc: no volumes available"); }

    // Map node_id -> Url
    let volumes: HashMap<String, Url> = nodes.iter()
        .map(|n| (n.node_id.clone(), Url::parse(&n.internal_url).expect("valid url")))
        .collect();

    let mut report = GcReport::default();
    let tombstone_ttl = humantime::parse_duration(&args.tombstone_ttl)?;
    let sweep_tmp_age = humantime::parse_duration(&args.sweep_tmp_age)?;
    let timeout = Duration::from_secs(args.http_timeout_secs);

    // Semaphores
    let global = Arc::new(Semaphore::new(args.concurrency));
    let per_node: Arc<HashMap<String, Arc<Semaphore>>> = Arc::new(
        volumes.keys()
            .map(|id| (id.clone(), Arc::new(Semaphore::new(args.per_node))))
            .collect()
    );

    sweep_tmp_on_all(&http, &volumes, sweep_tmp_age, timeout, &args, &global, &per_node, &mut report).await?;
    clean_tombstones(&http, &db, &volumes, tombstone_ttl, timeout, &args, &global, &per_node, &mut report).await?;
    if args.delete_extraneous || args.purge_orphans {
        clean_extraneous_and_orphans(&http, &db, &volumes, timeout, &args, &mut report).await?;
    }

    info!("{}", report);
    Ok(())
}


#[allow(clippy::too_many_arguments)]
async fn sweep_tmp_on_all(
    http: &Client,
    volumes: &HashMap<String, Url>,
    safe_age: Duration,
    timeout: Duration,
    args: &GcArgs,
    global: &Arc<Semaphore>,
    per_node: &Arc<HashMap<String, Arc<Semaphore>>>,
    report: &mut GcReport,
) -> Result<()> {
    let tasks = stream::iter(volumes.iter())
        .map(|(id, base)| {
            let http = http.clone();
            let id = id.clone();
            let mut url = base.clone();
            url.set_path("/admin/sweep_tmp");
            url.query_pairs_mut()
                .append_pair("safe_age_secs", &safe_age.as_secs().to_string());
            let global = Arc::clone(global);
            let per = Arc::clone(per_node);
            let dry = args.dry_run;

            async move {
                let _g = global.acquire_owned().await?;
                let _p = per.get(&id).unwrap().clone().acquire_owned().await?;
                if dry { return Ok::<(u64,bool), anyhow::Error>((0, false)); }
                let r = http.post(url).timeout(timeout).send().await?;
                if !r.status().is_success() { return Ok((0,false)); }
                let resp: SweepTmpResponse = r.json().await?;
                Ok((resp.removed, true))
            }
        })
        .buffer_unordered(volumes.len().max(1))
        .collect::<Vec<_>>()
        .await;

    for (removed, counted) in tasks.into_iter().flatten() {
        if counted { report.tmp_swept_volumes += 1; }
        report.tmp_removed_total += removed;
    }
    Ok(())
}


#[allow(clippy::too_many_arguments)]
async fn clean_tombstones(
    http: &Client,
    db: &KvDb,
    volumes: &HashMap<String, Url>,
    ttl: Duration,
    timeout: Duration,
    args: &GcArgs,
    global: &Arc<Semaphore>,
    per_node: &Arc<HashMap<String, Arc<Semaphore>>>,
    report: &mut GcReport,
) -> Result<()> {
    let ttl_ms: i128 = ttl.as_millis() as i128;
    let now_ms = utc_now_ms();

    for kv in db.iter() {
        let (k, v) = kv?;
        if !k.starts_with(META_KEY_PREFIX.as_bytes()) { continue; }
        let key_enc = std::str::from_utf8(&k)?.strip_prefix(META_KEY_PREFIX).unwrap().to_string();
        let meta: Meta = serde_json::from_slice(&v)?;
        if meta.state != TxState::Tombstoned { continue; }

        report.tombstones_scanned += 1;
        if now_ms - meta.created_ms < ttl_ms { continue; }
        report.tombstones_past_ttl += 1;
        if report.sample_tombstones.len() < 10 { report.sample_tombstones.push(key_enc.clone()); }

        // Decide delete targets
        let targets: Vec<&String> = if args.broadcast_deletes {
            volumes.keys().collect()
        } else {
            // Only meta.replicas by default
            meta.replicas.iter().filter(|id| volumes.contains_key(*id)).collect()
        };

        // Fan out deletes with per-node/global caps
        let sent = delete_on(
            http, volumes, &targets, &key_enc, timeout, args.dry_run, global, per_node
        ).await?;
        report.tombstone_deletes_sent += sent;

        if args.purge_tombstone_meta && (args.force_purge || sent == targets.len()) && !args.dry_run {
            db.delete(&format!("{}{}", META_KEY_PREFIX, key_enc))?;
            report.tombstone_metas_purged += 1;
        }
    }
    Ok(())
}


#[allow(clippy::too_many_arguments)]
async fn delete_on(
    http: &Client,
    volumes: &HashMap<String, Url>,
    node_ids: &[&String],
    key_enc: &str,
    timeout: Duration,
    dry_run: bool,
    global: &Arc<Semaphore>,
    per_node: &Arc<HashMap<String, Arc<Semaphore>>>,
) -> Result<usize> {
    let results = stream::iter(node_ids.iter())
        .map(|id| {
            let http = http.clone();
            let id = (*id).clone();
            let mut url = volumes[&id].clone();
            url.set_path("/internal/delete");
            url.query_pairs_mut().append_pair("key", key_enc);
            let global = Arc::clone(global);
            let per = Arc::clone(per_node);

            async move {
                let _g = global.acquire_owned().await?;
                let _p = per.get(&id).unwrap().clone().acquire_owned().await?;
                if dry_run { return Ok::<bool, anyhow::Error>(true); }
                let r = http.post(url).timeout(timeout).send().await;
                match r {
                    Ok(resp) if resp.status().is_success() => Ok(true),
                    Ok(resp) => { warn!(status=%resp.status(), node=%id, key=%key_enc, "delete non-success"); Ok(false) }
                    Err(e) => { warn!(%e, node=%id, key=%key_enc, "delete error"); Ok(false) }
                }
            }
        })
        .buffer_unordered(node_ids.len().max(1))
        .collect::<Vec<_>>()
        .await;

    Ok(results.into_iter().filter_map(|r| r.ok()).filter(|&ok| ok).count())
}


async fn clean_extraneous_and_orphans(
    http: &Client,
    db: &KvDb,
    volumes: &HashMap<String, Url>,
    timeout: Duration,
    args: &GcArgs,
    report: &mut GcReport,
) -> Result<()> {
    // Build expected: key -> expected replica set
    let mut expected: HashMap<String, HashSet<String>> = HashMap::new();
    for kv in db.iter() {
        let (k, v) = kv?;
        if !k.starts_with(META_KEY_PREFIX.as_bytes()) { continue; }
        let key_enc = std::str::from_utf8(&k)?.strip_prefix(META_KEY_PREFIX).unwrap().to_string();
        let meta: Meta = serde_json::from_slice(&v)?;
        if meta.state == TxState::Committed {
            expected.insert(key_enc, meta.replicas.into_iter().collect());
        }
    }

    for (node_id, base) in volumes {
        let mut after: Option<String> = None;
        loop {
            let mut url = base.clone();
            url.set_path("/admin/list");
            {
                let mut qp = url.query_pairs_mut();
                qp.append_pair("limit", "1000");
                if let Some(a) = &after { qp.append_pair("after", a); }
            }

            let r = http.get(url.clone()).timeout(timeout).send().await;
            let resp = match r {
                Ok(resp) if resp.status().is_success() => resp,
                Ok(resp) => { warn!(status=%resp.status(), node=%node_id, "list non-success"); break; }
                Err(e) => { warn!(%e, node=%node_id, "list error"); report.probe_errors += 1; break; }
            };
            let page: ListResponse = resp.json().await.with_context(|| format!("decode list page {}", url))?;
            report.list_pages_scanned += 1;

            for key_enc in page.keys {
                match expected.get(&key_enc) {
                    Some(repls) => {
                        if !repls.contains(node_id) {
                            report.extraneous_found += 1;
                            if report.sample_extras.len() < 10 {
                                report.sample_extras.push((key_enc.clone(), node_id.clone()));
                            }
                            if args.delete_extraneous && !args.dry_run {
                                let mut del = base.clone();
                                del.set_path("/internal/delete");
                                del.query_pairs_mut().append_pair("key", &key_enc);
                                let _ = http.post(del).timeout(timeout).send().await;
                                report.extraneous_deleted += 1;
                            }
                        }
                    }
                    None => {
                        report.orphans_found += 1;
                        if report.sample_orphans.len() < 10 {
                            report.sample_orphans.push(key_enc.clone());
                        }
                        if args.purge_orphans && !args.dry_run {
                            let mut del = base.clone();
                            del.set_path("/internal/delete");
                            del.query_pairs_mut().append_pair("key", &key_enc);
                            let _ = http.post(del).timeout(timeout).send().await;
                            report.orphans_deleted += 1;
                        }
                    }
                }
            }

            after = page.next_after;
            if after.is_none() { break; }
        }
    }
    Ok(())
}
