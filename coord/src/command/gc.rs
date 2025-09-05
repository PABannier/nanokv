use clap::Parser;
use humantime;
use std::path::PathBuf;
use std::sync::Arc;
use std::collections::{HashMap, HashSet};
use tokio::sync::Semaphore;
use std::fmt::Display;
use futures_util::{stream::FuturesUnordered, future::join_all, StreamExt};

use reqwest::Client;
use std::time::Duration;
use tracing::info;

use common::time_utils::utc_now_ms;
use common::schemas::{SweepTmpResponse, ListResponse};
use common::constants::META_KEY_PREFIX;

use crate::core::node::{discover_alive_nodes, build_explicit_volume_urls};
use crate::core::meta::{KvDb, Meta, TxState};

#[derive(Parser, Debug, Clone)]
pub struct GcArgs {
    #[arg(long)]
    index: PathBuf,

    /// If omitted, use node registry (node:* entries). Otherwise, explicit volumes.
    #[arg(long, value_delimiter = ',')]
    volumes: Option<Vec<String>>,

    /// Delete blobs for tombstones older than TTL (e.g., "7d", "12h")
    #[arg(long, default_value = "7d")]
    tombstone_ttl: String,

    /// After deleting blob copies, purge tombstone metas from RocksDB
    #[arg(long, default_value_t = false)]
    purge_tombstone_meta: bool,

    /// Sweep tmp/ on volumes for files older than this age (e.g., "1h")
    #[arg(long, default_value = "1h")]
    sweep_tmp_age: String,

    /// Also delete extra copies not in meta.replicas (over-replication)
    #[arg(long, default_value_t = false)]
    delete_extraneous: bool,

    /// Also delete files without any meta (orphans)
    #[arg(long, default_value_t = false)]
    purge_orphans: bool,

    /// Concurrency controls
    #[arg(long, default_value_t = 16)]
    concurrency: usize,

    #[arg(long, default_value_t = 2)]
    per_node: usize,

    /// Dry run: report only, make no changes
    #[arg(long, default_value_t = false)]
    dry_run: bool,
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
}

impl Display for GcReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "GC report:")?;
        writeln!(f, "  Tombstones scanned: {}", self.tombstones_scanned)?;
        writeln!(f, "  Tombstones past TTL: {}", self.tombstones_past_ttl)?;
        writeln!(f, "  Tombstone deletes sent: {}", self.tombstone_deletes_sent)?;
        writeln!(f, "  Tombstone metas purged: {}", self.tombstone_metas_purged)?;
        writeln!(f, "  Tmp swept volumes: {}", self.tmp_swept_volumes)?;
        writeln!(f, "  Tmp removed total: {}", self.tmp_removed_total)?;
        writeln!(f, "  Extraneous deleted: {}", self.extraneous_deleted)?;
        writeln!(f, "  Orphans deleted: {}", self.orphans_deleted)?;
        writeln!(f, "  Orphans found: {}", self.orphans_found)?;
        writeln!(f, "  Extraneous found: {}", self.extraneous_found)?;
        Ok(())
    }
}

pub async fn gc(args: GcArgs) -> anyhow::Result<()> {
    let http = Client::builder()
        .pool_idle_timeout(Duration::from_secs(30))
        .tcp_keepalive(Duration::from_secs(30))
        .build()?;
    let db = KvDb::open(&args.index)?;

    let nodes = match discover_alive_nodes(&db, true /* include suspect */) {
        Ok(n) => n,
        Err(_) => { build_explicit_volume_urls(args.volumes.clone()) }
    };
    if nodes.is_empty() { anyhow::bail!("gc: no volumes available"); }

    let volumes = nodes.iter()
        .map(|n| (n.node_id.clone(), n.internal_url.clone()))
        .collect::<Vec<(String, String)>>();

    let mut report = GcReport::default();

    let tombstone_ttl = humantime::parse_duration(&args.tombstone_ttl)?;
    let sweep_tmp_age = humantime::parse_duration(&args.sweep_tmp_age)?;

    // Semaphores
    let global = Arc::new(Semaphore::new(args.concurrency));
    let per_node: Arc<HashMap<String, Arc<Semaphore>>> =
        Arc::new(volumes.iter().map(|(id, _)| (id.clone(), Arc::new(Semaphore::new(args.per_node)))).collect());

    // Sweep tmp/ on volumes
    {
        let mut tasks = FuturesUnordered::new();
        for (id, base) in &volumes {
            let url = format!("{}/admin/sweep_tmp?safe_age_secs={}", base, sweep_tmp_age.as_secs());

            let _id = id.clone();
            let dry = args.dry_run;
            let http = http.clone();

            let global_sem = global.clone();
            let per_node_sem = per_node.get(id).unwrap().clone();

            tasks.push(tokio::spawn(async move {
                let _global_permit = global_sem.acquire().await.unwrap();
                let _per_node_permit = per_node_sem.acquire().await.unwrap();

                if dry { return Ok::<(u64,bool), anyhow::Error>((0, false)); }

                let r = http.post(&url).send().await?.error_for_status()?;
                let resp: SweepTmpResponse = r.json().await?;
                Ok::<_, anyhow::Error>((resp.removed, true))
            }));
        }

        while let Some(res) = tasks.next().await {
            match res {
                Ok(Ok((removed, counted))) => {
                    if counted { report.tmp_swept_volumes += 1; }
                    report.tmp_removed_total += removed;
                }
                _ => {}
            }
        }
    }

    // 2) Tombstone cleanup (with TTL)
    {
        let ttl_ms: i128 = tombstone_ttl.as_millis() as i128;
        let now_ms = utc_now_ms();

        for kv in db.iter() {
            let (k, v) = kv?;
            if !k.starts_with(META_KEY_PREFIX.as_bytes()) { continue; }
            let key_enc = std::str::from_utf8(&k)?.strip_prefix(META_KEY_PREFIX).unwrap().to_string();
            let meta: Meta = serde_json::from_slice(&v)?;

            if !matches!(meta.state, TxState::Tombstoned) { continue; }

            report.tombstones_scanned += 1;
            if now_ms - meta.created_ms < ttl_ms { continue; }
            report.tombstones_past_ttl += 1;

            // Fan-out deletes to all volumes (safe + idempotent)
            let deleted_targets = delete_on_all(&http, &volumes, &key_enc, &args, &global, &per_node).await;
            report.tombstone_deletes_sent += deleted_targets;

            if args.purge_tombstone_meta && !args.dry_run {
                db.delete(&format!("{}{}", META_KEY_PREFIX, key_enc))?;
                report.tombstone_metas_purged += 1;
            }
        }
    }

    // 3) Optional cleanup of extraneous replicas and orphans
    if args.delete_extraneous || args.purge_orphans {
        // Build in-memory set of keys with Committed meta + expected replicas
        let mut committed: HashMap<String, HashSet<String>> = HashMap::new(); // key_enc -> expected node_ids
        for kv in db.iter() {
            let (k, v) = kv?;
            if !k.starts_with(META_KEY_PREFIX.as_bytes()) { continue; }
            let key_enc = std::str::from_utf8(&k)?.strip_prefix(META_KEY_PREFIX).unwrap().to_string();
            let meta: Meta = serde_json::from_slice(&v)?;
            if matches!(meta.state, TxState::Committed) {
                committed.insert(key_enc, meta.replicas.into_iter().collect());
            }
        }

        // Walk volumes and inspect files
        for (node_id, base) in &volumes {
            let mut after: Option<String> = None;
            loop {
                let mut url = format!("{}/admin/list?limit=1000", base);
                if let Some(a) = &after { url.push_str("&after="); url.push_str(a); }
                let r = http.get(&url).send().await?.error_for_status()?;
                let page: ListResponse = r.json().await?;

                for key_enc in page.keys {
                    match committed.get(&key_enc) {
                        Some(expected) => {
                            // extraneous if this node_id is not in expected
                            if !expected.contains(node_id) {
                                report.extraneous_found += 1;
                                if args.delete_extraneous && !args.dry_run {
                                    let del = format!("{}/internal/delete?key={}", base, key_enc);
                                    let _ = http.post(&del).send().await;
                                    report.extraneous_deleted += 1;
                                }
                            }
                        }
                        None => {
                            // orphan (file without meta)
                            report.orphans_found += 1;
                            if args.purge_orphans && !args.dry_run {
                                let del = format!("{}/internal/delete?key={}", base, key_enc);
                                let _ = http.post(&del).send().await;
                                report.orphans_deleted += 1;
                            }
                        }
                    }
                }

                after = page.next_after;
                if after.is_none() { break; }
            }
        }
    }

    info!("{}", report);

    Ok(())
}

async fn delete_on_all(
    http: &Client,
    volumes: &[(String, String)],
    key_enc: &str,
    cfg: &GcArgs,
    global: &Arc<Semaphore>,
    per_node: &Arc<HashMap<String, Arc<Semaphore>>>
) -> usize {
    let mut futs = Vec::new();

    for (id, base) in volumes {
        let url = format!("{}/internal/delete?key={}", base, key_enc);
        let http = http.clone();
        let gid = global.clone();
        let pid = per_node.get(id).unwrap().clone();
        let dry = cfg.dry_run;
        futs.push(tokio::spawn(async move {
            let _gg = gid.acquire().await.unwrap();
            let _pp = pid.acquire().await.unwrap();
            if dry { return Ok::<bool, anyhow::Error>(true); }
            let _ = http.post(&url).send().await; // idempotent, ignore status
            Ok::<bool, anyhow::Error>(true)
        }));
    }
    let res = join_all(futs).await;
    res.into_iter().filter(|r| r.as_ref().ok().is_some()).count()
}
