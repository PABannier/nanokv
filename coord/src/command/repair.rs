use tracing::info;
use clap::Parser;
use std::fmt::Display;
use std::collections::HashSet;
use tokio::sync::Semaphore;
use std::sync::Arc;
use std::collections::HashMap;
use reqwest::Client;
use std::time::Duration;
use std::path::Path;
use serde_json;
use futures_util::{stream::FuturesUnordered};
use futures_util::StreamExt;

use common::constants::{META_KEY_PREFIX};

use crate::command::common::copy_one;
use crate::core::{
    meta::{KvDb, Meta, TxState},
    placement::{choose_top_n_alive, rank_nodes},
    node::NodeInfo,
};
use crate::command::common::{nodes_from_db, nodes_from_explicit, probe_matches, probe_exists};

#[derive(Parser, Debug, Clone)]
pub struct RepairArgs {
    /// RocksDB directory path
    #[arg(long)]
    index: String,

    /// Optional explicit volume URLs (internal URLs). If omitted, use node registry.
    #[arg(long, value_delimiter = ',')]
    volumes: Option<Vec<String>>,

    /// Number of replicas
    #[arg(long, default_value_t = 3)]
    n_replicas: usize,

    /// Concurrency of parallel copy jobs
    #[arg(long, default_value_t = 8)]
    concurrency: usize,

    /// Maximum concurrent operations per volume node
    #[arg(long, default_value_t = 2)]
    concurrency_per_node: usize,

    /// Include suspect nodes as potential sources
    #[arg(long, default_value_t = false)]
    include_suspect: bool,

    /// Dry run
    #[arg(long, default_value_t = false)]
    dry_run: bool,
}

#[derive(Debug, Default)]
struct RepairReport {
    pub scanned: usize,
    pub planned_copies: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub updated_metas: usize,
    pub skipped_tombstones: usize,
    pub skipped_no_source: usize,
}

impl Display for RepairReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Repair report:")?;
        writeln!(f, "  Scanned: {}", self.scanned)?;
        writeln!(f, "  Planned copies: {}", self.planned_copies)?;
        writeln!(f, "  Succeeded: {}", self.succeeded)?;
        writeln!(f, "  Failed: {}", self.failed)?;
        writeln!(f, "  Updated metas: {}", self.updated_metas)?;
        writeln!(f, "  Skipped tombstones: {}", self.skipped_tombstones)?;
        writeln!(f, "  Skipped no source: {}", self.skipped_no_source)?;
        Ok(())
    }
}

pub async fn repair(args: RepairArgs) -> anyhow::Result<()> {
    let db = KvDb::open(&Path::new(&args.index))?;
    let http = Client::builder()
        .pool_idle_timeout(Duration::from_secs(30))
        .tcp_keepalive(Duration::from_secs(30))
        .build()?;

    let mut report = RepairReport::default();

    // Discover alive nodes from db or use explicit volume URLs
    let nodes = if let Some(vs) = args.volumes.clone() {
        nodes_from_explicit(&vs)
    } else {
        nodes_from_db(&db, args.include_suspect)?
    };
    if nodes.is_empty() { anyhow::bail!("no nodes to repair"); }

    // Build node lookup maps
    let by_id = nodes.iter().map(|n| (n.node_id.clone(), n.clone())).collect::<HashMap<String, NodeInfo>>();
    let node_ids = nodes.iter().map(|n| n.node_id.clone()).collect::<HashSet<String>>();

    // Per-node concurrency limits
    let per_node_sems: Arc<HashMap<String, Arc<Semaphore>>> = Arc::new(
        node_ids.iter().map(|id| (id.clone(), Arc::new(Semaphore::new(args.concurrency_per_node)))).collect()
    );

    // Global concurrency
    let global_sem = Arc::new(Semaphore::new(args.concurrency));

    let mut tasks = FuturesUnordered::new();

    for kv in db.iter() {
        let (k, v) = kv?;
        if !k.starts_with(META_KEY_PREFIX.as_bytes()) { continue; }
        let key = std::str::from_utf8(&k)?.strip_prefix(META_KEY_PREFIX).unwrap().to_string();
        let meta = serde_json::from_slice::<Meta>(&v)?;

        if matches!(meta.state, TxState::Tombstoned) {
            report.skipped_tombstones += 1;
            continue;
        }
        if !matches!(meta.state, TxState::Committed) { continue; }
        report.scanned += 1;

        let present_ids = meta.replicas.iter().cloned().collect::<HashSet<String>>();

        let target_ids = choose_top_n_alive(&nodes, &key, args.n_replicas)
            .iter().map(|n| n.node_id.clone()).collect::<Vec<String>>();
        if target_ids.is_empty() {
            // no alive nodes -> skip; a later pass can handle when nodes return
            continue;
        }

        let mut need_ids = target_ids.iter()
            .filter(|n| !present_ids.contains(*n))
            .cloned().collect::<Vec<String>>();

        // If we still have < N present (e.g. all present are off-target), fill from other alives not in present
        if present_ids.len() + need_ids.len() < args.n_replicas {
            for id in &node_ids {
                if present_ids.len() + need_ids.len() >= args.n_replicas { break; }
                if !present_ids.contains(id) && !need_ids.contains(id) {
                    need_ids.push(id.clone());
                }
            }
        }

        if need_ids.is_empty() { continue; }

        let sources: Vec<String> = present_ids.iter().cloned().collect();

        // Create a copy job for each destination node
        for dst_id in need_ids {
            let dry_run = args.dry_run;

            let key = key.clone();
            let http = http.clone();
            let by_id = by_id.clone();
            let size = meta.size.clone();
            let sources = sources.clone();
            let etag = meta.etag_hex.clone();
            let global_sem = global_sem.clone();
            let per_node_sems = per_node_sems.clone();

            report.planned_copies += 1;

            tasks.push(tokio::spawn(async move {
                let _g_permit = global_sem.acquire().await.unwrap();
                let dst = match by_id.get(&dst_id) {
                    Some(n) => n.clone(),
                    None => anyhow::bail!("destination node not found"),
                };

                // Find a viable source (probe until one says exists+matches meta)
                let mut chosen_src: Option<NodeInfo> = None;
                for sid in sources {
                    if let Some(src) = by_id.get(&sid) {
                        if probe_matches(&http, src, &key, &etag, size).await.unwrap_or(false) {
                            chosen_src = Some(src.clone());
                            break;
                        }
                    }
                }
                let src = match chosen_src {
                    Some(s) => s,
                    None => return Err(anyhow::anyhow!("no valid source for key {}", key)),
                };

                if dry_run {
                    return Ok(());
                }

                // Per-node permit
                let _src_permit = per_node_sems.get(&src.node_id).unwrap().acquire().await.unwrap();
                let _dst_permit = per_node_sems.get(&dst.node_id).unwrap().acquire().await.unwrap();

                // Execute copy (prepare -> pull -> commit)
                copy_one(&http, &src, &dst, &key, size, &etag, "repair").await
            }));
        }
    }

    // Collect results and update meta for keys fully fixed
    while let Some(res) = tasks.next().await {
        match res {
            Ok(Ok(())) => report.succeeded += 1,
            Ok(Err(_)) => report.failed += 1,
            Err(_join) => report.failed += 1,
        }
    }

    // Final pass: refresh metas' replica lists (cheap but effective)
    // Walk metas again, recompute present by probing all Alive nodes; if >=N and includes target set, write updated replicas
    let alive_ids: Vec<String> = nodes.iter().map(|n| n.node_id.clone()).collect();

    for kv in db.iter() {
        let (k, v) = kv?;
        if !k.starts_with(META_KEY_PREFIX.as_bytes()) { continue; }
        let key_enc = std::str::from_utf8(&k)?.strip_prefix(META_KEY_PREFIX).unwrap().to_string();
        let mut meta: Meta = serde_json::from_slice(&v)?;
        if !matches!(meta.state, TxState::Committed) { continue; }

        let mut present_vec: Vec<String> = Vec::new();
        for id in &alive_ids {
            let n = &by_id[id];
            if probe_exists(&http, n, &key_enc).await.unwrap_or(false) {
                present_vec.push(id.clone());
            }
        }

        if present_vec.len() >= args.n_replicas {
            // Optionally reorder by HRW so head is first
            let ranked = rank_nodes(&key_enc, &nodes);
            let mut ordered: Vec<String> = ranked.into_iter()
                .filter(|n| present_vec.contains(&n.node_id))
                .take(args.n_replicas)
                .map(|n| n.node_id.clone()).collect();

            if ordered.len() < args.n_replicas {
                // fallback to any present to fill
                for id in present_vec {
                    if !ordered.contains(&id) && ordered.len() < args.n_replicas {
                        ordered.push(id);
                    }
                }
            }

            if meta.replicas != ordered {
                meta.replicas = ordered;
                db.put(&format!("{}{}", META_KEY_PREFIX, key_enc), &meta)?;
                report.updated_metas += 1;
            }
        }
    }

    info!("{}", report);

    Ok(())
}
