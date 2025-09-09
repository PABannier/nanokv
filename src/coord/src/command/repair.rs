use anyhow::{Context, Result, bail};
use clap::Parser;
use futures::{StreamExt, stream};
use reqwest::Client;
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::sync::Semaphore;
use tracing::{info, warn};

use crate::command::common::{
    copy_one, nodes_from_db, nodes_from_explicit, probe_exists, probe_matches,
};
use crate::core::{
    meta::{KvDb, Meta, TxState},
    node::NodeInfo,
    placement::{choose_top_n_alive, rank_nodes},
};
use common::constants::META_KEY_PREFIX;
use common::key_utils::{get_key_enc_from_meta_key, meta_key_for};

const J_REPAIR_PREFIX: &str = "repair"; // repair:{key}:{dst} -> Planned|InFlight|Committed|Failed(msg)

#[derive(Parser, Debug, Clone)]
pub struct RepairArgs {
    #[arg(long)]
    pub index: PathBuf,

    #[arg(long, value_delimiter = ',')]
    pub volumes: Option<Vec<String>>,

    #[arg(long, default_value_t = 3)]
    pub n_replicas: usize,

    #[arg(long, default_value_t = 8)]
    pub concurrency: usize,

    #[arg(long, default_value_t = 2)]
    pub concurrency_per_node: usize,

    #[arg(long, default_value_t = false)]
    pub include_suspect: bool,

    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
}

#[derive(Debug, Default)]
struct RepairReport {
    scanned: usize,
    planned_copies: usize,
    succeeded: usize,
    failed: usize,
    updated_metas: usize,
    skipped_tombstones: usize,
    skipped_no_source: usize,
}

impl std::fmt::Display for RepairReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Repair report:")?;
        writeln!(f, "  Scanned:           = {}", self.scanned)?;
        writeln!(f, "  Planned copies     = {}", self.planned_copies)?;
        writeln!(f, "  Succeeded:         = {}", self.succeeded)?;
        writeln!(f, "  Failed:            = {}", self.failed)?;
        writeln!(f, "  Updated metas      = {}", self.updated_metas)?;
        writeln!(f, "  Skipped tombstones = {}", self.skipped_tombstones)?;
        writeln!(f, "  Skipped no source  = {}", self.skipped_no_source)?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
enum MoveState {
    Planned,
    InFlight,
    Committed,
    Failed,
}

fn jkey(key_enc: &str, dst: &str) -> String {
    format!("{J_REPAIR_PREFIX}:{key_enc}:{dst}")
}

pub async fn repair(args: RepairArgs) -> Result<()> {
    let db = KvDb::open(&args.index)
        .with_context(|| format!("opening RocksDB at {}", args.index.display()))?;

    let http = Client::builder()
        .pool_idle_timeout(Duration::from_secs(30))
        .tcp_keepalive(Duration::from_secs(30))
        .build()?;

    // Discover nodes
    let nodes: Vec<NodeInfo> = if let Some(vs) = args.volumes.clone() {
        nodes_from_explicit(&vs)
    } else {
        nodes_from_db(&db, args.include_suspect)?
    };
    if nodes.is_empty() {
        bail!("no nodes to repair");
    }

    // Node lookups and semaphores
    let by_id: Arc<HashMap<String, NodeInfo>> = Arc::new(
        nodes
            .iter()
            .map(|n| (n.node_id.clone(), n.clone()))
            .collect(),
    );

    let per_node_sems: Arc<HashMap<String, Arc<Semaphore>>> = Arc::new(
        nodes
            .iter()
            .map(|n| {
                (
                    n.node_id.clone(),
                    Arc::new(Semaphore::new(args.concurrency_per_node)),
                )
            })
            .collect(),
    );
    let global_sem = Arc::new(Semaphore::new(args.concurrency));

    let mut report = RepairReport::default();

    // Stream the DB and schedule copy tasks lazily to avoid unbounded memory
    let mut moves = Vec::<(
        String, /*key*/
        u64,    /*size*/
        String, /*etag*/
        String, /*src*/
        String, /*dst*/
    )>::new();

    for kv in db.iter() {
        let (k, v) = kv?;
        if !k.starts_with(META_KEY_PREFIX.as_bytes()) {
            continue;
        }

        let key_enc = get_key_enc_from_meta_key(std::str::from_utf8(&k)?);
        let meta: Meta = serde_json::from_slice(&v)?;
        match meta.state {
            TxState::Tombstoned => {
                report.skipped_tombstones += 1;
                continue;
            }
            TxState::Committed => {}
            _ => continue,
        }
        report.scanned += 1;

        let present_ids: HashSet<String> = meta.replicas.iter().cloned().collect();

        let targets = choose_top_n_alive(&nodes, &key_enc, args.n_replicas);
        let target_ids: Vec<String> = targets.iter().map(|n| n.node_id.clone()).collect();
        if target_ids.is_empty() {
            // No alive nodes now; skip (a later run can fix)
            continue;
        }

        // Determine required destinations to reach target set (and N)
        let mut need: Vec<String> = target_ids
            .iter()
            .filter(|id| !present_ids.contains(*id))
            .cloned()
            .collect();

        if present_ids.len() + need.len() < args.n_replicas {
            for id in nodes.iter().map(|n| &n.node_id) {
                if present_ids.len() + need.len() >= args.n_replicas {
                    break;
                }
                if !present_ids.contains(id) && !need.contains(id) {
                    need.push(id.clone());
                }
            }
        }
        if need.is_empty() {
            continue;
        }

        // Build candidate sources (prefer present set; fall back to any alive with the file)
        let mut sources: Vec<String> = present_ids.iter().cloned().collect();
        if sources.is_empty() {
            // Try all alive nodes as potential sources (verify later)
            sources.extend(nodes.iter().map(|n| n.node_id.clone()));
        }

        // Pre-filter: skip dst if it already has valid copy
        for dst_id in need {
            if let Some(dst) = by_id.get(&dst_id)
                && probe_matches(&http, dst, &key_enc, &meta.etag_hex, meta.size)
                    .await
                    .unwrap_or(false)
            {
                // already good on dst; skip
                continue;
            }

            // Choose a source that actually has the valid blob
            let mut chosen_src: Option<String> = None;
            for sid in &sources {
                if let Some(src) = by_id.get(sid)
                    && probe_matches(&http, src, &key_enc, &meta.etag_hex, meta.size)
                        .await
                        .unwrap_or(false)
                {
                    chosen_src = Some(sid.clone());
                    break;
                }
            }
            if let Some(src) = chosen_src {
                if src != dst_id {
                    moves.push((
                        key_enc.clone(),
                        meta.size,
                        meta.etag_hex.clone(),
                        src,
                        dst_id,
                    ));
                }
            } else {
                report.skipped_no_source += 1;
                warn!(key=%key_enc, "repair: no valid source found");
            }
        }
    }

    // Execute copy moves with resumability
    let total = moves.len();
    report.planned_copies = total;

    let results = stream::iter(moves.into_iter())
        .map(|(key_enc, size, etag, src_id, dst_id)| {
            let db = db.clone();
            let http = http.clone();
            let by_id = Arc::clone(&by_id);
            let per_node_sems = Arc::clone(&per_node_sems);
            let global_sem = Arc::clone(&global_sem);
            let dry_run = args.dry_run;

            async move {
                // Journal: skip if already committed
                let jk = jkey(&key_enc, &dst_id);
                if let Some(MoveState::Committed) = db.get::<MoveState>(&jk).ok().flatten() {
                    return Ok::<bool, anyhow::Error>(true);
                }
                db.put(&jk, &MoveState::Planned)?;

                let src = by_id.get(&src_id).context("unknown src")?.clone();
                let dst = by_id.get(&dst_id).context("unknown dst")?.clone();

                // Acquire concurrency
                let _g = global_sem.acquire_owned().await?;
                let _src_p = per_node_sems.get(&src.node_id).unwrap().clone().acquire_owned().await?;
                let _dst_p = per_node_sems.get(&dst.node_id).unwrap().clone().acquire_owned().await?;

                if dry_run {
                    db.put(&jk, &MoveState::Committed)?; // mark planned-as-committed to let reruns skip work
                    return Ok(true);
                }

                db.put(&jk, &MoveState::InFlight)?;

                // Final pre-check (race): if dst got fixed by parallel op, skip
                if probe_matches(&http, &dst, &key_enc, &etag, size).await.unwrap_or(false) {
                    db.put(&jk, &MoveState::Committed)?;
                    return Ok(true);
                }

                // Tiny retry wrapper for copy_one (handles transient 5xx/timeouts)
                let mut attempt = 0;
                let result = loop {
                    attempt += 1;
                    let res = copy_one(&http, &src, &dst, &key_enc, size, &etag, "repair")
                        .await;
                    match res {
                        Ok(_) => break Ok(()),
                        Err(e) if attempt < 3 => {
                            warn!(%e, key=%key_enc, src=%src.node_id, dst=%dst.node_id, attempt, "copy retrying");
                            continue;
                        }
                        Err(e) => break Err(e),
                    }
                };

                match result {
                    Ok(()) => {
                        db.put(&jk, &MoveState::Committed)?;
                        Ok(true)
                    }
                    Err(e) => {
                        warn!(error=%e, key=%key_enc, src=%src.node_id, dst=%dst.node_id, "copy failed");
                        db.put(&jk, &MoveState::Failed)?;
                        Ok(false)
                    }
                }
            }
        })
        .buffer_unordered(args.concurrency)
        .collect::<Vec<_>>()
        .await;

    for r in results {
        match r {
            Ok(true) => report.succeeded += 1,
            Ok(false) => report.failed += 1,
            Err(e) => {
                report.failed += 1;
                warn!(%e, "repair task join error");
            }
        }
    }

    // Refresh metas (present set, HRW order)
    if !args.dry_run {
        refresh_metas(&db, &nodes, args.n_replicas, &mut report).await?;
    }

    info!("{}", report);
    Ok(())
}

async fn refresh_metas(
    db: &KvDb,
    nodes: &[NodeInfo],
    n_replicas: usize,
    report: &mut RepairReport,
) -> Result<()> {
    let http = Client::new();

    for kv in db.iter() {
        let (k, v) = kv?;
        if !k.starts_with(META_KEY_PREFIX.as_bytes()) {
            continue;
        }
        let key_enc = get_key_enc_from_meta_key(std::str::from_utf8(&k)?);

        let mut meta: Meta = serde_json::from_slice(&v)?;
        if meta.state != TxState::Committed {
            continue;
        }

        // Probe presence on all Alive nodes quickly
        let mut present: HashSet<String> = HashSet::new();
        for n in nodes {
            if probe_exists(&http, n, &key_enc).await.unwrap_or(false) {
                present.insert(n.node_id.clone());
            }
        }
        if present.len() < n_replicas {
            continue; // still under-replicated; a later run will try again
        }

        // Order by HRW and trim to N
        let ranked = rank_nodes(&key_enc, nodes);
        let mut ordered: Vec<String> = ranked
            .into_iter()
            .filter(|n| present.contains(&n.node_id))
            .take(n_replicas)
            .map(|n| n.node_id.clone())
            .collect();

        // If needed, fill remaining with any present to reach N
        if ordered.len() < n_replicas {
            for id in present {
                if ordered.len() >= n_replicas {
                    break;
                }
                if !ordered.contains(&id) {
                    ordered.push(id);
                }
            }
        }

        if !ordered.is_empty() && meta.replicas != ordered {
            meta.replicas = ordered;
            let meta_key = meta_key_for(&key_enc);
            db.put(&meta_key, &meta)?;
            report.updated_metas += 1;
        }
    }
    Ok(())
}
