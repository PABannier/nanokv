use anyhow::{Context, Result};
use tracing::info;
use clap::Parser;
use std::collections::{HashSet, HashMap};
use std::path::PathBuf;
use std::fs;
use serde::{Serialize, Deserialize};
use std::sync::Arc;
use tokio::sync::Semaphore;
use reqwest::Client;
use std::time::Duration;
use std::path::Path;
use serde_json;
use futures_util::{stream::FuturesUnordered};
use futures_util::StreamExt;

use crate::core::meta::{KvDb, Meta, TxState};
use crate::core::placement::{choose_top_n_alive, rank_nodes};
use crate::command::common::{nodes_from_db, nodes_from_explicit, copy_one, probe_exists};

use common::time_utils::utc_now_ms;
use common::constants::META_KEY_PREFIX;

use crate::core::node::NodeInfo;

#[derive(Parser, Debug, Clone)]
pub struct RebalanceArgs {
    #[arg(long)]
    index: PathBuf,

    /// Use registry; or override with explicit --volumes for offline runs
    #[arg(long, value_delimiter = ',')]
    volumes: Option<Vec<String>>,

    /// Target replication factor
    #[arg(long, default_value_t = 3)]
    replicas: usize,

    /// Write a plan file instead of running (JSON)
    #[arg(long)]
    plan_out: Option<PathBuf>,

    /// Execute a plan file (JSON produced by --plan-out)
    #[arg(long)]
    plan_in: Option<PathBuf>,

    /// Concurrency
    #[arg(long, default_value_t = 8)]
    concurrency: usize,

    /// Max concurrent copies per node (src or dst)
    #[arg(long, default_value_t = 2)]
    per_node: usize,

    /// Dry-run: compute/explain, make no changes
    #[arg(long, default_value_t = false)]
    dry_run: bool,
}

// Config knobs for planning/execution.
#[derive(Clone, Debug)]
struct RebalanceCfg {
    pub replicas: usize,
    pub concurrency: usize,
    pub per_node: usize,
    pub dry_run: bool,
}

// A move of one key from `src` to `dst`
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Move {
    pub key_enc: String,
    pub size: u64,
    pub etag: String,
    pub src: String,  // node_id
    pub dst: String,  // node_id
}

// A plan is the list of required ADD moves to reach current HRW placement.
// GC of no-longer-needed replicas is left to `gc --delete-extraneous`
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Plan {
    pub generated_at_ms: i128,
    pub target_replicas: usize,
    pub moves: Vec<Move>,
}

impl Plan {
    pub fn to_path(&self, p: &Path) -> Result<()> {
        let s = serde_json::to_string_pretty(self)?;
        fs::write(p, s)?;
        Ok(())
    }

    pub fn from_path(p: &Path) -> Result<Self> {
        let s = fs::read_to_string(p)?;
        Ok(serde_json::from_str(&s)?)
    }
}

// Journal entry for resumability
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
enum MoveState { Planned, InFlight, Committed, Failed(String) }

// Key in RocksDB journal: move:{key}:{dst}
const JOURNAL_KEY_PREFIX: &str = "move:";
fn jkey(key_enc: &str, dst: &str) -> String {
    format!("{}:{}:{}", JOURNAL_KEY_PREFIX, key_enc, dst)
}

fn get_journal(db: &KvDb, mv: &Move) -> Result<Option<MoveState>> {
    db.get::<MoveState>(&jkey(&mv.key_enc, &mv.dst)).map_err(Into::into)
}

fn set_journal(db: &KvDb, mv: &Move, state: MoveState) -> Result<()> {
    db.put::<MoveState>(&jkey(&mv.key_enc, &mv.dst), &state).map_err(Into::into)
}


pub async fn rebalance(args: RebalanceArgs) -> Result<()> {
    let db = KvDb::open(&Path::new(&args.index))?;

    let nodes = if let Some(vs) = args.volumes.clone() {
        nodes_from_explicit(&vs)
    } else {
        nodes_from_db(&db, false /* include suspect */)?
    };
    if nodes.is_empty() { anyhow::bail!("no nodes to rebalance"); }

    let cfg = RebalanceCfg {
        replicas: args.replicas,
        concurrency: args.concurrency,
        per_node: args.per_node,
        dry_run: args.dry_run,
    };

    if let Some(plan_in) = args.plan_in {
        let plan = Plan::from_path(&plan_in)?;
        let rep = execute(&db, &nodes, &cfg, plan).await?;
        info!("Rebalance executed: moves_done={} moves_failed={}", rep.done, rep.failed);
    } else {
        let plan = plan(&db, &nodes, &cfg).await?;
        if let Some(out) = args.plan_out {
            plan.to_path(&out)?;
            info!("Rebalance plan written to {}", out.display());
        } else {
            // Default: execute immediately
            let rep = execute(&db, &nodes, &cfg, plan).await?;
            info!("Rebalance executed: moves_done={} moves_failed={}", rep.done, rep.failed);
        }
    }

    Ok(())
}

async fn plan(db: &KvDb, nodes: &[NodeInfo], cfg: &RebalanceCfg) -> Result<Plan> {
    let mut moves = Vec::new();

    for kv in db.iter() {
        let (k, v) = kv?;
        if !k.starts_with(META_KEY_PREFIX.as_bytes()) { continue; }
        let key_enc = std::str::from_utf8(&k)?.strip_prefix(META_KEY_PREFIX).unwrap().to_string();
        let meta: Meta = serde_json::from_slice(&v)?;
        if !matches!(meta.state, TxState::Committed) { continue; }

        // Desired set (HRW top-N)
        let desired = choose_top_n_alive(&nodes, &key_enc, cfg.replicas)
            .iter().map(|n| n.node_id.clone()).collect::<Vec<String>>();

        // Present set (what Meta says we have)
        let present: HashSet<String> = meta.replicas.iter().cloned().collect();

        // For each missing desired dst, plan a move from some present src
        for dst in desired.iter().filter(|d| !present.contains(*d)) {
            let src = meta.replicas.iter().find(|id| present.contains(*id))
                .cloned()
                .or_else(|| meta.replicas.get(0).cloned());
            
            if let Some(src) = src {
                moves.push(Move {
                    key_enc: key_enc.clone(),
                    size: meta.size,
                    etag: meta.etag_hex.clone(),
                    src,
                    dst: dst.clone(),
                });
            }
        }
    }

    Ok(Plan {
        generated_at_ms: utc_now_ms(),
        target_replicas: cfg.replicas,
        moves,
    })
}

// Execution report
#[derive(Debug, Default)]
struct ExecReport {
    pub total: usize,
    pub done: usize,
    pub failed: usize,
}

async fn execute(db: &KvDb, nodes: &[NodeInfo], cfg: &RebalanceCfg, plan: Plan) -> Result<ExecReport> {
    let http = Client::builder()
        .pool_idle_timeout(Duration::from_secs(30))
        .tcp_keepalive(Duration::from_secs(30))
        .build()?;

    // Node maps & semaphores
    let by_id = nodes.iter().map(|n| (n.node_id.clone(), n.clone())).collect::<HashMap<String, NodeInfo>>();
    let per_node: Arc<HashMap<String, Arc<Semaphore>>> = Arc::new(
        nodes.iter().map(|n| (n.node_id.clone(), Arc::new(Semaphore::new(cfg.per_node)))).collect()
    );
    let global_sem = Arc::new(Semaphore::new(cfg.concurrency));

    let mut report = ExecReport::default();
    report.total = plan.moves.len();

    let mut tasks = FuturesUnordered::new();

    for mv in plan.moves {
        let db = db.clone();
        let http = http.clone();
        let by_id = by_id.clone();
        let per_node = per_node.clone();
        let global_sem = global_sem.clone();
        let dry = cfg.dry_run;

        tasks.push(tokio::spawn(async move {
            // Skip if already committed in journal
            if let Some(MoveState::Committed) = get_journal(&db, &mv).ok().flatten() {
                return Ok::<bool, anyhow::Error>(true);
            }

            set_journal(&db, &mv, MoveState::Planned).ok();

            // Resolve nodes
            let src = by_id.get(&mv.src).context("unknown src")?.clone();
            let dst = by_id.get(&mv.dst).context("unknown dst")?.clone();

            // Acquire concurrency slots
            let _g = global_sem.acquire().await.unwrap();
            let _src_slot = per_node.get(&src.node_id).unwrap().acquire().await.unwrap();
            let _dst_slot = per_node.get(&dst.node_id).unwrap().acquire().await.unwrap();

            if dry {
                set_journal(&db, &mv, MoveState::Committed).ok();
                return Ok(true);
            }

            set_journal(&db, &mv, MoveState::InFlight).ok();

            match copy_one(&http, &src, &dst, &mv.key_enc, mv.size, &mv.etag, "rebalance").await {
                Ok(_) => {
                    set_journal(&db, &mv, MoveState::Committed).ok();
                    Ok(true)
                }
                Err(e) => {
                    set_journal(&db, &mv, MoveState::Failed(e.to_string())).ok();
                    Ok(false)
                }
            }
        }));
    }

    while let Some(r) = tasks.next().await {
        match r {
            Ok(Ok(true)) => report.done += 1,
            Ok(Ok(false)) => report.failed += 1,
            Ok(Err(_e)) => report.failed += 1,
            Err(_join) => report.failed += 1,
        }
    }

    // Final pass: for keys that reached the new destinations, refresh metas order to HRW top-N
    refresh_metas(&db, nodes, cfg.replicas).await?;

    Ok(report)
}

async fn refresh_metas(db: &KvDb, nodes: &[NodeInfo], n: usize) -> Result<()> {
    let mut by_id: HashMap<String, NodeInfo> = HashMap::new();
    for nfo in nodes.iter() { by_id.insert(nfo.node_id.clone(), nfo.clone()); }

    let http = Client::new();

    for kv in db.iter() {
        let (k, v) = kv?;
        if !k.starts_with(META_KEY_PREFIX.as_bytes()) { continue; }
        let key_enc = std::str::from_utf8(&k)?.strip_prefix(META_KEY_PREFIX).unwrap().to_string();
        let mut meta: Meta = serde_json::from_slice(&v)?;
        if !matches!(meta.state, TxState::Committed) { continue; }

        // Probe which nodes *actually* have the blob
        let mut present: Vec<NodeInfo> = Vec::new();
        for id in meta.replicas.iter() {
            if let Some(n) = by_id.get(id) {
                if probe_exists(&http, n, &key_enc).await.unwrap_or(false) {
                    present.push(n.clone());
                }
            }
        }

        // Keep order by HRW and trim to N
        let ranked = rank_nodes(&key_enc, &nodes);
        let mut ordered: Vec<String> = ranked.into_iter()
            .filter(|n| present.iter().any(|p| p.node_id == n.node_id))
            .take(n)
            .map(|n| n.node_id.clone())
            .collect::<Vec<String>>();

        // If missing (e.g., some recently added), include them until N
        for p in present {
            if ordered.len() >= n { break; }
            if !ordered.contains(&p.node_id) { ordered.push(p.node_id.clone()); }
        }

        if !ordered.is_empty() && ordered != meta.replicas {
            meta.replicas = ordered;
            db.put(&format!("{}{}", META_KEY_PREFIX, key_enc), &meta)?;
        }
    }

    Ok(())
}