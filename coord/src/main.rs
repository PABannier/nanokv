use std::collections::HashMap;
use std::time::{Duration, Instant};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use clap::{Parser, Subcommand};
use common::constants::NODE_KEY_PREFIX;
use tokio::sync::Semaphore;
use tokio::sync::watch;
use axum::{routing::{put, get, post}, Router};
use axum_server::Server;
use tracing::{info, error};

use common::file_utils::init_dirs;

use coord::node::{NodeInfo, NodeRuntime, NodeStatus};
use coord::state::CoordinatorState;
use coord::cleanup::{startup_cleanup, sweep_tmp_orphans};
use coord::meta::KvDb;
use coord::routes::{delete_object, get_object, put_object, head_object, list_nodes, join_node};
use coord::verify::verify;


#[derive(Parser, Debug, Clone)]
#[command(version, about)]
struct Args {
    /// Data root directory; will create subdirs {blos,tmp,gc}
    #[arg(long, default_value="./data")]
    data: PathBuf,

    /// RocksDB directory (inside data by default)
    #[arg(long, default_value="./data/index")]
    index: PathBuf,

    /// Address to listen on
    #[arg(long, default_value = "0.0.0.0:8080")]
    listen: String,

    /// Max concurrent uploads
    #[arg(long, default_value_t = 4)]
    max_inflight: usize,

    /// Max allowed object size in bytes (default: 1 GB)
    #[arg(long, default_value_t = 1024 * 1024 * 1024u64)]
    max_size: u64,

    /// Heartbeat alive timeout (seconds)
    #[arg(long, default_value_t = 5)]
    hb_alive_secs: u64,

    /// Heartbeat down timeout (seconds)
    #[arg(long, default_value_t = 20)]
    hb_down_secs: u64,

    /// Node status sweep interval (seconds)
    #[arg(long, default_value_t = 1)]
    node_status_sweep_secs: u64,

    /// Pending grace before cleanup (seconds)
    #[arg(long, default_value_t = 600)]
    pending_grace_secs: u64,

    /// Deep verify re-hashes blob contents (slower)
    #[arg(long, default_value_t = false)]
    deep_verify: bool,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug, Clone)]
enum Cmd {
    /// Run the HTTP server
    Serve,
    /// Verify blobs vs RocksDB (optionally deep hash)
    Verify,
}


#[tokio::main]
async fn main() -> anyhow::Result<()>{
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_target(false)
        .compact()
        .init();

    let args = Args::parse();
    init_dirs(&args.data).await?;

    let db = KvDb::open(&args.index)?;

    match args.cmd {
        Cmd::Verify => {
            let report = verify(&args.data, &db, args.deep_verify).await?;
            report.print();
            if report.has_issues() { std::process::exit(2); }
            return Ok(())
        },
        Cmd::Serve => {
            // Cleanup before serving
            startup_cleanup(&args.data, &db, Duration::from_secs(args.pending_grace_secs)).await?;
            sweep_tmp_orphans(&args.data, &db).await?;
        }
    }

    let nodes = read_node_infos_from_db(&db)?;

    let state = CoordinatorState {
        http_client: reqwest::Client::new(),
        inflight: Arc::new(Semaphore::new(args.max_inflight)),
        nodes: Arc::new(RwLock::new(nodes)),
        db,
        max_size: args.max_size,
        hb_alive_secs: args.hb_alive_secs,
        hb_down_secs: args.hb_down_secs,
        node_status_sweep_secs: args.node_status_sweep_secs,
    };

    // Spawn node status sweeper
    let (shutdown_tx, shutdown_rx) = watch::channel::<bool>(false);
    let sweeper_handle = tokio::spawn(node_status_sweeper(
        state.clone(),
        Duration::from_secs(args.node_status_sweep_secs),
        shutdown_rx,
    ));

    let app = Router::new()
        .route("/{key}", put(put_object).get(get_object).delete(delete_object).head(head_object))
        .route("/admin/nodes", get(list_nodes))
        .route("/admin/join", post(join_node))
        .with_state(state.clone());

    info!("listening on {}", args.listen);
    let server = Server::bind(args.listen.parse()?)
        .serve(app.into_make_service());

    // Graceful shutdown: ctrl+c
    tokio::select! {
        res = server => { res?; }
        _ = tokio::signal::ctrl_c() => {}
    }

    // Stop sweeper
    let _ = shutdown_tx.send(true);
    let _ = sweeper_handle.await;

    Ok(())
}

fn read_node_infos_from_db(db: &KvDb) -> anyhow::Result<HashMap<String, NodeRuntime>> {
    let mut nodes = HashMap::new();
    for kv in db.iter() {
        let (k, v) = kv?;
        if !k.starts_with(NODE_KEY_PREFIX.as_bytes()) { continue; }
        let node_info: NodeInfo = serde_json::from_slice(&v)?;
        nodes.insert(node_info.node_id.clone(), NodeRuntime {
            info: node_info,
            last_seen: Instant::now(),
        });
    }
    Ok(nodes)
}

async fn node_status_sweeper(
    state: CoordinatorState, 
    interval: std::time::Duration,
    mut shutdown: watch::Receiver<bool>
) -> anyhow::Result<()> {
    let mut tick = tokio::time::interval(interval);

    loop {
        tokio::select! {
            _ = tick.tick() => {},
            _ = shutdown.changed() => { if *shutdown.borrow() { break; }}
        }

        let mut nodes = match state.nodes.write() {
            Ok(nodes) => nodes,
            Err(e) => {
                error!("failed to acquire nodes lock: {}", e);
                continue;
            }
        };

        let now = Instant::now();

        for node in nodes.values_mut() {
            let elapsed = now.saturating_duration_since(node.last_seen);
            let new_status = if elapsed <= Duration::from_secs(state.hb_alive_secs) {
                NodeStatus::Alive
            } else if elapsed <= Duration::from_secs(state.hb_down_secs) {
                NodeStatus::Suspect
            } else {
                NodeStatus::Down
            };
            if new_status != node.info.status {
                node.info.status = new_status;
                state.db.put(&format!("{}:{}", NODE_KEY_PREFIX, node.info.node_id), &node.info)?;
            }
        }
    }

    info!("node status sweeper stopped");

    Ok(())
}