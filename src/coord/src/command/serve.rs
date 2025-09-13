use axum::{
    Router,
    routing::{get, post, put},
};
use axum_server::Server;
use clap::Parser;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio::sync::watch;
use tracing::info;

use common::constants::NODE_KEY_PREFIX;
use common::url_utils::parse_socket_addr;

use crate::core::debug::debug_placement;
use crate::core::health::node_status_sweeper;
use crate::core::meta::KvDb;
use crate::core::node::{NodeInfo, NodeRuntime};
use crate::core::routes::{
    delete_object, get_object, head_object, heartbeat, join_node, list_nodes, put_object,
};
use crate::core::state::CoordinatorState;

#[derive(Parser, Debug, Clone)]
pub struct ServeArgs {
    /// Data root directory; will create subdirs {blos,tmp,gc}
    #[arg(long, default_value = "./data")]
    data: PathBuf,

    /// RocksDB directory (inside data by default)
    #[arg(long, default_value = "./data/index")]
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

    /// Number of replicas
    #[arg(long, default_value_t = 3)]
    n_replicas: usize,

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
}

pub async fn serve(serve_args: ServeArgs) -> anyhow::Result<()> {
    let db = KvDb::open(&serve_args.index)?;

    let nodes = read_node_infos_from_db(&db)?;

    let state = CoordinatorState {
        http_client: reqwest::Client::new(),
        inflight: Arc::new(Semaphore::new(serve_args.max_inflight)),
        nodes: Arc::new(RwLock::new(nodes)),
        db,
        n_replicas: serve_args.n_replicas,
        max_size: serve_args.max_size,
        hb_alive_secs: serve_args.hb_alive_secs,
        hb_down_secs: serve_args.hb_down_secs,
        node_status_sweep_secs: serve_args.node_status_sweep_secs,
    };

    // Spawn node status sweeper
    let (shutdown_tx, shutdown_rx) = watch::channel::<bool>(false);
    let sweeper_handle = tokio::spawn(node_status_sweeper(
        state.clone(),
        Duration::from_secs(serve_args.node_status_sweep_secs),
        shutdown_rx,
    ));

    let app = Router::new()
        .route(
            "/{key}",
            put(put_object)
                .get(get_object)
                .delete(delete_object)
                .head(head_object),
        )
        .route("/admin/nodes", get(list_nodes))
        .route("/admin/join", post(join_node))
        .route("/admin/heartbeat", post(heartbeat))
        // Debug endpoint (test-only)
        .route("/debug/placement/{key}", get(debug_placement))
        .with_state(state.clone());

    let socket_addr = parse_socket_addr(&serve_args.listen)?;
    let server = Server::bind(socket_addr).serve(app.into_make_service());

    info!("listening on {}", serve_args.listen);

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
        if !k.starts_with(NODE_KEY_PREFIX.as_bytes()) {
            continue;
        }
        let node_info: NodeInfo = serde_json::from_slice(&v)?;
        nodes.insert(
            node_info.node_id.clone(),
            NodeRuntime {
                info: node_info,
                last_seen: Instant::now(),
            },
        );
    }
    Ok(nodes)
}
