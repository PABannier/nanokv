use axum::{routing::{put, get, delete, post}, Router};
use axum_server::Server;
use tokio::time::Duration;
use clap::{Parser};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::info;

use common::file_utils::init_dirs;
use common::schemas::JoinRequest;

use volume::state::VolumeState;
use volume::store::disk_usage;
use volume::health::heartbeat_loop;
use volume::fault_injection::{
    FaultInjector,
    fail_prepare,
    fail_pull,
    fail_commit,
    fail_read_tmp,
    fail_etag_mismatch,
    inject_latency,
    pause_server,
    resume_server,
    kill_server,
    reset_faults
};
use volume::routes::{
    admin_list_handler,
    admin_blob_handler,
    admin_sweep_tmp_handler,
    prepare_handler,
    write_handler,
    read_handler,
    pull_handler,
    commit_handler,
    abort_handler,
    get_handler,
    delete_handler,
};

#[derive(Parser, Debug, Clone)]
#[command(version, about)]
struct Args {
    #[arg(long, default_value="./data")]
    data: PathBuf,
    #[arg(long)]
    coordinator_url: String,
    #[arg(long, default_value = "vol-1")]
    node_id: String,
    #[arg(long, default_value = "http://127.0.0.1:3001")]
    public_url: String,
    #[arg(long, default_value = "http://127.0.0.1:3001")]
    internal_url: String,
    #[arg(long, default_value_t = 1)]
    subvols: u16,
    #[arg(long, default_value_t = 1)]
    heartbeat_interval_secs: u64,
    #[arg(long, default_value_t = 5)]
    http_timeout_secs: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_target(false)
        .compact()
        .init();

    let args = Args::parse();
    init_dirs(&args.data).await?;

    let http_client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(30))
        .tcp_keepalive(Duration::from_secs(30))
        .http2_adaptive_window(true)
        .timeout(Duration::from_secs(args.http_timeout_secs))
        .build()?;

    let state = VolumeState {
        http_client,
        data_root: Arc::new(args.data.clone()),
        coordinator_url: args.coordinator_url,
        node_id: args.node_id,
        public_url: args.public_url.clone(),
        internal_url: args.internal_url,
        subvols: args.subvols,
        heartbeat_interval_secs: args.heartbeat_interval_secs,
        http_timeout_secs: args.http_timeout_secs,
        fault_injector: Arc::new(FaultInjector::new()),
    };

    join_cluster(&state).await?;

    // Spawn heartbeat loop with shutdown signal
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel::<bool>(false);
    let hb_handle = tokio::spawn(heartbeat_loop(
        state.clone(),
        Duration::from_secs(args.heartbeat_interval_secs),
        shutdown_rx,
    ));

    let app = Router::new()
        .route("/internal/prepare", post(prepare_handler))
        .route("/internal/write/{upload_id}", put(write_handler))
        .route("/internal/read/{upload_id}", get(read_handler))
        .route("/internal/pull", post(pull_handler))
        .route("/internal/commit", post(commit_handler))
        .route("/internal/abort", post(abort_handler))
        .route("/internal/delete/{key}", delete(delete_handler))
        .route("/blobs/{key}", get(get_handler))
        // Admin endpoints
        .route("/admin/list", get(admin_list_handler))
        .route("/admin/blob", get(admin_blob_handler))
        .route("/admin/sweep-tmp", post(admin_sweep_tmp_handler))
        // Fault injection endpoints (test-only)
        .route("/admin/fail/prepare", post(fail_prepare))
        .route("/admin/fail/pull", post(fail_pull))
        .route("/admin/fail/commit", post(fail_commit))
        .route("/admin/fail/read_tmp", post(fail_read_tmp))
        .route("/admin/fail/etag_mismatch", post(fail_etag_mismatch))
        .route("/admin/inject/latency", post(inject_latency))
        .route("/admin/pause", post(pause_server))
        .route("/admin/resume", post(resume_server))
        .route("/admin/kill", post(kill_server))
        .route("/admin/reset", post(reset_faults))
        .with_state(state);

    info!("listening on {}", args.public_url);
    let server = Server::bind(args.public_url.parse()?)
        .serve(app.into_make_service());

    // Graceful shutdown: ctrl+c
    tokio::select! {
        res = server => { res?; }
        _ = tokio::signal::ctrl_c() => {}
    }

    // Stop heartbeat
    let _ = shutdown_tx.send(true);
    let _ = hb_handle.await;

    Ok(())
}

async fn join_cluster(state: &VolumeState) -> anyhow::Result<()> {
    let (used, cap) = disk_usage(&state.data_root)?;

    let payload = JoinRequest {
        node_id: state.node_id.clone(),
        public_url: state.public_url.clone(),
        internal_url: state.internal_url.clone(),
        subvols: state.subvols,
        capacity_bytes: cap,
        used_bytes: used,
        version: Some(env!("CARGO_PKG_VERSION").to_string()),
    };

    let url = format!("{}/admin/join", state.coordinator_url);
    let req = state.http_client.post(url).json(&payload);

    let resp = req.send().await?;
    if !resp.status().is_success() {
        anyhow::bail!("join failed: {}", resp.status());
    }

    info!("joined coordinator as {}", state.node_id);

    Ok(())
}
