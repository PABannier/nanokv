mod cleanup;
mod constants;
mod error;
mod meta;
mod file_utils;
mod routes;
mod state;
mod kvdb;
mod verify;

use std::time::Duration;
use std::path::PathBuf;
use std::sync::Arc;
use clap::{Parser, Subcommand};
use tokio::sync::Semaphore;
use tokio::net::TcpListener;
use axum::{routing::put, Router};
use tracing::info;

use crate::cleanup::{startup_cleanup, sweep_tmp_orphans};
use crate::kvdb::KvDb;
use crate::state::AppState;
use crate::file_utils::init_dirs;
use crate::routes::{delete_object, get_object, put_object};
use crate::verify::verify;


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

    let state = AppState {
        data_root: Arc::new(args.data.clone()),
        inflight: Arc::new(Semaphore::new(args.max_inflight)),
        max_size: args.max_size,
        db,
    };

    let app = Router::new()
        .route("/{key}", put(put_object).get(get_object).delete(delete_object))
        .with_state(state);

    info!("listening on {}", args.listen);
    let listener = TcpListener::bind(args.listen).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
