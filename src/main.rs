mod error;
mod file_utils;
mod routes;
mod state;

use std::path::PathBuf;
use std::sync::Arc;
use clap::Parser;
use tokio::sync::Semaphore;
use tokio::net::TcpListener;
use axum::{routing::put, Router};

use crate::state::AppState;
use crate::file_utils::init_dirs;
use crate::routes::{delete_object, get_object, put_object};


#[derive(Parser, Debug, Clone)]
struct Args {
    /// Data root directory; will create subdirs {blos,tmp,gc}
    #[arg(long, default_value="./data")]
    data: PathBuf,

    /// Address to listen on
    #[arg(long, default_value = "0.0.0.0:8080")]
    listen: String,

    /// Max concurrent uploads
    #[arg(long, default_value_t = 4)]
    max_inflight: usize,

    /// Max allowed object size in bytes (default: 1 GB)
    #[arg(long, default_value_t = 1024 * 1024 * 1024u64)]
    max_size: u64,
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

    let state = AppState {
        data_root: Arc::new(args.data.clone()),
        inflight: Arc::new(Semaphore::new(args.max_inflight)),
        max_size: args.max_size,
    };

    let app = Router::new()
        .route("/:key", put(put_object).get(get_object).delete(delete_object))
        .with_state(state);

    let listener = TcpListener::bind("127.0.0.1:3000").await?;
    axum::serve(listener, app).await?;

    Ok(())
}
