use anyhow;
use clap::Parser;
use std::path::PathBuf;


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

    /// Global bandwidth cap (bytes/sec). 0 = unlimited.
    #[arg(long, default_value_t = 0)]
    bytes_per_sec: u64,

    /// Dry-run: compute/explain, make no changes
    #[arg(long, default_value_t = false)]
    dry_run: bool,
}


pub async fn rebalance(args: RebalanceArgs) -> anyhow::Result<()> {
    Ok(())
}