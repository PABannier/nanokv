use clap::{Parser, Subcommand};

use common::telemetry::init_telemetry;

use coord::command::gc::{GcArgs, gc};
use coord::command::rebalance::{RebalanceArgs, rebalance};
use coord::command::rebuild::{RebuildArgs, rebuild};
use coord::command::repair::{RepairArgs, repair};
use coord::command::serve::{ServeArgs, serve};
use coord::command::verify::{VerifyArgs, verify};

#[derive(Parser, Debug, Clone)]
#[command(version, about)]
struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug, Clone)]
enum Cmd {
    /// Run the HTTP server
    Serve(ServeArgs),
    /// Rebuild the index
    Rebuild(RebuildArgs),
    /// Verify the index
    Verify(VerifyArgs),
    /// Ensure all values are replicated
    Repair(RepairArgs),
    /// Rebalance the index
    Rebalance(RebalanceArgs),
    /// Collect tombstones
    Gc(GcArgs),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_telemetry("coord");

    let args = Args::parse();

    match args.cmd {
        Cmd::Serve(serve_args) => {
            serve(serve_args).await?;
        }
        Cmd::Rebuild(rebuild_args) => {
            rebuild(rebuild_args).await?;
        }
        Cmd::Verify(verify_args) => {
            verify(verify_args).await?;
        }
        Cmd::Rebalance(rebalance_args) => {
            rebalance(rebalance_args).await?;
        }
        Cmd::Repair(repair_args) => {
            repair(repair_args).await?;
        }
        Cmd::Gc(gc_args) => {
            gc(gc_args).await?;
        }
    }

    Ok(())
}
