use clap::{Parser, Subcommand};

use coord::command::serve::{ServeArgs, serve};
use coord::command::rebuild::{RebuildArgs, rebuild};
use coord::command::verify::{VerifyArgs, verify};
use coord::command::repair::{RepairArgs, repair};


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
}


#[tokio::main]
async fn main() -> anyhow::Result<()>{
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_target(false)
        .compact()
        .init();

    let args = Args::parse();

    match args.cmd {
        Cmd::Serve(serve_args) => { serve(serve_args).await?; },
        Cmd::Rebuild(rebuild_args) => { rebuild(rebuild_args).await?; },
        Cmd::Verify(verify_args) => { verify(verify_args).await?; },
        Cmd::Repair(repair_args) => { repair(repair_args).await?; },
    }

    Ok(())
}
