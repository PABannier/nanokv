use clap::{Parser, Subcommand};

use coord::command::serve::{ServeArgs, run_server};
use coord::command::rebuild::{RebuildArgs, rebuild_index};


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
        Cmd::Serve(serve_args) => { run_server(serve_args).await?; },
        Cmd::Rebuild(rebuild_args) => { rebuild_index(rebuild_args).await?; }
    }

    Ok(())
}
