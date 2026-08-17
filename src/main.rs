use std::process::ExitCode;

use clap::Parser;
use pulsemq_cli::cli::{Cli, Command};
use pulsemq_cli::{publish, request, subscribe};

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Pub(args) => publish::run(args).await,
        Command::Sub(args) => subscribe::run(args).await,
        Command::Request(args) => request::run(args).await,
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("pulsemq-cli: {e}");
            ExitCode::FAILURE
        }
    }
}
