use std::process::ExitCode;

use clap::Parser;
use wispmq_cli::cli::{Cli, Command};
use wispmq_cli::{bench, publish, request, subscribe};

fn main() -> ExitCode {
    let cli = Cli::parse();

    // The runtime is chosen per subcommand rather than by `#[tokio::main]`,
    // because the workloads differ. `pub`, `sub` and `request` do one thing at
    // a time and start faster on a single thread. `bench` is a load generator:
    // pinning its publishers and subscribers to one core would measure the
    // load generator rather than the broker.
    let result = match cli.command {
        Command::Pub(args) => current_thread_runtime().block_on(publish::run(args)),
        Command::Sub(args) => current_thread_runtime().block_on(subscribe::run(args)),
        Command::Request(args) => current_thread_runtime().block_on(request::run(args)),
        Command::Bench(args) => multi_thread_runtime().block_on(bench::run(args)),
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("wispmq-cli: {e}");
            ExitCode::FAILURE
        }
    }
}

fn current_thread_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

fn multi_thread_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}
