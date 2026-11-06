// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Entry point for the culvert daemon.
//! All comments and docstrings must be in English.

use clap::Parser;
use std::path::PathBuf;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use culvert::common::CancelFutureExt;
use culvert::config::ConfigLoader;
use culvert::consts::{DEFAULT_SHUTDOWN_HARD_DEADLINE, WATCHDOG_THREAD_NAME};
use culvert::supervisor::TunnelSupervisor;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// High-performance lightweight HTTP/3 & WSS bidirectional TCP tunnel system.
#[derive(Parser, Debug)]
#[command(name = "culvert", version, about, long_about = None)]
struct Cli {
    /// Path to the YAML configuration file.
    #[arg(short, long, default_value = "config.yaml")]
    config: PathBuf,

    /// Override the number of Tokio worker threads.
    #[arg(short = 'w', long)]
    worker_threads: Option<usize>,

    /// Check and validate configuration syntax, schema, and semantics without starting the daemon.
    #[arg(long)]
    check: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let app_config = match ConfigLoader::from_file(&cli.config) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("Configuration error in '{:?}': {}", cli.config, e);
            std::process::exit(1);
        }
    };

    if cli.check {
        println!(
            "Configuration check succeeded: '{:?}' is valid.",
            cli.config
        );
        return Ok(());
    }

    // Initialize structured logging respecting LogConfig
    let filter = match app_config.log.level.to_lowercase().as_str() {
        "trace" => tracing::Level::TRACE,
        "debug" => tracing::Level::DEBUG,
        "info" => tracing::Level::INFO,
        "warn" => tracing::Level::WARN,
        "error" => tracing::Level::ERROR,
        _ => tracing::Level::INFO,
    };
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(filter.as_str()));

    let fmt_builder = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_ansi(!app_config.log.no_color);

    if app_config.log.output.eq_ignore_ascii_case("stderr") {
        fmt_builder.with_writer(std::io::stderr).init();
    } else {
        fmt_builder.with_writer(std::io::stdout).init();
    }

    info!(config = ?cli.config, "Loaded culvert configuration");

    let threads = cli.worker_threads.or(app_config.worker_threads);

    let mut builder = match threads {
        Some(1) => {
            info!("Configuring single-thread Tokio current_thread runtime");
            tokio::runtime::Builder::new_current_thread()
        }
        Some(n) => {
            info!(
                threads = n,
                "Configuring multi-threaded Tokio runtime with custom worker threads"
            );
            let mut b = tokio::runtime::Builder::new_multi_thread();
            b.worker_threads(n);
            b
        }
        None => {
            info!("Configuring multi-threaded Tokio runtime auto-detecting CPU cores");
            tokio::runtime::Builder::new_multi_thread()
        }
    };

    builder.enable_all();

    let runtime = match builder.build() {
        Ok(rt) => rt,
        Err(e) => {
            error!("Failed to create Tokio runtime: {:?}", e);
            std::process::exit(1);
        }
    };

    runtime.block_on(run_daemon(app_config))
}

async fn run_daemon(
    app_config: culvert::config::model::AppConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    culvert::common::task::reset_shutdown_deadline();
    let cancel_token = CancellationToken::new();

    // Listen for graceful shutdown signals (SIGINT / SIGTERM / Ctrl+C)
    let watchdog = spawn_shutdown_handler(cancel_token.clone());

    let supervisor = TunnelSupervisor::new(app_config, cancel_token);

    if let Err(e) = supervisor.run().await {
        error!("Tunnel supervisor terminated with error: {:?}", e);
        watchdog.finish().await;
        std::process::exit(1);
    }

    info!("Culvert daemon exited cleanly.");
    // Disarm the hard deadline and await the watchdog thread. Without this,
    // the watchdog would force-exit 10s after the first signal even on a
    // perfectly clean shutdown.
    watchdog.finish().await;
    Ok(())
}

/// Termination signals are handled in two stages: the first one starts graceful
/// shutdown, a second signal (or the hard deadline) forces an immediate exit.
///
/// The signal stream must be re-armed: Tokio leaves the OS signal handler
/// installed after the first stream is dropped, so subsequent Ctrl+C would be
/// swallowed and the process could look stuck if any shutdown layer stalls.
/// Explicit lifecycle events for the OS-thread watchdog. `Done` disarms it.
enum WatchdogEvent {
    /// The first termination signal arrived; start the hard deadline.
    Start,
    /// Graceful shutdown completed; the watchdog must exit normally.
    Done,
}

/// Owns the watchdog thread and its disarm channel. `finish()` must be called
/// on every daemon exit path so the thread is joined, never detached.
struct ShutdownWatchdog {
    tx: std::sync::mpsc::Sender<WatchdogEvent>,
    signal_cancel: CancellationToken,
    signal_task: Option<tokio::task::JoinHandle<()>>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl ShutdownWatchdog {
    /// Disarms the deadline and awaits both owned tasks. Called on every daemon
    /// exit path, so no signal task or watchdog thread is ever left detached.
    async fn finish(mut self) {
        let _ = self.tx.send(WatchdogEvent::Done);
        self.signal_cancel.cancel();
        if let Some(task) = self.signal_task.take() {
            let _ = task.await;
        }
        // The watchdog has already received `Done`, so this join returns at
        // once instead of blocking a Tokio worker.
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for ShutdownWatchdog {
    fn drop(&mut self) {
        let _ = self.tx.send(WatchdogEvent::Done);
        self.signal_cancel.cancel();
        if let Some(task) = self.signal_task.take() {
            task.abort();
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn spawn_shutdown_handler(cancel_token: CancellationToken) -> ShutdownWatchdog {
    use std::sync::mpsc::{RecvTimeoutError, TryRecvError};

    // The hard deadline must not live on the Tokio runtime: if every worker is
    // pinned by busy relay tasks, a timer task never gets polled and the
    // deadline silently never fires. Run it on a dedicated OS thread instead.
    let (tx, rx) = std::sync::mpsc::channel::<WatchdogEvent>();
    let handle = std::thread::Builder::new()
        .name(WATCHDOG_THREAD_NAME.to_string())
        .spawn(move || {
            // Phase 1: wait for the first signal. A daemon that exits without
            // ever receiving one sends `Done` directly and must not be held up.
            match rx.recv() {
                Ok(WatchdogEvent::Start) => {}
                Ok(WatchdogEvent::Done) | Err(_) => return,
            }

            // Phase 2: the hard deadline is only meaningful until graceful
            // shutdown reports completion.
            match rx.recv_timeout(DEFAULT_SHUTDOWN_HARD_DEADLINE) {
                Ok(_) | Err(RecvTimeoutError::Disconnected) => (),
                Err(RecvTimeoutError::Timeout) => {
                    // `Done` may have landed exactly at the deadline.
                    if matches!(
                        rx.try_recv(),
                        Ok(WatchdogEvent::Done) | Err(TryRecvError::Disconnected)
                    ) {
                        return;
                    }
                    eprintln!("Graceful shutdown exceeded hard deadline; forcing exit");
                    std::process::exit(130);
                }
            }
        })
        .expect("failed to spawn shutdown watchdog thread");

    let signal_tx = tx.clone();
    let signal_cancel = CancellationToken::new();
    let signal_stop = signal_cancel.clone();
    let signal_task = tokio::spawn(async move {
        let Some(_) = culvert::platform::wait_for_shutdown_signal()
            .cancel_with(&signal_stop)
            .await
        else {
            return;
        };
        cancel_token.cancel();
        let _ = signal_tx.send(WatchdogEvent::Start);

        // Re-arm signal waiting: Tokio keeps the OS handler installed after
        // the first stream is dropped, so later signals would be swallowed.
        let Some(_) = culvert::platform::wait_for_shutdown_signal()
            .cancel_with(&signal_stop)
            .await
        else {
            return;
        };
        eprintln!("Second termination signal received; forcing immediate exit");
        std::process::exit(130);
    });

    ShutdownWatchdog {
        tx,
        signal_cancel,
        signal_task: Some(signal_task),
        handle: Some(handle),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// A clean exit without any signal must disarm the hard deadline and join
    /// the watchdog thread, not leave it detached until it force-exits.
    #[tokio::test]
    async fn watchdog_is_disarmed_and_joined_on_clean_exit() {
        let cancel = CancellationToken::new();
        let watchdog = spawn_shutdown_handler(cancel.clone());

        let finished = tokio::time::timeout(Duration::from_secs(2), watchdog.finish()).await;
        assert!(
            finished.is_ok(),
            "clean exit must join the watchdog instead of waiting for the deadline"
        );
        assert!(
            !cancel.is_cancelled(),
            "no termination signal was received, so cancellation must not fire"
        );
    }

    /// Dropping the watchdog without calling `finish()` must still disarm and
    /// join the OS thread, rather than leaking an active watchdog.
    #[tokio::test]
    async fn watchdog_is_joined_on_drop() {
        let cancel = CancellationToken::new();
        let watchdog = spawn_shutdown_handler(cancel.clone());
        drop(watchdog);
        assert!(!cancel.is_cancelled());
    }
}
