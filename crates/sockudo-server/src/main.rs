#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(unused_assignments)]

mod bootstrap;
pub mod cleanup;
mod history;
mod http_handler;
mod logging;
mod middleware;
mod presence_history;
#[cfg(feature = "push")]
mod push_http;
mod ws_handler;

pub use bootstrap::MetricsFactory;
use bootstrap::SockudoServer;

use clap::Parser;
use sockudo_core::error::{Error, Result};
use sockudo_core::options::ServerOptions;
use tracing::{error, info};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    config: Option<String>,
}

// jemalloc is the default allocator, Windows MSVC falls back to the system allocator.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let logging_handles = logging::init().map_err(Error::Internal)?;

    // Initialize crypto provider at the very beginning for any TLS usage
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|e| {
            Error::Internal(format!("Failed to install default crypto provider: {e:?}"))
        })?;

    let mut config = ServerOptions::default();
    info!("Starting with default configuration");

    // Try TOML first, fall back to JSON for backward compatibility
    if let Ok(file_config) = ServerOptions::load_from_file("config/config.toml").await {
        config = file_config;
        info!(
            config_path = "config/config.toml",
            "Loaded configuration file"
        );
    } else if let Ok(file_config) = ServerOptions::load_from_file("config/config.json").await {
        config = file_config;
        info!(
            config_path = "config/config.json",
            "Loaded configuration file"
        );
    } else {
        info!("No config/config.toml or config/config.json found. Using defaults.");
    }

    if let Some(config_path) = args.config {
        match ServerOptions::load_from_file(&config_path).await {
            Ok(file_config) => {
                config = file_config;
                info!(config_path = %config_path, "explicit configuration applied");
            }
            Err(error) => {
                error!(config_path = %config_path, error = %error, "explicit configuration load failed, retaining previous");
            }
        }
    }

    match config.override_from_env().await {
        Ok(()) => info!("Applied environment variable overrides"),
        Err(error) => {
            error!(error = %error, "environment variable overrides failed")
        }
    }

    let resolved_logging = logging::reload(&logging_handles, &config).map_err(Error::Internal)?;
    info!(
        output_format = resolved_logging.output_format,
        filter = %resolved_logging.filter,
        debug = config.debug,
        include_target = resolved_logging.include_target,
        colors_enabled = resolved_logging.colors_enabled,
        source_location = resolved_logging.source_location,
        "Logging reloaded with resolved configuration"
    );
    if config.logging.is_some() {
        info!("Custom logging configuration applied");
    }

    if let Err(e) = config.validate() {
        error!(error = %e, "configuration validation failed");
        return Err(Error::ConfigFile(format!(
            "Configuration validation failed: {}",
            e
        )));
    }

    info!(debug = config.debug, "configuration loading complete");

    info!("Starting Sockudo server initialization process with resolved configuration...");

    let server = match SockudoServer::new(config).await {
        Ok(s) => s,
        Err(e) => {
            error!(error = %e, "server instance creation failed");
            return Err(e);
        }
    };

    if let Err(e) = server.init().await {
        error!(error = %e, "server components initialization failed");
        return Err(e);
    }

    info!("Starting Sockudo server main services...");
    if let Err(e) = server.start().await {
        error!(error = %e, "server runtime error");
        if let Err(stop_err) = server.stop().await {
            error!(error = %stop_err, "server stop failed after runtime error");
        }
        return Err(e);
    }

    info!("Server main services concluded. Performing final shutdown...");
    if let Err(e) = server.stop().await {
        error!(error = %e, "final server stop failed");
    }

    info!("Sockudo server shutdown complete.");
    Ok(())
}
