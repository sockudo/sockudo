#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(unused_assignments)]

mod bootstrap;
pub mod cleanup;
mod history;
mod http_handler;
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
use tracing::{debug, error, info};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, reload, util::SubscriberInitExt};

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

    // Initialize crypto provider at the very beginning for any TLS usage
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|e| {
            Error::Internal(format!("Failed to install default crypto provider: {e:?}"))
        })?;

    // Helper function to get log directive based on debug mode
    fn get_log_directive(is_debug: bool) -> String {
        if is_debug {
            std::env::var("SOCKUDO_LOG_DEBUG")
                .unwrap_or_else(|_| "info,sockudo=debug,tower_http=debug".to_string())
        } else {
            std::env::var("SOCKUDO_LOG_PROD").unwrap_or_else(|_| "info".to_string())
        }
    }

    let initial_debug_from_env = if std::env::var("DEBUG").is_ok() {
        sockudo_core::utils::parse_bool_env("DEBUG", false)
    } else {
        sockudo_core::utils::parse_bool_env("DEBUG_MODE", false)
    };

    let use_json_format = std::env::var("LOG_OUTPUT_FORMAT").as_deref() == Ok("json");

    let (filter_reload_handle, fmt_reload_handle) = if use_json_format {
        let initial_log_directive = get_log_directive(initial_debug_from_env);
        let initial_env_filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new(initial_log_directive));

        tracing_subscriber::fmt()
            .json()
            .with_target(sockudo_core::utils::parse_bool_env(
                "LOG_INCLUDE_TARGET",
                true,
            ))
            .with_file(initial_debug_from_env)
            .with_line_number(initial_debug_from_env)
            .with_env_filter(initial_env_filter)
            .init();

        info!("Initial logging initialized: JSON format");
        (None, None)
    } else {
        let initial_log_directive = get_log_directive(initial_debug_from_env);
        let initial_env_filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new(initial_log_directive));

        let (filter_layer, filter_reload_handle) = reload::Layer::new(initial_env_filter);

        let initial_fmt_layer = fmt::layer()
            .with_target(true)
            .with_file(initial_debug_from_env)
            .with_line_number(initial_debug_from_env);

        let (fmt_layer, fmt_reload_handle) = reload::Layer::new(initial_fmt_layer);

        tracing_subscriber::registry()
            .with(filter_layer)
            .with(fmt_layer)
            .init();

        info!(
            "Initial logging initialized: Human format with DEBUG={}",
            initial_debug_from_env
        );

        (Some(filter_reload_handle), Some(fmt_reload_handle))
    };

    // --- Configuration Loading Order ---
    let mut config = ServerOptions::default();
    info!("Starting with default configuration");

    // Try TOML first, fall back to JSON for backward compatibility
    if let Ok(file_config) = ServerOptions::load_from_file("config/config.toml").await {
        config = file_config;
        info!("Loaded configuration from config/config.toml");
    } else if let Ok(file_config) = ServerOptions::load_from_file("config/config.json").await {
        config = file_config;
        info!("Loaded configuration from config/config.json");
    } else {
        info!("No config/config.toml or config/config.json found. Using defaults.");
    }

    if let Some(config_path) = args.config {
        match ServerOptions::load_from_file(&config_path).await {
            Ok(file_config) => {
                config = file_config;
                info!(
                    "Successfully loaded and applied configuration from {}",
                    config_path
                );
            }
            Err(e) => {
                error!(
                    "Failed to load configuration file {}: {}. Continuing with previously loaded config.",
                    config_path, e
                );
            }
        }
    }

    match config.override_from_env().await {
        Ok(_) => {
            info!("Applied environment variable overrides");
        }
        Err(e) => {
            error!("Failed to override config from environment: {e}");
        }
    }

    if let Err(e) = config.validate() {
        error!("Configuration validation failed: {}", e);
        return Err(Error::ConfigFile(format!(
            "Configuration validation failed: {}",
            e
        )));
    }

    // --- Update logging configuration if needed ---
    if let (Some(filter_handle), Some(fmt_handle)) = (filter_reload_handle, fmt_reload_handle) {
        let needs_logging_update =
            config.debug != initial_debug_from_env || config.logging.is_some();

        if needs_logging_update {
            if config.debug != initial_debug_from_env {
                info!(
                    "Debug mode changed from {} to {} after loading configuration, updating logger",
                    initial_debug_from_env, config.debug
                );
            }

            if config.logging.is_some() {
                info!("Custom logging configuration detected, updating logger format");
            }

            let new_log_directive = get_log_directive(config.debug);
            let new_env_filter = EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(new_log_directive));

            if let Err(e) = filter_handle.reload(new_env_filter) {
                error!("Failed to reload logging filter: {}", e);
            } else {
                debug!(
                    "Successfully updated logging filter for debug={}",
                    config.debug
                );
            }

            let new_fmt_layer = match &config.logging {
                Some(logging_config) => {
                    info!(
                        "Using human-readable log format with colors_enabled={}",
                        logging_config.colors_enabled
                    );
                    fmt::layer()
                        .with_ansi(logging_config.colors_enabled)
                        .with_target(logging_config.include_target)
                        .with_file(config.debug)
                        .with_line_number(config.debug)
                }
                None => fmt::layer()
                    .with_target(true)
                    .with_file(config.debug)
                    .with_line_number(config.debug),
            };

            if let Err(e) = fmt_handle.reload(new_fmt_layer) {
                error!("Failed to reload fmt layer: {}", e);
            } else {
                debug!("Successfully updated fmt layer");
            }
        }
    } else if use_json_format {
        info!("Logging was initialized with JSON format via environment variable");
    }

    info!(
        "Configuration loading complete. Debug mode: {}. Effective RUST_LOG/default filter: '{}'",
        config.debug,
        EnvFilter::try_from_default_env()
            .map(|f| f.to_string())
            .unwrap_or("None".to_string())
    );

    info!("Starting Sockudo server initialization process with resolved configuration...");

    let server = match SockudoServer::new(config).await {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to create server instance: {}", e);
            return Err(e);
        }
    };

    if let Err(e) = server.init().await {
        error!("Failed to initialize server components: {}", e);
        return Err(e);
    }

    info!("Starting Sockudo server main services...");
    if let Err(e) = server.start().await {
        error!("Server runtime error: {}", e);
        if let Err(stop_err) = server.stop().await {
            error!("Error during server stop after runtime error: {}", stop_err);
        }
        return Err(e);
    }

    info!("Server main services concluded. Performing final shutdown...");
    if let Err(e) = server.stop().await {
        error!("Error during final server stop: {}", e);
    }

    info!("Sockudo server shutdown complete.");
    Ok(())
}
