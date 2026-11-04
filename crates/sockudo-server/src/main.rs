#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(unused_assignments)]

mod bootstrap;
pub mod cleanup;
mod history;
mod http_handler;
mod logging;
#[cfg(feature = "mcp")]
mod mcp;
mod middleware;
mod presence_history;
#[cfg(feature = "push")]
mod push_http;
#[cfg(feature = "opentelemetry")]
mod telemetry;
mod ws_handler;

pub use bootstrap::MetricsFactory;
use bootstrap::SockudoServer;

use clap::Parser;
use sockudo_core::error::{Error, Result};
use sockudo_core::options::ServerOptions;
#[cfg(not(feature = "versioned-messages"))]
use tracing::warn;
use tracing::{error, info};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    config: Option<String>,
    /// Encrypt an app secret read from stdin for the app with this id, using
    /// SOCKUDO_APP_SECRET_ENCRYPTION_KEY or SOCKUDO_APP_SECRET_ENCRYPTION_KEY_FILE,
    /// print the `secret_enc` column value, and exit.
    #[arg(long, value_name = "APP_ID")]
    encrypt_app_secret: Option<String>,
    /// Rewrite compact append versions of the configured version store as
    /// self-contained records, then exit. Rollback maintenance: stop every
    /// server writing to the store first.
    #[arg(long)]
    materialize_append_storage: bool,
    /// Enable chunked append writes after every writer supports format 2.
    /// Maintenance command; stop writers before changing the store marker.
    #[arg(long, conflicts_with_all = ["disable_append_storage", "materialize_append_storage"])]
    enable_append_storage: bool,
    /// Disable compact writes before materialization and rollback.
    #[arg(long, conflicts_with = "materialize_append_storage")]
    disable_append_storage: bool,
    /// Check backend legacy-row size constraints without changing the marker
    /// or rewriting records. Stop writers for a stable result.
    #[arg(long, conflicts_with_all = ["enable_append_storage", "disable_append_storage", "materialize_append_storage", "rollback_append_storage"])]
    check_append_storage_rollback: bool,
    /// Preflight legacy-row limits, disable compact writes and materialize.
    /// Stop all writers first and keep them stopped until completion.
    #[arg(long, conflicts_with_all = ["enable_append_storage", "disable_append_storage", "materialize_append_storage"])]
    rollback_append_storage: bool,
}

/// Reports a configuration file that could not be loaded and exits non-zero.
/// Logging is not initialized yet, so this writes straight to stderr.
fn exit_with_config_error(message: &str) -> ! {
    eprintln!("sockudo: {message}");
    std::process::exit(1);
}

/// Prints the `secret_enc` value for `app_id`, sealing the secret read from
/// stdin with the configured app secret encryption key.
fn encrypt_app_secret(app_id: &str) -> Result<()> {
    use std::io::Read;

    let cipher = sockudo_app::AppSecretCipher::from_env()?.ok_or_else(|| {
        Error::Configuration(format!(
            "set {} or {} to encrypt app secrets",
            sockudo_app::secret_cipher::KEY_ENV,
            sockudo_app::secret_cipher::KEY_FILE_ENV
        ))
    })?;
    let mut secret = String::new();
    std::io::stdin()
        .read_to_string(&mut secret)
        .map_err(|e| Error::Internal(format!("failed to read secret from stdin: {e}")))?;
    let secret = secret.trim_end_matches(['\r', '\n']);
    if secret.is_empty() {
        return Err(Error::Configuration(
            "no secret on stdin to encrypt".to_string(),
        ));
    }
    println!("{}", cipher.encrypt(app_id, secret)?);
    Ok(())
}

/// Message produced before logging is initialized, replayed once it is.
enum StartupNote {
    Info(String),
    Error(String),
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

    if let Some(app_id) = args.encrypt_app_secret.as_deref() {
        return encrypt_app_secret(app_id);
    }

    // Logging is initialized from the loaded configuration, so anything worth
    // reporting while loading it is buffered here and replayed afterwards.
    let mut startup_notes: Vec<StartupNote> = Vec::new();
    let mut config = ServerOptions::default();

    if let Some(config_path) = args.config.as_deref() {
        // An explicitly requested file must load; silently falling back to the
        // bundled sample (or defaults) hides misconfiguration in production.
        match ServerOptions::load_from_file(config_path).await {
            Ok(file_config) => {
                config = file_config;
                startup_notes.push(StartupNote::Info(format!(
                    "explicit configuration applied: {config_path}"
                )));
            }
            Err(error) => {
                let message = format!("failed to load configuration file '{config_path}': {error}");
                exit_with_config_error(&message);
            }
        }
    } else {
        // Try TOML first, fall back to JSON for backward compatibility
        let mut loaded = false;
        for candidate in ["config/config.toml", "config/config.json"] {
            if !tokio::fs::try_exists(candidate).await.unwrap_or(false) {
                continue;
            }
            match ServerOptions::load_from_file(candidate).await {
                Ok(file_config) => {
                    config = file_config;
                    startup_notes.push(StartupNote::Info(format!(
                        "loaded configuration file: {candidate}"
                    )));
                    loaded = true;
                    break;
                }
                Err(error) => {
                    let message =
                        format!("failed to load configuration file '{candidate}': {error}");
                    exit_with_config_error(&message);
                }
            }
        }
        if !loaded {
            startup_notes.push(StartupNote::Info(
                "no config/config.toml or config/config.json found; using defaults".to_string(),
            ));
        }
    }

    match config.override_from_env().await {
        Ok(()) => startup_notes.push(StartupNote::Info(
            "applied environment variable overrides".to_string(),
        )),
        Err(error) => {
            let message = format!("environment variable overrides failed: {error}");
            eprintln!("sockudo: {message}");
            startup_notes.push(StartupNote::Error(message));
        }
    }

    if let Err(e) = config.validate() {
        return Err(Error::ConfigFile(format!(
            "Configuration validation failed: {}",
            e
        )));
    }

    #[cfg(not(feature = "opentelemetry"))]
    if config.opentelemetry.enabled {
        return Err(Error::ConfigFile(
            "OpenTelemetry is enabled in configuration, but this binary was built without the opentelemetry feature"
                .to_string(),
        ));
    }

    #[cfg(feature = "opentelemetry")]
    let telemetry =
        telemetry::Telemetry::initialize(&config.opentelemetry, &config.instance.process_id)
            .map_err(|error| {
                Error::Internal(format!("OpenTelemetry initialization failed: {error}"))
            })?;

    #[cfg(feature = "opentelemetry")]
    let logging_handles = logging::init(&config, &telemetry).map_err(Error::Internal)?;
    #[cfg(not(feature = "opentelemetry"))]
    let logging_handles = logging::init(&config).map_err(Error::Internal)?;

    let resolved_logging = logging::reload(&logging_handles, &config).map_err(Error::Internal)?;
    info!(
        output_format = resolved_logging.output_format,
        filter = %resolved_logging.filter,
        debug = config.debug,
        include_target = resolved_logging.include_target,
        colors_enabled = resolved_logging.colors_enabled,
        source_location = resolved_logging.source_location,
        "logging initialized with resolved configuration"
    );
    if config.logging.is_some() {
        info!("custom logging configuration applied");
    }
    for note in startup_notes {
        match note {
            StartupNote::Info(message) => info!("{message}"),
            StartupNote::Error(message) => error!("{message}"),
        }
    }

    #[cfg(not(feature = "versioned-messages"))]
    for setting in disable_uncompiled_surfaces(&mut config) {
        warn!(
            setting,
            "setting is enabled but this binary was built without the `versioned-messages` Cargo feature; disabling it (rebuild with --features versioned-messages)"
        );
    }

    #[cfg(feature = "opentelemetry")]
    {
        let (traces_enabled, metrics_enabled, logs_enabled) = telemetry.enabled_signals();
        info!(
            enabled = config.opentelemetry.enabled,
            traces_enabled, metrics_enabled, logs_enabled, "opentelemetry initialized"
        );
    }

    info!(debug = config.debug, "configuration loading complete");

    let result = if args.check_append_storage_rollback || args.rollback_append_storage {
        run_append_storage_rollback(config, args.rollback_append_storage).await
    } else if args.materialize_append_storage {
        materialize_append_storage(config).await
    } else if args.enable_append_storage || args.disable_append_storage {
        set_append_storage_enabled(config, args.enable_append_storage).await
    } else {
        run_server(config).await
    };

    #[cfg(feature = "opentelemetry")]
    if let Err(error) = telemetry.shutdown().await {
        error!(error = %error, "opentelemetry shutdown failed");
    }

    result
}

/// `versioned_messages` and `annotations` need stores that only the
/// `versioned-messages` feature builds; left enabled, every publish fails.
/// Run without those surfaces instead, as `mcp` does.
#[cfg(not(feature = "versioned-messages"))]
fn disable_uncompiled_surfaces(config: &mut ServerOptions) -> Vec<&'static str> {
    let mut disabled = Vec::new();
    for (setting, enabled) in [
        ("versioned_messages", &mut config.versioned_messages.enabled),
        ("annotations", &mut config.annotations.enabled),
    ] {
        if std::mem::take(enabled) {
            disabled.push(setting);
        }
    }
    disabled
}

/// Rows per page when materializing compact append storage.
const MATERIALIZE_BATCH_SIZE: usize = 500;

#[cfg(feature = "versioned-messages")]
async fn materialize_append_storage(config: ServerOptions) -> Result<()> {
    let store = history::create_version_store(
        &config.versioned_messages,
        &config.history,
        &config.database,
        &config.database_pooling,
    )
    .await?;
    info!(
        driver = ?config.versioned_messages.driver,
        "materializing compact append storage"
    );
    let rewritten = store
        .materialize_append_storage(MATERIALIZE_BATCH_SIZE)
        .await
        .inspect_err(|error| error!(error = %error, "append storage materialization failed"))?;
    info!(
        rewritten_count = rewritten,
        "append storage materialization complete"
    );
    Ok(())
}

#[cfg(not(feature = "versioned-messages"))]
async fn materialize_append_storage(_config: ServerOptions) -> Result<()> {
    Err(Error::Configuration(
        "append storage materialization requires the versioned-messages feature".to_string(),
    ))
}

#[cfg(feature = "versioned-messages")]
async fn set_append_storage_enabled(config: ServerOptions, enabled: bool) -> Result<()> {
    let store = history::create_version_store(
        &config.versioned_messages,
        &config.history,
        &config.database,
        &config.database_pooling,
    )
    .await?;
    store
        .set_append_storage_enabled(enabled)
        .await
        .inspect_err(|error| error!(error = %error, "append storage marker update failed"))?;
    info!(enabled, "append storage marker updated");
    Ok(())
}

#[cfg(not(feature = "versioned-messages"))]
async fn set_append_storage_enabled(_config: ServerOptions, _enabled: bool) -> Result<()> {
    Err(Error::Configuration(
        "append storage marker requires the versioned-messages feature".to_string(),
    ))
}

#[cfg(feature = "versioned-messages")]
async fn run_append_storage_rollback(config: ServerOptions, execute: bool) -> Result<()> {
    let store = history::create_version_store(
        &config.versioned_messages,
        &config.history,
        &config.database,
        &config.database_pooling,
    )
    .await?;
    store
        .validate_append_storage_rollback(MATERIALIZE_BATCH_SIZE)
        .await
        .inspect_err(|error| error!(error = %error, "append storage rollback preflight failed"))?;
    info!("append storage rollback preflight complete");
    if execute {
        store.set_append_storage_enabled(false).await.inspect_err(
            |error| error!(error = %error, "append storage rollback disable failed"),
        )?;
        let rewritten = store
            .materialize_append_storage(MATERIALIZE_BATCH_SIZE)
            .await
            .inspect_err(
                |error| error!(error = %error, "append storage rollback materialization failed"),
            )?;
        info!(
            rewritten_count = rewritten,
            "append storage rollback complete"
        );
    }
    Ok(())
}

#[cfg(not(feature = "versioned-messages"))]
async fn run_append_storage_rollback(_config: ServerOptions, _execute: bool) -> Result<()> {
    Err(Error::Configuration(
        "append storage rollback requires the versioned-messages feature".to_string(),
    ))
}

async fn run_server(config: ServerOptions) -> Result<()> {
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

#[cfg(all(test, not(feature = "versioned-messages")))]
mod uncompiled_surface_tests {
    use super::*;

    #[test]
    fn mutable_message_surfaces_are_disabled_without_the_feature() {
        let mut config = ServerOptions::default();
        config.versioned_messages.enabled = true;
        config.annotations.enabled = true;

        assert_eq!(
            disable_uncompiled_surfaces(&mut config),
            ["versioned_messages", "annotations"]
        );
        assert!(!config.versioned_messages.enabled);
        assert!(!config.annotations.enabled);
        assert!(disable_uncompiled_surfaces(&mut config).is_empty());
    }
}

#[cfg(test)]
mod args_tests {
    use super::Args;
    use clap::Parser;

    #[test]
    fn append_maintenance_actions_are_mutually_exclusive() {
        let actions = [
            "--enable-append-storage",
            "--disable-append-storage",
            "--materialize-append-storage",
            "--check-append-storage-rollback",
            "--rollback-append-storage",
        ];
        for (index, action) in actions.iter().enumerate() {
            assert!(Args::try_parse_from(["sockudo", action]).is_ok());
            for other in &actions[index + 1..] {
                assert!(Args::try_parse_from(["sockudo", action, other]).is_err());
            }
        }
    }
}
