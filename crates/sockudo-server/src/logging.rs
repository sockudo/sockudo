use sockudo_core::options::ServerOptions;
use tracing_subscriber::{
    EnvFilter, Layer, Registry, fmt, layer::SubscriberExt, reload, util::SubscriberInitExt,
};

#[cfg(feature = "opentelemetry")]
use tracing_subscriber::filter::filter_fn;

#[cfg(feature = "opentelemetry")]
use crate::telemetry::Telemetry;
#[cfg(feature = "opentelemetry")]
use opentelemetry_appender_tracing::layer::{OpenTelemetryTracingBridge, TracingSpanAttributes};

const DEBUG_FILTER: &str = "info,sockudo=debug,tower_http=debug";
const PRODUCTION_FILTER: &str = "info";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputFormat {
    Text,
    Json,
}

impl OutputFormat {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Json => "json",
        }
    }
}

#[derive(Debug)]
pub(crate) struct ResolvedLogging {
    pub output_format: &'static str,
    pub filter: String,
    pub include_target: bool,
    pub colors_enabled: bool,
    pub source_location: bool,
}

type FilterLayer = reload::Layer<EnvFilter, Registry>;
type FilteredRegistry = tracing_subscriber::layer::Layered<FilterLayer, Registry>;
type FormatLayer = Box<dyn Layer<FilteredRegistry> + Send + Sync>;

pub(crate) struct LoggingReloadHandles {
    output_format: OutputFormat,
    filter_handle: reload::Handle<EnvFilter, Registry>,
    format_handle: reload::Handle<FormatLayer, FilteredRegistry>,
}

#[cfg(feature = "opentelemetry")]
pub(crate) fn init(
    config: &ServerOptions,
    telemetry: &Telemetry,
) -> Result<LoggingReloadHandles, String> {
    init_inner(config, Some(telemetry))
}

#[cfg(not(feature = "opentelemetry"))]
pub(crate) fn init(config: &ServerOptions) -> Result<LoggingReloadHandles, String> {
    init_inner(config)
}

fn init_inner(
    config: &ServerOptions,
    #[cfg(feature = "opentelemetry")] telemetry: Option<&Telemetry>,
) -> Result<LoggingReloadHandles, String> {
    let output_format = output_format(std::env::var("LOG_OUTPUT_FORMAT").ok().as_deref());
    let resolved = resolve(config, output_format);
    let filter = EnvFilter::new(&resolved.filter);
    let (filter_layer, filter_handle) = reload::Layer::new(filter);
    let (format_layer, format_handle) = reload::Layer::new(format_layer(output_format, &resolved));
    let subscriber = tracing_subscriber::registry()
        .with(filter_layer)
        .with(format_layer);

    #[cfg(feature = "opentelemetry")]
    match (
        telemetry.and_then(Telemetry::tracer),
        telemetry.and_then(Telemetry::logger_provider),
    ) {
        (Some(tracer), Some(logger_provider)) => subscriber
            .with(
                tracing_opentelemetry::layer()
                    .with_tracer(tracer)
                    .with_filter(filter_fn(exportable_target)),
            )
            .with(
                OpenTelemetryTracingBridge::builder(logger_provider)
                    .with_tracing_span_attributes(TracingSpanAttributes::allowlist([
                        "app_id",
                        "socket_id",
                        "channel",
                        "user_id",
                        "worker_id",
                    ]))
                    .build()
                    .with_filter(filter_fn(exportable_target)),
            )
            .try_init(),
        (Some(tracer), None) => subscriber
            .with(
                tracing_opentelemetry::layer()
                    .with_tracer(tracer)
                    .with_filter(filter_fn(exportable_target)),
            )
            .try_init(),
        (None, Some(logger_provider)) => subscriber
            .with(
                OpenTelemetryTracingBridge::builder(logger_provider)
                    .with_tracing_span_attributes(TracingSpanAttributes::allowlist([
                        "app_id",
                        "socket_id",
                        "channel",
                        "user_id",
                        "worker_id",
                    ]))
                    .build()
                    .with_filter(filter_fn(exportable_target)),
            )
            .try_init(),
        (None, None) => subscriber.try_init(),
    }
    .map_err(|error| format!("failed to install global logging subscriber: {error}"))?;

    #[cfg(not(feature = "opentelemetry"))]
    subscriber
        .try_init()
        .map_err(|error| format!("failed to install global logging subscriber: {error}"))?;

    Ok(LoggingReloadHandles {
        output_format,
        filter_handle,
        format_handle,
    })
}

#[cfg(feature = "opentelemetry")]
fn exportable_target(metadata: &tracing::Metadata<'_>) -> bool {
    !matches!(
        metadata.target(),
        target if target.starts_with("opentelemetry")
            || target.starts_with("hyper")
            || target.starts_with("h2")
            || target.starts_with("tonic")
            || target.starts_with("reqwest")
    )
}

pub(crate) fn reload(
    handles: &LoggingReloadHandles,
    config: &ServerOptions,
) -> Result<ResolvedLogging, String> {
    let resolved = resolve(config, handles.output_format);
    handles
        .filter_handle
        .reload(EnvFilter::new(&resolved.filter))
        .map_err(|error| format!("failed to reload logging filter: {error}"))?;
    handles
        .format_handle
        .reload(format_layer(handles.output_format, &resolved))
        .map_err(|error| format!("failed to reload logging formatter: {error}"))?;
    Ok(resolved)
}

fn format_layer(output_format: OutputFormat, resolved: &ResolvedLogging) -> FormatLayer {
    match output_format {
        OutputFormat::Json => fmt::layer()
            .json()
            .with_target(resolved.include_target)
            .with_file(resolved.source_location)
            .with_line_number(resolved.source_location)
            .boxed(),
        OutputFormat::Text => fmt::layer()
            .with_ansi(resolved.colors_enabled)
            .with_target(resolved.include_target)
            .with_file(resolved.source_location)
            .with_line_number(resolved.source_location)
            .boxed(),
    }
}

fn resolve(config: &ServerOptions, output_format: OutputFormat) -> ResolvedLogging {
    let mut filter = resolved_filter(
        config.debug,
        std::env::var("RUST_LOG").ok(),
        std::env::var("SOCKUDO_LOG_DEBUG").ok(),
        std::env::var("SOCKUDO_LOG_PROD").ok(),
    );
    filter = with_telemetry_span_filter(
        filter,
        config.opentelemetry.enabled && config.opentelemetry.traces_enabled,
    );
    let include_target = config
        .logging
        .as_ref()
        .is_none_or(|value| value.include_target);
    let colors_enabled = config
        .logging
        .as_ref()
        .is_none_or(|value| value.colors_enabled);

    ResolvedLogging {
        output_format: output_format.as_str(),
        filter,
        include_target,
        colors_enabled,
        source_location: config.debug,
    }
}

fn output_format(value: Option<&str>) -> OutputFormat {
    match value {
        Some("json") => OutputFormat::Json,
        _ => OutputFormat::Text,
    }
}

fn resolved_filter(
    debug: bool,
    rust_log: Option<String>,
    debug_override: Option<String>,
    prod_override: Option<String>,
) -> String {
    rust_log.unwrap_or_else(|| {
        if debug {
            debug_override.unwrap_or_else(|| DEBUG_FILTER.to_owned())
        } else {
            prod_override.unwrap_or_else(|| PRODUCTION_FILTER.to_owned())
        }
    })
}

fn with_telemetry_span_filter(mut filter: String, telemetry_traces_enabled: bool) -> String {
    if telemetry_traces_enabled {
        // Keep explicit instrumentation spans independent from local log verbosity. The formatter
        // does not emit span lifecycle events, so this does not turn trace-level logs on.
        filter.push_str(",sockudo_telemetry=trace");
    }
    filter
}

fn bootstrap_debug() -> bool {
    if std::env::var_os("DEBUG").is_some() {
        bool_env("DEBUG", false)
    } else {
        bool_env("DEBUG_MODE", false)
    }
}

fn bool_env(name: &str, default: bool) -> bool {
    sockudo_core::utils::parse_bool_env(name, default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_format_is_explicit_and_safe_by_default() {
        assert_eq!(output_format(Some("json")), OutputFormat::Json);
        assert_eq!(output_format(Some("JSON")), OutputFormat::Text);
        assert_eq!(output_format(None), OutputFormat::Text);
    }

    #[test]
    fn resolved_filter_precedence_is_stable() {
        assert_eq!(
            resolved_filter(true, Some("warn".into()), Some("debug".into()), None),
            "warn"
        );
        assert_eq!(
            resolved_filter(true, None, Some("debug".into()), None),
            "debug"
        );
        assert_eq!(
            resolved_filter(false, None, None, Some("error".into())),
            "error"
        );
        assert_eq!(resolved_filter(false, None, None, None), PRODUCTION_FILTER);
        assert_eq!(resolved_filter(true, None, None, None), DEBUG_FILTER);
    }

    #[test]
    fn telemetry_spans_are_independent_from_local_log_verbosity() {
        assert_eq!(
            with_telemetry_span_filter("warn".to_string(), true),
            "warn,sockudo_telemetry=trace"
        );
        assert_eq!(
            with_telemetry_span_filter("warn".to_string(), false),
            "warn"
        );
    }

    #[test]
    fn text_and_json_resolve_the_same_runtime_settings() {
        let mut config = ServerOptions {
            debug: true,
            ..ServerOptions::default()
        };
        let logging = config.logging.get_or_insert_default();
        logging.include_target = false;
        logging.colors_enabled = false;

        let text = resolve(&config, OutputFormat::Text);
        let json = resolve(&config, OutputFormat::Json);
        assert_eq!(text.include_target, json.include_target);
        assert_eq!(text.colors_enabled, json.colors_enabled);
        assert_eq!(text.source_location, json.source_location);
    }
}
