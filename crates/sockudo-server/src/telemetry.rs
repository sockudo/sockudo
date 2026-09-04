use axum::body::Body;
use axum::extract::MatchedPath;
use axum::http::Request;
use axum::middleware::Next;
use axum::response::Response;
use opentelemetry::propagation::{
    Extractor, Injector, TextMapCompositePropagator, TextMapPropagator,
};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::{KeyValue, global};
use opentelemetry_http::HeaderExtractor;
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::{
    BatchConfigBuilder as LogBatchConfigBuilder, BatchLogProcessor, SdkLoggerProvider,
};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::propagation::{BaggagePropagator, TraceContextPropagator};
use opentelemetry_sdk::resource::{
    EnvResourceDetector, SdkProvidedResourceDetector, TelemetryResourceDetector,
};
use opentelemetry_sdk::trace::{
    BatchConfigBuilder as TraceBatchConfigBuilder, BatchSpanProcessor, SdkTracer, SdkTracerProvider,
};
use sockudo_core::options::OpenTelemetryConfig;
use std::collections::BTreeMap;
use std::time::Duration;
use tracing::{Instrument, field, info_span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

#[derive(Debug, Clone, Copy)]
enum Signal {
    Traces,
    Metrics,
    Logs,
}

impl Signal {
    const fn name(self) -> &'static str {
        match self {
            Self::Traces => "traces",
            Self::Metrics => "metrics",
            Self::Logs => "logs",
        }
    }

    const fn protocol_env(self) -> &'static str {
        match self {
            Self::Traces => "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
            Self::Metrics => "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL",
            Self::Logs => "OTEL_EXPORTER_OTLP_LOGS_PROTOCOL",
        }
    }

    const fn endpoint_env(self) -> &'static str {
        match self {
            Self::Traces => "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
            Self::Metrics => "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
            Self::Logs => "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
        }
    }

    const fn timeout_env(self) -> &'static str {
        match self {
            Self::Traces => "OTEL_EXPORTER_OTLP_TRACES_TIMEOUT",
            Self::Metrics => "OTEL_EXPORTER_OTLP_METRICS_TIMEOUT",
            Self::Logs => "OTEL_EXPORTER_OTLP_LOGS_TIMEOUT",
        }
    }

    const fn exporter_env(self) -> &'static str {
        match self {
            Self::Traces => "OTEL_TRACES_EXPORTER",
            Self::Metrics => "OTEL_METRICS_EXPORTER",
            Self::Logs => "OTEL_LOGS_EXPORTER",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Transport {
    Grpc,
    HttpProtobuf,
    HttpJson,
}

impl Transport {
    const fn protocol(self) -> Protocol {
        match self {
            Self::Grpc => Protocol::Grpc,
            Self::HttpProtobuf => Protocol::HttpBinary,
            Self::HttpJson => Protocol::HttpJson,
        }
    }
}

/// Owns every OpenTelemetry provider so all buffered signals can be flushed on shutdown.
#[derive(Debug)]
pub(crate) struct Telemetry {
    tracer_provider: Option<SdkTracerProvider>,
    meter_provider: Option<SdkMeterProvider>,
    logger_provider: Option<SdkLoggerProvider>,
    shutdown_timeout: Duration,
}

impl Telemetry {
    pub(crate) fn initialize(
        config: &OpenTelemetryConfig,
        service_instance_id: &str,
    ) -> Result<Self, String> {
        let shutdown_timeout = Duration::from_millis(config.export_timeout_ms);
        if !config.enabled || sdk_disabled() {
            configure_propagation(config);
            return Ok(Self {
                tracer_provider: None,
                meter_provider: None,
                logger_provider: None,
                shutdown_timeout,
            });
        }

        configure_propagation(config);
        let resource = resource(config, service_instance_id);
        let traces_enabled = signal_enabled(config.traces_enabled, Signal::Traces)?;
        let metrics_enabled = signal_enabled(config.metrics_enabled, Signal::Metrics)?;
        let logs_enabled = signal_enabled(config.logs_enabled, Signal::Logs)?;

        // Validate and construct all exporters before starting any processor threads. This keeps a
        // later signal's configuration error from leaving an earlier signal partially initialized.
        let span_exporter = traces_enabled
            .then(|| build_span_exporter(config))
            .transpose()?;
        let metric_exporter = metrics_enabled
            .then(|| build_metric_exporter(config))
            .transpose()?;
        let log_exporter = logs_enabled
            .then(|| build_log_exporter(config))
            .transpose()?;

        let tracer_provider = if let Some(exporter) = span_exporter {
            let processor = BatchSpanProcessor::builder(exporter)
                .with_batch_config(trace_batch_config(config))
                .build();
            let provider = SdkTracerProvider::builder()
                .with_resource(resource.clone())
                .with_span_processor(processor)
                .build();
            global::set_tracer_provider(provider.clone());
            Some(provider)
        } else {
            None
        };

        let meter_provider = if let Some(exporter) = metric_exporter {
            let mut reader = PeriodicReader::builder(exporter);
            if std::env::var_os("OTEL_METRIC_EXPORT_INTERVAL").is_none() {
                reader =
                    reader.with_interval(Duration::from_millis(config.metric_export_interval_ms));
            }
            let provider = SdkMeterProvider::builder()
                .with_resource(resource.clone())
                .with_reader(reader.build())
                .build();
            global::set_meter_provider(provider.clone());
            Some(provider)
        } else {
            None
        };

        let logger_provider = if let Some(exporter) = log_exporter {
            let processor = BatchLogProcessor::builder(exporter)
                .with_batch_config(log_batch_config(config))
                .build();
            Some(
                SdkLoggerProvider::builder()
                    .with_resource(resource)
                    .with_log_processor(processor)
                    .build(),
            )
        } else {
            None
        };

        Ok(Self {
            tracer_provider,
            meter_provider,
            logger_provider,
            shutdown_timeout,
        })
    }

    pub(crate) fn tracer(&self) -> Option<SdkTracer> {
        self.tracer_provider
            .as_ref()
            .map(|provider| provider.tracer("sockudo"))
    }

    pub(crate) fn logger_provider(&self) -> Option<&SdkLoggerProvider> {
        self.logger_provider.as_ref()
    }

    pub(crate) fn enabled_signals(&self) -> (bool, bool, bool) {
        (
            self.tracer_provider.is_some(),
            self.meter_provider.is_some(),
            self.logger_provider.is_some(),
        )
    }

    pub(crate) async fn shutdown(self) -> Result<(), String> {
        let Self {
            tracer_provider,
            meter_provider,
            logger_provider,
            shutdown_timeout,
        } = self;

        tokio::task::spawn_blocking(move || {
            let mut errors = Vec::new();
            if let Some(provider) = tracer_provider
                && let Err(error) = provider.shutdown_with_timeout(shutdown_timeout)
            {
                errors.push(format!("traces: {error}"));
            }
            if let Some(provider) = logger_provider
                && let Err(error) = provider.shutdown_with_timeout(shutdown_timeout)
            {
                errors.push(format!("logs: {error}"));
            }
            if let Some(provider) = meter_provider
                && let Err(error) = provider.shutdown_with_timeout(shutdown_timeout)
            {
                errors.push(format!("metrics: {error}"));
            }
            if errors.is_empty() {
                Ok(())
            } else {
                Err(errors.join("; "))
            }
        })
        .await
        .map_err(|error| format!("telemetry shutdown worker failed: {error}"))?
    }
}

/// Creates an HTTP server span using only low-cardinality route templates.
pub(crate) async fn trace_http_request(request: Request<Body>, next: Next) -> Response {
    if matches!(
        request.uri().path(),
        "/live" | "/up" | "/accept-traffic" | "/metrics"
    ) {
        return next.run(request).await;
    }

    let method = request.method().clone();
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(MatchedPath::as_str)
        .unwrap_or("unmatched")
        .to_owned();
    let operation_name = format!("{} {route}", method.as_str());
    let parent = global::get_text_map_propagator(|propagator| {
        propagator.extract(&HeaderExtractor(request.headers()))
    });
    let span = info_span!(
        target: "sockudo_telemetry",
        "http.server.request",
        otel.kind = "server",
        otel.name = %operation_name,
        http.request.method = %method,
        http.route = %route,
        http.response.status_code = field::Empty,
        otel.status_code = field::Empty,
    );
    let _ = span.set_parent(parent);
    let started = std::time::Instant::now();
    let response = next.run(request).instrument(span.clone()).await;
    let status = response.status();
    span.record("http.response.status_code", status.as_u16());
    if status.is_server_error() {
        span.record("otel.status_code", "ERROR");
    }
    metrics::histogram!(
        "http.server.request.duration",
        "http.request.method" => method.as_str().to_owned(),
        "http.route" => route,
    )
    .record(started.elapsed().as_secs_f64());
    response
}

pub(crate) fn capture_job_context(job: &mut sockudo_core::webhook_types::JobData) {
    if !job.trace_context.is_empty() {
        return;
    }
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(
            &tracing::Span::current().context(),
            &mut MapInjector(&mut job.trace_context),
        );
    });
}

pub(crate) fn push_job_consumer_span(
    job: &sockudo_core::webhook_types::JobData,
    queue_name: &str,
) -> tracing::Span {
    let span = info_span!(
        target: "sockudo_telemetry",
        "messaging.process",
        otel.kind = "consumer",
        otel.name = "push job process",
        messaging.system = "sockudo.queue",
        messaging.destination.name = queue_name,
        messaging.operation.name = "process",
        app_id = %job.app_id,
    );
    let parent = global::get_text_map_propagator(|propagator| {
        propagator.extract(&MapExtractor(&job.trace_context))
    });
    let _ = span.set_parent(parent);
    span
}

struct MapInjector<'a>(&'a mut BTreeMap<String, String>);

impl Injector for MapInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_owned(), value);
    }
}

struct MapExtractor<'a>(&'a BTreeMap<String, String>);

impl Extractor for MapExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(String::as_str).collect()
    }
}

fn resource(config: &OpenTelemetryConfig, service_instance_id: &str) -> Resource {
    let mut attributes = config
        .resource_attributes
        .iter()
        .map(|(key, value)| KeyValue::new(key.clone(), value.clone()))
        .collect::<Vec<_>>();
    attributes.push(KeyValue::new("service.name", config.service_name.clone()));
    attributes.push(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")));
    if !service_instance_id.is_empty() {
        attributes.push(KeyValue::new(
            "service.instance.id",
            service_instance_id.to_owned(),
        ));
    }
    if let Some(namespace) = &config.service_namespace {
        attributes.push(KeyValue::new("service.namespace", namespace.clone()));
    }
    if let Some(environment) = &config.deployment_environment {
        attributes.push(KeyValue::new(
            "deployment.environment.name",
            environment.clone(),
        ));
    }

    Resource::builder_empty()
        .with_attributes(attributes)
        .with_detector(Box::new(SdkProvidedResourceDetector))
        .with_detector(Box::new(TelemetryResourceDetector))
        // Standard OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES win over Sockudo fallbacks.
        .with_detector(Box::new(EnvResourceDetector::new()))
        .build()
}

fn trace_batch_config(config: &OpenTelemetryConfig) -> opentelemetry_sdk::trace::BatchConfig {
    let mut builder = TraceBatchConfigBuilder::default();
    if std::env::var_os("OTEL_BSP_SCHEDULE_DELAY").is_none() {
        builder =
            builder.with_scheduled_delay(Duration::from_millis(config.batch_scheduled_delay_ms));
    }
    if std::env::var_os("OTEL_BSP_MAX_QUEUE_SIZE").is_none() {
        builder = builder.with_max_queue_size(config.batch_max_queue_size);
    }
    if std::env::var_os("OTEL_BSP_MAX_EXPORT_BATCH_SIZE").is_none() {
        builder = builder.with_max_export_batch_size(config.batch_max_export_batch_size);
    }
    builder.build()
}

fn log_batch_config(config: &OpenTelemetryConfig) -> opentelemetry_sdk::logs::BatchConfig {
    let mut builder = LogBatchConfigBuilder::default();
    if std::env::var_os("OTEL_BLRP_SCHEDULE_DELAY").is_none() {
        builder =
            builder.with_scheduled_delay(Duration::from_millis(config.batch_scheduled_delay_ms));
    }
    if std::env::var_os("OTEL_BLRP_MAX_QUEUE_SIZE").is_none() {
        builder = builder.with_max_queue_size(config.batch_max_queue_size);
    }
    if std::env::var_os("OTEL_BLRP_MAX_EXPORT_BATCH_SIZE").is_none() {
        builder = builder.with_max_export_batch_size(config.batch_max_export_batch_size);
    }
    builder.build()
}

fn sdk_disabled() -> bool {
    std::env::var("OTEL_SDK_DISABLED")
        .ok()
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

fn signal_enabled(config_enabled: bool, signal: Signal) -> Result<bool, String> {
    if !config_enabled {
        return Ok(false);
    }
    match std::env::var(signal.exporter_env()) {
        Ok(value) if value.eq_ignore_ascii_case("none") => Ok(false),
        Ok(value) if value.eq_ignore_ascii_case("otlp") => Ok(true),
        Ok(value) => Err(format!(
            "{} contains unsupported exporter {value:?}; supported values are otlp and none",
            signal.exporter_env()
        )),
        Err(_) => Ok(true),
    }
}

fn configure_propagation(config: &OpenTelemetryConfig) {
    let requested = std::env::var("OTEL_PROPAGATORS").ok();
    let trace_context = requested
        .as_ref()
        .map_or(config.propagation_trace_context, |value| {
            value
                .split(',')
                .any(|name| name.trim().eq_ignore_ascii_case("tracecontext"))
        });
    let baggage = requested
        .as_ref()
        .map_or(config.propagation_baggage, |value| {
            value
                .split(',')
                .any(|name| name.trim().eq_ignore_ascii_case("baggage"))
        });

    let mut propagators: Vec<Box<dyn TextMapPropagator + Send + Sync>> = Vec::new();
    if trace_context {
        propagators.push(Box::new(TraceContextPropagator::new()));
    }
    if baggage {
        propagators.push(Box::new(BaggagePropagator::new()));
    }
    global::set_text_map_propagator(TextMapCompositePropagator::new(propagators));
}

fn transport(signal: Signal) -> Result<Transport, String> {
    let value = std::env::var(signal.protocol_env())
        .ok()
        .or_else(|| std::env::var("OTEL_EXPORTER_OTLP_PROTOCOL").ok());
    parse_transport(value.as_deref(), signal)
}

fn parse_transport(value: Option<&str>, signal: Signal) -> Result<Transport, String> {
    match value.map(str::trim) {
        Some("grpc") => Ok(Transport::Grpc),
        Some("http/json") => Ok(Transport::HttpJson),
        Some("http/protobuf") | None => Ok(Transport::HttpProtobuf),
        Some(value) => Err(format!(
            "unsupported OTLP protocol {value:?} for {}; supported values are grpc, http/protobuf, and http/json",
            signal.name()
        )),
    }
}

fn endpoint_override(
    config: &OpenTelemetryConfig,
    signal: Signal,
    transport: Transport,
) -> Option<String> {
    if std::env::var_os(signal.endpoint_env()).is_some()
        || std::env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT").is_some()
    {
        return None;
    }
    let endpoint = config.endpoint.as_ref()?;
    Some(endpoint_for_transport(endpoint, signal, transport))
}

fn endpoint_for_transport(endpoint: &str, signal: Signal, transport: Transport) -> String {
    match transport {
        Transport::Grpc => endpoint.to_owned(),
        Transport::HttpProtobuf | Transport::HttpJson => {
            format!("{}/v1/{}", endpoint.trim_end_matches('/'), signal.name())
        }
    }
}

fn exporter_timeout_override(config: &OpenTelemetryConfig, signal: Signal) -> Option<Duration> {
    (std::env::var_os(signal.timeout_env()).is_none()
        && std::env::var_os("OTEL_EXPORTER_OTLP_TIMEOUT").is_none())
    .then(|| Duration::from_millis(config.export_timeout_ms))
}

macro_rules! build_exporter {
    ($exporter:ident, $config:expr, $signal:expr) => {{
        let transport = transport($signal)?;
        let endpoint = endpoint_override($config, $signal, transport);
        let timeout = exporter_timeout_override($config, $signal);
        let exporter = match transport {
            Transport::Grpc => {
                let mut builder = opentelemetry_otlp::$exporter::builder()
                    .with_tonic()
                    .with_protocol(Protocol::Grpc);
                if let Some(endpoint) = endpoint {
                    builder = builder.with_endpoint(endpoint);
                }
                if let Some(timeout) = timeout {
                    builder = builder.with_timeout(timeout);
                }
                builder.build()
            }
            Transport::HttpProtobuf | Transport::HttpJson => {
                let mut builder = opentelemetry_otlp::$exporter::builder()
                    .with_http()
                    .with_protocol(transport.protocol());
                if let Some(endpoint) = endpoint {
                    builder = builder.with_endpoint(endpoint);
                }
                if let Some(timeout) = timeout {
                    builder = builder.with_timeout(timeout);
                }
                builder.build()
            }
        };
        exporter.map_err(|error| {
            format!(
                "failed to configure OTLP {} exporter: {error}",
                $signal.name()
            )
        })
    }};
}

fn build_span_exporter(
    config: &OpenTelemetryConfig,
) -> Result<opentelemetry_otlp::SpanExporter, String> {
    build_exporter!(SpanExporter, config, Signal::Traces)
}

fn build_metric_exporter(
    config: &OpenTelemetryConfig,
) -> Result<opentelemetry_otlp::MetricExporter, String> {
    build_exporter!(MetricExporter, config, Signal::Metrics)
}

fn build_log_exporter(
    config: &OpenTelemetryConfig,
) -> Result<opentelemetry_otlp::LogExporter, String> {
    build_exporter!(LogExporter, config, Signal::Logs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_base_endpoint_gets_signal_path() {
        let config = OpenTelemetryConfig {
            endpoint: Some("http://collector:4318/".to_string()),
            ..OpenTelemetryConfig::default()
        };
        assert_eq!(
            endpoint_for_transport(
                config.endpoint.as_deref().unwrap(),
                Signal::Traces,
                Transport::HttpProtobuf,
            ),
            "http://collector:4318/v1/traces"
        );
        assert_eq!(
            endpoint_for_transport(
                config.endpoint.as_deref().unwrap(),
                Signal::Metrics,
                Transport::HttpJson,
            ),
            "http://collector:4318/v1/metrics"
        );
    }

    #[test]
    fn unsupported_otlp_protocol_is_rejected() {
        assert_eq!(
            parse_transport(Some("grpc"), Signal::Traces).unwrap(),
            Transport::Grpc
        );
        let error = parse_transport(Some("zipkin"), Signal::Traces).unwrap_err();
        assert!(error.contains("unsupported OTLP protocol"));
        assert!(error.contains("traces"));
    }
}
