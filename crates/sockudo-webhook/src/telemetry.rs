use sockudo_core::webhook_types::JobData;
#[cfg(feature = "opentelemetry")]
use std::collections::BTreeMap;
use tracing::Span;

#[cfg(feature = "opentelemetry")]
use opentelemetry::global;
#[cfg(feature = "opentelemetry")]
use opentelemetry::propagation::{Extractor, Injector};
#[cfg(feature = "opentelemetry")]
use tracing::info_span;
#[cfg(feature = "opentelemetry")]
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub(crate) fn capture(job: &mut JobData) {
    #[cfg(feature = "opentelemetry")]
    if job.trace_context.is_empty() {
        global::get_text_map_propagator(|propagator| {
            propagator.inject_context(
                &Span::current().context(),
                &mut TraceCarrier(&mut job.trace_context),
            );
        });
    }

    #[cfg(not(feature = "opentelemetry"))]
    let _ = job;
}

pub(crate) fn consumer_span(job: &JobData) -> Span {
    #[cfg(feature = "opentelemetry")]
    {
        let span = info_span!(
            target: "sockudo_telemetry",
            "messaging.process",
            otel.kind = "consumer",
            otel.name = "webhook process",
            messaging.system = "sockudo.queue",
            messaging.destination.name = "webhooks",
            messaging.operation.name = "process",
            app_id = %job.app_id,
            webhook_job_id = job.job_id.as_deref().unwrap_or("legacy"),
        );
        let parent = global::get_text_map_propagator(|propagator| {
            propagator.extract(&TraceCarrierRef(&job.trace_context))
        });
        let _ = span.set_parent(parent);
        span
    }

    #[cfg(not(feature = "opentelemetry"))]
    {
        let _ = job;
        Span::none()
    }
}

#[cfg(feature = "opentelemetry")]
pub(crate) fn inject_http_headers(span: &Span, headers: &mut reqwest::header::HeaderMap) {
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&span.context(), &mut HeaderInjector(headers));
    });
}

#[cfg(not(feature = "opentelemetry"))]
pub(crate) fn inject_http_headers(_span: &Span, _headers: &mut reqwest::header::HeaderMap) {}

#[cfg(feature = "opentelemetry")]
struct TraceCarrier<'a>(&'a mut BTreeMap<String, String>);

#[cfg(feature = "opentelemetry")]
impl Injector for TraceCarrier<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_owned(), value);
    }
}

#[cfg(feature = "opentelemetry")]
struct TraceCarrierRef<'a>(&'a BTreeMap<String, String>);

#[cfg(feature = "opentelemetry")]
impl Extractor for TraceCarrierRef<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(String::as_str).collect()
    }
}

#[cfg(feature = "opentelemetry")]
struct HeaderInjector<'a>(&'a mut reqwest::header::HeaderMap);

#[cfg(feature = "opentelemetry")]
impl Injector for HeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        let Ok(name) = reqwest::header::HeaderName::from_bytes(key.as_bytes()) else {
            return;
        };
        let Ok(value) = reqwest::header::HeaderValue::from_str(&value) else {
            return;
        };
        self.0.insert(name, value);
    }
}
