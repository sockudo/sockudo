use crate::horizontal_adapter::{BroadcastMessage, RequestBody};
use std::collections::BTreeMap;
use tracing::Span;

#[cfg(feature = "opentelemetry")]
use opentelemetry::global;
#[cfg(feature = "opentelemetry")]
use opentelemetry::propagation::{Extractor, Injector};
#[cfg(feature = "opentelemetry")]
use tracing::trace_span;
#[cfg(feature = "opentelemetry")]
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub(crate) fn current_context() -> BTreeMap<String, String> {
    #[cfg(feature = "opentelemetry")]
    {
        let mut carrier = TraceCarrier::default();
        global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&Span::current().context(), &mut carrier);
        });
        carrier.0
    }

    #[cfg(not(feature = "opentelemetry"))]
    BTreeMap::new()
}

pub(crate) fn broadcast_consumer_span(broadcast: &BroadcastMessage) -> Span {
    #[cfg(feature = "opentelemetry")]
    {
        let span = trace_span!(
            target: "sockudo_telemetry",
            "messaging.receive",
            otel.kind = "consumer",
            otel.name = "sockudo broadcast receive",
            messaging.system = "sockudo.horizontal",
            messaging.operation.name = "receive",
            app_id = %broadcast.app_id,
            channel = %broadcast.channel,
        );
        let parent = global::get_text_map_propagator(|propagator| {
            propagator.extract(&TraceCarrierRef(&broadcast.trace_context))
        });
        let _ = span.set_parent(parent);
        span
    }

    #[cfg(not(feature = "opentelemetry"))]
    {
        let _ = broadcast;
        Span::none()
    }
}

pub(crate) fn request_consumer_span(request: &RequestBody) -> Span {
    #[cfg(feature = "opentelemetry")]
    {
        let span = trace_span!(
            target: "sockudo_telemetry",
            "messaging.receive",
            otel.kind = "consumer",
            otel.name = "sockudo request receive",
            messaging.system = "sockudo.horizontal",
            messaging.operation.name = "receive",
            app_id = %request.app_id,
        );
        let parent = global::get_text_map_propagator(|propagator| {
            propagator.extract(&TraceCarrierRef(&request.trace_context))
        });
        let _ = span.set_parent(parent);
        span
    }

    #[cfg(not(feature = "opentelemetry"))]
    {
        let _ = request;
        Span::none()
    }
}

#[cfg(feature = "opentelemetry")]
#[derive(Default)]
struct TraceCarrier(BTreeMap<String, String>);

#[cfg(feature = "opentelemetry")]
impl Injector for TraceCarrier {
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
