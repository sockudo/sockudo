//! Bridge from the workspace's `metrics` facade to the OpenTelemetry metrics API.

use metrics::{
    Counter, CounterFn, Gauge, GaugeFn, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder,
    SharedString, Unit,
};
use opentelemetry::{KeyValue, global, metrics::Meter};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

#[derive(Clone, Debug)]
struct MetricMetadata {
    unit: Option<Unit>,
    description: SharedString,
}

/// Records every metric emitted through `metrics` into the configured global
/// OpenTelemetry meter provider.
#[derive(Clone, Debug)]
pub(crate) struct OpenTelemetryRecorder {
    meter: Meter,
    metadata: Arc<Mutex<HashMap<KeyName, MetricMetadata>>>,
}

impl OpenTelemetryRecorder {
    pub(crate) fn new() -> Self {
        Self {
            meter: global::meter("sockudo"),
            metadata: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn metadata(&self) -> MutexGuard<'_, HashMap<KeyName, MetricMetadata>> {
        self.metadata
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn attributes(key: &Key) -> Vec<KeyValue> {
        key.labels()
            .map(|label| KeyValue::new(label.key().to_owned(), label.value().to_owned()))
            .collect()
    }
}

impl Recorder for OpenTelemetryRecorder {
    fn describe_counter(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.metadata()
            .insert(key, MetricMetadata { unit, description });
    }

    fn describe_gauge(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.metadata()
            .insert(key, MetricMetadata { unit, description });
    }

    fn describe_histogram(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.metadata()
            .insert(key, MetricMetadata { unit, description });
    }

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        let mut builder = self.meter.u64_counter(key.name().to_owned());
        if let Some(metadata) = self.metadata().get(key.name()).cloned() {
            if let Some(unit) = metadata.unit {
                builder = builder.with_unit(unit.as_canonical_label());
            }
            builder = builder.with_description(metadata.description.to_string());
        }

        Counter::from_arc(Arc::new(OtelCounter {
            instrument: builder.build(),
            attributes: Self::attributes(key),
            value: AtomicU64::new(0),
        }))
    }

    fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        let mut builder = self.meter.f64_gauge(key.name().to_owned());
        if let Some(metadata) = self.metadata().get(key.name()).cloned() {
            if let Some(unit) = metadata.unit {
                builder = builder.with_unit(unit.as_canonical_label());
            }
            builder = builder.with_description(metadata.description.to_string());
        }

        Gauge::from_arc(Arc::new(OtelGauge {
            instrument: builder.build(),
            attributes: Self::attributes(key),
            value: AtomicU64::new(0_f64.to_bits()),
        }))
    }

    fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        let mut builder = self.meter.f64_histogram(key.name().to_owned());
        if let Some(metadata) = self.metadata().get(key.name()).cloned() {
            if let Some(unit) = metadata.unit {
                builder = builder.with_unit(unit.as_canonical_label());
            }
            builder = builder.with_description(metadata.description.to_string());
        }

        Histogram::from_arc(Arc::new(OtelHistogram {
            instrument: builder.build(),
            attributes: Self::attributes(key),
        }))
    }
}

#[derive(Debug)]
struct OtelCounter {
    instrument: opentelemetry::metrics::Counter<u64>,
    attributes: Vec<KeyValue>,
    value: AtomicU64,
}

impl CounterFn for OtelCounter {
    fn increment(&self, value: u64) {
        self.value.fetch_add(value, Ordering::Relaxed);
        self.instrument.add(value, &self.attributes);
    }

    fn absolute(&self, value: u64) {
        let mut previous = self.value.load(Ordering::Relaxed);
        while value > previous {
            match self.value.compare_exchange_weak(
                previous,
                value,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.instrument.add(value - previous, &self.attributes);
                    return;
                }
                Err(observed) => previous = observed,
            }
        }
    }
}

#[derive(Debug)]
struct OtelGauge {
    instrument: opentelemetry::metrics::Gauge<f64>,
    attributes: Vec<KeyValue>,
    value: AtomicU64,
}

impl OtelGauge {
    fn update(&self, update: impl Fn(f64) -> f64) {
        let mut current = self.value.load(Ordering::Relaxed);
        loop {
            let next = update(f64::from_bits(current));
            match self.value.compare_exchange_weak(
                current,
                next.to_bits(),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.instrument.record(next, &self.attributes);
                    return;
                }
                Err(observed) => current = observed,
            }
        }
    }
}

impl GaugeFn for OtelGauge {
    fn increment(&self, value: f64) {
        self.update(|current| current + value);
    }

    fn decrement(&self, value: f64) {
        self.update(|current| current - value);
    }

    fn set(&self, value: f64) {
        self.value.store(value.to_bits(), Ordering::Relaxed);
        self.instrument.record(value, &self.attributes);
    }
}

#[derive(Debug)]
struct OtelHistogram {
    instrument: opentelemetry::metrics::Histogram<f64>,
    attributes: Vec<KeyValue>,
}

impl HistogramFn for OtelHistogram {
    fn record(&self, value: f64) {
        self.instrument.record(value, &self.attributes);
    }
}
