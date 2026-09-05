use metrics::{Key, Label, Level, Metadata};
use std::sync::Arc;
static METADATA: Metadata<'static> =
    Metadata::new(module_path!(), Level::INFO, Some(module_path!()));
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

pub(super) trait MetricLabelValues {
    fn values(&self) -> impl ExactSizeIterator<Item = &str>;
}

impl<T: AsRef<str>> MetricLabelValues for &[T] {
    fn values(&self) -> impl ExactSizeIterator<Item = &str> {
        self.iter().map(AsRef::as_ref)
    }
}
impl<T: AsRef<str>, const N: usize> MetricLabelValues for &[T; N] {
    fn values(&self) -> impl ExactSizeIterator<Item = &str> {
        self.iter().map(AsRef::as_ref)
    }
}
impl<T: AsRef<str>> MetricLabelValues for &Vec<T> {
    fn values(&self) -> impl ExactSizeIterator<Item = &str> {
        self.iter().map(AsRef::as_ref)
    }
}

type CachedHandle<T> = (Box<[Box<str>]>, T);
/// Cache only a bounded number of combinations. Overflow still records normally;
/// hash collisions are compared against all labels and never merge distinct series.
pub(super) struct HandleCache<T> {
    entries: RwLock<HashMap<u64, CachedHandle<T>>>,
}
impl<T> Default for HandleCache<T> {
    fn default() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
        }
    }
}
impl<T: Clone> HandleCache<T> {
    fn get(
        &self,
        values: impl MetricLabelValues,
        names: &'static [&'static str],
        register: impl FnOnce(Vec<Label>) -> T,
    ) -> T {
        debug_assert_eq!(values.values().len(), names.len());
        let mut hasher = DefaultHasher::new();
        for value in values.values() {
            value.hash(&mut hasher);
        }
        let hash = hasher.finish();
        {
            let entries = self.entries.read().unwrap_or_else(|e| e.into_inner());
            if let Some((labels, handle)) = entries.get(&hash)
                && labels.iter().map(AsRef::as_ref).eq(values.values())
            {
                return handle.clone();
            }
        }
        let mut entries = self.entries.write().unwrap_or_else(|e| e.into_inner());
        if let Some((labels, handle)) = entries.get(&hash)
            && labels.iter().map(AsRef::as_ref).eq(values.values())
        {
            return handle.clone();
        }
        let labels = names
            .iter()
            .zip(values.values())
            .map(|(name, value)| Label::new(*name, value.to_owned()))
            .collect();
        let handle = register(labels);
        if entries.len() < 1024 && !entries.contains_key(&hash) {
            entries.insert(
                hash,
                (values.values().map(Box::from).collect(), handle.clone()),
            );
        }
        handle
    }
}

pub(super) struct Gauge {
    pub(super) key: Arc<Key>,
    pub(super) value: AtomicU64,
}
impl Gauge {
    pub(super) fn set(&self, value: f64) {
        self.value.store(value.to_bits(), Ordering::Relaxed);
        metrics::with_recorder(|recorder| recorder.register_gauge(&self.key, &METADATA).set(value));
    }
    pub(super) fn get(&self) -> f64 {
        f64::from_bits(self.value.load(Ordering::Relaxed))
    }
}

pub(super) struct GaugeVec {
    pub(super) name: String,
    pub(super) label_names: &'static [&'static str],
    pub(super) handles: HandleCache<Arc<Key>>,
}
impl GaugeVec {
    pub(super) fn with_label_values(&self, values: impl MetricLabelValues) -> GaugeWithLabels {
        GaugeWithLabels(self.handles.get(values, self.label_names, |labels| {
            Arc::new(Key::from_parts(self.name.clone(), labels))
        }))
    }
    pub(super) fn reset(&self) {}
}
pub(super) struct GaugeWithLabels(Arc<Key>);
impl GaugeWithLabels {
    pub(super) fn add(&self, value: f64) {
        if value.is_sign_negative() {
            metrics::with_recorder(|r| r.register_gauge(&self.0, &METADATA).decrement(value.abs()));
        } else {
            metrics::with_recorder(|r| r.register_gauge(&self.0, &METADATA).increment(value));
        }
    }
    pub(super) fn inc(&self) {
        metrics::with_recorder(|r| r.register_gauge(&self.0, &METADATA).increment(1.0));
    }
    pub(super) fn dec(&self) {
        metrics::with_recorder(|r| r.register_gauge(&self.0, &METADATA).decrement(1.0));
    }
    pub(super) fn set(&self, value: f64) {
        metrics::with_recorder(|r| r.register_gauge(&self.0, &METADATA).set(value));
    }
}

pub(super) struct CounterVec {
    pub(super) name: String,
    pub(super) label_names: &'static [&'static str],
    pub(super) handles: HandleCache<Arc<Key>>,
}
impl CounterVec {
    pub(super) fn with_label_values(&self, values: impl MetricLabelValues) -> CounterWithLabels {
        CounterWithLabels(self.handles.get(values, self.label_names, |labels| {
            Arc::new(Key::from_parts(self.name.clone(), labels))
        }))
    }
}
pub(super) struct CounterWithLabels(Arc<Key>);
impl CounterWithLabels {
    pub(super) fn inc(&self) {
        metrics::with_recorder(|r| r.register_counter(&self.0, &METADATA).increment(1));
    }
    pub(super) fn inc_by(&self, value: f64) {
        if value.is_sign_positive() {
            metrics::with_recorder(|r| {
                r.register_counter(&self.0, &METADATA)
                    .increment(value as u64)
            });
        }
    }
}

pub(super) struct HistogramVec {
    pub(super) name: String,
    pub(super) label_names: &'static [&'static str],
    pub(super) handles: HandleCache<Arc<Key>>,
}
impl HistogramVec {
    pub(super) fn with_label_values(&self, values: impl MetricLabelValues) -> HistogramWithLabels {
        HistogramWithLabels(self.handles.get(values, self.label_names, |labels| {
            Arc::new(Key::from_parts(self.name.clone(), labels))
        }))
    }
}
pub(super) struct HistogramWithLabels(Arc<Key>);
impl HistogramWithLabels {
    pub(super) fn observe(&self, value: f64) {
        metrics::with_recorder(|r| r.register_histogram(&self.0, &METADATA).record(value));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cached_keys_follow_the_active_recorder_and_preserve_overflow_labels() {
        let counter = CounterVec {
            name: "services_scope_total".into(),
            label_names: &["app_id"],
            handles: Default::default(),
        };
        let first = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let second = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        // A warm call before a recorder exists must not freeze a noop handle.
        counter.with_label_values(&["same"]).inc();
        metrics::with_local_recorder(&first, || counter.with_label_values(&["same"]).inc());
        metrics::with_local_recorder(&second, || {
            counter.with_label_values(&["same"]).inc();
            for n in 0..2048 {
                counter.with_label_values(&[format!("app-{n}")]).inc();
            }
        });
        assert!(
            first
                .handle()
                .render()
                .contains("services_scope_total{app_id=\"same\"} 1")
        );
        let rendered = second.handle().render();
        assert!(rendered.contains("services_scope_total{app_id=\"same\"} 1"));
        assert_eq!(
            rendered
                .lines()
                .filter(|line| line.starts_with("services_scope_total{"))
                .count(),
            2049
        );
        assert_eq!(counter.handles.entries.read().unwrap().len(), 1024);
    }
}
