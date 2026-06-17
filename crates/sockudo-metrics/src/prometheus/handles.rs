use metrics::{counter, gauge, histogram};
use std::sync::atomic::{AtomicU64, Ordering};

pub(super) trait MetricLabelValues {
    fn to_pairs(self, label_names: &[&str]) -> Vec<(String, String)>;
}

fn labels_from_iter<'a>(
    label_names: &[&str],
    values: impl IntoIterator<Item = &'a str>,
) -> Vec<(String, String)> {
    let values = values.into_iter().collect::<Vec<_>>();
    debug_assert_eq!(
        label_names.len(),
        values.len(),
        "metric label/value count mismatch"
    );

    label_names
        .iter()
        .zip(values)
        .map(|(name, value)| ((*name).to_owned(), value.to_owned()))
        .collect()
}

impl<'a> MetricLabelValues for &'a [&'a str] {
    fn to_pairs(self, label_names: &[&str]) -> Vec<(String, String)> {
        labels_from_iter(label_names, self.iter().copied())
    }
}

impl<'a, const N: usize> MetricLabelValues for &'a [&'a str; N] {
    fn to_pairs(self, label_names: &[&str]) -> Vec<(String, String)> {
        labels_from_iter(label_names, self.iter().copied())
    }
}

impl<'a, const N: usize> MetricLabelValues for &'a [&'a String; N] {
    fn to_pairs(self, label_names: &[&str]) -> Vec<(String, String)> {
        labels_from_iter(label_names, self.iter().map(|value| value.as_str()))
    }
}

impl<'a> MetricLabelValues for &'a [&'a String] {
    fn to_pairs(self, label_names: &[&str]) -> Vec<(String, String)> {
        labels_from_iter(label_names, self.iter().map(|value| value.as_str()))
    }
}

impl MetricLabelValues for &Vec<String> {
    fn to_pairs(self, label_names: &[&str]) -> Vec<(String, String)> {
        labels_from_iter(label_names, self.iter().map(String::as_str))
    }
}

pub(super) struct Gauge {
    pub(super) name: String,
    pub(super) value: AtomicU64,
}

impl Gauge {
    pub(super) fn set(&self, value: f64) {
        self.value.store(value.to_bits(), Ordering::Relaxed);
        gauge!(self.name.clone()).set(value);
    }

    pub(super) fn get(&self) -> f64 {
        f64::from_bits(self.value.load(Ordering::Relaxed))
    }
}

pub(super) struct GaugeVec {
    pub(super) name: String,
    pub(super) label_names: &'static [&'static str],
}

impl GaugeVec {
    pub(super) fn with_label_values(&self, values: impl MetricLabelValues) -> GaugeWithLabels {
        GaugeWithLabels {
            name: self.name.clone(),
            labels: values.to_pairs(self.label_names),
        }
    }

    pub(super) fn reset(&self) {}
}

pub(super) struct GaugeWithLabels {
    pub(super) name: String,
    pub(super) labels: Vec<(String, String)>,
}

impl GaugeWithLabels {
    pub(super) fn inc(&self) {
        gauge!(self.name.clone(), &self.labels).increment(1.0);
    }

    pub(super) fn dec(&self) {
        gauge!(self.name.clone(), &self.labels).decrement(1.0);
    }

    pub(super) fn set(&self, value: f64) {
        gauge!(self.name.clone(), &self.labels).set(value);
    }
}

pub(super) struct CounterVec {
    pub(super) name: String,
    pub(super) label_names: &'static [&'static str],
}

impl CounterVec {
    pub(super) fn with_label_values(&self, values: impl MetricLabelValues) -> CounterWithLabels {
        CounterWithLabels {
            name: self.name.clone(),
            labels: values.to_pairs(self.label_names),
        }
    }
}

pub(super) struct CounterWithLabels {
    pub(super) name: String,
    pub(super) labels: Vec<(String, String)>,
}

impl CounterWithLabels {
    pub(super) fn inc(&self) {
        counter!(self.name.clone(), &self.labels).increment(1);
    }

    pub(super) fn inc_by(&self, value: f64) {
        if value.is_sign_positive() {
            counter!(self.name.clone(), &self.labels).increment(value as u64);
        }
    }
}

pub(super) struct HistogramVec {
    pub(super) name: String,
    pub(super) label_names: &'static [&'static str],
}

impl HistogramVec {
    pub(super) fn with_label_values(&self, values: impl MetricLabelValues) -> HistogramWithLabels {
        HistogramWithLabels {
            name: self.name.clone(),
            labels: values.to_pairs(self.label_names),
        }
    }
}

pub(super) struct HistogramWithLabels {
    pub(super) name: String,
    pub(super) labels: Vec<(String, String)>,
}

impl HistogramWithLabels {
    pub(super) fn observe(&self, value: f64) {
        histogram!(self.name.clone(), &self.labels).record(value);
    }
}
