use sonic_rs::prelude::*;
use sonic_rs::{Value, json};

pub(super) fn prometheus_text_to_json(text: &str) -> Value {
    let mut raw = json!({});

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let Some((metric, value)) = line.rsplit_once(' ') else {
            continue;
        };
        let Ok(value) = value.parse::<f64>() else {
            continue;
        };

        let (name, labels) = parse_metric_and_labels(metric);
        if labels.as_object().is_some_and(|labels| labels.is_empty()) {
            raw[name] = json!(value);
        } else {
            if !raw.as_object().unwrap().contains_key(&name) {
                raw[name] = json!([]);
            }
            let mut metric_with_labels = labels;
            metric_with_labels["value"] = json!(value);
            raw[name].as_array_mut().unwrap().push(metric_with_labels);
        }
    }

    raw
}

fn parse_metric_and_labels(metric: &str) -> (&str, Value) {
    let Some(label_start) = metric.find('{') else {
        return (metric, json!({}));
    };
    let Some(label_end) = metric.rfind('}') else {
        return (metric, json!({}));
    };

    let name = &metric[..label_start];
    let labels = &metric[label_start + 1..label_end];
    let mut label_json = json!({});

    for label in labels.split(',') {
        let Some((key, value)) = label.split_once("=\"") else {
            continue;
        };
        let value = value.strip_suffix('"').unwrap_or(value);
        label_json[key] = json!(value);
    }

    (name, label_json)
}
