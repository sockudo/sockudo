use crate::annotations::{
    Annotation, AnnotationAction, AnnotationProjectionOptions, AnnotationSummarizer,
    AnnotationSummary, IdentifiedAnnotationSummary, MultipleAnnotationSummary,
    TotalAnnotationSummary,
};
use crate::error::{Error, Result};
use std::collections::{BTreeMap, BTreeSet};

pub trait AnnotationSummarizerEngine {
    fn apply(&mut self, event: &Annotation) -> Result<()>;
    fn finish(self: Box<Self>) -> AnnotationSummary;
}

pub fn new_engine(
    summarizer: AnnotationSummarizer,
    options: AnnotationProjectionOptions,
) -> Box<dyn AnnotationSummarizerEngine> {
    match summarizer {
        AnnotationSummarizer::Total => Box::new(total::TotalSummarizer::default()),
        AnnotationSummarizer::Flag => Box::new(flag::FlagSummarizer::new(options)),
        AnnotationSummarizer::Distinct => Box::new(distinct::DistinctSummarizer::new(options)),
        AnnotationSummarizer::Unique => Box::new(unique::UniqueSummarizer::new(options)),
        AnnotationSummarizer::Multiple => Box::new(multiple::MultipleSummarizer::new(options)),
    }
}

pub mod total {
    use super::*;

    #[derive(Default)]
    pub struct TotalSummarizer {
        total: u64,
    }

    impl AnnotationSummarizerEngine for TotalSummarizer {
        fn apply(&mut self, event: &Annotation) -> Result<()> {
            self.total = match event.action {
                AnnotationAction::Create => self.total.saturating_add(1),
                AnnotationAction::Delete => self.total.saturating_sub(1),
            };
            Ok(())
        }

        fn finish(self: Box<Self>) -> AnnotationSummary {
            AnnotationSummary::Total(TotalAnnotationSummary { total: self.total })
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::annotation_summarizers::tests::{event, summary_total};

        #[test]
        fn repeated_creates_increment_and_delete_decrements() {
            let mut summarizer = TotalSummarizer::default();
            summarizer
                .apply(&event(
                    "ann:1",
                    "reaction:total.v1",
                    AnnotationAction::Create,
                    None,
                    None,
                    None,
                ))
                .unwrap();
            summarizer
                .apply(&event(
                    "ann:2",
                    "reaction:total.v1",
                    AnnotationAction::Create,
                    None,
                    Some("client-1"),
                    None,
                ))
                .unwrap();
            summarizer
                .apply(&event(
                    "ann:3",
                    "reaction:total.v1",
                    AnnotationAction::Delete,
                    None,
                    None,
                    None,
                ))
                .unwrap();

            assert_eq!(summary_total(Box::new(summarizer).finish()), 1);
        }
    }
}

pub mod flag {
    use super::*;

    pub struct FlagSummarizer {
        clients: BTreeSet<String>,
        options: AnnotationProjectionOptions,
    }

    impl FlagSummarizer {
        pub fn new(options: AnnotationProjectionOptions) -> Self {
            Self {
                clients: BTreeSet::new(),
                options,
            }
        }
    }

    impl AnnotationSummarizerEngine for FlagSummarizer {
        fn apply(&mut self, event: &Annotation) -> Result<()> {
            let client_id = required_client_id(event)?;
            match event.action {
                AnnotationAction::Create => {
                    self.clients.insert(client_id.to_string());
                }
                AnnotationAction::Delete => {
                    self.clients.remove(client_id);
                }
            }
            Ok(())
        }

        fn finish(self: Box<Self>) -> AnnotationSummary {
            AnnotationSummary::Flag(identified_summary(&self.clients, self.options))
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::annotation_summarizers::tests::{event, summary_flag};

        #[test]
        fn tracks_distinct_identified_clients() {
            let mut summarizer = FlagSummarizer::new(Default::default());
            for annotation in [
                event(
                    "ann:1",
                    "reaction:flag.v1",
                    AnnotationAction::Create,
                    None,
                    Some("client-1"),
                    None,
                ),
                event(
                    "ann:2",
                    "reaction:flag.v1",
                    AnnotationAction::Create,
                    None,
                    Some("client-1"),
                    None,
                ),
                event(
                    "ann:3",
                    "reaction:flag.v1",
                    AnnotationAction::Create,
                    None,
                    Some("client-2"),
                    None,
                ),
            ] {
                summarizer.apply(&annotation).unwrap();
            }

            let summary = summary_flag(Box::new(summarizer).finish());
            assert_eq!(summary.total, 2);
            assert_eq!(summary.client_ids, vec!["client-1", "client-2"]);
        }
    }
}

pub mod distinct {
    use super::*;

    pub struct DistinctSummarizer {
        names: BTreeMap<String, BTreeSet<String>>,
        options: AnnotationProjectionOptions,
    }

    impl DistinctSummarizer {
        pub fn new(options: AnnotationProjectionOptions) -> Self {
            Self {
                names: BTreeMap::new(),
                options,
            }
        }
    }

    impl AnnotationSummarizerEngine for DistinctSummarizer {
        fn apply(&mut self, event: &Annotation) -> Result<()> {
            let name = required_name(event)?;
            let client_id = required_client_id(event)?;
            match event.action {
                AnnotationAction::Create => {
                    self.names
                        .entry(name.to_string())
                        .or_default()
                        .insert(client_id.to_string());
                }
                AnnotationAction::Delete => {
                    remove_client_from_name(&mut self.names, name, client_id);
                }
            }
            Ok(())
        }

        fn finish(self: Box<Self>) -> AnnotationSummary {
            AnnotationSummary::Distinct(
                self.names
                    .into_iter()
                    .map(|(name, clients)| (name, identified_summary(&clients, self.options)))
                    .collect(),
            )
        }
    }
}

pub mod unique {
    use super::*;

    pub struct UniqueSummarizer {
        names: BTreeMap<String, BTreeSet<String>>,
        client_names: BTreeMap<String, String>,
        options: AnnotationProjectionOptions,
    }

    impl UniqueSummarizer {
        pub fn new(options: AnnotationProjectionOptions) -> Self {
            Self {
                names: BTreeMap::new(),
                client_names: BTreeMap::new(),
                options,
            }
        }
    }

    impl AnnotationSummarizerEngine for UniqueSummarizer {
        fn apply(&mut self, event: &Annotation) -> Result<()> {
            let name = required_name(event)?;
            let client_id = required_client_id(event)?;
            match event.action {
                AnnotationAction::Create => {
                    if let Some(previous_name) = self
                        .client_names
                        .insert(client_id.to_string(), name.to_string())
                        && previous_name != name
                    {
                        remove_client_from_name(&mut self.names, &previous_name, client_id);
                    }
                    self.names
                        .entry(name.to_string())
                        .or_default()
                        .insert(client_id.to_string());
                }
                AnnotationAction::Delete => {
                    if let Some(active_name) = self.client_names.remove(client_id) {
                        remove_client_from_name(&mut self.names, &active_name, client_id);
                    }
                }
            }
            Ok(())
        }

        fn finish(self: Box<Self>) -> AnnotationSummary {
            AnnotationSummary::Unique(
                self.names
                    .into_iter()
                    .map(|(name, clients)| (name, identified_summary(&clients, self.options)))
                    .collect(),
            )
        }
    }
}

pub mod multiple {
    use super::*;

    #[derive(Default)]
    struct MultipleBucketState {
        client_counts: BTreeMap<String, u64>,
        total_unidentified: u64,
    }

    pub struct MultipleSummarizer {
        names: BTreeMap<String, MultipleBucketState>,
        options: AnnotationProjectionOptions,
    }

    impl MultipleSummarizer {
        pub fn new(options: AnnotationProjectionOptions) -> Self {
            Self {
                names: BTreeMap::new(),
                options,
            }
        }
    }

    impl AnnotationSummarizerEngine for MultipleSummarizer {
        fn apply(&mut self, event: &Annotation) -> Result<()> {
            let name = required_name(event)?;
            let bucket = self.names.entry(name.to_string()).or_default();
            match event.action {
                AnnotationAction::Create => {
                    let count = event.effective_count();
                    if let Some(client_id) = event.client_id.as_deref() {
                        validate_required_text("annotation client_id", Some(client_id))?;
                        let current = bucket
                            .client_counts
                            .entry(client_id.to_string())
                            .or_default();
                        *current = current.saturating_add(count);
                    } else {
                        bucket.total_unidentified = bucket.total_unidentified.saturating_add(count);
                    }
                }
                AnnotationAction::Delete => {
                    if let Some(client_id) = event.client_id.as_deref() {
                        bucket.client_counts.remove(client_id);
                    }
                }
            }
            Ok(())
        }

        fn finish(self: Box<Self>) -> AnnotationSummary {
            AnnotationSummary::Multiple(
                self.names
                    .into_iter()
                    .map(|(name, bucket)| (name, multiple_summary(bucket, self.options)))
                    .collect(),
            )
        }
    }

    fn multiple_summary(
        bucket: MultipleBucketState,
        options: AnnotationProjectionOptions,
    ) -> MultipleAnnotationSummary {
        let total_client_ids = bucket.client_counts.len() as u64;
        let total_identified = bucket.client_counts.values().copied().sum::<u64>();
        let clipped = options
            .client_id_limit
            .is_some_and(|limit| bucket.client_counts.len() > limit);
        let client_counts = match options.client_id_limit {
            Some(limit) => bucket.client_counts.into_iter().take(limit).collect(),
            None => bucket.client_counts,
        };

        MultipleAnnotationSummary {
            total: total_identified.saturating_add(bucket.total_unidentified),
            client_counts,
            total_unidentified: bucket.total_unidentified,
            clipped,
            total_client_ids,
        }
    }
}

fn required_name(event: &Annotation) -> Result<&str> {
    let name = event.name.as_deref();
    validate_required_text("annotation name", name)?;
    Ok(name.expect("validated annotation name must exist"))
}

fn required_client_id(event: &Annotation) -> Result<&str> {
    let client_id = event.client_id.as_deref();
    validate_required_text("annotation client_id", client_id)?;
    Ok(client_id.expect("validated annotation client_id must exist"))
}

fn validate_required_text(label: &str, value: Option<&str>) -> Result<()> {
    match value {
        Some(value) if !value.trim().is_empty() => Ok(()),
        _ => Err(Error::InvalidMessageFormat(format!(
            "{label} is required for this annotation type"
        ))),
    }
}

fn remove_client_from_name(
    names: &mut BTreeMap<String, BTreeSet<String>>,
    name: &str,
    client_id: &str,
) {
    if let Some(clients) = names.get_mut(name) {
        clients.remove(client_id);
        if clients.is_empty() {
            names.remove(name);
        }
    }
}

fn identified_summary(
    clients: &BTreeSet<String>,
    options: AnnotationProjectionOptions,
) -> IdentifiedAnnotationSummary {
    let total = clients.len() as u64;
    let clipped = options
        .client_id_limit
        .is_some_and(|limit| clients.len() > limit);
    let client_ids = match options.client_id_limit {
        Some(limit) => clients.iter().take(limit).cloned().collect(),
        None => clients.iter().cloned().collect(),
    };

    IdentifiedAnnotationSummary {
        total,
        client_ids,
        clipped,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::annotations::{AnnotationId, AnnotationSerial, AnnotationType};
    use crate::versioned_messages::MessageSerial;
    use sonic_rs::json;

    pub fn event(
        serial_value: &str,
        annotation_type: &str,
        action: AnnotationAction,
        name: Option<&str>,
        client_id: Option<&str>,
        count: Option<u64>,
    ) -> Annotation {
        Annotation {
            id: AnnotationId::new(format!("ann:{serial_value}")).unwrap(),
            action,
            serial: AnnotationSerial::new(serial_value).unwrap(),
            message_serial: MessageSerial::new("msg:1").unwrap(),
            annotation_type: AnnotationType::new(annotation_type).unwrap(),
            name: name.map(str::to_string),
            client_id: client_id.map(str::to_string),
            count,
            data: Some(json!({"raw": serial_value})),
            encoding: None,
            timestamp: 1,
        }
    }

    pub fn summary_total(summary: AnnotationSummary) -> u64 {
        let AnnotationSummary::Total(summary) = summary else {
            panic!("expected total summary");
        };
        summary.total
    }

    pub fn summary_flag(summary: AnnotationSummary) -> IdentifiedAnnotationSummary {
        let AnnotationSummary::Flag(summary) = summary else {
            panic!("expected flag summary");
        };
        summary
    }

    #[test]
    fn distinct_allows_same_client_in_multiple_names() {
        let mut summarizer = distinct::DistinctSummarizer::new(Default::default());
        for annotation in [
            event(
                "ann:1",
                "reaction:distinct.v1",
                AnnotationAction::Create,
                Some("like"),
                Some("client-1"),
                None,
            ),
            event(
                "ann:2",
                "reaction:distinct.v1",
                AnnotationAction::Create,
                Some("laugh"),
                Some("client-1"),
                None,
            ),
        ] {
            summarizer.apply(&annotation).unwrap();
        }

        let AnnotationSummary::Distinct(names) = Box::new(summarizer).finish() else {
            panic!("expected distinct summary");
        };
        assert_eq!(names["like"].total, 1);
        assert_eq!(names["laugh"].total, 1);
    }

    #[test]
    fn unique_moves_client_between_names() {
        let mut summarizer = unique::UniqueSummarizer::new(Default::default());
        for annotation in [
            event(
                "ann:1",
                "reaction:unique.v1",
                AnnotationAction::Create,
                Some("like"),
                Some("client-1"),
                None,
            ),
            event(
                "ann:2",
                "reaction:unique.v1",
                AnnotationAction::Create,
                Some("laugh"),
                Some("client-1"),
                None,
            ),
        ] {
            summarizer.apply(&annotation).unwrap();
        }

        let AnnotationSummary::Unique(names) = Box::new(summarizer).finish() else {
            panic!("expected unique summary");
        };
        assert!(!names.contains_key("like"));
        assert_eq!(names["laugh"].client_ids, vec!["client-1"]);
    }

    #[test]
    fn multiple_tracks_identified_and_unidentified_counts() {
        let mut summarizer = multiple::MultipleSummarizer::new(Default::default());
        for annotation in [
            event(
                "ann:1",
                "reaction:multiple.v1",
                AnnotationAction::Create,
                Some("vote"),
                Some("client-1"),
                Some(2),
            ),
            event(
                "ann:2",
                "reaction:multiple.v1",
                AnnotationAction::Create,
                Some("vote"),
                None,
                Some(4),
            ),
        ] {
            summarizer.apply(&annotation).unwrap();
        }

        let AnnotationSummary::Multiple(names) = Box::new(summarizer).finish() else {
            panic!("expected multiple summary");
        };
        assert_eq!(names["vote"].total, 6);
        assert_eq!(names["vote"].total_unidentified, 4);
        assert_eq!(names["vote"].client_counts["client-1"], 2);
    }
}
