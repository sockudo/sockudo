use super::types::*;
use crate::annotation_summarizers::{self as engines, AnnotationSummarizerEngine};
use crate::error::Result;
use std::collections::HashSet;

// Reuse the canonical engines. Snapshot cost follows the public summary size;
// applying an event does not replay the retained event log.
enum Engine {
    Total(engines::total::TotalSummarizer),
    Flag(engines::flag::FlagSummarizer),
    Distinct(engines::distinct::DistinctSummarizer),
    Unique(engines::unique::UniqueSummarizer),
    Multiple(engines::multiple::MultipleSummarizer),
}
macro_rules! with_engine {
    ($this:expr, $engine:ident, $body:expr) => {
        match $this {
            Engine::Total($engine) => $body,
            Engine::Flag($engine) => $body,
            Engine::Distinct($engine) => $body,
            Engine::Unique($engine) => $body,
            Engine::Multiple($engine) => $body,
        }
    };
}
impl Engine {
    fn new(kind: AnnotationSummarizer) -> Self {
        let options = AnnotationProjectionOptions::default();
        match kind {
            AnnotationSummarizer::Total => Self::Total(Default::default()),
            AnnotationSummarizer::Flag => Self::Flag(engines::flag::FlagSummarizer::new(options)),
            AnnotationSummarizer::Distinct => {
                Self::Distinct(engines::distinct::DistinctSummarizer::new(options))
            }
            AnnotationSummarizer::Unique => {
                Self::Unique(engines::unique::UniqueSummarizer::new(options))
            }
            AnnotationSummarizer::Multiple => {
                Self::Multiple(engines::multiple::MultipleSummarizer::new(options))
            }
        }
    }
    fn apply(&mut self, event: &Annotation) -> Result<()> {
        with_engine!(self, engine, engine.apply(event))
    }
    fn summary(&self) -> AnnotationSummary {
        with_engine!(self, engine, Box::new(engine.clone()).finish())
    }
}

pub struct IncrementalProjection {
    engine: Engine,
    create_ids: HashSet<AnnotationId>,
    seen_create_ids: HashSet<AnnotationId>,
    deleted_create_ids: HashSet<AnnotationId>,
    prior_deletes: HashSet<AnnotationId>,
    pub projection: StoredAnnotationProjection,
}

impl IncrementalProjection {
    pub fn rebuild<'a>(
        request: &AnnotationProjectionRequest,
        events: impl Iterator<Item = &'a StoredAnnotationEvent> + Clone,
    ) -> Result<Self> {
        let engine = Engine::new(request.annotation_type.summarizer()?);
        let mut this = Self {
            projection: StoredAnnotationProjection {
                app_id: request.app_id.clone(),
                channel_id: request.channel_id.clone(),
                message_serial: request.message_serial.clone(),
                annotation_type: request.annotation_type.clone(),
                summary: engine.summary(),
                last_annotation_serial: None,
                updated_at_ms: crate::history::now_ms(),
            },
            engine,
            create_ids: events
                .clone()
                .filter(|record| record.annotation.action == AnnotationAction::Create)
                .map(|record| record.annotation.id.clone())
                .collect(),
            seen_create_ids: HashSet::new(),
            deleted_create_ids: HashSet::new(),
            prior_deletes: HashSet::new(),
        };
        for record in events {
            this.apply_known(&record.annotation)?;
        }
        this.projection.summary = this.engine.summary();
        Ok(this)
    }

    pub fn append(&mut self, event: &Annotation) -> Result<bool> {
        if self
            .projection
            .last_annotation_serial
            .as_ref()
            .is_some_and(|serial| event.serial <= *serial)
            || (event.action == AnnotationAction::Create && self.prior_deletes.contains(&event.id))
        {
            // A later create can retroactively change unmatched-delete semantics;
            // arbitrary imports therefore require the canonical rebuild oracle.
            return Ok(false);
        }
        if event.action == AnnotationAction::Create {
            self.create_ids.insert(event.id.clone());
        }
        self.apply_known(event)?;
        self.projection.summary = self.engine.summary();
        self.projection.updated_at_ms = crate::history::now_ms();
        Ok(true)
    }

    fn apply_known(&mut self, event: &Annotation) -> Result<()> {
        self.projection.last_annotation_serial = Some(event.serial.clone());
        match event.action {
            AnnotationAction::Create => {
                if self.deleted_create_ids.contains(&event.id) {
                    return Ok(());
                }
                self.seen_create_ids.insert(event.id.clone());
            }
            AnnotationAction::Delete => {
                self.prior_deletes.insert(event.id.clone());
                if self.create_ids.contains(&event.id) {
                    self.deleted_create_ids.insert(event.id.clone());
                    if !self.seen_create_ids.contains(&event.id) {
                        return Ok(());
                    }
                }
            }
        }
        self.engine.apply(event)
    }
}
