use sockudo_core::error::{Error, Result};
use sockudo_core::queue::{QueueBackendKind, QueueJobId, QueueJobRequest};
use sockudo_core::webhook_types::JobData;

#[derive(Debug)]
pub(crate) struct PreparedBrokerBatch {
    pub(crate) data: Vec<JobData>,
    pub(crate) ids: Vec<QueueJobId>,
}

pub(crate) fn prepare_default_batch(
    backend: QueueBackendKind,
    jobs: Vec<QueueJobRequest>,
) -> Result<PreparedBrokerBatch> {
    let mut data = Vec::with_capacity(jobs.len());
    let mut ids = Vec::with_capacity(jobs.len());

    for job in jobs {
        if job.options.delay_ms > 0
            || job.options.deduplication_key.is_some()
            || job.options.max_attempts.is_some()
        {
            return Err(Error::Queue(format!(
                "queue backend {} does not implement Queue v2 job options",
                backend.as_str()
            )));
        }
        ids.push(QueueJobId(
            job.options
                .job_id
                .unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string()),
        ));
        data.push(job.data);
    }

    Ok(PreparedBrokerBatch { data, ids })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sockudo_core::queue::QueueJobOptions;

    #[test]
    fn preserves_ids_for_supported_default_jobs() {
        let prepared = prepare_default_batch(
            QueueBackendKind::Kafka,
            vec![QueueJobRequest {
                data: JobData::default(),
                options: QueueJobOptions {
                    job_id: Some("stable-id".to_string()),
                    ..QueueJobOptions::default()
                },
            }],
        )
        .expect("default batch should be supported");

        assert_eq!(prepared.ids, vec![QueueJobId("stable-id".to_string())]);
        assert_eq!(prepared.data, vec![JobData::default()]);
    }

    #[test]
    fn rejects_options_the_backend_cannot_honor() {
        let error = prepare_default_batch(
            QueueBackendKind::Nats,
            vec![QueueJobRequest {
                data: JobData::default(),
                options: QueueJobOptions {
                    delay_ms: 1,
                    ..QueueJobOptions::default()
                },
            }],
        )
        .expect_err("unsupported delay must not be silently discarded");

        assert!(error.to_string().contains("does not implement Queue v2"));
    }
}
