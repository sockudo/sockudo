use sockudo_core::error::{Error, Result};
use sockudo_core::queue::{QueueBackendKind, QueueJobId, QueueJobRequest};
use sockudo_core::webhook_types::JobData;

#[derive(Debug)]
pub(crate) struct PreparedBrokerBatch {
    pub(crate) data: Vec<JobData>,
    pub(crate) ids: Vec<QueueJobId>,
    /// Per-job `delay_ms`, parallel to `data`. All zero unless the backend accepts delays.
    #[cfg_attr(not(feature = "pulsar"), allow(dead_code))]
    pub(crate) delays_ms: Vec<u64>,
}

#[cfg_attr(
    not(any(
        feature = "google-pubsub",
        feature = "iggy",
        feature = "kafka",
        feature = "nats",
        feature = "rabbitmq",
        feature = "sns"
    )),
    allow(dead_code)
)]
pub(crate) fn prepare_default_batch(
    backend: QueueBackendKind,
    jobs: Vec<QueueJobRequest>,
) -> Result<PreparedBrokerBatch> {
    prepare_batch(backend, jobs, false)
}

/// Like [`prepare_default_batch`], but keeps `delay_ms` for backends that deliver
/// delayed messages natively.
#[cfg(feature = "pulsar")]
pub(crate) fn prepare_delayed_batch(
    backend: QueueBackendKind,
    jobs: Vec<QueueJobRequest>,
) -> Result<PreparedBrokerBatch> {
    prepare_batch(backend, jobs, true)
}

fn prepare_batch(
    backend: QueueBackendKind,
    jobs: Vec<QueueJobRequest>,
    delayed_delivery: bool,
) -> Result<PreparedBrokerBatch> {
    let mut data = Vec::with_capacity(jobs.len());
    let mut ids = Vec::with_capacity(jobs.len());
    let mut delays_ms = Vec::with_capacity(jobs.len());

    for mut job in jobs {
        if (job.options.delay_ms > 0 && !delayed_delivery)
            || job.options.deduplication_key.is_some()
            || job.options.max_attempts.is_some()
        {
            return Err(Error::Queue(format!(
                "queue backend {} does not implement Queue v2 job options",
                backend.as_str()
            )));
        }
        let id = QueueJobId(
            job.options
                .job_id
                .or_else(|| job.data.job_id.clone())
                .unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string()),
        );
        job.data.job_id = Some(id.0.clone());
        ids.push(id);
        data.push(job.data);
        delays_ms.push(job.options.delay_ms);
    }

    Ok(PreparedBrokerBatch {
        data,
        ids,
        delays_ms,
    })
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
        assert_eq!(
            prepared.data,
            vec![JobData {
                job_id: Some("stable-id".to_string()),
                ..JobData::default()
            }]
        );
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

    #[cfg(feature = "pulsar")]
    #[test]
    fn delayed_batch_keeps_delays_and_still_rejects_other_options() {
        let prepared = prepare_delayed_batch(
            QueueBackendKind::Pulsar,
            vec![
                QueueJobRequest {
                    data: JobData::default(),
                    options: QueueJobOptions {
                        delay_ms: 1_500,
                        ..QueueJobOptions::default()
                    },
                },
                QueueJobRequest {
                    data: JobData::default(),
                    options: QueueJobOptions::default(),
                },
            ],
        )
        .expect("delays are supported");
        assert_eq!(prepared.delays_ms, vec![1_500, 0]);
        assert_eq!(prepared.data.len(), 2);

        for options in [
            QueueJobOptions {
                deduplication_key: Some("key".to_string()),
                ..QueueJobOptions::default()
            },
            QueueJobOptions {
                max_attempts: Some(3),
                ..QueueJobOptions::default()
            },
        ] {
            let error = prepare_delayed_batch(
                QueueBackendKind::Pulsar,
                vec![QueueJobRequest {
                    data: JobData::default(),
                    options,
                }],
            )
            .expect_err("only delays are supported");
            assert!(error.to_string().contains("does not implement Queue v2"));
        }
    }
}
