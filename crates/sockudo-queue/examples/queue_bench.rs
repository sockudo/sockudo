use parking_lot::Mutex;
use sockudo_core::options::{QueueReliabilityConfig, RedisTlsOptions, SentinelSpec};
use sockudo_core::queue::{QueueInterface, QueueJobOptions, QueueJobRequest};
use sockudo_core::webhook_types::{JobData, JobPayload};
use sockudo_queue::{RedisClusterQueueManager, RedisQueueManager};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let topology = args.next().unwrap_or_else(|| "redis".to_string());
    let jobs = parse_arg(args.next(), 10_000_usize, "jobs")?;
    let concurrency = parse_arg(args.next(), 16_usize, "concurrency")?;
    let batch_size = parse_arg(args.next(), 1_000_usize, "batch size")?;
    let callback_delay_us = std::env::var("QUEUE_BENCH_CALLBACK_DELAY_US")
        .ok()
        .map(|value| value.parse::<u64>())
        .transpose()?
        .unwrap_or_default();
    let response_timeout_ms = std::env::var("QUEUE_BENCH_RESPONSE_TIMEOUT_MS")
        .ok()
        .map(|value| value.parse::<u64>())
        .transpose()?
        .unwrap_or(5_000);
    let explicit_job_ids = std::env::var("QUEUE_BENCH_EXPLICIT_JOB_IDS")
        .ok()
        .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"));
    if jobs == 0 || concurrency == 0 || batch_size == 0 {
        return Err("jobs, concurrency, and batch size must be greater than zero".into());
    }

    let prefix = format!("sockudo_bench_{}", uuid::Uuid::new_v4().simple());
    let reliability = QueueReliabilityConfig {
        max_attempts: 3,
        retry_base_delay_ms: 10,
        retry_max_delay_ms: 100,
        retry_jitter: 0.0,
        lease_duration_ms: 30_000,
        lease_renew_interval_ms: 10_000,
        worker_poll_interval_ms: 100,
        shutdown_timeout_ms: 30_000,
        completed_retention: 0,
        failed_retention: 1_000,
        event_retention: 0,
        deduplication_ttl_ms: 60_000,
        max_batch_size: batch_size,
        ..QueueReliabilityConfig::default()
    };

    let manager: Box<dyn QueueInterface> = match topology.as_str() {
        "redis" => Box::new(
            RedisQueueManager::new_with_config(
                &std::env::var("QUEUE_BENCH_REDIS_URL")
                    .unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string()),
                None,
                &prefix,
                concurrency,
                response_timeout_ms,
                reliability,
            )
            .await?,
        ),
        "sentinel" => {
            let sentinel_hosts = std::env::var("QUEUE_BENCH_SENTINELS")
                .unwrap_or_else(|_| "127.0.0.1:26379,127.0.0.1:26380,127.0.0.1:26381".to_string());
            let hosts = sentinel_hosts
                .split(',')
                .map(parse_host_port)
                .collect::<Result<Vec<_>, _>>()?;
            let spec = SentinelSpec {
                hosts,
                master_name: std::env::var("QUEUE_BENCH_SENTINEL_MASTER")
                    .unwrap_or_else(|_| "sockudo-master".to_string()),
                db: 0,
                redis_username: None,
                redis_password: std::env::var("QUEUE_BENCH_REDIS_PASSWORD").ok(),
                sentinel_username: None,
                sentinel_password: std::env::var("QUEUE_BENCH_SENTINEL_PASSWORD").ok(),
                master_tls: RedisTlsOptions::default(),
                sentinel_tls: RedisTlsOptions::default(),
            };
            Box::new(
                RedisQueueManager::new_with_config(
                    "redis://sentinel-resolved/",
                    Some(spec),
                    &prefix,
                    concurrency,
                    response_timeout_ms,
                    reliability,
                )
                .await?,
            )
        }
        "cluster" => {
            let nodes = std::env::var("QUEUE_BENCH_CLUSTER_NODES")
                .unwrap_or_else(|_| {
                    "redis://127.0.0.1:7000,redis://127.0.0.1:7001,redis://127.0.0.1:7002"
                        .to_string()
                })
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToString::to_string)
                .collect();
            Box::new(
                RedisClusterQueueManager::new_with_config(
                    nodes,
                    &prefix,
                    concurrency,
                    response_timeout_ms,
                    reliability,
                )
                .await?,
            )
        }
        other => {
            return Err(
                format!("unknown topology {other}; use redis, sentinel, or cluster").into(),
            );
        }
    };

    manager.check_health().await?;
    let completed = Arc::new(AtomicUsize::new(0));
    let notify = Arc::new(Notify::new());
    let latencies = Arc::new(Mutex::new(Vec::with_capacity(jobs)));
    let completed_for_processor = Arc::clone(&completed);
    let notify_for_processor = Arc::clone(&notify);
    let latencies_for_processor = Arc::clone(&latencies);
    manager
        .process_queue(
            "benchmark",
            Box::new(move |job| {
                let completed = Arc::clone(&completed_for_processor);
                let notify = Arc::clone(&notify_for_processor);
                let latencies = Arc::clone(&latencies_for_processor);
                Box::pin(async move {
                    if callback_delay_us > 0 {
                        tokio::time::sleep(Duration::from_micros(callback_delay_us)).await;
                    }
                    let enqueued_at = u64::try_from(job.payload.time_ms).unwrap_or_default();
                    latencies.lock().push(now_ms().saturating_sub(enqueued_at));
                    completed.fetch_add(1, Ordering::Release);
                    notify.notify_one();
                    Ok(())
                })
            }),
        )
        .await?;

    let end_to_end_started = Instant::now();
    let enqueue_started = Instant::now();
    let mut requests = Vec::with_capacity(jobs);
    for index in 0..jobs {
        requests.push(QueueJobRequest {
            data: JobData {
                app_key: "bench".to_string(),
                app_id: "bench".to_string(),
                app_secret: "not-a-secret".to_string(),
                payload: JobPayload {
                    time_ms: i64::try_from(now_ms()).unwrap_or(i64::MAX),
                    events: Vec::new(),
                },
                original_signature: format!("benchmark-{index}"),
            },
            options: QueueJobOptions {
                job_id: explicit_job_ids.then(|| format!("job-{index:020}")),
                ..QueueJobOptions::default()
            },
        });
    }
    let request_build_elapsed = enqueue_started.elapsed();
    let ids = manager.enqueue_batch("benchmark", requests).await?;
    if ids.len() != jobs {
        return Err(format!("enqueue returned {} IDs for {jobs} jobs", ids.len()).into());
    }
    let enqueue_elapsed = enqueue_started.elapsed();

    tokio::time::timeout(Duration::from_secs(120), async {
        while completed.load(Ordering::Acquire) < jobs {
            notify.notified().await;
        }
    })
    .await
    .map_err(|_| {
        format!(
            "timed out after processing {} of {jobs} jobs",
            completed.load(Ordering::Acquire)
        )
    })?;
    let stats = tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            if let Ok(stats) = manager.stats("benchmark").await
                && stats.ready.unwrap_or_default() == 0
                && stats.active.unwrap_or_default() == 0
                && stats.delayed.unwrap_or_default() == 0
            {
                break stats;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .map_err(|_| "timed out waiting for all benchmark jobs to settle")?;
    let end_to_end_elapsed = end_to_end_started.elapsed();

    let mut latency = latencies.lock().clone();
    latency.sort_unstable();
    let p50 = percentile(&latency, 0.50);
    let p95 = percentile(&latency, 0.95);
    let p99 = percentile(&latency, 0.99);
    let enqueue_rate = jobs as f64 / enqueue_elapsed.as_secs_f64();
    let end_to_end_rate = jobs as f64 / end_to_end_elapsed.as_secs_f64();
    let deliveries = completed.load(Ordering::Acquire);
    println!(
        "{{\"topology\":\"{}\",\"jobs\":{},\"deliveries\":{},\"duplicates\":{},\"concurrency\":{},\"batch_size\":{},\"explicit_job_ids\":{},\"callback_delay_us\":{},\"response_timeout_ms\":{},\"request_build_seconds\":{:.6},\"enqueue_seconds\":{:.6},\"enqueue_jobs_per_second\":{:.2},\"end_to_end_seconds\":{:.6},\"end_to_end_jobs_per_second\":{:.2},\"latency_ms_p50\":{},\"latency_ms_p95\":{},\"latency_ms_p99\":{},\"ready\":{},\"active\":{},\"delayed\":{},\"dead_letter\":{}}}",
        topology,
        jobs,
        deliveries,
        deliveries.saturating_sub(jobs),
        concurrency,
        batch_size,
        explicit_job_ids,
        callback_delay_us,
        response_timeout_ms,
        request_build_elapsed.as_secs_f64(),
        enqueue_elapsed.as_secs_f64(),
        enqueue_rate,
        end_to_end_elapsed.as_secs_f64(),
        end_to_end_rate,
        p50,
        p95,
        p99,
        stats.ready.unwrap_or_default(),
        stats.active.unwrap_or_default(),
        stats.delayed.unwrap_or_default(),
        stats.dead_letter.unwrap_or_default(),
    );
    drop(latency);
    manager.disconnect().await?;
    Ok(())
}

fn parse_arg<T>(
    value: Option<String>,
    default: T,
    name: &str,
) -> Result<T, Box<dyn std::error::Error>>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value.map_or(Ok(default), |value| {
        value
            .parse()
            .map_err(|error| format!("invalid {name} {value}: {error}").into())
    })
}

fn parse_host_port(value: &str) -> Result<(String, u16), Box<dyn std::error::Error>> {
    let (host, port) = value
        .trim()
        .rsplit_once(':')
        .ok_or_else(|| format!("invalid Sentinel host {value}"))?;
    Ok((host.to_string(), port.parse()?))
}

fn percentile(values: &[u64], percentile: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let index = ((values.len() - 1) as f64 * percentile).round() as usize;
    values[index]
}

fn now_ms() -> u64 {
    u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default()
}
