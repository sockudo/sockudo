//! Ignored performance fixtures use actual local brokers and verify every admitted outcome.
#![cfg(any(
    feature = "redis",
    feature = "nats",
    feature = "kafka",
    feature = "rabbitmq"
))]
#[allow(dead_code)]
#[path = "adapter/transports/test_helpers.rs"]
mod helpers;
#[cfg(feature = "kafka")]
use futures_util::StreamExt;
use helpers::*;
use sockudo_adapter::horizontal_transport::HorizontalTransport;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

fn resident_kib() -> usize {
    std::fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmRSS:")
                .map(|value| value.split_whitespace().next().unwrap().parse().unwrap())
        })
        .unwrap()
}
async fn ready<T: HorizontalTransport>(transport: &T, collector: &MessageCollector) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        transport
            .publish_broadcast(&create_test_broadcast("ready"))
            .await
            .unwrap();
        if collector.wait_for_broadcast(100).await.is_some() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "listener readiness timed out"
        );
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    collector.clear().await;
}

async fn ingress<T: HorizontalTransport>(config: T::Config, kind: &str, reply_to: Option<String>) {
    let transport = T::new(config).await.unwrap();
    let collector = MessageCollector::new();
    let mut handlers = create_test_handlers(collector.clone());
    let gate = Arc::new(Semaphore::new(1));
    let entered = Arc::new(AtomicUsize::new(0));
    let finished = Arc::new(AtomicUsize::new(0));
    let gaps = Arc::new(AtomicUsize::new(0));
    let observe_gaps = gaps.clone();
    handlers.on_ingress_gap = Arc::new(move |_, _| {
        observe_gaps.fetch_add(1, Ordering::SeqCst);
    });
    let (work_gate, work_entered, work_finished, work_collector) = (
        gate.clone(),
        entered.clone(),
        finished.clone(),
        collector.clone(),
    );
    handlers.on_broadcast = Arc::new(move |message| {
        let (gate, entered, finished, collector) = (
            work_gate.clone(),
            work_entered.clone(),
            work_finished.clone(),
            work_collector.clone(),
        );
        Box::pin(async move {
            entered.fetch_add(1, Ordering::SeqCst);
            let _permit = gate.acquire().await.unwrap();
            collector.collect_broadcast(message).await;
            finished.fetch_add(1, Ordering::SeqCst);
        })
    });
    transport.start_listeners(handlers).await.unwrap();
    ready(&transport, &collector).await;
    let bounded = std::env::var("SOCKUDO_EXPECT_BOUNDED_INGRESS").as_deref() == Ok("1");
    for payload in [1024, 16384] {
        for sample in 0..9 {
            collector.clear().await;
            entered.store(0, Ordering::SeqCst);
            finished.store(0, Ordering::SeqCst);
            gaps.store(0, Ordering::SeqCst);
            let hold = gate.acquire().await.unwrap();
            for serial in 0..1024 {
                let mut message = create_test_broadcast("blocked");
                message.message = format!("{serial}:{}", "x".repeat(payload));
                transport.publish_broadcast(&message).await.unwrap();
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
            let active = entered.load(Ordering::SeqCst);
            let rss = resident_kib();
            let control = Instant::now();
            if let Some(reply_to) = reply_to.as_deref() {
                transport
                    .publish_request_with_reply(&create_test_request(), reply_to)
                    .await
                    .unwrap();
            } else {
                transport
                    .publish_request(&create_test_request())
                    .await
                    .unwrap();
            }
            assert!(collector.wait_for_response(3000).await.is_some());
            let control_ns = control.elapsed().as_nanos();
            drop(hold);
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            let mut last = 0;
            let mut stable = 0;
            loop {
                tokio::time::sleep(Duration::from_millis(25)).await;
                let count = finished.load(Ordering::SeqCst);
                if count == 1024 {
                    break;
                }
                if count == last && count > 0 {
                    stable += 1;
                } else {
                    stable = 0;
                }
                if stable == 8 && gaps.load(Ordering::SeqCst) > 0 {
                    break;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "admitted records did not drain"
                );
                last = count;
            }
            let records = collector.get_broadcasts().await;
            let received = records
                .iter()
                .map(|message| {
                    message
                        .message
                        .split_once(':')
                        .unwrap()
                        .0
                        .parse::<usize>()
                        .unwrap()
                })
                .collect::<Vec<_>>();
            assert!(received.iter().all(|serial| *serial < 1024));
            assert!(
                received.windows(2).all(|pair| pair[0] < pair[1]),
                "admitted records must be unique and retain channel order"
            );
            if !bounded || kind != "nats" {
                assert_eq!(received, (0..records.len()).collect::<Vec<_>>());
            }
            assert!(
                records
                    .iter()
                    .all(|message| message.message.split_once(':').unwrap().1.len() == payload)
            );
            if bounded {
                assert_eq!(active, 1, "one active handler per channel");
                assert!(
                    records.len() <= 130,
                    "bounded SDK queue plus ordered dispatcher"
                );
                assert!(
                    gaps.load(Ordering::SeqCst) > 0,
                    "loss must explicitly invalidate continuity"
                );
                // NATS can refill its SDK queue between dropped batches. Its
                // bounded event queue can also coalesce slow-consumer notices:
                // a notice invalidates continuity, not one specific serial.
            } else {
                assert_eq!(records.len(), 1024);
                assert_eq!(active, 1024);
            }
            println!(
                "F2,kind={kind},payload={payload},sample={sample},active={active},rss_kib={rss},control_ns={control_ns},admitted={},gap_notifications={}",
                records.len(),
                gaps.load(Ordering::SeqCst)
            );
        }
    }
}
#[cfg(feature = "redis")]
#[tokio::test]
#[ignore = "requires isolated Redis at16379"]
async fn benchmark_redis_ingress() {
    ingress::<sockudo_adapter::transports::RedisTransport>(get_redis_config(), "redis", None).await;
}
#[cfg(feature = "nats")]
#[tokio::test]
#[ignore = "requires isolated NATS at14222"]
async fn benchmark_nats_ingress() {
    let config = get_nats_config();
    let reply_to = format!("{}.responses", config.prefix);
    ingress::<sockudo_adapter::transports::NatsTransport>(config, "nats", Some(reply_to)).await;
}

#[cfg(feature = "kafka")]
fn kafka_config() -> sockudo_core::options::KafkaAdapterConfig {
    let mut config = sockudo_core::options::KafkaAdapterConfig {
        brokers: vec![std::env::var("SOCKUDO_KAFKA_TEST_BROKER").unwrap()],
        prefix: format!("fanout-perf-{}", uuid::Uuid::new_v4().simple()),
        request_timeout_ms: 300,
        ..Default::default()
    };
    if std::env::var("SOCKUDO_EXPECT_PARTITIONED_KAFKA").as_deref() == Ok("1") {
        config.topic_epoch = Some("bench".into());
        config.partitions = 4;
    }
    config
}
#[cfg(feature = "kafka")]
#[tokio::test]
#[ignore = "requires isolated Kafka and explicit topology expectation"]
async fn benchmark_kafka_partitioned_fanout() {
    use sockudo_adapter::transports::KafkaTransport;
    let config = kafka_config();
    let publisher = KafkaTransport::new(config.clone()).await.unwrap();
    let second = KafkaTransport::new(config).await.unwrap();
    let a = MessageCollector::new();
    let b = MessageCollector::new();
    let counts = [Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0))];
    for (transport, collector, count) in [
        (&publisher, &a, counts[0].clone()),
        (&second, &b, counts[1].clone()),
    ] {
        let mut handlers = create_test_handlers(collector.clone());
        let collector = collector.clone();
        handlers.on_broadcast = Arc::new(move |message| {
            let collector = collector.clone();
            let count = count.clone();
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(1)).await;
                collector.collect_broadcast(message).await;
                count.fetch_add(1, Ordering::SeqCst);
            })
        });
        transport.start_listeners(handlers).await.unwrap();
    }
    ready(&publisher, &a).await;
    assert!(b.wait_for_broadcast(3000).await.is_some());
    for sample in 0..9 {
        a.clear().await;
        b.clear().await;
        for count in &counts {
            count.store(0, Ordering::SeqCst);
        }
        let started = Instant::now();
        futures_util::stream::iter(0..16)
            .map(|channel| {
                let publisher = publisher.clone();
                async move {
                    for serial in 0..32 {
                        let mut message = create_test_broadcast("load");
                        message.channel = format!("channel-{channel}");
                        message.message = format!("{serial}:{}", "x".repeat(1024));
                        publisher.publish_broadcast(&message).await.unwrap();
                    }
                }
            })
            .buffer_unordered(16)
            .collect::<Vec<_>>()
            .await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while counts
            .iter()
            .any(|count| count.load(Ordering::SeqCst) != 512)
        {
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let nanos = started.elapsed().as_nanos();
        for collector in [&a, &b] {
            let received = collector.get_broadcasts().await;
            assert_eq!(received.len(), 512);
            for channel in 0..16 {
                let records = received
                    .iter()
                    .filter(|message| message.channel == format!("channel-{channel}"))
                    .collect::<Vec<_>>();
                assert_eq!(
                    records
                        .iter()
                        .map(|message| message
                            .message
                            .split_once(':')
                            .unwrap()
                            .0
                            .parse::<usize>()
                            .unwrap())
                        .collect::<Vec<_>>(),
                    (0..32).collect::<Vec<_>>()
                );
                assert!(
                    records
                        .iter()
                        .all(|message| message.message.split_once(':').unwrap().1.len() == 1024)
                );
            }
        }
        println!("F5,sample={sample},ns={nanos},channels=16,delivered=1024,handler_delay_ms=1");
    }
}

#[cfg(feature = "kafka")]
#[tokio::test]
#[ignore = "explicitly pauses only the audit Kafka container while measuring executor timer progress"]
async fn benchmark_kafka_blocked_health() {
    use sockudo_adapter::transports::KafkaTransport;
    struct Paused(String);
    impl Drop for Paused {
        fn drop(&mut self) {
            let _ = std::process::Command::new("podman")
                .args(["unpause", &self.0])
                .stdout(std::process::Stdio::null())
                .status();
        }
    }
    let container = std::env::var("SOCKUDO_KAFKA_BENCH_CONTAINER").unwrap();
    assert!(container.starts_with("sockudo-perf-"));
    let transport = KafkaTransport::new(kafka_config()).await.unwrap();
    transport.check_health().await.unwrap();
    for sample in 0..9 {
        assert!(
            std::process::Command::new("podman")
                .args(["pause", &container])
                .stdout(std::process::Stdio::null())
                .status()
                .unwrap()
                .success()
        );
        let guard = Paused(container.clone());
        let health = transport.clone();
        let started = Instant::now();
        let task = tokio::spawn(async move { health.check_health().await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        let timer_ns = started.elapsed().as_nanos();
        assert!(
            task.await.unwrap().is_err(),
            "paused broker cannot answer metadata"
        );
        drop(guard);
        transport.check_health().await.unwrap();
        println!(
            "F5-health,sample={sample},timer_ns={timer_ns},deadline_ms=10,metadata_timeout_ms=300"
        );
    }
}

#[cfg(feature = "rabbitmq")]
#[tokio::test]
#[ignore = "requires isolated RabbitMQ; holds delivery while comparing prefetch and explicit queue rejection"]
async fn benchmark_rabbitmq_ingress() {
    use sockudo_adapter::transports::RabbitMqTransport;
    let config = sockudo_core::options::RabbitMqAdapterConfig {
        url: std::env::var("SOCKUDO_RABBITMQ_TEST_URL").unwrap(),
        prefix: format!("fanout-rabbit-perf-{}", uuid::Uuid::new_v4().simple()),
        ..Default::default()
    };
    let transport = RabbitMqTransport::new(config).await.unwrap();
    let collector = MessageCollector::new();
    let gate = Arc::new(Semaphore::new(1));
    let mut handlers = create_test_handlers(collector.clone());
    let (work_gate, work_collector) = (gate.clone(), collector.clone());
    handlers.on_broadcast = Arc::new(move |message| {
        let (gate, collector) = (work_gate.clone(), work_collector.clone());
        Box::pin(async move {
            let _permit = gate.acquire().await.unwrap();
            collector.collect_broadcast(message).await;
        })
    });
    transport.start_listeners(handlers).await.unwrap();
    ready(&transport, &collector).await;
    let bounded = std::env::var("SOCKUDO_EXPECT_BOUNDED_INGRESS").as_deref() == Ok("1");
    for payload in [1024, 1024 * 1024] {
        for sample in 0..9 {
            collector.clear().await;
            let hold = gate.acquire().await.unwrap();
            let mut admitted = Vec::new();
            let started = Instant::now();
            for serial in 0..192 {
                let mut message = create_test_broadcast("blocked");
                message.message = format!("{serial}:{}", "x".repeat(payload));
                if transport.publish_broadcast(&message).await.is_ok() {
                    admitted.push(serial);
                }
            }
            let publish_ns = started.elapsed().as_nanos();
            tokio::time::sleep(Duration::from_millis(150)).await;
            let rss = resident_kib();
            let control = Instant::now();
            transport
                .publish_request(&create_test_request())
                .await
                .unwrap();
            assert!(collector.wait_for_response(5000).await.is_some());
            let control_ns = control.elapsed().as_nanos();
            drop(hold);
            assert!(
                wait_for_condition(
                    || async { collector.get_broadcasts().await.len() == admitted.len() },
                    15000
                )
                .await
            );
            let records = collector.get_broadcasts().await;
            assert_eq!(
                records
                    .iter()
                    .map(|record| record
                        .message
                        .split_once(':')
                        .unwrap()
                        .0
                        .parse::<usize>()
                        .unwrap())
                    .collect::<Vec<_>>(),
                admitted,
                "every acknowledged publish must drain exactly once and in order"
            );
            assert!(
                records
                    .iter()
                    .all(|record| record.message.split_once(':').unwrap().1.len() == payload)
            );
            if bounded && payload == 1024 * 1024 {
                assert!(
                    admitted.len() <= 128,
                    "64 prefetched records plus 64MiB broker queue"
                );
                assert!(!admitted.is_empty());
            } else {
                assert_eq!(admitted.len(), 192);
            }
            println!(
                "F2-rabbitmq,payload={payload},sample={sample},offered=192,admitted={},rejected={},rss_kib={rss},publish_ns={publish_ns},control_ns={control_ns}",
                admitted.len(),
                192 - admitted.len()
            );
        }
    }
}
