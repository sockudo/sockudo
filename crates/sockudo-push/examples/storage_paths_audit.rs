pub use sockudo_push::{domain, metrics, pipeline, storage};
#[cfg(all(
    feature = "postgres",
    feature = "mysql",
    feature = "dynamodb",
    feature = "surrealdb",
    feature = "scylladb"
))]
mod audit {
    #[allow(dead_code)]
    mod current_repair {
        include!("storage_paths_audit/current_repair.rs");
    }
    use sockudo_push::{
        cleanup::PushCleanupPolicy,
        domain::*,
        pipeline::*,
        reconcile::{PushPublishLogRepairWorker, PushRepairPolicy},
        storage::*,
    };
    use std::{
        alloc::{GlobalAlloc, Layout, System},
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        time::Instant,
    };
    struct Allocator;
    static ALLOCS: AtomicU64 = AtomicU64::new(0);
    static BYTES: AtomicU64 = AtomicU64::new(0);
    #[global_allocator]
    static ALLOCATOR: Allocator = Allocator;
    unsafe impl GlobalAlloc for Allocator {
        unsafe fn alloc(&self, l: Layout) -> *mut u8 {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(l.size() as u64, Ordering::Relaxed);
            unsafe { System.alloc(l) }
        }
        unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
            unsafe { System.dealloc(p, l) }
        }
        unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(n as u64, Ordering::Relaxed);
            unsafe { System.realloc(p, l, n) }
        }
    }
    #[derive(Debug)]
    struct FixtureAddress(std::net::SocketAddr);
    #[async_trait::async_trait]
    impl scylla::policies::address_translator::AddressTranslator for FixtureAddress {
        async fn translate_address(
            &self,
            _: &scylla::policies::address_translator::UntranslatedPeer,
        ) -> Result<std::net::SocketAddr, scylla::errors::TranslationError> {
            Ok(self.0)
        }
    }
    async fn fixture(backend: &str) -> DynPushStore {
        let id = format!("push_audit_{}_{}", std::process::id(), now_ms());
        eprintln!("fixture_backend={backend} fixture_id={id}");
        match backend {
            "postgres" => {
                let url = std::env::var("SOCKUDO_PUSH_TEST_POSTGRES_URL").unwrap();
                let admin = sqlx::PgPool::connect(&url).await.unwrap();
                sqlx::query(sqlx::AssertSqlSafe(
                    format!("CREATE DATABASE {id}").as_str(),
                ))
                .execute(&admin)
                .await
                .unwrap();
                let options = url
                    .parse::<sqlx::postgres::PgConnectOptions>()
                    .unwrap()
                    .database(&id);
                let pool = sqlx::PgPool::connect_with(options).await.unwrap();
                for file in [
                    "ops/migrations/postgres/001_push_schema.sql",
                    "ops/migrations/postgres/003_push_lifecycle.sql",
                ] {
                    sqlx::raw_sql(&std::fs::read_to_string(file).unwrap())
                        .execute(&pool)
                        .await
                        .unwrap();
                }
                let parents:Vec<(String,String)>=sqlx::query_as("SELECT c.relname::text,p.partstrat::text FROM pg_partitioned_table p JOIN pg_class c ON c.oid=p.partrelid WHERE c.relname LIKE 'push_%' AND NOT EXISTS(SELECT 1 FROM pg_inherits i WHERE i.inhparent=c.oid)").fetch_all(&pool).await.unwrap();
                for (table, strategy) in parents {
                    assert!(
                        table
                            .bytes()
                            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
                    );
                    let bounds = if strategy == "h" {
                        "FOR VALUES WITH (MODULUS 1, REMAINDER 0)"
                    } else {
                        "DEFAULT"
                    };
                    sqlx::query(sqlx::AssertSqlSafe(
                        format!("CREATE TABLE {table}_audit PARTITION OF {table} {bounds}")
                            .as_str(),
                    ))
                    .execute(&pool)
                    .await
                    .unwrap();
                }
                Arc::new(sockudo_push::PostgresPushStore::new(pool))
            }
            "mysql" => {
                let url = std::env::var("SOCKUDO_PUSH_TEST_MYSQL_URL").unwrap();
                let admin = sqlx::MySqlPool::connect(&url).await.unwrap();
                sqlx::query(sqlx::AssertSqlSafe(
                    format!("CREATE DATABASE {id}").as_str(),
                ))
                .execute(&admin)
                .await
                .unwrap();
                let options = url
                    .parse::<sqlx::mysql::MySqlConnectOptions>()
                    .unwrap()
                    .database(&id);
                let pool = sqlx::MySqlPool::connect_with(options).await.unwrap();
                for file in [
                    "ops/migrations/mysql/003_push_schema.sql",
                    "ops/migrations/mysql/005_push_lifecycle.sql",
                ] {
                    sqlx::raw_sql(&std::fs::read_to_string(file).unwrap())
                        .execute(&pool)
                        .await
                        .unwrap();
                }
                Arc::new(sockudo_push::MySqlPushStore::new(pool))
            }
            "dynamodb" => {
                let config = aws_sdk_dynamodb::config::Builder::new()
                    .behavior_version_latest()
                    .region(aws_sdk_dynamodb::config::Region::new("us-east-1"))
                    .credentials_provider(aws_sdk_dynamodb::config::Credentials::new(
                        "test", "test", None, None, "audit",
                    ))
                    .endpoint_url(std::env::var("SOCKUDO_PUSH_TEST_DYNAMODB_URL").unwrap())
                    .build();
                Arc::new(
                    sockudo_push::DynamoDbPushStore::new(
                        aws_sdk_dynamodb::Client::from_conf(config),
                        id,
                    )
                    .await
                    .unwrap(),
                )
            }
            "surrealdb" => {
                let db = surrealdb::engine::any::connect(
                    std::env::var("SOCKUDO_PUSH_TEST_SURREAL_URL").unwrap(),
                )
                .await
                .unwrap();
                db.signin(surrealdb::opt::auth::Root {
                    username: "root".into(),
                    password: "root".into(),
                })
                .await
                .unwrap();
                db.use_ns(id).use_db("audit").await.unwrap();
                Arc::new(
                    sockudo_push::SurrealDbPushStore::new(db, "push")
                        .await
                        .unwrap(),
                )
            }
            "scylladb" => {
                let address = std::env::var("SOCKUDO_PUSH_TEST_SCYLLA_ADDRESS").unwrap();
                let session = Arc::new(
                    scylla::client::session_builder::SessionBuilder::new()
                        .known_node(&address)
                        .address_translator(Arc::new(FixtureAddress(address.parse().unwrap())))
                        .disallow_shard_aware_port(true)
                        .build()
                        .await
                        .unwrap(),
                );
                Arc::new(
                    sockudo_push::ScyllaDbPushStore::new(session, &id, "push", "SimpleStrategy", 1)
                        .await
                        .unwrap(),
                )
            }
            _ => panic!("unknown backend"),
        }
    }
    fn status(id: &str, queued: bool) -> PublishStatus {
        PublishStatus {
            app_id: "audit".into(),
            publish_id: id.into(),
            state: if queued {
                PublishLifecycleState::Queued
            } else {
                PublishLifecycleState::Succeeded
            },
            counters: PublishCounters {
                planned: 1,
                succeeded: if queued { 0 } else { 1 },
                ..Default::default()
            },
            fanout_regime: Some(FanoutRegime::FastPath),
            retry_after_ms: None,
            error_reason: None,
        }
    }
    fn event(index: u64, at: u64) -> PublishLogEvent {
        let id = format!("publish-{index:04}");
        PublishLogEvent {
            app_id: "audit".into(),
            publish_id: id.clone(),
            event_id: format!("event-{index:04}"),
            occurred_at_ms: at + index,
            intent: PublishIntent {
                app_id: "audit".into(),
                publish_id: id,
                targets: vec![PublishTarget::Device {
                    device_id: "device".into(),
                }],
                payload: PushPayload {
                    template_id: None,
                    template_data: sonic_rs::json!({"padding":"x".repeat(4096)}),
                    title: Some("fixture".into()),
                    body: None,
                    icon: None,
                    sound: None,
                    collapse_key: None,
                },
                provider_overrides: vec![],
                not_before_ms: None,
                expires_at_ms: None,
            },
            fanout_regime: FanoutRegime::FastPath,
            expected_recipients: 1,
            fast_threshold: 1,
            shard_size: 1,
        }
    }
    async fn seed(store: &DynPushStore, count: u64, at: u64, shards: bool) {
        for index in 0..count {
            let event = event(index, at);
            store
                .put_publish_status(status(&event.publish_id, false))
                .await
                .unwrap();
            store.append_publish_log_event(event.clone()).await.unwrap();
            if shards {
                store
                    .put_fanout_shard(ShardJob {
                        app_id: "audit".into(),
                        publish_id: event.publish_id,
                        shard_id: "lifecycle-plan-v1".into(),
                        target: event.intent.targets[0].clone(),
                        payload: event.intent.payload,
                        provider_overrides: vec![],
                        not_before_ms: None,
                        expires_at_ms: None,
                        cursor: None,
                        page_size: 1,
                        shard_size: 1,
                        emitted_recipients: 1,
                        emitted_batches: 1,
                        status: ShardJobStatus::Complete,
                    })
                    .await
                    .unwrap();
            }
        }
    }
    async fn log_page(store: &DynPushStore) -> Page<PublishLogEvent> {
        for _ in 0..8 {
            match store.list_publish_log_events("audit", 8, None).await {
                Ok(page) => return page,
                Err(error)
                    if error.to_string().contains("backfill")
                        || error.to_string().contains("index") => {}
                Err(error) => panic!("{error}"),
            }
        }
        panic!("index migration did not finish")
    }
    pub async fn run() {
        let args = std::env::args().collect::<Vec<_>>();
        let backend = &args[1];
        let mode = &args[2];
        let current = args[3] == "current";
        if mode == "repair" {
            for rep in 0..3 {
                let store = fixture(backend).await;
                let at = now_ms();
                seed(&store, 16, at, false).await;
                let last = event(16, at);
                store
                    .put_publish_status(status(&last.publish_id, true))
                    .await
                    .unwrap();
                store.append_publish_log_event(last).await.unwrap();
                let _ = log_page(&store).await;
                let queue = Arc::new(MemoryPushQueue::new());
                let old = at + 2 * sockudo_push::retry::MAX_RETRY_AGE_MS;
                ALLOCS.store(0, Ordering::Relaxed);
                BYTES.store(0, Ordering::Relaxed);
                let start = Instant::now();
                let mut scanned = 0;
                let mut requeued = 0;
                if current {
                    let worker = current_repair::PushPublishLogRepairWorker::new(
                        store,
                        queue.clone(),
                        "audit",
                    )
                    .with_policy(current_repair::PushRepairPolicy {
                        batch_size: 8,
                        min_age_ms: 1,
                        lock_ttl_ms: 30000,
                    });
                    for _ in 0..3 {
                        let report = worker.run_once_for_app("audit", old).await.unwrap();
                        scanned += report.scanned;
                        requeued += report.requeued;
                    }
                } else {
                    let worker = PushPublishLogRepairWorker::new(store, queue.clone(), "audit")
                        .with_policy(PushRepairPolicy {
                            batch_size: 8,
                            min_age_ms: 1,
                            lock_ttl_ms: 30000,
                        });
                    for _ in 0..3 {
                        let report = worker.run_once_for_app("audit", old).await.unwrap();
                        scanned += report.scanned;
                        requeued += report.requeued;
                    }
                }
                let us = start.elapsed().as_micros();
                let allocations = ALLOCS.load(Ordering::Relaxed);
                let allocated_bytes = BYTES.load(Ordering::Relaxed);
                assert_eq!(requeued, if current { 1 } else { 0 });
                assert_eq!(
                    queue
                        .lag(PushQueueStage::PublishLog)
                        .await
                        .unwrap()
                        .ready_depth,
                    requeued
                );
                println!(
                    "p6repair,backend={backend},rep={rep},old_logs=16,queued=1,ticks=3,us={us},allocations={allocations},allocated_bytes={allocated_bytes},scanned={scanned},requeued={requeued}"
                );
            }
        } else if mode == "p2" {
            let store = fixture(backend).await;
            let at = now_ms();
            seed(&store, 64, at, false).await;
            let warm = log_page(&store).await;
            assert_eq!(warm.items.len(), 8);
            for rep in 0..3 {
                ALLOCS.store(0, Ordering::Relaxed);
                BYTES.store(0, Ordering::Relaxed);
                let start = Instant::now();
                let page = log_page(&store).await;
                let us = start.elapsed().as_micros();
                let allocations = ALLOCS.load(Ordering::Relaxed);
                let allocated_bytes = BYTES.load(Ordering::Relaxed);
                assert_eq!(page.items.len(), 8);
                for (index, row) in page.items.iter().enumerate() {
                    assert_eq!(row.publish_id, format!("publish-{index:04}"));
                }
                assert!(page.next_cursor.is_some());
                println!(
                    "p2store,backend={backend},rep={rep},rows=64,page=8,returned=8,us={us},allocations={allocations},allocated_bytes={allocated_bytes}"
                );
            }
            let (mut cursor, mut count) = (None, 0);
            loop {
                let page = store
                    .list_publish_log_events("audit", 8, cursor)
                    .await
                    .unwrap();
                count += page.items.len();
                cursor = page.next_cursor;
                if cursor.is_none() {
                    break;
                }
            }
            assert_eq!(count, 64);
        } else {
            for rep in 0..3 {
                let store = fixture(backend).await;
                let at = now_ms();
                seed(&store, 16, at, true).await;
                let last = event(16, at);
                store
                    .put_publish_status(status(&last.publish_id, true))
                    .await
                    .unwrap();
                store.append_publish_log_event(last).await.unwrap();
                let _ = log_page(&store).await;
                let policy = PushCleanupPolicy {
                    publish_status_retention_ms: 1,
                    delivery_event_retention_ms: 0,
                    operator_event_retention_ms: 0,
                    dead_letter_retention_ms: 0,
                    batch_size: 8,
                    max_deleted_per_tick: 8,
                };
                let old = at + 2 * sockudo_push::retry::MAX_RETRY_AGE_MS;
                ALLOCS.store(0, Ordering::Relaxed);
                BYTES.store(0, Ordering::Relaxed);
                let start = Instant::now();
                let mut deleted = 0;
                for _ in 0..64 {
                    let report = store
                        .cleanup_expired_push_data(policy.request_at(old))
                        .await
                        .unwrap();
                    assert!(report.total_deleted() <= 8);
                    deleted += report.total_deleted();
                }
                let us = start.elapsed().as_micros();
                let allocations = ALLOCS.load(Ordering::Relaxed);
                let allocated_bytes = BYTES.load(Ordering::Relaxed);
                let (mut cursor, mut logs) = (None, 0);
                loop {
                    let page = store
                        .list_publish_log_events("audit", 8, cursor)
                        .await
                        .unwrap();
                    logs += page.items.len();
                    cursor = page.next_cursor;
                    if cursor.is_none() {
                        break;
                    }
                }
                let mut shards = 0;
                for index in 0..16 {
                    shards += usize::from(
                        store
                            .get_fanout_shard(
                                "audit",
                                &format!("publish-{index:04}"),
                                "lifecycle-plan-v1",
                            )
                            .await
                            .unwrap()
                            .is_some(),
                    );
                }
                assert_eq!((logs, shards), if current { (1, 0) } else { (17, 16) });
                assert_eq!(
                    store
                        .get_publish_status("audit", "publish-0016")
                        .await
                        .unwrap()
                        .unwrap()
                        .state,
                    PublishLifecycleState::Queued
                );
                let queue = Arc::new(MemoryPushQueue::new());
                let repair = PushPublishLogRepairWorker::new(store, queue.clone(), "audit")
                    .with_policy(PushRepairPolicy {
                        batch_size: 8,
                        min_age_ms: 1,
                        lock_ttl_ms: 30000,
                    });
                let mut requeued = 0;
                let repair_start = Instant::now();
                for _ in 0..3 {
                    requeued += repair
                        .run_once_for_app("audit", old)
                        .await
                        .unwrap()
                        .requeued;
                }
                let repair_us = repair_start.elapsed().as_micros();
                assert_eq!(requeued, if current { 1 } else { 0 });
                println!(
                    "p6store,backend={backend},rep={rep},publishes=16,ticks=64,us={us},allocations={allocations},allocated_bytes={allocated_bytes},deleted={deleted},retained_logs={logs},retained_shards={shards},repair_us={repair_us},requeued={requeued}"
                );
            }
        }
    }
}
#[cfg(all(
    feature = "postgres",
    feature = "mysql",
    feature = "dynamodb",
    feature = "surrealdb",
    feature = "scylladb"
))]
#[tokio::main]
async fn main() {
    audit::run().await
}
#[cfg(not(all(
    feature = "postgres",
    feature = "mysql",
    feature = "dynamodb",
    feature = "surrealdb",
    feature = "scylladb"
)))]
fn main() {
    eprintln!("requires all five push store features")
}
