//! Actual database checks. Every fixture uses fresh app IDs and retained state
//! between processor instances, including the status-commit/completion window.
use super::*;
use crate::storage::{DynPushStore, IdempotencyRecord};
use std::sync::Arc;

pub(crate) async fn exercise_feedback(store: DynPushStore) {
    use super::tests::{device, rejected_result, status_with_planned};
    for boundary in 0..4 {
        let app_id = format!("p5-{}-{boundary}", now_ms());
        let mut status = status_with_planned("publish-1", 1);
        status.app_id = app_id.clone();
        store.put_publish_status(status).await.unwrap();
        let mut registered = device("device-1");
        registered.app_id = app_id.clone();
        store.upsert_device(registered).await.unwrap();
        let mut result = rejected_result(
            "publish-1",
            "device-1",
            "transient",
            ProviderFailureClass::DeviceTransient,
        );
        result.app_id = app_id.clone();
        let feedback = DeliveryFeedback::from_result(result);
        let id = feedback_receipt_id(&feedback);
        let queue = Arc::new(crate::pipeline::MemoryPushQueue::new());
        let processor = PushFeedbackProcessor::new(store.clone(), queue.clone());
        let pending_time = now_ms();
        store
            .put_idempotency_record_if_absent(IdempotencyRecord {
                app_id: app_id.clone(),
                key: format!("delivery-pending:{id}"),
                publish_id: "publish-1".into(),
                expires_at_ms: pending_time + PushFeedbackProcessor::FEEDBACK_IDEMPOTENCY_TTL_MS,
            })
            .await
            .unwrap();
        if boundary >= 1 {
            let prepared = processor
                .prepare_feedback(feedback.clone())
                .await
                .unwrap()
                .unwrap();
            if boundary >= 2 {
                processor
                    .commit_feedback_status(&app_id, "publish-1", std::slice::from_ref(&prepared))
                    .await
                    .unwrap();
                // An unrelated writer must preserve the exact outcome receipt.
                let expected = store
                    .get_versioned_publish_status(&app_id, "publish-1")
                    .await
                    .unwrap()
                    .unwrap();
                let mut next = expected.status.clone();
                next.retry_after_ms = Some(123);
                assert!(
                    store
                        .compare_and_swap_publish_status(&expected, next)
                        .await
                        .unwrap()
                        .applied()
                );
            }
            if boundary >= 3 {
                store
                    .put_idempotency_record_if_absent(IdempotencyRecord {
                        app_id: app_id.clone(),
                        key: format!("delivery-result:{id}"),
                        publish_id: "publish-1".into(),
                        expires_at_ms: pending_time
                            + PushFeedbackProcessor::FEEDBACK_IDEMPOTENCY_TTL_MS,
                    })
                    .await
                    .unwrap();
            }
        }
        drop(processor);
        let restarted = PushFeedbackProcessor::new(store.clone(), queue);
        restarted.apply_feedback(feedback.clone()).await.unwrap();
        restarted.apply_feedback(feedback).await.unwrap();
        let status = store
            .get_versioned_publish_status(&app_id, "publish-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status.status.counters.failed, 1, "boundary {boundary}");
        assert_eq!(status.status.counters.dispatched, 1);
        assert!(status.pending_feedback.is_empty());
        assert_eq!(
            store
                .get_device(&app_id, "device-1")
                .await
                .unwrap()
                .unwrap()
                .push
                .failure_count,
            1
        );
        assert_eq!(
            store
                .list_delivery_events(&app_id, "publish-1", 10, None)
                .await
                .unwrap()
                .items
                .len(),
            1
        );
        println!(
            "feedback_restart_boundary={boundary} device_failures=1 dispatched=1 failed=1 event_count=1 receipts=0"
        );
    }
}

#[cfg(feature = "postgres")]
#[tokio::test]
#[ignore = "requires SOCKUDO_PUSH_TEST_POSTGRES_URL"]
async fn postgres_feedback_restart_boundaries() {
    let url = std::env::var("SOCKUDO_PUSH_TEST_POSTGRES_URL").unwrap();
    let admin = sqlx::PgPool::connect(&url).await.unwrap();
    let database = format!(
        "sockudo_feedback_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    sqlx::query(sqlx::AssertSqlSafe(
        format!("CREATE DATABASE {database}").as_str(),
    ))
    .execute(&admin)
    .await
    .unwrap();
    let options = url
        .parse::<sqlx::postgres::PgConnectOptions>()
        .unwrap()
        .database(&database);
    let pool = sqlx::PgPool::connect_with(options).await.unwrap();
    sqlx::raw_sql(include_str!(
        "../../../../ops/migrations/postgres/001_push_schema.sql"
    ))
    .execute(&pool)
    .await
    .unwrap();
    sqlx::raw_sql(include_str!(
        "../../../../ops/migrations/postgres/003_push_lifecycle.sql"
    ))
    .execute(&pool)
    .await
    .unwrap();
    // The operator chooses production partition counts. This isolated fixture
    // creates one partition only for parents that have no partitions yet.
    let parents: Vec<(String, String)> = sqlx::query_as("SELECT c.relname::text, p.partstrat::text FROM pg_partitioned_table p JOIN pg_class c ON c.oid = p.partrelid WHERE c.relname LIKE 'push_%' AND NOT EXISTS (SELECT 1 FROM pg_inherits i WHERE i.inhparent = c.oid)")
        .fetch_all(&pool).await.unwrap();
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
        let ddl = format!("CREATE TABLE IF NOT EXISTS {table}_audit PARTITION OF {table} {bounds}");
        sqlx::query(sqlx::AssertSqlSafe(ddl.as_str()))
            .execute(&pool)
            .await
            .unwrap();
    }
    let store: DynPushStore = Arc::new(crate::sql::PostgresPushStore::new(pool.clone()));
    let restarted: DynPushStore = Arc::new(crate::sql::PostgresPushStore::new(pool.clone()));
    exercise_feedback(store.clone()).await;
    crate::lifecycle::tests::exercise_retention(store, restarted)
        .await
        .unwrap();
    pool.close().await;
    sqlx::query(sqlx::AssertSqlSafe(
        format!("DROP DATABASE {database}").as_str(),
    ))
    .execute(&admin)
    .await
    .unwrap();
    admin.close().await;
}

#[cfg(feature = "mysql")]
#[tokio::test]
#[ignore = "requires SOCKUDO_PUSH_TEST_MYSQL_URL"]
async fn mysql_feedback_restart_boundaries() {
    let url = std::env::var("SOCKUDO_PUSH_TEST_MYSQL_URL").unwrap();
    let admin = sqlx::MySqlPool::connect(&url).await.unwrap();
    let database = format!(
        "sockudo_feedback_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    sqlx::query(sqlx::AssertSqlSafe(
        format!("CREATE DATABASE {database}").as_str(),
    ))
    .execute(&admin)
    .await
    .unwrap();
    let options = url
        .parse::<sqlx::mysql::MySqlConnectOptions>()
        .unwrap()
        .database(&database);
    let pool = sqlx::MySqlPool::connect_with(options).await.unwrap();
    sqlx::raw_sql(include_str!(
        "../../../../ops/migrations/mysql/003_push_schema.sql"
    ))
    .execute(&pool)
    .await
    .unwrap();
    sqlx::raw_sql(include_str!(
        "../../../../ops/migrations/mysql/005_push_lifecycle.sql"
    ))
    .execute(&pool)
    .await
    .unwrap();
    let store: DynPushStore = Arc::new(crate::sql::MySqlPushStore::new(pool.clone()));
    let restarted: DynPushStore = Arc::new(crate::sql::MySqlPushStore::new(pool.clone()));
    exercise_feedback(store.clone()).await;
    crate::lifecycle::tests::exercise_retention(store, restarted)
        .await
        .unwrap();
    pool.close().await;
    sqlx::query(sqlx::AssertSqlSafe(
        format!("DROP DATABASE {database}").as_str(),
    ))
    .execute(&admin)
    .await
    .unwrap();
    admin.close().await;
}

#[cfg(feature = "dynamodb")]
#[tokio::test]
#[ignore = "requires SOCKUDO_PUSH_TEST_DYNAMODB_URL"]
async fn dynamodb_feedback_restart_boundaries() {
    let config = aws_sdk_dynamodb::config::Builder::new()
        .behavior_version_latest()
        .region(aws_sdk_dynamodb::config::Region::new("us-east-1"))
        .credentials_provider(aws_sdk_dynamodb::config::Credentials::new(
            "test", "test", None, None, "audit",
        ))
        .endpoint_url(std::env::var("SOCKUDO_PUSH_TEST_DYNAMODB_URL").unwrap())
        .build();
    let client = aws_sdk_dynamodb::Client::from_conf(config);
    let table = format!("p5-feedback-{}", now_ms());
    let store: DynPushStore = Arc::new(
        crate::nosql::DynamoDbPushStore::new(client.clone(), table.clone())
            .await
            .unwrap(),
    );
    let restarted: DynPushStore = Arc::new(
        crate::nosql::DynamoDbPushStore::new(client, table)
            .await
            .unwrap(),
    );
    exercise_feedback(store.clone()).await;
    crate::lifecycle::tests::exercise_retention(store, restarted)
        .await
        .unwrap();
}

#[cfg(feature = "surrealdb")]
#[tokio::test]
#[ignore = "requires SOCKUDO_PUSH_TEST_SURREAL_URL"]
async fn surreal_feedback_restart_boundaries() {
    let db =
        surrealdb::engine::any::connect(std::env::var("SOCKUDO_PUSH_TEST_SURREAL_URL").unwrap())
            .await
            .unwrap();
    db.signin(surrealdb::opt::auth::Root {
        username: "root".into(),
        password: "root".into(),
    })
    .await
    .unwrap();
    db.use_ns(format!("p5_feedback_{}", now_ms()))
        .use_db("audit")
        .await
        .unwrap();
    let store: DynPushStore = Arc::new(
        crate::nosql::SurrealDbPushStore::new(db.clone(), "feedback")
            .await
            .unwrap(),
    );
    let restarted: DynPushStore = Arc::new(
        crate::nosql::SurrealDbPushStore::new(db, "feedback")
            .await
            .unwrap(),
    );
    exercise_feedback(store.clone()).await;
    crate::lifecycle::tests::exercise_retention(store, restarted)
        .await
        .unwrap();
}
