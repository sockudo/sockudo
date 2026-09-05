//! Every test uses fresh synthetic tables; no pre-existing app data is touched.
#![cfg(any(
    feature = "postgres",
    feature = "mysql",
    feature = "dynamodb",
    feature = "surrealdb",
    feature = "scylladb"
))]
use sockudo_core::app::{App, AppManager, AppPolicy};
fn fixture_name() -> String {
    format!(
        "readiness_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}
async fn exercise(manager: impl AppManager) {
    assert!(!manager.has_apps().await.unwrap());
    let disabled = App::from_policy(
        "disabled".into(),
        "synthetic-key".into(),
        "synthetic-secret".into(),
        false,
        AppPolicy::default(),
    );
    manager.create_app(disabled).await.unwrap();
    assert!(
        manager.has_apps().await.unwrap(),
        "existence must include disabled apps"
    );
    assert_eq!(manager.get_apps().await.unwrap().len(), 1);
    assert!(
        !manager
            .find_by_id("disabled")
            .await
            .unwrap()
            .unwrap()
            .enabled
    );
    manager.delete_app("disabled").await.unwrap();
    assert!(!manager.has_apps().await.unwrap());
}
#[cfg(any(feature = "postgres", feature = "mysql"))]
fn sql_config(variable: &str) -> sockudo_core::options::DatabaseConnection {
    let url = url::Url::parse(&std::env::var(variable).unwrap()).unwrap();
    sockudo_core::options::DatabaseConnection {
        host: url.host_str().unwrap().into(),
        port: url.port().unwrap(),
        username: url.username().into(),
        password: url.password().unwrap_or_default().into(),
        database: url.path().trim_start_matches('/').into(),
        table_name: fixture_name(),
        pool_min: Some(1),
        pool_max: Some(2),
        ..Default::default()
    }
}
#[cfg(feature = "postgres")]
#[tokio::test]
#[ignore = "requires SOCKUDO_APP_TEST_POSTGRES_URL"]
async fn postgres_readiness_is_authoritative_and_preserves_disabled_apps() {
    exercise(
        sockudo_app::pg_app_manager::PgSQLAppManager::new(
            sql_config("SOCKUDO_APP_TEST_POSTGRES_URL"),
            Default::default(),
        )
        .await
        .unwrap(),
    )
    .await;
}
#[cfg(feature = "mysql")]
#[tokio::test]
#[ignore = "requires SOCKUDO_APP_TEST_MYSQL_URL"]
async fn mysql_readiness_is_authoritative_and_preserves_disabled_apps() {
    exercise(
        sockudo_app::mysql_app_manager::MySQLAppManager::new(
            sql_config("SOCKUDO_APP_TEST_MYSQL_URL"),
            Default::default(),
        )
        .await
        .unwrap(),
    )
    .await;
}
#[cfg(feature = "dynamodb")]
#[tokio::test]
#[ignore = "requires SOCKUDO_APP_TEST_DYNAMODB_URL"]
async fn dynamodb_readiness_is_authoritative_and_preserves_disabled_apps() {
    use sockudo_app::dynamodb_app_manager::*;
    let table = fixture_name();
    let sdk = aws_sdk_dynamodb::config::Builder::new()
        .behavior_version_latest()
        .region(aws_sdk_dynamodb::config::Region::new("us-east-1"))
        .credentials_provider(aws_sdk_dynamodb::config::Credentials::new(
            "synthetic",
            "synthetic",
            None,
            None,
            "fixture",
        ))
        .endpoint_url(std::env::var("SOCKUDO_APP_TEST_DYNAMODB_URL").unwrap())
        .build();
    let client = aws_sdk_dynamodb::Client::from_conf(sdk);
    client
        .create_table()
        .table_name(&table)
        .attribute_definitions(
            aws_sdk_dynamodb::types::AttributeDefinition::builder()
                .attribute_name("id")
                .attribute_type(aws_sdk_dynamodb::types::ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .key_schema(
            aws_sdk_dynamodb::types::KeySchemaElement::builder()
                .attribute_name("id")
                .key_type(aws_sdk_dynamodb::types::KeyType::Hash)
                .build()
                .unwrap(),
        )
        .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();
    exercise(
        DynamoDbAppManager::new(DynamoDbConfig {
            endpoint: Some(std::env::var("SOCKUDO_APP_TEST_DYNAMODB_URL").unwrap()),
            table_name: table.clone(),
            access_key: Some("synthetic".into()),
            secret_key: Some("synthetic".into()),
            ..Default::default()
        })
        .await
        .unwrap(),
    )
    .await;
    client
        .delete_table()
        .table_name(table)
        .send()
        .await
        .unwrap();
}
#[cfg(feature = "surrealdb")]
#[tokio::test]
#[ignore = "requires SOCKUDO_APP_TEST_SURREAL_URL"]
async fn surreal_readiness_is_authoritative_and_preserves_disabled_apps() {
    use sockudo_app::surrealdb_app_manager::*;
    exercise(
        SurrealDbAppManager::new(SurrealDbConfig {
            url: std::env::var("SOCKUDO_APP_TEST_SURREAL_URL").unwrap(),
            database: fixture_name(),
            ..Default::default()
        })
        .await
        .unwrap(),
    )
    .await;
}
#[cfg(feature = "scylladb")]
#[tokio::test]
#[ignore = "requires SOCKUDO_APP_TEST_SCYLLA_NODE"]
async fn scylla_readiness_is_authoritative_and_preserves_disabled_apps() {
    use sockudo_app::scylla_app_manager::*;
    exercise(
        ScyllaDbAppManager::new(ScyllaDbConfig {
            nodes: vec![std::env::var("SOCKUDO_APP_TEST_SCYLLA_NODE").unwrap()],
            keyspace: fixture_name(),
            replication_factor: 1,
            ..Default::default()
        })
        .await
        .unwrap(),
    )
    .await;
}
