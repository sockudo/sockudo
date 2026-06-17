use super::*;
use sockudo_core::history_conformance::HistoryStoreConformance;

async fn is_mysql_available() -> bool {
    let url = "mysql://root:root123@127.0.0.1:13306/sockudo";
    MySqlPoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .is_ok()
}

async fn build_store() -> Arc<dyn HistoryStore + Send + Sync> {
    let db = DatabaseConnection {
        host: "127.0.0.1".to_string(),
        port: 13306,
        username: "root".to_string(),
        password: "root123".to_string(),
        database: "sockudo".to_string(),
        ..Default::default()
    };
    let pooling = DatabasePooling::default();
    let config = HistoryConfig {
        enabled: true,
        backend: sockudo_core::options::HistoryBackend::Mysql,
        mysql: sockudo_core::options::MySqlHistoryConfig {
            table_prefix: format!(
                "sockudo_ht_{}",
                &uuid::Uuid::new_v4().simple().to_string()[..12]
            ),
            ..sockudo_core::options::MySqlHistoryConfig::default()
        },
        ..HistoryConfig::default()
    };

    create_mysql_history_store(&db, &pooling, config, None, None)
        .await
        .unwrap()
}

#[tokio::test]
async fn mysql_history_store_conformance_serial_and_stream_continuity() {
    if !is_mysql_available().await {
        eprintln!("Skipping test: MySQL not available");
        return;
    }
    let store = build_store().await;
    HistoryStoreConformance::assert_serial_monotonicity(store.clone())
        .await
        .unwrap();
    HistoryStoreConformance::assert_stream_id_continuity(store)
        .await
        .unwrap();
}

#[tokio::test]
async fn mysql_history_store_conformance_pagination_and_reset_semantics() {
    if !is_mysql_available().await {
        eprintln!("Skipping test: MySQL not available");
        return;
    }
    let store = build_store().await;
    HistoryStoreConformance::assert_cursor_pagination(store.clone())
        .await
        .unwrap();
    HistoryStoreConformance::assert_purge_and_reset_semantics(store)
        .await
        .unwrap();
}
