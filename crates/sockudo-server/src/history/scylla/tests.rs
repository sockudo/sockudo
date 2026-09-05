use super::*;
use sockudo_core::history_conformance::HistoryStoreConformance;

pub(in crate::history) async fn fixture_session() -> std::result::Result<Arc<Session>, String> {
    use scylla::errors::TranslationError;
    use scylla::policies::address_translator::{AddressTranslator, UntranslatedPeer};
    struct FixtureAddress(std::net::SocketAddr);
    #[async_trait::async_trait]
    impl AddressTranslator for FixtureAddress {
        async fn translate_address(
            &self,
            _: &UntranslatedPeer,
        ) -> std::result::Result<std::net::SocketAddr, TranslationError> {
            Ok(self.0)
        }
    }
    let address =
        std::env::var("SOCKUDO_C8_SCYLLA_ADDR").unwrap_or_else(|_| "127.0.0.1:19042".into());
    SessionBuilder::new()
        .known_node(address.clone())
        .address_translator(Arc::new(FixtureAddress(address.parse().unwrap())))
        .disallow_shard_aware_port(true)
        .build()
        .await
        .map(Arc::new)
        .map_err(|error| error.to_string())
}

async fn is_scylla_available() -> bool {
    let Ok(session) = fixture_session().await else {
        return false;
    };
    session
        .query_unpaged("SELECT cluster_name FROM system.local", ())
        .await
        .is_ok()
}

async fn build_store() -> Arc<dyn HistoryStore + Send + Sync> {
    let db = ScyllaDbSettings {
        nodes: vec!["127.0.0.1:19042".to_string()],
        keyspace: format!("sockudo_history_test_{}", uuid::Uuid::new_v4().simple()),
        username: None,
        password: None,
        table_name: "applications".to_string(),
        replication_class: "SimpleStrategy".to_string(),
        replication_factor: 1,
    };
    let config = HistoryConfig {
        enabled: true,
        backend: sockudo_core::options::HistoryBackend::ScyllaDb,
        scylladb: sockudo_core::options::ScyllaDbHistoryConfig {
            table_prefix: format!("sockudo_history_{}", uuid::Uuid::new_v4().simple()),
            ..sockudo_core::options::ScyllaDbHistoryConfig::default()
        },
        ..HistoryConfig::default()
    };
    Arc::new(
        ScyllaHistoryStore::from_session(&db, config, None, None, fixture_session().await.unwrap())
            .await
            .unwrap(),
    )
}

#[tokio::test]
async fn scylla_history_store_conformance_serial_and_stream_continuity() {
    if !is_scylla_available().await {
        eprintln!("Skipping test: ScyllaDB not available");
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
async fn scylla_history_store_conformance_pagination_and_reset_semantics() {
    if !is_scylla_available().await {
        eprintln!("Skipping test: ScyllaDB not available");
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
