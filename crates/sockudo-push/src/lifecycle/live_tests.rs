use scylla::policies::address_translator::{AddressTranslator, UntranslatedPeer};
use std::sync::Arc;

#[derive(Debug)]
struct FixtureAddress(std::net::SocketAddr);
#[async_trait::async_trait]
impl AddressTranslator for FixtureAddress {
    async fn translate_address(
        &self,
        _: &UntranslatedPeer,
    ) -> Result<std::net::SocketAddr, scylla::errors::TranslationError> {
        Ok(self.0)
    }
}

#[tokio::test]
#[ignore = "requires SOCKUDO_PUSH_TEST_SCYLLA_ADDRESS"]
async fn scylla_lifecycle_restart_and_store_conformance() {
    let endpoint = std::env::var("SOCKUDO_PUSH_TEST_SCYLLA_ADDRESS").unwrap();
    let session = Arc::new(
        scylla::client::session_builder::SessionBuilder::new()
            .known_node(&endpoint)
            .address_translator(Arc::new(FixtureAddress(endpoint.parse().unwrap())))
            .disallow_shard_aware_port(true)
            .build()
            .await
            .unwrap(),
    );
    let keyspace = format!("p6_lifecycle_{}", crate::pipeline::now_ms());
    let store = Arc::new(
        crate::ScyllaDbPushStore::new(session.clone(), &keyspace, "push", "SimpleStrategy", 1)
            .await
            .unwrap(),
    );
    let restarted = Arc::new(
        crate::ScyllaDbPushStore::new(session, &keyspace, "push", "SimpleStrategy", 1)
            .await
            .unwrap(),
    );
    crate::feedback::live_tests::exercise_feedback(store.clone()).await;
    crate::conformance::PushStoreConformance::assert_cursor_pagination_and_channel_fanout(
        store.as_ref().clone(),
    )
    .await
    .unwrap();
    crate::lifecycle::tests::exercise_retention(store, restarted)
        .await
        .unwrap();
}
