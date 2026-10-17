use futures_util::future::join_all;
use sockudo_adapter::horizontal_transport::HorizontalTransport;
use sockudo_adapter::transports::IggyTransport;
use sockudo_core::error::Result;
use sockudo_core::options::IggyConfig;
use uuid::Uuid;

use super::test_helpers::*;

fn live_iggy_config() -> IggyConfig {
    let suffix = Uuid::new_v4().simple().to_string();
    IggyConfig {
        connection_string: std::env::var("SOCKUDO_IGGY_TEST_CONNECTION_STRING")
            .unwrap_or_else(|_| "iggy://iggy:iggy@127.0.0.1:18090".to_string()),
        cluster_seeds: std::env::var("SOCKUDO_IGGY_TEST_CLUSTER_SEEDS")
            .map(|seeds| {
                seeds
                    .split(',')
                    .map(|seed| seed.trim().to_string())
                    .collect()
            })
            .unwrap_or_default(),
        stream: format!("sockudo-live-{suffix}"),
        topic_prefix: format!("sockudo-live-adapter-{suffix}"),
        request_timeout_ms: 5000,
        poll_interval_ms: 25,
        poll_batch_size: 10,
        start_from_latest: false,
        nodes_number: Some(2),
        ..Default::default()
    }
}

#[tokio::test]
#[ignore = "requires a running Apache Iggy broker; set SOCKUDO_IGGY_TEST_CONNECTION_STRING"]
async fn live_iggy_transport_broadcasts_requests_and_responses() -> Result<()> {
    let config = live_iggy_config();
    let receiver = IggyTransport::new(config.clone()).await?;
    let publisher = IggyTransport::new(config).await?;

    receiver.check_health().await?;
    publisher.check_health().await?;

    let collector = MessageCollector::new();
    receiver
        .start_listeners(create_test_handlers(collector.clone()))
        .await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    let broadcast = create_test_broadcast("iggy-live-broadcast");
    publisher.publish_broadcast(&broadcast).await?;
    let received = collector.wait_for_broadcast(5_000).await;
    assert!(received.is_some(), "Iggy broadcast was not received");
    assert!(received.unwrap().message.contains("iggy-live-broadcast"));

    collector.clear().await;
    let publish_results = join_all((0..64).map(|index| {
        let publisher = &publisher;
        async move {
            publisher
                .publish_broadcast(&create_test_broadcast(&format!("iggy-live-batch-{index}")))
                .await
        }
    }))
    .await;
    for result in publish_results {
        result?;
    }
    let received_batch = wait_for_condition(
        || {
            let collector = collector.clone();
            async move { collector.get_broadcasts().await.len() == 64 }
        },
        5_000,
    )
    .await;
    assert!(
        received_batch,
        "Iggy concurrent broadcast batch was not fully received"
    );

    let request = create_test_request();
    let request_id = request.request_id.clone();
    publisher.publish_request(&request).await?;
    let received_request = collector.wait_for_request(5_000).await;
    assert!(received_request.is_some(), "Iggy request was not received");
    assert_eq!(received_request.unwrap().request_id, request_id);

    let response = collector.wait_for_response(5_000).await;
    assert!(response.is_some(), "Iggy response was not received");
    assert_eq!(response.unwrap().request_id, request_id);

    Ok(())
}
