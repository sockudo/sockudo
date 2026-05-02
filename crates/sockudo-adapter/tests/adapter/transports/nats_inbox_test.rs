use sockudo_adapter::horizontal_adapter::ResponseBody;
use sockudo_adapter::horizontal_transport::HorizontalTransport;
use sockudo_adapter::transports::NatsTransport;
use sockudo_core::error::Result;
use std::sync::Arc;
use tokio::sync::Mutex;

use super::test_helpers::*;

/// Test that responses arrive via the shared inbox wildcard subscription
/// when requests use publish_request_with_reply.
#[tokio::test]
async fn test_shared_inbox_request_reply_flow() -> Result<()> {
    let config = get_nats_config();
    let transport = NatsTransport::new(config.clone()).await?;

    // Collect responses that arrive via the shared inbox and global response
    // subject. Both use on_response which we capture via the collector.
    let collector = MessageCollector::new();
    let handlers = create_test_handlers(collector.clone());
    transport.start_listeners(handlers).await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Publish request with reply-to pointing to the shared inbox prefix
    let inbox = transport.new_inbox().unwrap();
    let request = create_test_request();
    let request_id = request.request_id.clone();
    transport
        .publish_request_with_reply(&request, &inbox)
        .await?;

    // Response should arrive via the shared inbox subscription
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_millis(500);
    loop {
        if let Some(response) = collector.wait_for_response(50).await {
            assert_eq!(response.request_id, request_id);
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("Timed out waiting for inbox response");
        }
    }

    Ok(())
}

/// Test that requests without reply_to do NOT generate a response on the
/// global subject.  Fire-and-forget requests (heartbeats, presence
/// replication, dead-node notifications) are published without a reply
/// address and nobody is waiting for their response.  Publishing to the
/// global response subject in that case fans out to every node, wasting
/// bandwidth and overwhelming subscriptions under presence-heavy load.
#[tokio::test]
async fn test_no_reply_to_does_not_publish_response() -> Result<()> {
    let config = get_nats_config();
    let transport = NatsTransport::new(config.clone()).await?;

    // Start listeners
    let collector = MessageCollector::new();
    let handlers = create_test_handlers(collector.clone());
    transport.start_listeners(handlers).await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Publish request WITHOUT reply_to (fire-and-forget)
    let request = create_test_request();
    transport.publish_request(&request).await?;

    // No response should arrive on the global subject
    let received = collector.wait_for_response(500).await;
    assert!(
        received.is_none(),
        "Fire-and-forget requests (no reply_to) must not generate responses on the global subject"
    );

    Ok(())
}

/// Test that two transports with separate inbox prefixes receive only their
/// own responses, not each other's.
#[tokio::test]
async fn test_two_transports_inbox_isolation() -> Result<()> {
    let config = get_nats_config();
    let transport_a = NatsTransport::new(config.clone()).await?;
    let transport_b = NatsTransport::new(config.clone()).await?;

    // Both start listeners (both are responders)
    let collector_a = MessageCollector::new();
    transport_a
        .start_listeners(create_test_handlers(collector_a.clone()))
        .await?;
    let collector_b = MessageCollector::new();
    transport_b
        .start_listeners(create_test_handlers(collector_b.clone()))
        .await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Transport A sends a request with its inbox
    let inbox_a = transport_a.new_inbox().unwrap();
    let request = create_test_request();
    let request_id = request.request_id.clone();
    transport_a
        .publish_request_with_reply(&request, &inbox_a)
        .await?;

    // Wait for responses on transport A's collector.
    // Both transport_a and transport_b receive the request and respond to inbox_a.
    // transport_a's shared inbox subscription receives both responses.
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(1);
    let responses_a = Arc::new(Mutex::new(Vec::<ResponseBody>::new()));
    loop {
        if let Some(response) = collector_a.wait_for_response(50).await {
            let mut resps = responses_a.lock().await;
            resps.push(response);
            if resps.len() >= 2 {
                assert!(resps.iter().all(|r| r.request_id == request_id));
                break;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            let count = responses_a.lock().await.len();
            panic!("Timed out: expected 2 inbox responses, got {}", count);
        }
    }

    Ok(())
}
