use sockudo_adapter::horizontal_adapter::ResponseBody;
use sockudo_adapter::horizontal_transport::{BoxFuture, HorizontalTransport, ResponseHandler};
use sockudo_adapter::transports::NatsTransport;
use sockudo_core::error::Result;
use std::sync::Arc;
use tokio::sync::Mutex;

use super::test_helpers::*;

#[tokio::test]
async fn test_inbox_request_reply_flow() -> Result<()> {
    let config = get_nats_config();
    let transport = NatsTransport::new(config.clone()).await?;

    // Start listeners (responder side)
    let collector = MessageCollector::new();
    let handlers = create_test_handlers(collector.clone());
    transport.start_listeners(handlers).await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Set up inbox subscription (requester side)
    let inbox = transport.new_inbox().unwrap();
    let inbox_responses = Arc::new(Mutex::new(Vec::new()));
    let inbox_responses_clone = inbox_responses.clone();
    let handler: ResponseHandler = Arc::new(move |response| {
        let responses = inbox_responses_clone.clone();
        Box::pin(async move {
            responses.lock().await.push(response);
        }) as BoxFuture<'static, ()>
    });
    let _guard = transport.subscribe_response_inbox(&inbox, handler).await?;

    // Publish request with reply-to
    let request = create_test_request();
    let request_id = request.request_id.clone();
    transport
        .publish_request_with_reply(&request, &inbox)
        .await?;

    // Response should arrive at the inbox
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_millis(500);
    loop {
        let responses = inbox_responses.lock().await;
        if !responses.is_empty() {
            assert_eq!(responses[0].request_id, request_id);
            break;
        }
        drop(responses);
        if tokio::time::Instant::now() >= deadline {
            panic!("Timed out waiting for inbox response");
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }

    Ok(())
}

#[tokio::test]
async fn test_no_reply_to_falls_back_to_global_subject() -> Result<()> {
    let config = get_nats_config();
    let transport = NatsTransport::new(config.clone()).await?;

    // Start listeners
    let collector = MessageCollector::new();
    let handlers = create_test_handlers(collector.clone());
    transport.start_listeners(handlers).await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Publish request WITHOUT reply_to (simulates old pod)
    let request = create_test_request();
    let request_id = request.request_id.clone();
    transport.publish_request(&request).await?;

    // Response should arrive via global subject
    let received = collector.wait_for_response(500).await;
    assert!(
        received.is_some(),
        "Response should arrive via global subject when no reply_to"
    );
    assert_eq!(received.unwrap().request_id, request_id);

    Ok(())
}

#[tokio::test]
async fn test_inbox_guard_drop_stops_receiving() -> Result<()> {
    let config = get_nats_config();
    let transport = NatsTransport::new(config.clone()).await?;

    let inbox = transport.new_inbox().unwrap();
    let received = Arc::new(Mutex::new(Vec::<ResponseBody>::new()));
    let received_clone = received.clone();
    let handler: ResponseHandler = Arc::new(move |response| {
        let received = received_clone.clone();
        Box::pin(async move {
            received.lock().await.push(response);
        }) as BoxFuture<'static, ()>
    });

    let guard = transport
        .subscribe_response_inbox(&inbox, handler)
        .await?
        .unwrap();

    // Drop the guard
    drop(guard);
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Publish after drop — should not be received
    let response = create_test_response("should-not-arrive");
    let response_data = sonic_rs::to_vec(&response).unwrap();
    transport
        .client()
        .publish(async_nats::Subject::from(inbox), response_data.into())
        .await
        .map_err(|e| sockudo_core::error::Error::Internal(e.to_string()))?;

    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    assert!(
        received.lock().await.is_empty(),
        "Should not receive messages after guard dropped"
    );

    Ok(())
}

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

    // Transport A sends a request with inbox
    let inbox_a = transport_a.new_inbox().unwrap();
    let responses_a = Arc::new(Mutex::new(Vec::new()));
    let responses_a_clone = responses_a.clone();
    let handler_a: ResponseHandler = Arc::new(move |response| {
        let responses = responses_a_clone.clone();
        Box::pin(async move {
            responses.lock().await.push(response);
        }) as BoxFuture<'static, ()>
    });
    let _guard_a = transport_a
        .subscribe_response_inbox(&inbox_a, handler_a)
        .await?;

    let request = create_test_request();
    let request_id = request.request_id.clone();
    transport_a
        .publish_request_with_reply(&request, &inbox_a)
        .await?;

    // Wait for responses
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(1);
    loop {
        let resps = responses_a.lock().await;
        // Both transport_a and transport_b receive the request and respond to inbox_a.
        // transport_a's own listener also responds. So we expect 2 responses.
        if resps.len() >= 2 {
            assert!(resps.iter().all(|r| r.request_id == request_id));
            break;
        }
        drop(resps);
        if tokio::time::Instant::now() >= deadline {
            let count = responses_a.lock().await.len();
            panic!("Timed out: expected 2 inbox responses, got {}", count);
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }

    Ok(())
}
