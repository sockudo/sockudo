use super::*;
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tokio::time::{Duration, sleep, timeout};

#[tokio::test]
async fn request_cleanup_operates_on_live_pending_requests() {
    let adapter = HorizontalAdapter::new();
    adapter.requests_timeout.store(1, Ordering::Relaxed);
    adapter.start_request_cleanup();
    adapter.pending_requests.insert(
        "expired".to_string(),
        PendingRequest {
            start_time: Instant::now() - Duration::from_secs(1),
            app_id: "app".to_string(),
            responses: Vec::new(),
            notify: Arc::new(Notify::new()),
        },
    );

    timeout(Duration::from_secs(2), async {
        loop {
            if !adapter.pending_requests.contains_key("expired") {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("cleanup should remove expired live requests");
}

#[tokio::test]
async fn send_request_wakes_on_notify_without_polling_delay() {
    let adapter = Arc::new(HorizontalAdapter::new());
    adapter.pending_requests.insert(
        "req-1".to_string(),
        PendingRequest {
            start_time: Instant::now(),
            app_id: "app".to_string(),
            responses: Vec::new(),
            notify: Arc::new(Notify::new()),
        },
    );

    let wait_adapter = Arc::clone(&adapter);
    let waiter = tokio::spawn(async move {
        wait_adapter
            .wait_for_request_responses("req-1", Instant::now(), Duration::from_millis(40), 1)
            .await
    });

    sleep(Duration::from_millis(10)).await;
    adapter
        .process_response(ResponseBody {
            request_id: "req-1".to_string(),
            node_id: "node-2".to_string(),
            app_id: "app".to_string(),
            members: AHashMap::new(),
            channels_with_sockets_count: AHashMap::new(),
            socket_ids: vec!["socket-1".to_string()],
            sockets_count: 1,
            exists: true,
            channels: HashSet::new(),
            members_count: 0,
            responses_received: 0,
            expected_responses: 0,
            complete: true,
        })
        .await
        .unwrap();

    let result = timeout(Duration::from_millis(20), waiter)
        .await
        .expect("notify should wake waiter before timeout");
    let (responses, timed_out) = result.unwrap();
    assert_eq!(responses.len(), 1);
    assert!(!timed_out, "waiter should complete via notify, not timeout");
}
