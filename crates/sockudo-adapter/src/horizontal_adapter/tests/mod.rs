use super::*;
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tokio::time::{Duration, sleep, timeout};

#[test]
fn legacy_horizontal_payloads_default_to_empty_trace_context() {
    let request: RequestBody = sonic_rs::from_str(
        r#"{
            "request_id":"request-1",
            "node_id":"node-1",
            "app_id":"app-1",
            "request_type":"Heartbeat",
            "channel":null,
            "socket_id":null,
            "user_id":null,
            "user_info":null,
            "timestamp":1,
            "dead_node_id":null,
            "target_node_id":null
        }"#,
    )
    .expect("legacy horizontal request should deserialize");
    assert!(request.trace_context.is_empty());

    let broadcast: BroadcastMessage = sonic_rs::from_str(
        r#"{
            "node_id":"node-1",
            "app_id":"app-1",
            "channel":"channel-1",
            "message":"{}",
            "except_socket_id":null
        }"#,
    )
    .expect("legacy horizontal broadcast should deserialize");
    assert!(broadcast.trace_context.is_empty());

    let encoded = sonic_rs::to_string(&broadcast).expect("broadcast should serialize");
    assert!(!encoded.contains("trace_context"));
}

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

#[test]
fn test_is_fire_and_forget_true_for_all_eight() {
    assert!(RequestType::PresenceMemberJoined.is_fire_and_forget());
    assert!(RequestType::PresenceMemberLeft.is_fire_and_forget());
    assert!(RequestType::PresenceMemberUpdated.is_fire_and_forget());
    assert!(RequestType::Heartbeat.is_fire_and_forget());
    assert!(RequestType::NodeDead.is_fire_and_forget());
    assert!(RequestType::PresenceStateSync.is_fire_and_forget());
    assert!(RequestType::ChannelCountUpdate.is_fire_and_forget());
    assert!(RequestType::ChannelCountSync.is_fire_and_forget());
}

#[test]
fn test_is_fire_and_forget_false_for_all_query_types() {
    assert!(!RequestType::ChannelMembers.is_fire_and_forget());
    assert!(!RequestType::ChannelSockets.is_fire_and_forget());
    assert!(!RequestType::ChannelSocketsCount.is_fire_and_forget());
    assert!(!RequestType::SocketExistsInChannel.is_fire_and_forget());
    assert!(!RequestType::TerminateUserConnections.is_fire_and_forget());
    assert!(!RequestType::ChannelsWithSocketsCount.is_fire_and_forget());
    assert!(!RequestType::Sockets.is_fire_and_forget());
    assert!(!RequestType::Channels.is_fire_and_forget());
    assert!(!RequestType::SocketsCount.is_fire_and_forget());
    assert!(!RequestType::ChannelMembersCount.is_fire_and_forget());
    assert!(!RequestType::CountUserConnectionsInChannel.is_fire_and_forget());
    assert!(!RequestType::BatchChannelSocketsCount.is_fire_and_forget());
}
