use ahash::AHashMap;
use async_trait::async_trait;
use sockudo_adapter::ConnectionManager;
use sockudo_adapter::connection_manager::{ChannelSocketCount, HorizontalAdapterInterface};
use sockudo_adapter::presence::PresenceManager;
use sockudo_core::app::{App, AppPolicy};
use sockudo_core::channel::PresenceMemberInfo;
use sockudo_core::error::Result;
use sockudo_core::namespace::Namespace;
use sockudo_core::presence_history::{
    MemoryPresenceHistoryStore, PresenceHistoryDirection, PresenceHistoryEventCause,
    PresenceHistoryEventKind, PresenceHistoryReadRequest, PresenceHistoryRetentionPolicy,
    PresenceHistoryStore,
};
use sockudo_core::websocket::{SocketId, WebSocketBufferConfig, WebSocketRef};
use sockudo_protocol::messages::PusherMessage;
use sockudo_protocol::{ProtocolVersion, WireFormat};
use sockudo_ws::axum_integration::WebSocketWriter;
use std::any::Any;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::Barrier;

struct ScriptedCM {
    counts: StdMutex<VecDeque<usize>>,
}

impl ScriptedCM {
    fn new(counts: Vec<usize>) -> Self {
        Self {
            counts: StdMutex::new(counts.into()),
        }
    }
}

#[async_trait]
impl ConnectionManager for ScriptedCM {
    async fn init(&self) {}

    async fn get_namespace(&self, _app_id: &str) -> Option<Arc<Namespace>> {
        None
    }

    async fn add_socket(
        &self,
        _socket_id: SocketId,
        _socket: WebSocketWriter,
        _app_id: &str,
        _app_manager: Arc<dyn sockudo_core::app::AppManager + Send + Sync>,
        _buffer_config: WebSocketBufferConfig,
        _protocol_version: ProtocolVersion,
        _wire_format: WireFormat,
        _echo_messages: bool,
    ) -> Result<()> {
        Ok(())
    }

    async fn get_connection(&self, _socket_id: &SocketId, _app_id: &str) -> Option<WebSocketRef> {
        None
    }

    async fn remove_connection(&self, _socket_id: &SocketId, _app_id: &str) -> Result<()> {
        Ok(())
    }

    async fn send_message(
        &self,
        _app_id: &str,
        _socket_id: &SocketId,
        _message: PusherMessage,
    ) -> Result<()> {
        Ok(())
    }

    async fn send(
        &self,
        _channel: &str,
        _message: PusherMessage,
        _except: Option<&SocketId>,
        _app_id: &str,
        _start_time_ms: Option<f64>,
    ) -> Result<()> {
        Ok(())
    }

    async fn get_channel_members(
        &self,
        _app_id: &str,
        _channel: &str,
    ) -> Result<AHashMap<String, PresenceMemberInfo>> {
        Ok(AHashMap::new())
    }

    async fn get_channel_sockets(&self, _app_id: &str, _channel: &str) -> Result<Vec<SocketId>> {
        Ok(Vec::new())
    }

    async fn remove_channel(&self, _app_id: &str, _channel: &str) {}

    async fn is_in_channel(
        &self,
        _app_id: &str,
        _channel: &str,
        _socket_id: &SocketId,
    ) -> Result<bool> {
        Ok(false)
    }

    async fn get_user_sockets(&self, _user_id: &str, _app_id: &str) -> Result<Vec<WebSocketRef>> {
        Ok(Vec::new())
    }

    async fn cleanup_connection(&self, _app_id: &str, _ws: WebSocketRef) {}

    async fn terminate_connection(&self, _app_id: &str, _user_id: &str) -> Result<()> {
        Ok(())
    }

    async fn add_channel_to_sockets(&self, _app_id: &str, _channel: &str, _socket_id: &SocketId) {}

    async fn get_channel_socket_count_info(
        &self,
        _app_id: &str,
        _channel: &str,
    ) -> ChannelSocketCount {
        ChannelSocketCount {
            count: 0,
            complete: true,
        }
    }

    async fn get_channel_socket_count(&self, _app_id: &str, _channel: &str) -> usize {
        0
    }

    async fn add_to_channel(
        &self,
        _app_id: &str,
        _channel: &str,
        _socket_id: &SocketId,
    ) -> Result<bool> {
        Ok(false)
    }

    async fn remove_from_channel(
        &self,
        _app_id: &str,
        _channel: &str,
        _socket_id: &SocketId,
    ) -> Result<bool> {
        Ok(false)
    }

    async fn get_presence_member(
        &self,
        _app_id: &str,
        _channel: &str,
        _socket_id: &SocketId,
    ) -> Option<PresenceMemberInfo> {
        None
    }

    async fn terminate_user_connections(&self, _app_id: &str, _user_id: &str) -> Result<()> {
        Ok(())
    }

    async fn add_user(&self, _ws: WebSocketRef) -> Result<()> {
        Ok(())
    }

    async fn remove_user(&self, _ws: WebSocketRef) -> Result<()> {
        Ok(())
    }

    async fn remove_user_socket(
        &self,
        _user_id: &str,
        _socket_id: &SocketId,
        _app_id: &str,
    ) -> Result<()> {
        Ok(())
    }

    async fn count_user_connections_in_channel(
        &self,
        _user_id: &str,
        _app_id: &str,
        _channel: &str,
        _excluding_socket: Option<&SocketId>,
    ) -> Result<usize> {
        Ok(self.counts.lock().unwrap().pop_front().unwrap_or_default())
    }

    async fn get_channels_with_socket_count(
        &self,
        _app_id: &str,
    ) -> Result<AHashMap<String, usize>> {
        Ok(AHashMap::new())
    }

    async fn get_sockets_count(&self, _app_id: &str) -> Result<usize> {
        Ok(0)
    }

    async fn get_namespaces(&self) -> Result<Vec<(String, Arc<Namespace>)>> {
        Ok(Vec::new())
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    async fn check_health(&self) -> Result<()> {
        Ok(())
    }

    fn get_node_id(&self) -> String {
        "test-node".to_string()
    }

    fn as_horizontal_adapter(&self) -> Option<&dyn HorizontalAdapterInterface> {
        None
    }
}

fn test_app() -> App {
    App::from_policy(
        "app-1".to_string(),
        "key".to_string(),
        "secret".to_string(),
        true,
        AppPolicy::default(),
    )
}

fn retention() -> PresenceHistoryRetentionPolicy {
    PresenceHistoryRetentionPolicy {
        retention_window_seconds: 3600,
        max_events_per_channel: None,
        max_bytes_per_channel: None,
    }
}

fn read_request(app: &App, channel: &str) -> PresenceHistoryReadRequest {
    PresenceHistoryReadRequest {
        app_id: app.id.clone(),
        channel: channel.to_string(),
        direction: PresenceHistoryDirection::OldestFirst,
        limit: 20,
        cursor: None,
        bounds: Default::default(),
    }
}

#[tokio::test]
async fn test_single_member_added_on_first_join() {
    let manager = PresenceManager::new();
    let cm: Arc<dyn ConnectionManager + Send + Sync> = Arc::new(ScriptedCM::new(vec![0]));
    let store: Arc<dyn PresenceHistoryStore + Send + Sync> =
        Arc::new(MemoryPresenceHistoryStore::new(Default::default()));
    let app = test_app();
    let socket = SocketId::new();

    manager
        .handle_member_added(
            Arc::clone(&cm),
            Arc::clone(&store),
            true,
            None,
            None,
            &app,
            "presence-single-join",
            "user-1",
            None,
            Some(&socket),
            Some(retention()),
        )
        .await
        .unwrap();

    let page = store
        .read_page(read_request(&app, "presence-single-join"))
        .await
        .unwrap();

    assert_eq!(
        page.items.len(),
        1,
        "first join must emit exactly one member_added"
    );
    assert_eq!(page.items[0].event, PresenceHistoryEventKind::MemberAdded);
}

/// A second connection from the same user must NOT emit a duplicate `member_added`.
///
/// The scripted counts simulate non-rewind path semantics:
///   - socket_a joins: count=0 (no other connections) → emits member_added
///   - socket_b joins: count=1 (socket_a already present) → suppressed
#[tokio::test]
async fn test_no_duplicate_member_added_on_second_join() {
    let manager = PresenceManager::new();
    let cm: Arc<dyn ConnectionManager + Send + Sync> = Arc::new(ScriptedCM::new(vec![0, 1]));
    let store: Arc<dyn PresenceHistoryStore + Send + Sync> =
        Arc::new(MemoryPresenceHistoryStore::new(Default::default()));
    let app = test_app();
    let socket_a = SocketId::new();
    let socket_b = SocketId::new();

    manager
        .handle_member_added(
            Arc::clone(&cm),
            Arc::clone(&store),
            true,
            None,
            None,
            &app,
            "presence-dup-join",
            "user-1",
            None,
            Some(&socket_a),
            Some(retention()),
        )
        .await
        .unwrap();

    manager
        .handle_member_added(
            Arc::clone(&cm),
            Arc::clone(&store),
            true,
            None,
            None,
            &app,
            "presence-dup-join",
            "user-1",
            None,
            Some(&socket_b),
            Some(retention()),
        )
        .await
        .unwrap();

    let page = store
        .read_page(read_request(&app, "presence-dup-join"))
        .await
        .unwrap();

    assert_eq!(
        page.items.len(),
        1,
        "second connection from the same user must not emit a duplicate member_added"
    );
    assert_eq!(page.items[0].event, PresenceHistoryEventKind::MemberAdded);
}

#[tokio::test]
async fn test_single_member_removed_on_last_leave() {
    let manager = PresenceManager::new();
    let cm: Arc<dyn ConnectionManager + Send + Sync> = Arc::new(ScriptedCM::new(vec![0, 0]));
    let store: Arc<dyn PresenceHistoryStore + Send + Sync> =
        Arc::new(MemoryPresenceHistoryStore::new(Default::default()));
    let app = test_app();
    let socket = SocketId::new();

    manager
        .handle_member_added(
            Arc::clone(&cm),
            Arc::clone(&store),
            true,
            None,
            None,
            &app,
            "presence-last-leave",
            "user-1",
            None,
            Some(&socket),
            Some(retention()),
        )
        .await
        .unwrap();

    manager
        .handle_member_removed(
            &cm,
            Arc::clone(&store),
            true,
            None,
            None,
            &app,
            "presence-last-leave",
            "user-1",
            Some(&socket),
            PresenceHistoryEventCause::Disconnect,
            None,
            Some(retention()),
        )
        .await
        .unwrap();

    let page = store
        .read_page(read_request(&app, "presence-last-leave"))
        .await
        .unwrap();

    assert_eq!(
        page.items.len(),
        2,
        "expected exactly one member_added followed by one member_removed"
    );
    assert_eq!(page.items[0].event, PresenceHistoryEventKind::MemberAdded);
    assert_eq!(page.items[1].event, PresenceHistoryEventKind::MemberRemoved);
}

/// `member_removed` must be suppressed while at least one other connection
/// for the same user remains active.
///
///   - socket_a joins: count=0 → emits member_added
///   - socket_b joins: count=1 (socket_a present) → suppressed
///   - socket_a leaves: count=1 (socket_b still present) → member_removed suppressed
#[tokio::test]
async fn test_no_member_removed_while_other_connections_exist() {
    let manager = PresenceManager::new();
    let cm: Arc<dyn ConnectionManager + Send + Sync> = Arc::new(ScriptedCM::new(vec![0, 1, 1]));
    let store: Arc<dyn PresenceHistoryStore + Send + Sync> =
        Arc::new(MemoryPresenceHistoryStore::new(Default::default()));
    let app = test_app();
    let socket_a = SocketId::new();
    let socket_b = SocketId::new();

    manager
        .handle_member_added(
            Arc::clone(&cm),
            Arc::clone(&store),
            true,
            None,
            None,
            &app,
            "presence-early-leave",
            "user-1",
            None,
            Some(&socket_a),
            Some(retention()),
        )
        .await
        .unwrap();

    manager
        .handle_member_added(
            Arc::clone(&cm),
            Arc::clone(&store),
            true,
            None,
            None,
            &app,
            "presence-early-leave",
            "user-1",
            None,
            Some(&socket_b),
            Some(retention()),
        )
        .await
        .unwrap();

    manager
        .handle_member_removed(
            &cm,
            Arc::clone(&store),
            true,
            None,
            None,
            &app,
            "presence-early-leave",
            "user-1",
            Some(&socket_a),
            PresenceHistoryEventCause::Disconnect,
            None,
            Some(retention()),
        )
        .await
        .unwrap();

    let page = store
        .read_page(read_request(&app, "presence-early-leave"))
        .await
        .unwrap();

    assert_eq!(
        page.items.len(),
        1,
        "member_removed must be suppressed while socket_b is still connected"
    );
    assert_eq!(page.items[0].event, PresenceHistoryEventKind::MemberAdded);
}

/// Two concurrent `handle_member_added` calls for the same user must emit
/// exactly one `member_added` event.
///
/// The per-user `Mutex` inside `PresenceManager` serialises the two tasks,
/// ensuring only the first caller (who sees count=0) emits the event.
///
/// This covers both subscription orderings:
/// - non-rewind path: socket not yet subscribed when `handle_member_added` is
///   called — the Mutex ensures the VecDeque is popped [0, 1].
/// - rewind path: socket already subscribed before the call — same [0, 1]
///   pop sequence applies because the Mutex still serialises access.
#[tokio::test]
async fn test_concurrent_joins_same_user() {
    let manager = Arc::new(PresenceManager::new());
    let cm: Arc<dyn ConnectionManager + Send + Sync> = Arc::new(ScriptedCM::new(vec![0, 1]));
    let store: Arc<dyn PresenceHistoryStore + Send + Sync> =
        Arc::new(MemoryPresenceHistoryStore::new(Default::default()));
    let app = test_app();
    let socket_a = SocketId::new();
    let socket_b = SocketId::new();

    let barrier = Arc::new(Barrier::new(2));

    let (m_a, cm_a, s_a, app_a, bar_a) = (
        Arc::clone(&manager),
        Arc::clone(&cm),
        Arc::clone(&store),
        app.clone(),
        Arc::clone(&barrier),
    );
    let task_a = tokio::spawn(async move {
        bar_a.wait().await;
        m_a.handle_member_added(
            cm_a,
            s_a,
            true,
            None,
            None,
            &app_a,
            "presence-concurrent-joins",
            "user-1",
            None,
            Some(&socket_a),
            Some(retention()),
        )
        .await
        .expect("task A: handle_member_added failed");
    });

    let (m_b, cm_b, s_b, app_b, bar_b) = (
        Arc::clone(&manager),
        Arc::clone(&cm),
        Arc::clone(&store),
        app.clone(),
        Arc::clone(&barrier),
    );
    let task_b = tokio::spawn(async move {
        bar_b.wait().await;
        m_b.handle_member_added(
            cm_b,
            s_b,
            true,
            None,
            None,
            &app_b,
            "presence-concurrent-joins",
            "user-1",
            None,
            Some(&socket_b),
            Some(retention()),
        )
        .await
        .expect("task B: handle_member_added failed");
    });

    let (ra, rb) = tokio::join!(task_a, task_b);
    ra.expect("task A panicked");
    rb.expect("task B panicked");

    let page = store
        .read_page(read_request(&app, "presence-concurrent-joins"))
        .await
        .unwrap();

    assert_eq!(
        page.items.len(),
        1,
        "concurrent joins for the same user must emit exactly one member_added"
    );
    assert_eq!(page.items[0].event, PresenceHistoryEventKind::MemberAdded);
}

/// Concurrent `handle_member_removed` (socket_a disconnecting) and
/// `handle_member_added` (socket_b reconnecting) for the same user must not
/// produce phantom state.
///
/// With the per-user `Mutex`, the two operations are fully serialised.
/// Both operations see count=0 in their scripted window, so:
/// - If leave wins the lock first: last-leave → member_removed; then join: first-join → member_added
/// - If join wins first:            first-join → member_added; then leave: last-leave → member_removed
///
/// Either ordering yields exactly 1 `member_added` + 1 `member_removed`.
#[tokio::test]
async fn test_concurrent_leave_join_race() {
    let manager = Arc::new(PresenceManager::new());
    let cm: Arc<dyn ConnectionManager + Send + Sync> = Arc::new(ScriptedCM::new(vec![0, 0]));
    let store: Arc<dyn PresenceHistoryStore + Send + Sync> =
        Arc::new(MemoryPresenceHistoryStore::new(Default::default()));
    let app = test_app();
    let socket_a = SocketId::new();
    let socket_b = SocketId::new();

    let barrier = Arc::new(Barrier::new(2));

    let (m_leave, cm_leave, s_leave, app_leave, bar_leave) = (
        Arc::clone(&manager),
        Arc::clone(&cm),
        Arc::clone(&store),
        app.clone(),
        Arc::clone(&barrier),
    );
    let task_leave = tokio::spawn(async move {
        bar_leave.wait().await;
        m_leave
            .handle_member_removed(
                &cm_leave,
                s_leave,
                true,
                None,
                None,
                &app_leave,
                "presence-race",
                "user-1",
                Some(&socket_a),
                PresenceHistoryEventCause::Disconnect,
                None,
                Some(retention()),
            )
            .await
            .expect("leave task: handle_member_removed failed");
    });

    let (m_join, cm_join, s_join, app_join, bar_join) = (
        Arc::clone(&manager),
        Arc::clone(&cm),
        Arc::clone(&store),
        app.clone(),
        Arc::clone(&barrier),
    );
    let task_join = tokio::spawn(async move {
        bar_join.wait().await;
        m_join
            .handle_member_added(
                cm_join,
                s_join,
                true,
                None,
                None,
                &app_join,
                "presence-race",
                "user-1",
                None,
                Some(&socket_b),
                Some(retention()),
            )
            .await
            .expect("join task: handle_member_added failed");
    });

    let (rl, rj) = tokio::join!(task_leave, task_join);
    rl.expect("leave task panicked");
    rj.expect("join task panicked");

    let page = store
        .read_page(read_request(&app, "presence-race"))
        .await
        .unwrap();

    let added = page
        .items
        .iter()
        .filter(|i| i.event == PresenceHistoryEventKind::MemberAdded)
        .count();
    let removed = page
        .items
        .iter()
        .filter(|i| i.event == PresenceHistoryEventKind::MemberRemoved)
        .count();

    assert_eq!(
        added, 1,
        "member_added must fire exactly once in the concurrent leave/join race"
    );
    assert_eq!(
        removed, 1,
        "member_removed must fire exactly once in the concurrent leave/join race"
    );
}
