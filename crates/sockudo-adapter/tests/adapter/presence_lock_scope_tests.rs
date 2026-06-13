//! Lock-scope behavioral regression tests for `PresenceManager`.
//!
//! These tests document the timing guarantees provided by the per-user
//! `Mutex` inside `PresenceManager`:
//!
//! 1. Two concurrent first-joins for the same user emit exactly one
//!    `member_added` event.
//! 2. Two concurrent last-leaves for the same user emit exactly one
//!    `member_removed` event.
//! 3. The webhook (proxied by the `PresenceHistoryStore` write) fires as part
//!    of the atomic check-and-act for the first join.
//! 4. The broadcast (proxied by the `PresenceHistoryStore` write) fires as
//!    part of the atomic check-and-act for the last leave.
//!
//! The member broadcasts run inside the per-user lock (so wire order matches
//! decision order) while the webhook enqueue and history recording happen
//! after lock release.  These tests assert *what* fires, not the relative
//! ordering of I/O and lock release.

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

/// Scripted connection manager whose `count_user_connections_in_channel`
/// returns values from a pre-seeded `VecDeque`, popped in FIFO order.
///
/// The per-user `Mutex` inside `PresenceManager` serialises all callers,
/// so the pop order is deterministic regardless of which tokio task reaches
/// the call first.
///
/// `ScriptedCM` is private in `presence_concurrency_tests`, so it is
/// redefined here rather than imported.
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

/// Two concurrent first-joins for the same user must emit exactly one
/// `member_added` event.
///
/// The per-user `Mutex` inside `PresenceManager` serialises the two tasks.
/// The scripted `VecDeque` pops in FIFO order: the first task to acquire the
/// lock sees `count=0` (first join → emits), the second sees `count=1`
/// (another connection already active → suppressed).
#[tokio::test]
async fn test_presence_lock_prevents_duplicate_member_added() {
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
            "lock-scope-dup-added",
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
            "lock-scope-dup-added",
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
        .read_page(read_request(&app, "lock-scope-dup-added"))
        .await
        .unwrap();

    assert_eq!(
        page.items.len(),
        1,
        "per-user lock must prevent duplicate member_added: expected exactly 1 event, got {}",
        page.items.len()
    );
    assert_eq!(page.items[0].event, PresenceHistoryEventKind::MemberAdded);
}

/// Two concurrent last-leaves for the same user must emit exactly one
/// `member_removed` event.
///
/// The per-user `Mutex` serialises the two tasks.  The scripted `VecDeque`
/// pops in FIFO order: the first task to acquire the lock sees `count=0`
/// (last leave → emits), the second sees `count=1` (another connection
/// detected → suppressed).
///
/// Scripting `[0, 1]` rather than `[0, 0]` is intentional: with `[0, 0]`
/// both tasks would see `count=0` and both would emit, defeating the
/// purpose.  The Mutex guarantees the VecDeque is popped in acquisition
/// order, so `[0, 1]` correctly models "only the true last-leave emits".
#[tokio::test]
async fn test_presence_lock_prevents_duplicate_member_removed() {
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
        m_a.handle_member_removed(
            &cm_a,
            s_a,
            true,
            None,
            None,
            &app_a,
            "lock-scope-dup-removed",
            "user-1",
            Some(&socket_a),
            PresenceHistoryEventCause::Disconnect,
            None,
            0,
            Some(retention()),
        )
        .await
        .expect("task A: handle_member_removed failed");
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
        m_b.handle_member_removed(
            &cm_b,
            s_b,
            true,
            None,
            None,
            &app_b,
            "lock-scope-dup-removed",
            "user-1",
            Some(&socket_b),
            PresenceHistoryEventCause::Disconnect,
            None,
            0,
            Some(retention()),
        )
        .await
        .expect("task B: handle_member_removed failed");
    });

    let (ra, rb) = tokio::join!(task_a, task_b);
    ra.expect("task A panicked");
    rb.expect("task B panicked");

    let page = store
        .read_page(read_request(&app, "lock-scope-dup-removed"))
        .await
        .unwrap();

    assert_eq!(
        page.items.len(),
        1,
        "per-user lock must prevent duplicate member_removed: expected exactly 1 event, got {}",
        page.items.len()
    );
    assert_eq!(page.items[0].event, PresenceHistoryEventKind::MemberRemoved);
}

/// The webhook fires as part of the check-and-act for the first join.
///
/// `PresenceHistoryStore` is the observable proxy: after `handle_member_added`
/// returns the record must exist, proving the webhook fired for the
/// connection-count decision made under the per-user lock.  The webhook
/// enqueue and history write themselves run after lock release — this test
/// asserts *that* the event fires, not *when* relative to lock release.
#[tokio::test]
async fn test_presence_webhook_order_with_member_added() {
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
            "lock-scope-webhook-added",
            "user-1",
            None,
            Some(&socket),
            Some(retention()),
        )
        .await
        .unwrap();

    let page = store
        .read_page(read_request(&app, "lock-scope-webhook-added"))
        .await
        .unwrap();

    assert_eq!(
        page.items.len(),
        1,
        "member_added webhook must fire exactly once for a first join (count=0): got {} records",
        page.items.len()
    );
    assert_eq!(
        page.items[0].event,
        PresenceHistoryEventKind::MemberAdded,
        "first join must produce a MemberAdded record proving the webhook fired for the locked decision"
    );
}

/// The broadcast fires as part of the check-and-act for the last leave.
///
/// `PresenceHistoryStore` is the observable proxy: after `handle_member_removed`
/// returns the record must exist, proving the broadcast fired for the
/// connection-count decision made under the per-user lock.  The broadcast runs
/// inside the lock; the history write runs after release — this test asserts
/// *that* the event fires, not *when* relative to lock release.
#[tokio::test]
async fn test_presence_broadcast_order_with_member_removed() {
    let manager = PresenceManager::new();
    // count=0 for the join (first and only connection), count=0 for the
    // removal (leaving as the last connection).
    let cm: Arc<dyn ConnectionManager + Send + Sync> = Arc::new(ScriptedCM::new(vec![0, 0]));
    let store: Arc<dyn PresenceHistoryStore + Send + Sync> =
        Arc::new(MemoryPresenceHistoryStore::new(Default::default()));
    let app = test_app();
    let socket = SocketId::new();

    // First join
    manager
        .handle_member_added(
            Arc::clone(&cm),
            Arc::clone(&store),
            true,
            None,
            None,
            &app,
            "lock-scope-broadcast-removed",
            "user-1",
            None,
            Some(&socket),
            Some(retention()),
        )
        .await
        .unwrap();

    // Last leave
    manager
        .handle_member_removed(
            &cm,
            Arc::clone(&store),
            true,
            None,
            None,
            &app,
            "lock-scope-broadcast-removed",
            "user-1",
            Some(&socket),
            PresenceHistoryEventCause::Disconnect,
            None,
            0,
            Some(retention()),
        )
        .await
        .unwrap();

    let page = store
        .read_page(read_request(&app, "lock-scope-broadcast-removed"))
        .await
        .unwrap();

    assert_eq!(
        page.items.len(),
        2,
        "expected exactly one member_added + one member_removed (broadcast fired inside lock scope): got {} records",
        page.items.len()
    );
    assert_eq!(
        page.items[0].event,
        PresenceHistoryEventKind::MemberAdded,
        "first record must be MemberAdded"
    );
    assert_eq!(
        page.items[1].event,
        PresenceHistoryEventKind::MemberRemoved,
        "member_removed broadcast must fire exactly once for a last leave (count=0)"
    );
}
