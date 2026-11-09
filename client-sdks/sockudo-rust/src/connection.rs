//! The connection task: supervisor (connect, backoff, close-code policy) and
//! session (one live WebSocket).
//!
//! One task owns the socket. Inbound frames are decoded and dispatched in
//! wire order on this task; outbound frames arrive through a bounded command
//! channel and are written in batches (`feed` … `flush`). No lock is held
//! across an `.await`.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use sockudo_ws::Message;
use sonic_rs::{JsonValueTrait, Value};
use tokio::sync::{OwnedMutexGuard, mpsc, watch};
use tokio::time::{Instant, sleep_until};

use crate::channel::SubState;
use crate::channel::{authorized_frame, disconnect_all, prepare_resubscribe, start_subscription};
use crate::client::{Command, ConnectionState, Shared};
use crate::codec;
use crate::dedup::Deduplicator;
use crate::error::{Error, Result};
use crate::event::Event;
use crate::options::Transport;
use crate::protocol::{TOKEN_EXPIRED_CODE, TOKEN_REVOKED_CODE, strip_internal, strip_platform};
use crate::recovery::{ResumeFailed, ResumeSuccess};
use crate::token::{self, TokenAuthData, TokenRequest, TokenRequestReason};
use crate::transport::{self, WsStream};
use crate::user;

/// Most commands drained into one write batch.
const MAX_BATCH: usize = 256;

type Commands = OwnedMutexGuard<mpsc::Receiver<Command>>;

/// What a close code asks the client to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CloseAction {
    /// 4000: reconnect immediately over TLS only.
    TlsOnly,
    /// 4001–4099 and ≥ 4300: do not reconnect.
    Refused,
    /// 1002–1004, 4100–4199: reconnect after a backoff.
    Backoff,
    /// 4200–4299: reconnect immediately.
    Retry,
}

/// The Pusher close-code ranges.
pub(crate) fn close_action(code: u16) -> Option<CloseAction> {
    match code {
        0..=3999 => (1002..=1004)
            .contains(&code)
            .then_some(CloseAction::Backoff),
        4000 => Some(CloseAction::TlsOnly),
        4001..=4099 => Some(CloseAction::Refused),
        4100..=4199 => Some(CloseAction::Backoff),
        4200..=4299 => Some(CloseAction::Retry),
        _ => Some(CloseAction::Refused),
    }
}

/// Entry point of the connection task.
pub(crate) async fn run(shared: Arc<Shared>, generation: u64) {
    // Waiting for the receiver also waits for a previous task to exit.
    let mut commands = Arc::clone(&shared.cmd_rx).lock_owned().await;
    if shared.generation.load(std::sync::atomic::Ordering::SeqCst) != generation {
        return; // superseded by a newer connect()
    }
    let mut shutdown = shared.shutdown.subscribe();
    if *shutdown.borrow_and_update() {
        return;
    }
    #[cfg(feature = "http")]
    let stats = shared
        .timeline
        .is_some()
        .then(|| tokio::spawn(crate::stats::run(Arc::clone(&shared))));
    let mut supervisor = Supervisor::new(Arc::clone(&shared), generation);
    supervisor.run(&mut commands, &mut shutdown).await;
    #[cfg(feature = "http")]
    if let Some(stats) = stats {
        stats.abort();
    }
    disconnect_all(&shared);
    if let Some(delta) = shared.delta.as_ref() {
        delta.lock().clear_all();
    }
    if !matches!(shared.state(), ConnectionState::Failed) {
        shared.set_state(ConnectionState::Disconnected);
    }
}

enum SessionEnd {
    /// Manual disconnect or shutdown.
    Stop,
    /// The peer closed the socket.
    Closed { code: u16, reason: String },
    /// The socket failed.
    Lost(Error),
}

struct Supervisor {
    shared: Arc<Shared>,
    generation: u64,
    dedup: Option<Deduplicator>,
    attempts: u32,
    force_wss: bool,
    attempted_fallback: bool,
    last_failed: Option<Transport>,
    ever_connected: bool,
    unavailable_at: Option<Instant>,
}

impl Supervisor {
    fn new(shared: Arc<Shared>, generation: u64) -> Self {
        let dedup = shared
            .config
            .message_deduplication
            .then(|| Deduplicator::new(shared.config.deduplication_capacity));
        Self {
            shared,
            generation,
            dedup,
            attempts: 0,
            force_wss: false,
            attempted_fallback: false,
            last_failed: None,
            ever_connected: false,
            unavailable_at: None,
        }
    }

    /// Await `future` while honouring disconnect, shutdown and the
    /// unavailable timer. `None` means stop.
    async fn wait<F: Future>(
        &mut self,
        commands: &mut Commands,
        shutdown: &mut watch::Receiver<bool>,
        future: F,
    ) -> Option<F::Output> {
        tokio::pin!(future);
        loop {
            let unavailable_at = self.unavailable_at;
            tokio::select! {
                output = &mut future => return Some(output),
                _ = shutdown.changed() => return None,
                command = commands.recv() => match command {
                    Some(Command::Disconnect { generation }) if generation >= self.generation => return None,
                    Some(_) => {} // frames for a dead connection are dropped
                    None => return None,
                },
                () = sleep_opt(unavailable_at) => {
                    self.unavailable_at = None;
                    self.shared.set_state(ConnectionState::Unavailable);
                }
            }
        }
    }

    fn next_transport(&mut self, transports: &[Transport]) -> Transport {
        if self.force_wss {
            return Transport::Wss;
        }
        if self.last_failed == Some(Transport::Ws)
            && !self.attempted_fallback
            && transports.contains(&Transport::Wss)
        {
            self.attempted_fallback = true;
            return Transport::Wss;
        }
        self.attempted_fallback = false;
        transports[0]
    }

    /// Reserve the next attempt, or stop when the budget is spent.
    fn next_delay(&mut self, immediate: bool) -> Option<Duration> {
        if let Some(max) = self.shared.config.max_reconnect_attempts
            && self.attempts >= max
        {
            self.shared.report_error(Error::ReconnectExhausted {
                attempts: self.attempts,
            });
            return None;
        }
        let config = &self.shared.config;
        let delay = if immediate {
            Duration::ZERO
        } else {
            transport::reconnect_delay(
                self.attempts,
                config.max_reconnect_gap,
                config.reconnect_jitter,
            )
        };
        self.attempts += 1;
        self.shared.log_timeline(
            || sonic_rs::json!({ "action": "retry", "delay": delay.as_millis() as u64 }),
        );
        Some(delay)
    }

    fn enter_retrying(&mut self) {
        if self.unavailable_at.is_none() && self.shared.state() != ConnectionState::Unavailable {
            self.unavailable_at = Some(Instant::now() + self.shared.config.unavailable_timeout);
        }
        if self.shared.state() != ConnectionState::Unavailable {
            self.shared.set_state(ConnectionState::Reconnecting);
        }
    }

    async fn run(&mut self, commands: &mut Commands, shutdown: &mut watch::Receiver<bool>) {
        let transports = self.shared.config.transports();
        self.unavailable_at = Some(Instant::now() + self.shared.config.unavailable_timeout);
        loop {
            let reason = if self.ever_connected {
                TokenRequestReason::Reconnect
            } else {
                TokenRequestReason::Initial
            };
            let shared = Arc::clone(&self.shared);
            let token = match self
                .wait(commands, shutdown, prepare_token(&shared, reason))
                .await
            {
                None => return,
                Some(Ok(token)) => token,
                Some(Err(error)) => {
                    self.shared.report_error(error);
                    match self.next_delay(false) {
                        Some(delay) => {
                            self.enter_retrying();
                            if self
                                .wait(commands, shutdown, tokio::time::sleep(delay))
                                .await
                                .is_none()
                            {
                                return;
                            }
                            continue;
                        }
                        None => return,
                    }
                }
            };

            let transport = self.next_transport(&transports);
            tracing::debug!(url = %transport::display_url(&self.shared.config, transport), "connecting");
            let attempt = tokio::time::timeout(
                self.shared.config.connect_timeout,
                transport::connect(&shared, transport, token.as_deref()),
            );
            let end = match self.wait(commands, shutdown, attempt).await {
                None => return,
                Some(Err(_elapsed)) => {
                    self.last_failed = Some(transport);
                    SessionEnd::Lost(Error::Timeout)
                }
                Some(Ok(Err(error @ Error::Unsupported(_)))) => {
                    self.shared.report_error(error);
                    self.shared.set_state(ConnectionState::Failed);
                    return;
                }
                Some(Ok(Err(error))) => {
                    self.last_failed = Some(transport);
                    SessionEnd::Lost(error)
                }
                Some(Ok(Ok(stream))) => {
                    let epoch = {
                        let mut conn = self.shared.conn.lock();
                        conn.epoch += 1;
                        conn.compressed = stream.is_compressed();
                        conn.epoch
                    };
                    let mut session = Session::new(self, epoch);
                    let end = session.run(stream, commands, shutdown).await;
                    let established = session.established;
                    drop(session);
                    self.after_session();
                    if !established {
                        self.last_failed = Some(transport);
                    } else {
                        self.last_failed = None;
                    }
                    end
                }
            };

            let immediate = match end {
                SessionEnd::Stop => return,
                SessionEnd::Closed { code, reason } => {
                    tracing::debug!(code, %reason, "connection closed");
                    match close_action(code) {
                        Some(CloseAction::Refused) => {
                            self.shared
                                .report_error(Error::ConnectionRefused { code, reason });
                            return;
                        }
                        Some(CloseAction::TlsOnly) => {
                            self.force_wss = true;
                            true
                        }
                        Some(CloseAction::Retry) => true,
                        Some(CloseAction::Backoff) | None => {
                            if !reason.is_empty() || code >= 4000 {
                                self.shared
                                    .report_error(Error::ConnectionClosed { code, reason });
                            }
                            false
                        }
                    }
                }
                SessionEnd::Lost(error) => {
                    self.shared.report_error(error);
                    false
                }
            };
            let Some(delay) = self.next_delay(immediate) else {
                return;
            };
            self.enter_retrying();
            if !delay.is_zero()
                && self
                    .wait(commands, shutdown, tokio::time::sleep(delay))
                    .await
                    .is_none()
            {
                return;
            }
        }
    }

    fn after_session(&mut self) {
        self.shared.conn.lock().compressed = false;
        disconnect_all(&self.shared);
        user::on_disconnect(&self.shared);
        if let Some(delta) = self.shared.delta.as_ref() {
            // Delta state is per socket on the server too.
            delta.lock().clear_all();
        }
    }
}

async fn sleep_opt(at: Option<Instant>) {
    match at {
        Some(at) => sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// Choose the token for the next connection attempt.
async fn prepare_token(shared: &Arc<Shared>, reason: TokenRequestReason) -> Result<Option<String>> {
    {
        let mut state = shared.token.lock();
        if let Some(token) = shared.config.token.as_ref()
            && !state.static_used
        {
            state.static_used = true;
            state.current =
                token::normalize(TokenAuthData::new(token.clone()), token::now_seconds()).ok();
            return Ok(Some(token.clone()));
        }
        if !shared.config.has_token_provider() {
            // A static token keeps being presented; it is never refreshed.
            return Ok(state.current.as_ref().map(|current| current.token.clone()));
        }
    }
    let data = fetch_token(shared, reason).await?;
    let normalized = token::normalize(data, token::now_seconds())?;
    let value = normalized.token.clone();
    shared.token.lock().current = Some(normalized);
    Ok(Some(value))
}

async fn fetch_token(shared: &Arc<Shared>, reason: TokenRequestReason) -> Result<TokenAuthData> {
    let request = TokenRequest {
        socket_id: shared.socket_id(),
        reason,
    };
    if let Some(provider) = shared.config.token_provider.as_ref() {
        return provider.token(request).await.map_err(|error| match error {
            Error::TokenProvider(message) => Error::TokenProvider(message),
            other => Error::TokenProvider(other.to_string()),
        });
    }
    #[cfg(feature = "http")]
    if let Some(endpoint) = shared.config.auth_url.as_ref() {
        return crate::http::fetch_token(&shared.http, endpoint, &request).await;
    }
    Err(Error::TokenProvider(
        "no capability-token provider is configured".into(),
    ))
}

/// Refresh the token in the background and send `auth` on `epoch`.
fn spawn_token_refresh(shared: &Arc<Shared>, epoch: u64, reason: TokenRequestReason) {
    if !shared.config.has_token_provider() {
        return;
    }
    {
        let mut state = shared.token.lock();
        if state.refreshing {
            return;
        }
        state.refreshing = true;
    }
    let shared = Arc::clone(shared);
    tokio::spawn(async move {
        let result = async {
            let data = fetch_token(&shared, reason).await?;
            let normalized = token::normalize(data, token::now_seconds())?;
            let data = codec::to_json(&sonic_rs::json!({ "token": normalized.token.as_str() }))?;
            shared.token.lock().current = Some(normalized);
            let message = shared.encode(&shared.prefix.event("auth"), None, &data)?;
            shared.send_frames(epoch, vec![message]).await?;
            shared
                .cmd_tx
                .send(Command::RescheduleTokenRefresh { epoch })
                .await
                .map_err(|_| Error::Closed)
        }
        .await;
        shared.token.lock().refreshing = false;
        if let Err(error) = result {
            shared.report_error(error);
        }
    });
}

struct Session<'a> {
    sup: &'a mut Supervisor,
    shared: Arc<Shared>,
    epoch: u64,
    out: Vec<Message>,
    established: bool,
    activity_timeout: Duration,
    last_activity: Instant,
    pong_deadline: Option<Instant>,
    establish_deadline: Option<Instant>,
    refresh_at: Option<Instant>,
    end: Option<SessionEnd>,
}

impl<'a> Session<'a> {
    fn new(sup: &'a mut Supervisor, epoch: u64) -> Self {
        let shared = Arc::clone(&sup.shared);
        let now = Instant::now();
        Self {
            activity_timeout: shared.config.activity_timeout,
            establish_deadline: Some(now + shared.config.connect_timeout),
            shared,
            sup,
            epoch,
            out: Vec::new(),
            established: false,
            last_activity: now,
            pong_deadline: None,
            refresh_at: None,
            end: None,
        }
    }

    async fn run(
        &mut self,
        mut ws: WsStream,
        commands: &mut Commands,
        shutdown: &mut watch::Receiver<bool>,
    ) -> SessionEnd {
        loop {
            let ping_at = if self.established && self.pong_deadline.is_none() {
                Some(self.last_activity + self.activity_timeout)
            } else {
                None
            };
            let pong_deadline = self.pong_deadline;
            let establish_deadline = self.establish_deadline;
            let refresh_at = self.refresh_at;

            tokio::select! {
                _ = shutdown.changed() => {
                    let _ = ws.close(1000, "client shutdown").await;
                    return SessionEnd::Stop;
                }
                frame = ws.next() => {
                    match frame {
                        Some(Ok(Message::Text(bytes))) => self.handle_frame(bytes, true),
                        Some(Ok(Message::Binary(bytes))) => self.handle_frame(bytes, false),
                        Some(Ok(Message::Ping(_) | Message::Pong(_))) => self.touch(),
                        Some(Ok(Message::Close(reason))) => {
                            let (code, reason) = reason.map_or((1005, String::new()), |r| (r.code, r.reason));
                            return SessionEnd::Closed { code, reason };
                        }
                        Some(Err(sockudo_ws::Error::Closed(reason))) => {
                            let (code, reason) = reason.map_or((1005, String::new()), |r| (r.code, r.reason));
                            return SessionEnd::Closed { code, reason };
                        }
                        Some(Err(error)) => return SessionEnd::Lost(error.into()),
                        None => return SessionEnd::Closed { code: 1006, reason: String::new() },
                    }
                    if let Some(end) = self.end.take() {
                        let _ = flush(&mut ws, &mut self.out).await;
                        return end;
                    }
                    if let Err(error) = flush(&mut ws, &mut self.out).await {
                        return SessionEnd::Lost(error);
                    }
                }
                command = commands.recv() => {
                    let Some(command) = command else {
                        return SessionEnd::Stop;
                    };
                    let stop = self.drain_commands(command, commands);
                    if let Err(error) = flush(&mut ws, &mut self.out).await {
                        return SessionEnd::Lost(error);
                    }
                    if stop {
                        let _ = ws.close(1000, "client disconnect").await;
                        return SessionEnd::Stop;
                    }
                }
                () = sleep_opt(ping_at) => {
                    if let Ok(message) = self.shared.encode(&self.shared.prefix.event("ping"), None, b"{}") {
                        self.out.push(message);
                    }
                    self.pong_deadline = Some(Instant::now() + self.shared.config.pong_timeout);
                    if let Err(error) = flush(&mut ws, &mut self.out).await {
                        return SessionEnd::Lost(error);
                    }
                }
                () = sleep_opt(pong_deadline) => {
                    let _ = ws.close(1000, "pong timeout").await;
                    return SessionEnd::Lost(Error::PongTimeout);
                }
                () = sleep_opt(establish_deadline) => {
                    let _ = ws.close(1000, "handshake timeout").await;
                    return SessionEnd::Lost(Error::Timeout);
                }
                () = sleep_opt(refresh_at) => {
                    self.refresh_at = None;
                    spawn_token_refresh(&self.shared, self.epoch, TokenRequestReason::Refresh);
                }
            }
        }
    }

    /// Collect a batch of commands; returns whether to stop.
    fn drain_commands(&mut self, first: Command, commands: &mut Commands) -> bool {
        let mut next = Some(first);
        let mut drained = 0;
        while let Some(command) = next.take() {
            match command {
                Command::Frames { epoch, messages } => {
                    if epoch == self.epoch {
                        self.out.extend(messages);
                    }
                }
                Command::RescheduleTokenRefresh { epoch } => {
                    if epoch == self.epoch {
                        self.schedule_token_refresh();
                    }
                }
                Command::Disconnect { generation } => {
                    if generation >= self.sup.generation {
                        return true;
                    }
                }
            }
            drained += 1;
            if drained < MAX_BATCH {
                next = commands.try_recv().ok();
            }
        }
        false
    }

    fn touch(&mut self) {
        self.last_activity = Instant::now();
        self.pong_deadline = None;
    }

    fn schedule_token_refresh(&mut self) {
        self.refresh_at = None;
        if !self.shared.config.has_token_provider() {
            return;
        }
        let delay = {
            let state = self.shared.token.lock();
            state.current.as_ref().and_then(|current| {
                token::refresh_delay(
                    current,
                    token::now_seconds(),
                    self.shared.config.auth_refresh_leeway,
                )
            })
        };
        self.refresh_at = delay.map(|delay| Instant::now() + delay);
    }

    fn handle_frame(&mut self, raw: Bytes, is_text: bool) {
        self.touch();
        // The WebSocket layer hands out zero-copy slices of its receive
        // buffer. `Event::raw` may outlive this frame (streams, user clones),
        // and a retained slice would pin the whole buffer chunk, so give each
        // event an exactly-sized copy.
        let raw = Bytes::copy_from_slice(&raw);
        match codec::decode(raw, is_text, self.shared.config.wire_format) {
            Ok(event) => self.process(Arc::new(event), true),
            Err(error) => self.shared.report_error(error),
        }
    }

    /// The inbound pipeline: delta bookkeeping, dedup, recovery positions,
    /// protocol handling, then channel and global dispatch.
    fn process(&mut self, event: Arc<Event>, track_delta: bool) {
        if event.event.is_empty() {
            return;
        }
        let shared = Arc::clone(&self.shared);
        let name = event.event.as_str();
        let platform = strip_platform(name);
        let internal = strip_internal(name);

        // Bases must be recorded before deduplication: the server computes the
        // next delta against this message even if we have seen its id before.
        if track_delta
            && platform.is_none()
            && internal.is_none()
            && let (Some(delta), Some(sequence), Some(channel)) = (
                shared.delta.as_ref(),
                event.sequence,
                event.channel.as_deref(),
            )
        {
            delta.lock().handle_full_message(
                channel,
                name,
                event.conflation_key.as_deref(),
                sequence,
                &event.raw,
            );
        }

        if let (Some(dedup), Some(id)) = (self.sup.dedup.as_mut(), event.message_id.as_deref())
            && dedup.check_and_track(id)
        {
            return;
        }

        if shared.config.connection_recovery
            && let Some(channel) = event.channel.as_deref()
        {
            shared.update_position(channel, &event);
        }

        if let Some(platform) = platform
            && self.handle_platform(platform, &event)
        {
            return;
        }
        if internal == Some("watchlist_events") {
            user::on_watchlist_events(&shared, &event);
            return;
        }

        if let Some(channel) = event
            .channel
            .as_deref()
            .and_then(|name| shared.channel(name))
        {
            channel.handle(&shared, &event, &mut self.out);
        }
        if internal.is_none() {
            shared.dispatcher.emit(&event);
        }
    }

    /// Protocol events. Returns `true` when fully handled.
    fn handle_platform(&mut self, platform: &str, event: &Arc<Event>) -> bool {
        let shared = Arc::clone(&self.shared);
        match platform {
            "connection_established" => {
                self.on_established(event);
                true
            }
            "ping" => {
                if let Ok(message) = shared.encode(&shared.prefix.event("pong"), None, b"{}") {
                    self.out.push(message);
                }
                true
            }
            "pong" => true,
            "error" => {
                let payload = event.value();
                let code = payload.get("code").and_then(|code| code.as_i64());
                let message = payload
                    .get("message")
                    .and_then(|message| message.as_str())
                    .unwrap_or_default()
                    .to_owned();
                if code == Some(TOKEN_EXPIRED_CODE) {
                    spawn_token_refresh(&shared, self.epoch, TokenRequestReason::Expired);
                }
                shared.report_error(Error::Server { code, message });
                shared.dispatcher.emit(event);
                true
            }
            "auth_success" => {
                {
                    let mut state = shared.token.lock();
                    token::apply_auth_success(
                        &mut state.current,
                        &event.value(),
                        token::now_seconds(),
                    );
                }
                self.schedule_token_refresh();
                shared.dispatcher.emit(event);
                true
            }
            "token_expired" => {
                let payload = event.value();
                let code = payload.get("code").and_then(|code| code.as_i64());
                let reason = payload
                    .get("reason")
                    .and_then(|r| r.as_str())
                    .map(str::to_owned);
                match code {
                    Some(TOKEN_EXPIRED_CODE) => {
                        if shared.config.has_token_provider() {
                            spawn_token_refresh(&shared, self.epoch, TokenRequestReason::Expired);
                        } else {
                            shared.report_error(Error::TokenExpired { reason });
                        }
                    }
                    Some(TOKEN_REVOKED_CODE) => {
                        self.refresh_at = None;
                        shared.report_error(Error::TokenRevoked { reason });
                    }
                    _ => {}
                }
                shared.dispatcher.emit(event);
                true
            }
            "signin_success" => {
                user::on_signin_success(&shared, event);
                true
            }
            "resume_success" => {
                let summary = ResumeSuccess::from_value(&event.value());
                if shared.config.connection_recovery {
                    let mut positions = shared.positions.lock();
                    for recovered in summary.recovered {
                        if let Some(position) = recovered.position {
                            positions.insert(recovered.channel, position);
                        }
                    }
                }
                for failed in summary.failed {
                    self.on_resume_failed(&failed.channel);
                }
                shared.dispatcher.emit(event);
                true
            }
            "resume_failed" => {
                let failed = ResumeFailed::from_value(&event.value());
                self.on_resume_failed(&failed.channel);
                shared.dispatcher.emit(event);
                true
            }
            "delta_compression_enabled" => {
                if let Some(delta) = shared.delta.as_ref() {
                    delta.lock().handle_enabled(&event.data);
                }
                shared.dispatcher.emit(event);
                true
            }
            "delta_cache_sync" => {
                if let (Some(delta), Some(channel)) =
                    (shared.delta.as_ref(), event.channel.as_deref())
                    && let Err(error) = delta.lock().handle_cache_sync(channel, &event.data)
                {
                    shared.report_error(error);
                }
                true
            }
            "delta" => {
                self.on_delta(event);
                true
            }
            _ => false,
        }
    }

    fn on_established(&mut self, event: &Arc<Event>) {
        let shared = Arc::clone(&self.shared);
        let payload = event.value();
        let Some(socket_id) = payload
            .get("socket_id")
            .and_then(|id| id.as_str())
            .map(str::to_owned)
        else {
            self.end = Some(SessionEnd::Lost(Error::Decode(
                "connection_established without socket_id".into(),
            )));
            return;
        };
        if let Some(seconds) = payload
            .get("activity_timeout")
            .and_then(|value| value.as_f64())
            && seconds.is_finite()
            && seconds > 0.0
        {
            self.activity_timeout = self.activity_timeout.min(Duration::from_secs_f64(seconds));
        }
        self.established = true;
        self.establish_deadline = None;
        self.sup.attempts = 0;
        self.sup.ever_connected = true;
        self.sup.unavailable_at = None;

        shared.conn.lock().socket_id = Some(socket_id);
        shared.set_state(ConnectionState::Connected);

        if let Some(delta) = shared.delta.as_ref() {
            let payload = {
                let delta = delta.lock();
                delta
                    .auto_enable()
                    .then(|| delta.enable_payload())
                    .flatten()
            };
            if let Some(payload) = payload
                && let Ok(data) = codec::to_json(&payload)
                && let Ok(message) = shared.encode(
                    &shared.prefix.event("enable_delta_compression"),
                    None,
                    &data,
                )
            {
                self.out.push(message);
            }
        }

        let (frames, needs_auth) = prepare_resubscribe(&shared, self.epoch);
        self.out.extend(frames);
        let resume = self.resume_frame();
        if needs_auth.is_empty() {
            self.out.extend(resume);
        } else {
            // Authorize every protected channel, then send their subscribes
            // and only afterwards `resume`, so replay targets live subscriptions.
            let epoch = self.epoch;
            let task_shared = Arc::clone(&shared);
            tokio::spawn(async move {
                let frames = futures_util::future::join_all(
                    needs_auth
                        .iter()
                        .map(|channel| authorized_frame(&task_shared, channel, epoch)),
                )
                .await;
                let mut messages: Vec<Message> = frames.into_iter().flatten().collect();
                messages.extend(resume);
                if !messages.is_empty() {
                    let _ = task_shared.send_frames(epoch, messages).await;
                }
            });
        }

        self.schedule_token_refresh();
        user::attempt_sign_in(&shared);
        shared.flush_timeline();
    }

    fn resume_frame(&self) -> Option<Message> {
        if !self.shared.config.connection_recovery {
            return None;
        }
        let positions = {
            let positions = self.shared.positions.lock();
            if positions.is_empty() {
                return None;
            }
            let mut object = sonic_rs::Object::new();
            for (channel, position) in positions.iter() {
                object.insert(channel, sonic_rs::to_value(position).ok()?);
            }
            object
        };
        let data =
            codec::to_json(&sonic_rs::json!({ "channel_positions": Value::from(positions) }))
                .ok()?;
        self.shared
            .encode(&self.shared.prefix.event("resume"), None, &data)
            .ok()
    }

    fn on_resume_failed(&mut self, channel_name: &str) {
        let shared = Arc::clone(&self.shared);
        if channel_name.is_empty() {
            return;
        }
        shared.positions.lock().remove(channel_name);
        if let Some(delta) = shared.delta.as_ref() {
            delta.lock().clear_channel(channel_name);
        }
        // Continuity was lost: subscribe afresh so live delivery restarts.
        if let Some(channel) = shared.channel(channel_name) {
            // A resubscribe also resets the server's per-socket delta state for
            // the channel, matching the client state cleared above.
            let restart = {
                let mut state = channel.state.lock();
                match state.sub {
                    SubState::Pending { .. } => false,
                    SubState::Subscribed | SubState::Unsubscribed => {
                        state.sub = SubState::Pending {
                            epoch: self.epoch,
                            cancelled: false,
                        };
                        true
                    }
                }
            };
            if restart {
                start_subscription(&shared, &channel, self.epoch);
            }
        }
    }

    fn on_delta(&mut self, event: &Arc<Event>) {
        let shared = Arc::clone(&self.shared);
        let (Some(delta), Some(channel)) = (shared.delta.as_ref(), event.channel.as_deref()) else {
            return;
        };
        if shared.channel(channel).is_none() {
            return; // in flight after unsubscribe
        }
        let result = delta.lock().handle_delta(channel, &event.data);
        match result {
            Ok(reconstructed) => match codec::json::decode(reconstructed.bytes) {
                Ok(mut full) => {
                    if full.event.is_empty() {
                        full.event = reconstructed.event;
                    }
                    if full.channel.is_none() {
                        full.channel = Some(channel.to_owned());
                    }
                    self.process(Arc::new(full), false);
                }
                Err(error) => self.resync(channel, error),
            },
            Err(error) => self.resync(channel, error),
        }
    }

    fn resync(&mut self, channel: &str, error: Error) {
        let shared = Arc::clone(&self.shared);
        if let Some(delta) = shared.delta.as_ref() {
            delta.lock().resync(channel);
        }
        if let Ok(data) = codec::to_json(&sonic_rs::json!({ "channel": channel }))
            && let Ok(message) =
                shared.encode(&shared.prefix.event("delta_sync_error"), None, &data)
        {
            self.out.push(message);
        }
        shared.report_error(error);
    }
}

async fn flush(ws: &mut WsStream, out: &mut Vec<Message>) -> Result<()> {
    if out.is_empty() {
        return Ok(());
    }
    for message in out.drain(..) {
        ws.feed(message).await?;
    }
    ws.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn close_codes_follow_pusher_ranges() {
        assert_eq!(close_action(1000), None);
        assert_eq!(close_action(1006), None);
        assert_eq!(close_action(1002), Some(CloseAction::Backoff));
        assert_eq!(close_action(4000), Some(CloseAction::TlsOnly));
        assert_eq!(close_action(4001), Some(CloseAction::Refused));
        assert_eq!(close_action(4099), Some(CloseAction::Refused));
        assert_eq!(close_action(4100), Some(CloseAction::Backoff));
        assert_eq!(close_action(4201), Some(CloseAction::Retry));
        assert_eq!(close_action(4301), Some(CloseAction::Refused));
    }
}
