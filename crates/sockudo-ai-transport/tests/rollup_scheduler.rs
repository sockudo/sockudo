use sockudo_ai_transport::{
    DeferredFanoutContext, RollupConfig, RollupDeliveryReason, RollupEngine,
};
use sockudo_core::message_envelope::MessageEnvelope;
use sockudo_core::websocket::SocketId;
use sockudo_protocol::messages::{AiExtras, MessageData, MessageExtras, PusherMessage};
use sockudo_protocol::versioned_messages::{
    MessageAction, MessageVersionMetadata, apply_runtime_metadata,
};
use std::collections::HashMap;
use std::sync::{Arc, Barrier};

fn append(serial: &str, data: &str, fragment: &str, version: i64) -> PusherMessage {
    let mut message = PusherMessage {
        event: Some(MessageAction::Append.v2_event_name()),
        channel: Some("ai:scheduler".to_string()),
        data: Some(MessageData::String(data.to_string())),
        name: None,
        user_id: None,
        tags: None,
        sequence: None,
        conflation_key: None,
        message_id: None,
        stream_id: None,
        serial: None,
        idempotency_key: None,
        extras: Some(MessageExtras {
            ai: Some(AiExtras {
                transport: Some(HashMap::from([
                    ("status".to_string(), "streaming".to_string()),
                    ("append-fragment".to_string(), fragment.to_string()),
                ])),
                codec: None,
            }),
            ..MessageExtras::default()
        }),
        delta_sequence: None,
        delta_conflation_key: None,
    };
    apply_runtime_metadata(
        &mut message,
        MessageAction::Append,
        serial,
        &MessageVersionMetadata {
            serial: format!("v{version}"),
            client_id: None,
            timestamp_ms: version,
            description: None,
            metadata: None,
        },
        Some(version as u64),
    );
    message
}

fn terminal(serial: &str, version: i64) -> PusherMessage {
    let mut message = append(serial, "terminal", "!", version);
    message
        .extras
        .as_mut()
        .and_then(|extras| extras.ai.as_mut())
        .and_then(|ai| ai.transport.as_mut())
        .expect("transport headers")
        .insert("status".to_string(), "complete".to_string());
    message
}

fn context(
    socket: SocketId,
    force_full_message: bool,
    delivery_serial: u64,
) -> DeferredFanoutContext {
    let envelope = MessageEnvelope {
        delivery_serial: Some(delivery_serial),
        ..MessageEnvelope::default()
    };
    DeferredFanoutContext {
        exclude_socket: Some(socket),
        start_time_ms: Some(delivery_serial as f64),
        force_full_message,
        envelope: Some(envelope),
    }
}

#[test]
fn boundary_deadline_polls_only_due_entries() {
    let engine = RollupEngine::new(RollupConfig::default());
    let socket = SocketId { high: 1, low: 2 };
    let _ = engine.ingest_with_context(
        "app",
        "ai:scheduler",
        append("m1", "a", "a", 1),
        0,
        context(socket, false, 1),
    );
    let _ = engine.ingest_with_context(
        "app",
        "ai:scheduler",
        append("m1", "ab", "b", 2),
        10,
        context(socket, false, 2),
    );

    let early = engine.poll_due(39, 16);
    assert!(early.deliveries.is_empty());
    assert!(early.examined_entries <= engine.config().shards * 2);

    let due = engine.poll_due(40, 16);
    assert_eq!(due.deliveries.len(), 1);
    assert_eq!(due.deliveries[0].reason, RollupDeliveryReason::Deadline);
}

#[test]
fn terminal_removal_invalidates_stale_deadlines() {
    let engine = RollupEngine::new(RollupConfig::default());
    let socket = SocketId { high: 3, low: 4 };
    let _ = engine.ingest_with_context(
        "app",
        "ai:scheduler",
        append("m1", "a", "a", 1),
        0,
        context(socket, false, 1),
    );
    let _ = engine.ingest_with_context(
        "app",
        "ai:scheduler",
        terminal("m1", 2),
        1,
        context(socket, false, 2),
    );

    assert!(engine.poll_due(40, 16).deliveries.is_empty());
    assert!(engine.poll_due(60_000, 16).deliveries.is_empty());
    assert_eq!(engine.active_streams(), 0);
}

#[test]
fn backward_clock_jump_does_not_flush_early() {
    let engine = RollupEngine::new(RollupConfig::default());
    let socket = SocketId { high: 5, low: 6 };
    let _ = engine.ingest_with_context(
        "app",
        "ai:scheduler",
        append("m1", "a", "a", 1),
        100,
        context(socket, false, 1),
    );
    let _ = engine.ingest_with_context(
        "app",
        "ai:scheduler",
        append("m1", "ab", "b", 2),
        90,
        context(socket, false, 2),
    );

    assert!(engine.poll_due(50, 16).deliveries.is_empty());
    assert!(engine.poll_due(139, 16).deliveries.is_empty());
    assert_eq!(engine.poll_due(140, 16).deliveries.len(), 1);
}

#[test]
fn deferred_delivery_keeps_latest_mixed_mode_echo_and_continuity_context() {
    let engine = RollupEngine::new(RollupConfig::default());
    let first_socket = SocketId { high: 7, low: 8 };
    let later_socket = SocketId { high: 9, low: 10 };
    let _ = engine.ingest_with_context(
        "app",
        "ai:scheduler",
        append("m1", "a", "a", 1),
        0,
        context(first_socket, false, 1),
    );
    let _ = engine.ingest_with_context(
        "app",
        "ai:scheduler",
        append("m1", "ab", "b", 2),
        10,
        context(later_socket, true, 2),
    );

    let delivery = engine
        .poll_due(40, 16)
        .deliveries
        .pop()
        .expect("due delivery");
    assert_eq!(delivery.context.exclude_socket, Some(later_socket));
    assert!(delivery.context.force_full_message);
    assert_eq!(
        delivery
            .context
            .envelope
            .as_ref()
            .and_then(|envelope| envelope.delivery_serial),
        Some(2)
    );
}

#[test]
fn first_delivery_retains_no_payload_and_tracking_overhead_is_bounded() {
    let engine = RollupEngine::new(RollupConfig::default());
    let _ = engine.ingest(
        "app",
        "ai:scheduler",
        append("m1", &"x".repeat(1_000_000), "x", 1),
        0,
    );

    let stats = engine.tracking_stats();
    assert_eq!(stats.active_streams, 1);
    assert_eq!(stats.pending_payload_streams, 0);
    assert!(RollupEngine::tracking_overhead_bytes_per_stream() <= 512);
}

#[test]
fn empty_poll_work_is_independent_of_active_stream_cardinality() {
    let engine = RollupEngine::new(RollupConfig {
        max_active_streams_per_channel: 60_000,
        ..RollupConfig::default()
    });
    for index in 0..50_000 {
        let _ = engine.ingest(
            "app",
            &format!("ai:scheduler:{index}"),
            append(&format!("m{index}"), "x", "x", index as i64),
            0,
        );
    }

    let poll = engine.poll_due(1, 64);
    assert!(poll.deliveries.is_empty());
    assert!(poll.examined_entries <= engine.config().shards * 2);
}

#[test]
fn shutdown_drain_flushes_pending_content_once_and_removes_tracking() {
    let engine = RollupEngine::new(RollupConfig::default());
    let socket = SocketId { high: 11, low: 12 };
    let _ = engine.ingest_with_context(
        "app",
        "ai:shutdown",
        append("m1", "a", "a", 1),
        0,
        context(socket, false, 1),
    );
    let _ = engine.ingest_with_context(
        "app",
        "ai:shutdown",
        append("m1", "ab", "b", 2),
        10,
        context(socket, true, 2),
    );

    let drained = engine.drain_pending(1);
    assert_eq!(drained.deliveries.len(), 1);
    assert_eq!(
        drained.deliveries[0].reason,
        RollupDeliveryReason::ShutdownDrain
    );
    assert!(drained.deliveries[0].context.force_full_message);
    assert_eq!(drained.active_stream_deltas[0].delta, -1);
    assert_eq!(engine.active_streams(), 0);
    assert!(engine.drain_pending(1).deliveries.is_empty());
    assert!(engine.poll_due(10_000, 16).deliveries.is_empty());
}

#[test]
fn terminal_race_never_reorders_a_pending_flush_after_terminal() {
    let engine = Arc::new(RollupEngine::new(RollupConfig::default()));
    let socket = SocketId { high: 13, low: 14 };
    let _ = engine.ingest_with_context(
        "app",
        "ai:terminal-race",
        append("m1", "a", "a", 1),
        0,
        context(socket, false, 1),
    );
    let barrier = Arc::new(Barrier::new(3));

    let append_worker = {
        let engine = Arc::clone(&engine);
        let barrier = Arc::clone(&barrier);
        std::thread::spawn(move || {
            barrier.wait();
            engine.ingest_with_context(
                "app",
                "ai:terminal-race",
                append("m1", "ab", "b", 2),
                10,
                context(socket, false, 2),
            )
        })
    };
    let terminal_worker = {
        let engine = Arc::clone(&engine);
        let barrier = Arc::clone(&barrier);
        std::thread::spawn(move || {
            barrier.wait();
            engine.ingest_with_context(
                "app",
                "ai:terminal-race",
                terminal("m1", 3),
                11,
                context(socket, true, 3),
            )
        })
    };
    barrier.wait();
    let append_result = append_worker.join().unwrap();
    let terminal_result = terminal_worker.join().unwrap();

    if let Some(terminal_index) = terminal_result
        .deliveries
        .iter()
        .position(|delivery| delivery.reason == RollupDeliveryReason::Terminal)
        && let Some(flush_index) = terminal_result
            .deliveries
            .iter()
            .position(|delivery| delivery.reason == RollupDeliveryReason::TerminalFlush)
    {
        assert!(flush_index < terminal_index);
    }
    assert!(append_result.deliveries.len() <= 1);
    assert!(engine.active_streams() <= 1);
}

#[test]
fn terminal_wins_safely_after_due_discovery_but_before_claim() {
    let engine = RollupEngine::new(RollupConfig::default());
    let socket = SocketId { high: 15, low: 16 };
    let _ = engine.ingest_with_context(
        "app",
        "ai:claim-race",
        append("m1", "a", "a", 1),
        0,
        context(socket, false, 1),
    );
    let _ = engine.ingest_with_context(
        "app",
        "ai:claim-race",
        append("m1", "ab", "b", 2),
        10,
        context(socket, false, 2),
    );
    let token = engine
        .poll_due_tokens(40, 1)
        .tokens
        .pop()
        .expect("due token");

    let terminal = engine.ingest_with_context(
        "app",
        "ai:claim-race",
        terminal("m1", 3),
        41,
        context(socket, true, 3),
    );
    assert_eq!(terminal.deliveries.len(), 2);
    assert_eq!(
        terminal.deliveries[0].reason,
        RollupDeliveryReason::TerminalFlush
    );
    assert_eq!(
        terminal.deliveries[1].reason,
        RollupDeliveryReason::Terminal
    );
    assert!(engine.claim_due(token).deliveries.is_empty());
}

#[test]
fn at_least_999_per_mille_due_by_window_plus_five_ms_under_documented_load() {
    const STREAMS: usize = 2_000;
    let engine = RollupEngine::new(RollupConfig::default());
    for index in 0..STREAMS {
        let channel = format!("ai:deadline:{index}");
        let serial = format!("m{index}");
        let _ = engine.ingest(
            "app",
            &channel,
            append(&serial, "a", "a", (index * 2) as i64),
            0,
        );
        let _ = engine.ingest(
            "app",
            &channel,
            append(&serial, "ab", "b", (index * 2 + 1) as i64),
            10,
        );
    }

    let poll = engine.poll_due(45, STREAMS * 2);
    let on_time_per_mille = poll.deliveries.len() * 1_000 / STREAMS;
    assert!(on_time_per_mille >= 999, "on-time={on_time_per_mille}‰");
}
