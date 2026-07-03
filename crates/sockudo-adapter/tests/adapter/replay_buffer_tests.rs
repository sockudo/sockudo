#[cfg(feature = "recovery")]
mod replay_buffer_regression {
    use bytes::Bytes;
    use sockudo_adapter::replay_buffer::{ReplayBuffer, ReplayLookup};
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn test_store_and_retrieve_messages() {
        let buf = ReplayBuffer::new(100, Duration::from_secs(60));

        buf.store("app1", "ch", None, 1, Bytes::from("msg1"));
        buf.store("app1", "ch", None, 2, Bytes::from("msg2"));
        buf.store("app1", "ch", None, 3, Bytes::from("msg3"));

        let msgs = buf
            .get_messages_after("app1", "ch", 0)
            .expect("should recover from 0");
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0], Bytes::from("msg1"));
        assert_eq!(msgs[1], Bytes::from("msg2"));
        assert_eq!(msgs[2], Bytes::from("msg3"));

        let msgs = buf
            .get_messages_after("app1", "ch", 1)
            .expect("should recover from 1");
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0], Bytes::from("msg2"));
        assert_eq!(msgs[1], Bytes::from("msg3"));

        let msgs = buf
            .get_messages_after("app1", "ch", 3)
            .expect("caught-up client should get Recovered(empty)");
        assert!(msgs.is_empty());

        assert!(buf.get_messages_after("app1", "unknown-ch", 0).is_none());
    }

    #[test]
    fn test_store_updates_stream_id() {
        let buf = ReplayBuffer::new(100, Duration::from_secs(60));

        buf.store("app1", "ch", Some("stream-1"), 1, Bytes::from("msg1"));
        buf.store("app1", "ch", Some("stream-2"), 2, Bytes::from("msg2"));

        match buf.get_messages_after_position("app1", "ch", Some("stream-1"), 0) {
            ReplayLookup::StreamReset { current_stream_id } => {
                assert_eq!(current_stream_id.as_deref(), Some("stream-2"));
            }
            other => panic!("expected StreamReset, got {}", variant_name(&other)),
        }

        match buf.get_messages_after_position("app1", "ch", Some("stream-2"), 0) {
            ReplayLookup::Expired => {}
            other => panic!("expected Expired, got {}", variant_name(&other)),
        }

        buf.store("app1", "ch", None, 3, Bytes::from("msg3"));

        match buf.get_messages_after_position("app1", "ch", Some("stream-2"), 0) {
            ReplayLookup::StreamReset { current_stream_id } => {
                assert!(current_stream_id.is_none());
            }
            other => panic!(
                "expected StreamReset after clearing stream, got {}",
                variant_name(&other)
            ),
        }
    }

    #[tokio::test]
    async fn test_concurrent_store_and_read() {
        let buf = Arc::new(ReplayBuffer::new(500, Duration::from_secs(60)));
        let mut handles = Vec::new();

        for task_idx in 0..10_u64 {
            let buf = Arc::clone(&buf);
            handles.push(tokio::spawn(async move {
                for j in 0..100_u64 {
                    let serial = task_idx * 100 + j + 1;
                    buf.store(
                        "app1",
                        "concurrent-ch",
                        Some("stream-1"),
                        serial,
                        Bytes::from(format!("msg-{}", serial)),
                    );
                }
            }));
        }

        for _ in 0..5 {
            let buf = Arc::clone(&buf);
            handles.push(tokio::spawn(async move {
                let _ = buf.get_messages_after("app1", "concurrent-ch", 0);
                let _ =
                    buf.get_messages_after_position("app1", "concurrent-ch", Some("stream-1"), 0);
            }));
        }

        for handle in handles {
            handle.await.expect("task panicked under concurrent access");
        }

        let _ = buf.get_messages_after("app1", "concurrent-ch", 0);
    }

    #[test]
    fn test_eviction_under_capacity() {
        let buf = ReplayBuffer::new(5, Duration::from_secs(60));

        for i in 1..=7_u64 {
            buf.store("app1", "ch", None, i, Bytes::from(format!("msg-{}", i)));
        }

        assert!(
            buf.get_messages_after("app1", "ch", 0).is_none(),
            "client that only saw the initial position must fail closed after evictions"
        );

        assert!(
            buf.get_messages_after("app1", "ch", 1).is_none(),
            "client that missed evicted messages must get None"
        );

        let msgs = buf
            .get_messages_after("app1", "ch", 2)
            .expect("boundary client should recover");
        assert_eq!(msgs.len(), 5);
    }

    #[tokio::test]
    async fn test_ttl_expiry() {
        let buf = ReplayBuffer::new(100, Duration::from_millis(50));
        buf.store("app1", "ttl-ch", None, 1, Bytes::from("fresh"));

        assert_eq!(
            buf.get_messages_after("app1", "ttl-ch", 0)
                .expect("readable before TTL")
                .len(),
            1
        );

        tokio::time::sleep(Duration::from_millis(120)).await;

        assert!(buf.get_messages_after("app1", "ttl-ch", 0).is_none());

        let buf2 = ReplayBuffer::new(100, Duration::from_millis(50));
        buf2.store("app2", "ttl-ch", None, 1, Bytes::from("will-expire"));
        tokio::time::sleep(Duration::from_millis(120)).await;
        buf2.evict_expired();

        assert!(buf2.get_messages_after("app2", "ttl-ch", 0).is_none());
    }

    #[test]
    fn test_next_serial_monotonic() {
        let buf = ReplayBuffer::new(100, Duration::from_secs(60));

        assert_eq!(buf.next_serial("app1", "ch"), 1);
        assert_eq!(buf.next_serial("app1", "ch"), 2);
        assert_eq!(buf.next_serial("app1", "ch"), 3);

        assert_eq!(buf.next_serial("app1", "other-ch"), 1);
        assert_eq!(buf.next_serial("app2", "ch"), 1);
    }

    #[test]
    fn test_empty_subscription_position_recovers_later_messages() {
        let buf = ReplayBuffer::new(100, Duration::from_secs(60));

        let initial = buf.current_position("app1", "idle");
        assert_eq!(initial.serial, 0);

        match buf.get_messages_after_position(
            "app1",
            "idle",
            Some(initial.stream_id.as_str()),
            initial.serial,
        ) {
            ReplayLookup::Recovered(messages) => assert!(messages.is_empty()),
            other => panic!("expected empty recovery, got {}", variant_name(&other)),
        }

        let next = buf.next_position("app1", "idle");
        assert_eq!(next.stream_id, initial.stream_id);
        assert_eq!(next.serial, 1);
        buf.store(
            "app1",
            "idle",
            Some(next.stream_id.as_str()),
            next.serial,
            Bytes::from("offline-msg"),
        );

        match buf.get_messages_after_position(
            "app1",
            "idle",
            Some(initial.stream_id.as_str()),
            initial.serial,
        ) {
            ReplayLookup::Recovered(messages) => {
                assert_eq!(messages, vec![Bytes::from("offline-msg")]);
            }
            other => panic!("expected recovered message, got {}", variant_name(&other)),
        }
    }

    #[test]
    fn test_ensure_position_aligns_empty_stream_with_durable_history() {
        let buf = ReplayBuffer::new(100, Duration::from_secs(60));

        let initial = buf.ensure_position("app1", "durable", "stream-1", 7);
        assert_eq!(initial.stream_id, "stream-1");
        assert_eq!(initial.serial, 7);

        let next = buf.next_position("app1", "durable");
        assert_eq!(next.stream_id, "stream-1");
        assert_eq!(next.serial, 8);
    }

    fn variant_name(lookup: &ReplayLookup) -> &'static str {
        match lookup {
            ReplayLookup::Recovered(_) => "Recovered",
            ReplayLookup::Expired => "Expired",
            ReplayLookup::StreamReset { .. } => "StreamReset",
        }
    }
}
