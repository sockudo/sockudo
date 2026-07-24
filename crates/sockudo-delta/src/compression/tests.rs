use super::*;

#[test]
fn explicit_vcdiff_round_trips_short_compatibility_payloads() {
    let base = br#"{"count":1,"status":"active"}"#;
    let target = br#"{"count":2,"status":"active"}"#;
    let delta = compute_vcdiff(base, target).expect("VCDIFF encoding");
    let decoded = oxidelta::compress::decoder::decode_all(base, &delta).expect("VCDIFF decoding");
    assert_eq!(decoded, target);
}
use sockudo_core::delta_types::DeltaCompressionConfig;

#[tokio::test]
async fn test_compression_first_message() {
    let config = DeltaCompressionConfig {
        min_message_size: 20, // Lower threshold for testing
        ..Default::default()
    };
    let manager = DeltaCompressionManager::new(config);
    let socket_id = SocketId::new();

    manager.enable_for_socket(&socket_id);

    let message = b"{\"test\":\"data\",\"value\":123,\"extra_field\":\"to_make_it_longer\"}";
    let result = manager
        .compress_message(&socket_id, "test-channel", "test-event", message, None)
        .await
        .unwrap();

    match result {
        CompressionResult::FullMessage { sequence, .. } => {
            assert_eq!(sequence, 0);
        }
        _ => panic!("Expected full message for first message"),
    }
}

#[tokio::test]
async fn test_compression_small_message_uncompressed() {
    let config = DeltaCompressionConfig {
        min_message_size: 100,
        ..Default::default()
    };
    let manager = DeltaCompressionManager::new(config);
    let socket_id = SocketId::new();

    manager.enable_for_socket(&socket_id);

    let small_message = b"small";
    let result = manager
        .compress_message(
            &socket_id,
            "test-channel",
            "test-event",
            small_message,
            None,
        )
        .await
        .unwrap();

    match result {
        CompressionResult::Uncompressed => {}
        _ => panic!("Expected uncompressed for small message"),
    }
}

#[tokio::test]
async fn test_compression_delta_creation() {
    let config = DeltaCompressionConfig {
        min_message_size: 20, // Lower threshold for testing
        ..Default::default()
    };
    let manager = DeltaCompressionManager::new(config);
    let socket_id = SocketId::new();

    manager.enable_for_socket(&socket_id);

    let message1 = b"{\"test\":\"data\",\"value\":123,\"extra\":\"content_here_to_make_longer\"}";
    let message2 = b"{\"test\":\"data\",\"value\":456,\"extra\":\"content_here_to_make_longer\"}";

    // First message
    let result1 = manager
        .compress_message(&socket_id, "test-channel", "test-event", message1, None)
        .await
        .unwrap();
    // Store the first message
    manager
        .store_sent_message(
            &socket_id,
            "test-channel",
            "test-event",
            message1.to_vec(),
            matches!(result1, CompressionResult::FullMessage { .. }),
            None,
        )
        .await
        .unwrap();

    // Second message should produce delta
    let result = manager
        .compress_message(&socket_id, "test-channel", "test-event", message2, None)
        .await
        .unwrap();
    // Store the second message
    manager
        .store_sent_message(
            &socket_id,
            "test-channel",
            "test-event",
            message2.to_vec(),
            matches!(result, CompressionResult::FullMessage { .. }),
            None,
        )
        .await
        .unwrap();

    match result {
        CompressionResult::Delta {
            delta, sequence, ..
        } => {
            assert_eq!(sequence, 1);
            assert!(delta.len() < message2.len());
        }
        _ => panic!("Expected delta for similar message"),
    }
}

#[tokio::test]
async fn test_xdelta3_algorithm() {
    let config = DeltaCompressionConfig {
        algorithm: DeltaAlgorithm::Xdelta3,
        min_message_size: 20,
        ..Default::default()
    };
    let manager = DeltaCompressionManager::new(config);
    let socket_id = SocketId::new();

    manager.enable_for_socket(&socket_id);

    let message1 = b"{\"test\":\"data\",\"value\":123,\"extra\":\"content_here_to_make_longer\"}";
    let message2 = b"{\"test\":\"data\",\"value\":456,\"extra\":\"content_here_to_make_longer\"}";

    // First message
    let result1 = manager
        .compress_message(&socket_id, "test-channel", "test-event", message1, None)
        .await
        .unwrap();
    manager
        .store_sent_message(
            &socket_id,
            "test-channel",
            "test-event",
            message1.to_vec(),
            matches!(result1, CompressionResult::FullMessage { .. }),
            None,
        )
        .await
        .unwrap();

    // Second message should produce delta
    let result = manager
        .compress_message(&socket_id, "test-channel", "test-event", message2, None)
        .await
        .unwrap();
    manager
        .store_sent_message(
            &socket_id,
            "test-channel",
            "test-event",
            message2.to_vec(),
            matches!(result, CompressionResult::FullMessage { .. }),
            None,
        )
        .await
        .unwrap();

    match result {
        CompressionResult::Delta {
            delta, sequence, ..
        } => {
            assert_eq!(sequence, 1);
            assert!(!delta.is_empty());
        }
        _ => panic!("Expected delta for xdelta3 algorithm"),
    }
}

#[tokio::test]
async fn test_algorithm_comparison() {
    let algorithms = [DeltaAlgorithm::Fossil, DeltaAlgorithm::Xdelta3];

    let message1 = b"{\"counter\":0,\"data\":\"some_data_that_stays_the_same\"}";
    let message2 = b"{\"counter\":1,\"data\":\"some_data_that_stays_the_same\"}";

    for algorithm in &algorithms {
        let config = DeltaCompressionConfig {
            algorithm: *algorithm,
            min_message_size: 20,
            ..Default::default()
        };
        let manager = DeltaCompressionManager::new(config);
        let socket_id = SocketId::new();

        manager.enable_for_socket(&socket_id);

        // First message
        let result1 = manager
            .compress_message(&socket_id, "test-channel", "test-event", message1, None)
            .await
            .unwrap();
        manager
            .store_sent_message(
                &socket_id,
                "test-channel",
                "test-event",
                message1.to_vec(),
                matches!(result1, CompressionResult::FullMessage { .. }),
                None,
            )
            .await
            .unwrap();

        // Second message
        let result = manager
            .compress_message(&socket_id, "test-channel", "test-event", message2, None)
            .await
            .unwrap();
        manager
            .store_sent_message(
                &socket_id,
                "test-channel",
                "test-event",
                message2.to_vec(),
                matches!(result, CompressionResult::FullMessage { .. }),
                None,
            )
            .await
            .unwrap();

        // All algorithms should produce a delta
        match result {
            CompressionResult::Delta { delta, .. } => {
                assert!(
                    !delta.is_empty(),
                    "Algorithm {:?} produced empty delta",
                    algorithm
                );
            }
            _ => panic!("Algorithm {:?} did not produce delta", algorithm),
        }
    }
}

// =========================================================================
// ENCRYPTED CHANNEL DETECTION TESTS
// =========================================================================

#[test]
fn test_is_encrypted_channel() {
    // Positive cases - encrypted channels
    assert!(DeltaCompressionManager::is_encrypted_channel(
        "private-encrypted-chat"
    ));
    assert!(DeltaCompressionManager::is_encrypted_channel(
        "private-encrypted-"
    ));
    assert!(DeltaCompressionManager::is_encrypted_channel(
        "private-encrypted-my-channel"
    ));
    assert!(DeltaCompressionManager::is_encrypted_channel(
        "private-encrypted-123"
    ));

    // Negative cases - not encrypted channels
    assert!(!DeltaCompressionManager::is_encrypted_channel(
        "private-chat"
    ));
    assert!(!DeltaCompressionManager::is_encrypted_channel(
        "presence-room"
    ));
    assert!(!DeltaCompressionManager::is_encrypted_channel(
        "public-channel"
    ));
    assert!(!DeltaCompressionManager::is_encrypted_channel(
        "encrypted-private"
    )); // wrong prefix order
    assert!(!DeltaCompressionManager::is_encrypted_channel(
        "privateencrypted-chat"
    )); // no dash
    assert!(!DeltaCompressionManager::is_encrypted_channel(""));
}

#[tokio::test]
async fn test_encrypted_channel_skips_compression() {
    let config = DeltaCompressionConfig {
        min_message_size: 10,
        ..Default::default()
    };
    let manager = DeltaCompressionManager::new(config);
    let socket_id = SocketId::new();

    manager.enable_for_socket(&socket_id);

    // Large message that would normally be compressed
    let message = b"{\"encrypted_data\":\"very_long_encrypted_payload_that_is_definitely_over_100_bytes_to_trigger_compression_normally\"}";

    // For encrypted channel, should return Uncompressed
    let result = manager
        .compress_message(
            &socket_id,
            "private-encrypted-chat",
            "message",
            message,
            None,
        )
        .await
        .unwrap();

    match result {
        CompressionResult::Uncompressed => {
            // Expected - encrypted channels should skip compression
        }
        _ => panic!(
            "Expected Uncompressed for encrypted channel, got: {:?}",
            result
        ),
    }
}

// =========================================================================
// COMPRESSION WITH PER-CHANNEL ALGORITHM TESTS
// =========================================================================

#[tokio::test]
async fn test_compression_uses_per_channel_algorithm() {
    let config = DeltaCompressionConfig {
        algorithm: DeltaAlgorithm::Fossil, // Global default
        min_message_size: 20,
        ..Default::default()
    };
    let manager = DeltaCompressionManager::new(config);
    let socket_id = SocketId::new();

    manager.enable_for_socket(&socket_id);

    // Set Xdelta3 for this channel
    manager.set_channel_delta_settings(
        &socket_id,
        "test-channel",
        Some(true),
        Some(DeltaAlgorithm::Xdelta3),
    );

    // First message (full)
    let message1 = b"{\"test\":\"data\",\"value\":123,\"extra\":\"content_to_make_longer\"}";
    let result1 = manager
        .compress_message(&socket_id, "test-channel", "event", message1, None)
        .await
        .unwrap();

    match result1 {
        CompressionResult::FullMessage { .. } => {}
        _ => panic!("Expected full message for first message"),
    }

    // Store the message
    manager
        .store_sent_message(
            &socket_id,
            "test-channel",
            "event",
            message1.to_vec(),
            true,
            None,
        )
        .await
        .unwrap();

    // Second message (should be delta with Xdelta3)
    let message2 = b"{\"test\":\"data\",\"value\":456,\"extra\":\"content_to_make_longer\"}";
    let result2 = manager
        .compress_message(&socket_id, "test-channel", "event", message2, None)
        .await
        .unwrap();

    match result2 {
        CompressionResult::Delta { algorithm, .. } => {
            assert_eq!(algorithm, DeltaAlgorithm::Xdelta3);
        }
        CompressionResult::FullMessage { .. } => {
            // Acceptable if delta wasn't beneficial
        }
        _ => panic!("Expected delta or full message"),
    }
}
