// Conflation-key extraction from message payloads.

use sonic_rs::Value;
use sonic_rs::prelude::*;

use crate::manager::DeltaCompressionManager;

impl DeltaCompressionManager {
    /// Extract conflation key from message bytes using a specific path (public for broadcast optimization).
    /// Returns empty string if extraction fails.
    pub fn extract_conflation_key_from_path(&self, message_bytes: &[u8], key_path: &str) -> String {
        self.extract_conflation_key_with_path(message_bytes, key_path)
    }

    /// Get the conflation key path from config (public for broadcast optimization)
    pub fn get_conflation_key_path(&self) -> Option<&String> {
        self.config.conflation_key_path.as_ref()
    }

    /// Extract conflation key from message bytes using a specific path.
    /// Returns empty string if extraction fails.
    pub(crate) fn extract_conflation_key_with_path(
        &self,
        message_bytes: &[u8],
        key_path: &str,
    ) -> String {
        let to_string = |v: &Value| {
            if let Some(s) = v.as_str() {
                s.to_string()
            } else if let Some(n) = v.as_number() {
                n.to_string()
            } else if let Some(b) = v.as_bool() {
                b.to_string()
            } else {
                format!("{}", v)
            }
        };

        let json_value: Value = match sonic_rs::from_slice(message_bytes) {
            Ok(v) => v,
            Err(_) => return String::new(),
        };

        let parts: Vec<&str> = key_path.split('.').collect();
        let mut current = &json_value;

        for part in &parts {
            current = match current.get(part) {
                Some(v) => v,
                None => {
                    if parts.len() == 1 {
                        if let Some(data_obj) = json_value.get("data").and_then(|v| v.as_object())
                            && let Some(v) = data_obj.get(part)
                        {
                            return to_string(v);
                        }
                        if let Some(data_str_val) = json_value.get("data")
                            && let Some(data_str) = data_str_val.as_str()
                            && let Ok(data_obj) = sonic_rs::from_str::<sonic_rs::Object>(data_str)
                            && let Some(v) = data_obj.get(part)
                        {
                            return to_string(v);
                        }
                    }

                    return String::new();
                }
            };
        }

        to_string(current)
    }

    /// Extract conflation key from message bytes based on globally configured path.
    /// Returns empty string if no conflation key is configured or extraction fails.
    #[allow(dead_code)]
    fn extract_conflation_key(&self, message_bytes: &[u8]) -> String {
        let key_path = match &self.config.conflation_key_path {
            Some(path) => path,
            None => return String::new(),
        };

        self.extract_conflation_key_with_path(message_bytes, key_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CompressionResult;
    use sockudo_core::delta_types::DeltaCompressionConfig;
    use sockudo_core::websocket::SocketId;

    #[test]
    fn test_conflation_key_extraction_root_level() {
        let config = DeltaCompressionConfig {
            conflation_key_path: Some("asset".to_string()),
            min_message_size: 20,
            ..Default::default()
        };
        let manager = DeltaCompressionManager::new(config);

        let msg1 = br#"{"asset":"BTC","price":"100.00"}"#;
        let msg2 = br#"{"asset":"ETH","price":"1.00"}"#;
        let msg3 = br#"{"asset":"BTC","price":"100.01"}"#;

        assert_eq!(manager.extract_conflation_key(msg1), "BTC");
        assert_eq!(manager.extract_conflation_key(msg2), "ETH");
        assert_eq!(manager.extract_conflation_key(msg3), "BTC");
    }

    #[test]
    fn test_conflation_key_extraction_nested() {
        let config = DeltaCompressionConfig {
            conflation_key_path: Some("data.symbol".to_string()),
            min_message_size: 20,
            ..Default::default()
        };
        let manager = DeltaCompressionManager::new(config);

        let msg = br#"{"data":{"symbol":"AAPL","value":150}}"#;
        assert_eq!(manager.extract_conflation_key(msg), "AAPL");
    }

    #[test]
    fn test_conflation_key_extraction_no_path() {
        let config = DeltaCompressionConfig {
            conflation_key_path: None,
            min_message_size: 20,
            ..Default::default()
        };
        let manager = DeltaCompressionManager::new(config);

        let msg = br#"{"asset":"BTC","price":"100.00"}"#;
        assert_eq!(manager.extract_conflation_key(msg), "");
    }

    #[tokio::test]
    async fn test_conflation_key_separate_delta_states() {
        let config = DeltaCompressionConfig {
            conflation_key_path: Some("asset".to_string()),
            min_message_size: 20,
            ..Default::default()
        };
        let manager = DeltaCompressionManager::new(config);
        let socket_id = SocketId::new();

        manager.enable_for_socket(&socket_id);

        // Messages for different assets
        let btc1 = br#"{"asset":"BTC","price":"100.00","volume":"1000","timestamp":"2024-01-01T00:00:00Z"}"#;
        let eth1 =
            br#"{"asset":"ETH","price":"1.00","volume":"500","timestamp":"2024-01-01T00:00:00Z"}"#;
        let btc2 = br#"{"asset":"BTC","price":"100.01","volume":"1000","timestamp":"2024-01-01T00:00:01Z"}"#;
        let eth2 =
            br#"{"asset":"ETH","price":"1.01","volume":"500","timestamp":"2024-01-01T00:00:01Z"}"#;

        // First BTC message - should be full
        let result1 = manager
            .compress_message(&socket_id, "prices", "update", btc1, None)
            .await
            .unwrap();
        assert!(matches!(
            result1,
            CompressionResult::FullMessage { sequence: 0, .. }
        ));
        manager
            .store_sent_message(
                &socket_id,
                "prices",
                "update",
                btc1.to_vec(),
                matches!(result1, CompressionResult::FullMessage { .. }),
                None,
            )
            .await
            .unwrap();

        // First ETH message - should be full (different conflation key)
        let result2 = manager
            .compress_message(&socket_id, "prices", "update", eth1, None)
            .await
            .unwrap();
        assert!(matches!(
            result2,
            CompressionResult::FullMessage { sequence: 0, .. }
        ));
        manager
            .store_sent_message(
                &socket_id,
                "prices",
                "update",
                eth1.to_vec(),
                matches!(result2, CompressionResult::FullMessage { .. }),
                None,
            )
            .await
            .unwrap();

        // Second BTC message - should be delta (compared to first BTC)
        let result3 = manager
            .compress_message(&socket_id, "prices", "update", btc2, None)
            .await
            .unwrap();
        manager
            .store_sent_message(
                &socket_id,
                "prices",
                "update",
                btc2.to_vec(),
                matches!(result3, CompressionResult::FullMessage { .. }),
                None,
            )
            .await
            .unwrap();
        match result3 {
            CompressionResult::Delta {
                delta, sequence, ..
            } => {
                assert_eq!(sequence, 1);
                // Delta should be small since only price changed slightly
                assert!(delta.len() < btc2.len() / 2);
            }
            _ => panic!("Expected delta for second BTC message"),
        }

        // Second ETH message - should be delta (compared to first ETH)
        let result4 = manager
            .compress_message(&socket_id, "prices", "update", eth2, None)
            .await
            .unwrap();
        manager
            .store_sent_message(
                &socket_id,
                "prices",
                "update",
                eth2.to_vec(),
                matches!(result4, CompressionResult::FullMessage { .. }),
                None,
            )
            .await
            .unwrap();
        match result4 {
            CompressionResult::Delta {
                delta, sequence, ..
            } => {
                assert_eq!(sequence, 1);
                // Delta should be small since only price changed slightly
                assert!(delta.len() < eth2.len() / 2);
            }
            _ => panic!("Expected delta for second ETH message"),
        }
    }

    #[tokio::test]
    async fn test_conflation_improves_compression_efficiency() {
        // Without conflation - interleaved messages have poor compression
        let config_no_conflation = DeltaCompressionConfig {
            conflation_key_path: None,
            min_message_size: 20,
            ..Default::default()
        };
        let manager_no_conflation = DeltaCompressionManager::new(config_no_conflation);
        let socket_id_1 = SocketId::new();
        manager_no_conflation.enable_for_socket(&socket_id_1);

        // With conflation - messages grouped by asset have good compression
        let config_conflation = DeltaCompressionConfig {
            conflation_key_path: Some("asset".to_string()),
            min_message_size: 20,
            ..Default::default()
        };
        let manager_conflation = DeltaCompressionManager::new(config_conflation);
        let socket_id_2 = SocketId::new();
        manager_conflation.enable_for_socket(&socket_id_2);

        let btc1 = br#"{"asset":"BTC","price":"100.00","volume":"1000","extra_data":"some_long_string_here"}"#;
        let eth1 = br#"{"asset":"ETH","price":"1.00","volume":"500","extra_data":"some_long_string_here"}"#;
        let btc2 = br#"{"asset":"BTC","price":"100.01","volume":"1000","extra_data":"some_long_string_here"}"#;

        // Without conflation: BTC1 -> ETH1 -> BTC2
        manager_no_conflation
            .compress_message(&socket_id_1, "prices", "update", btc1, None)
            .await
            .unwrap();
        manager_no_conflation
            .compress_message(&socket_id_1, "prices", "update", eth1, None)
            .await
            .unwrap();
        let result_no_conflation = manager_no_conflation
            .compress_message(&socket_id_1, "prices", "update", btc2, None)
            .await
            .unwrap();

        // With conflation: BTC1 -> ETH1 -> BTC2 (but BTC2 compares to BTC1)
        manager_conflation
            .compress_message(&socket_id_2, "prices", "update", btc1, None)
            .await
            .unwrap();
        manager_conflation
            .compress_message(&socket_id_2, "prices", "update", eth1, None)
            .await
            .unwrap();
        let result_conflation = manager_conflation
            .compress_message(&socket_id_2, "prices", "update", btc2, None)
            .await
            .unwrap();

        // With conflation should produce smaller delta
        match (result_no_conflation, result_conflation) {
            (
                CompressionResult::Delta {
                    delta: delta_no_conflation,
                    ..
                },
                CompressionResult::Delta {
                    delta: delta_conflation,
                    ..
                },
            ) => {
                // Conflation should produce smaller or equal delta
                assert!(
                    delta_conflation.len() <= delta_no_conflation.len(),
                    "Conflation delta ({}) should be <= non-conflation delta ({})",
                    delta_conflation.len(),
                    delta_no_conflation.len()
                );
            }
            (CompressionResult::FullMessage { .. }, CompressionResult::Delta { .. }) => {
                // Even better - without conflation sends full, with conflation sends delta
            }
            _ => {
                // Other combinations are acceptable
            }
        }
    }

    #[tokio::test]
    async fn test_conflation_state_cleanup() {
        let config = DeltaCompressionConfig {
            conflation_key_path: Some("asset".to_string()),
            max_conflation_states_per_channel: Some(2), // Only keep 2 conflation groups
            min_message_size: 20,
            ..Default::default()
        };
        let manager = DeltaCompressionManager::new(config);
        let socket_id = SocketId::new();
        manager.enable_for_socket(&socket_id);

        // Send messages for 3 different assets
        let btc = br#"{"asset":"BTC","price":"100.00","data":"some_long_content_here"}"#;
        let eth = br#"{"asset":"ETH","price":"1.00","data":"some_long_content_here"}"#;
        let ada = br#"{"asset":"ADA","price":"0.50","data":"some_long_content_here"}"#;

        let result1 = manager
            .compress_message(&socket_id, "prices", "update", btc, None)
            .await
            .unwrap();
        manager
            .store_sent_message(
                &socket_id,
                "prices",
                "update",
                btc.to_vec(),
                matches!(result1, CompressionResult::FullMessage { .. }),
                None,
            )
            .await
            .unwrap();

        let result2 = manager
            .compress_message(&socket_id, "prices", "update", eth, None)
            .await
            .unwrap();
        manager
            .store_sent_message(
                &socket_id,
                "prices",
                "update",
                eth.to_vec(),
                matches!(result2, CompressionResult::FullMessage { .. }),
                None,
            )
            .await
            .unwrap();

        let result3 = manager
            .compress_message(&socket_id, "prices", "update", ada, None)
            .await
            .unwrap();
        manager
            .store_sent_message(
                &socket_id,
                "prices",
                "update",
                ada.to_vec(),
                matches!(result3, CompressionResult::FullMessage { .. }),
                None,
            )
            .await
            .unwrap();

        // Get socket state and check channel state
        let socket_state = manager.socket_states.get(&socket_id).unwrap();
        let channel_state = socket_state.get_channel_state("prices").unwrap();

        // Collect which states exist for debugging
        let existing_keys: Vec<String> = channel_state
            .conflation_groups
            .iter()
            .map(|e| e.key().clone())
            .collect();

        // Should have 2 conflation groups - limit is now enforced on insert, not just cleanup
        assert_eq!(
            channel_state.conflation_groups.len(),
            2,
            "Expected 2 conflation groups after limit enforcement on insert, got: {:?}",
            existing_keys
        );

        // Cleanup should not change anything since limit is already enforced
        manager.cleanup().await;

        let channel_state_after = socket_state.get_channel_state("prices").unwrap();
        assert_eq!(channel_state_after.conflation_groups.len(), 2);
    }

    #[test]
    fn test_conflation_key_with_number_type() {
        let config = DeltaCompressionConfig {
            conflation_key_path: Some("id".to_string()),
            min_message_size: 20,
            ..Default::default()
        };
        let manager = DeltaCompressionManager::new(config);

        let msg1 = br#"{"id":123,"data":"content"}"#;
        let msg2 = br#"{"id":456,"data":"content"}"#;

        assert_eq!(manager.extract_conflation_key(msg1), "123");
        assert_eq!(manager.extract_conflation_key(msg2), "456");
    }
}
