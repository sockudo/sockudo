use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

#[derive(Debug)]
struct Mapping {
    first: u16,
    last: u16,
    file: &'static str,
    test: &'static str,
    command: &'static str,
}

macro_rules! mapping {
    ($first:literal $(..= $last:literal)?, $file:literal, $test:literal, $command:literal) => {
        Mapping {
            first: $first,
            last: mapping!(@last $first $(, $last)?),
            file: $file,
            test: $test,
            command: $command,
        }
    };
    (@last $first:literal, $last:literal) => { $last };
    (@last $first:literal) => { $first };
}

const COVERAGE: &[Mapping] = &[
    mapping!(
        1..=2,
        "crates/sockudo-ai-transport/tests/ait_s_conformance.rs",
        "ait_s001_002_message_actions_and_v2_event_names_match_spec",
        "cargo test -p sockudo-ai-transport --test ait_s_conformance ait_s001_002_message_actions_and_v2_event_names_match_spec"
    ),
    mapping!(
        3,
        "crates/sockudo-ai-transport/tests/ait_s_conformance.rs",
        "ait_s003_versioned_realtime_message_flattens_payload_and_validates_serials",
        "cargo test -p sockudo-ai-transport --test ait_s_conformance ait_s003_versioned_realtime_message_flattens_payload_and_validates_serials"
    ),
    mapping!(
        4..=5,
        "crates/sockudo-ai-transport/tests/ait_s_conformance.rs",
        "ait_s004_005_serial_newtypes_reject_empty_whitespace_and_overlong_values",
        "cargo test -p sockudo-ai-transport --test ait_s_conformance ait_s004_005_serial_newtypes_reject_empty_whitespace_and_overlong_values"
    ),
    mapping!(
        6..=10,
        "crates/sockudo-ai-transport/tests/ait_s_conformance.rs",
        "ait_s006_to_s010_version_chain_and_replay_validation_fail_closed",
        "cargo test -p sockudo-ai-transport --test ait_s_conformance ait_s006_to_s010_version_chain_and_replay_validation_fail_closed"
    ),
    mapping!(
        11..=12,
        "crates/sockudo-ai-transport/tests/ait_s_conformance.rs",
        "ait_s011_012_memory_version_store_projects_latest_visible_state",
        "cargo test -p sockudo-ai-transport --test ait_s_conformance ait_s011_012_memory_version_store_projects_latest_visible_state"
    ),
    mapping!(
        13,
        "crates/sockudo-server/src/http_handler/versioned_messages/mutations/tests.rs",
        "update_message_allows_authenticated_owner_with_own_capability",
        "cargo test -p sockudo --features ai-transport update_message_allows_authenticated_owner_with_own_capability"
    ),
    mapping!(
        14,
        "crates/sockudo-server/src/http_handler/versioned_messages/mutations/tests.rs",
        "delete_message_allows_authenticated_any_capability",
        "cargo test -p sockudo --features ai-transport delete_message_allows_authenticated_any_capability"
    ),
    mapping!(
        15,
        "crates/sockudo-server/src/http_handler/versioned_messages/mutations/tests.rs",
        "append_message_op_id_replay_returns_duplicate_serial_ack",
        "cargo test -p sockudo --features ai-transport append_message_op_id_replay_returns_duplicate_serial_ack"
    ),
    mapping!(
        16..=18,
        "crates/sockudo-server/src/http_handler/ai/tests.rs",
        "ai_transport_authz_matrix_enumerates_every_operation_and_principal",
        "cargo test -p sockudo --features ai-transport ai_transport_authz_matrix_enumerates_every_operation_and_principal"
    ),
    mapping!(
        19,
        "crates/sockudo-adapter/src/local_adapter/tests/mod.rs",
        "v1_compatible_message_strips_v2_only_fields_for_plain_messages",
        "cargo test -p sockudo-adapter --lib v1_compatible_message_strips_v2_only_fields_for_plain_messages"
    ),
    mapping!(
        20,
        "crates/sockudo-protocol/tests/protocol_compliance.rs",
        "test_v2_delivery_includes_extras",
        "cargo test -p sockudo-protocol --test protocol_compliance test_v2_delivery_includes_extras"
    ),
    mapping!(
        21,
        "crates/sockudo-protocol/tests/protocol_compliance.rs",
        "test_validate_headers_rejects_nested_objects",
        "cargo test -p sockudo-protocol --test protocol_compliance test_validate_headers_rejects_nested_objects"
    ),
    mapping!(
        22..=23,
        "crates/sockudo-server/src/http_handler/ai/tests.rs",
        "ai_http_publish_idempotency_key_header_returns_cached_serial_ack",
        "cargo test -p sockudo --features ai-transport ai_http_publish_idempotency_key_header_returns_cached_serial_ack"
    ),
    mapping!(
        24,
        "crates/sockudo-server/src/http_handler/ai/tests.rs",
        "ai_batch_http_publish_returns_serial_acks_in_request_order",
        "cargo test -p sockudo --features ai-transport ai_batch_http_publish_returns_serial_acks_in_request_order"
    ),
    mapping!(
        25,
        "crates/sockudo-adapter/src/handler/connection_management.rs",
        "test_extras_idempotency_key_cache_key_format",
        "cargo test -p sockudo-adapter --lib test_extras_idempotency_key_cache_key_format"
    ),
    mapping!(
        26..=30,
        "crates/sockudo-ai-transport/tests/ait_s_conformance.rs",
        "ait_s026_to_s030_history_cursor_and_bounds_validation_are_strict",
        "cargo test -p sockudo-ai-transport --test ait_s_conformance ait_s026_to_s030_history_cursor_and_bounds_validation_are_strict"
    ),
    mapping!(
        31..=32,
        "crates/sockudo-server/src/http_handler/versioned_messages/tests.rs",
        "events_create_then_update_substitutes_latest_visible_history",
        "cargo test -p sockudo --features ai-transport events_create_then_update_substitutes_latest_visible_history"
    ),
    mapping!(
        33..=38,
        "crates/sockudo-adapter/tests/adapter/handler/runtime_rewind_recovery_e2e_test.rs",
        "rewind_handoff_has_no_gap_and_suppresses_duplicates_e2e",
        "cargo test -p sockudo-adapter --test integration rewind_handoff_has_no_gap_and_suppresses_duplicates_e2e"
    ),
    mapping!(
        39..=44,
        "crates/sockudo-adapter/src/handler/recovery.rs",
        "cold_recovery_success_uses_durable_history",
        "cargo test -p sockudo-adapter --lib cold_recovery_success_uses_durable_history"
    ),
    mapping!(
        45..=48,
        "crates/sockudo-adapter/tests/adapter/handler/annotations_test.rs",
        "publish_annotation_delivers_summary_and_delete_delivers_decrement",
        "cargo test -p sockudo-adapter --test integration publish_annotation_delivers_summary_and_delete_delivers_decrement"
    ),
    mapping!(
        49,
        "crates/sockudo-push/src/domain.rs",
        "device_identity_tokens_are_hashed_and_verifiable",
        "cargo test -p sockudo-push device_identity_tokens_are_hashed_and_verifiable"
    ),
    mapping!(
        50,
        "crates/sockudo-push/src/domain.rs",
        "channel_subscriptions_are_separate_app_scoped_push_rows",
        "cargo test -p sockudo-push channel_subscriptions_are_separate_app_scoped_push_rows"
    ),
    mapping!(
        51..=52,
        "crates/sockudo-server/src/http_handler/events/tests.rs",
        "matching_channel_push_rule_accepts_existing_channel_subscription_fanout",
        "cargo test -p sockudo matching_channel_push_rule_accepts_existing_channel_subscription_fanout"
    ),
    mapping!(
        53..=54,
        "crates/sockudo-ai-transport/tests/ait_s_conformance.rs",
        "ait_s053_054_ai_feature_and_runtime_channel_gate_defaults_are_closed",
        "cargo test -p sockudo-ai-transport --test ait_s_conformance ait_s053_054_ai_feature_and_runtime_channel_gate_defaults_are_closed"
    ),
    mapping!(
        55..=64,
        "crates/sockudo-ai-transport/tests/ait_s_conformance.rs",
        "ait_s055_to_s064_ai_event_and_header_rules_match_registry",
        "cargo test -p sockudo-ai-transport --test ait_s_conformance ait_s055_to_s064_ai_event_and_header_rules_match_registry"
    ),
    mapping!(
        65..=66,
        "crates/sockudo-server/src/http_handler/ai/tests.rs",
        "ai_http_publish_returns_serial_ack_and_message_id_dedupes",
        "cargo test -p sockudo --features ai-transport ai_http_publish_returns_serial_ack_and_message_id_dedupes"
    ),
    mapping!(
        67,
        "crates/sockudo-server/src/http_handler/versioned_messages/mutations/tests.rs",
        "append_message_op_id_replay_returns_duplicate_serial_ack",
        "cargo test -p sockudo --features ai-transport append_message_op_id_replay_returns_duplicate_serial_ack"
    ),
    mapping!(
        68,
        "crates/sockudo-server/src/http_handler/ai/tests.rs",
        "ai_http_publish_returns_serial_ack_and_message_id_dedupes",
        "cargo test -p sockudo --features ai-transport ai_http_publish_returns_serial_ack_and_message_id_dedupes"
    ),
    mapping!(
        69..=73,
        "crates/sockudo-ai-transport/tests/ait_s_conformance.rs",
        "ait_s069_to_s073_rollup_table_and_reduced_state_are_deterministic",
        "cargo test -p sockudo-ai-transport --test ait_s_conformance ait_s069_to_s073_rollup_table_and_reduced_state_are_deterministic"
    ),
    mapping!(
        74..=77,
        "crates/sockudo-adapter/src/handler/subscription_management.rs",
        "subscription_success_includes_attach_serial_when_captured",
        "cargo test -p sockudo-adapter --lib subscription_success_includes_attach_serial_when_captured"
    ),
    mapping!(
        78,
        "crates/sockudo-core/src/capability_token.rs",
        "validates_hs256_token_and_builds_capabilities",
        "cargo test -p sockudo-core validates_hs256_token_and_builds_capabilities"
    ),
    mapping!(
        79,
        "crates/sockudo-core/src/capability_token.rs",
        "wildcard_prefix_does_not_match_bare_namespace",
        "cargo test -p sockudo-core wildcard_prefix_does_not_match_bare_namespace"
    ),
    mapping!(
        80,
        "crates/sockudo-core/src/capability_token.rs",
        "rejects_lifetime_over_24_hours",
        "cargo test -p sockudo-core rejects_lifetime_over_24_hours"
    ),
    mapping!(
        81,
        "crates/sockudo-adapter/src/handler/signin_management.rs",
        "token_authenticated_connections_cannot_sign_in_again",
        "cargo test -p sockudo-adapter --lib token_authenticated_connections_cannot_sign_in_again"
    ),
    mapping!(
        82,
        "crates/sockudo-adapter/src/handler/signin_management.rs",
        "non_token_connections_can_use_legacy_signin",
        "cargo test -p sockudo-adapter --lib non_token_connections_can_use_legacy_signin"
    ),
    mapping!(
        83,
        "crates/sockudo-adapter/src/handler/presence_update.rs",
        "parse_presence_update_accepts_json_data",
        "cargo test -p sockudo-adapter --lib parse_presence_update_accepts_json_data"
    ),
    mapping!(
        84..=85,
        "crates/sockudo-ai-transport/tests/ait_s_conformance.rs",
        "ait_s084_085_presence_timeout_defaults_off_and_can_be_configured",
        "cargo test -p sockudo-ai-transport --test ait_s_conformance ait_s084_085_presence_timeout_defaults_off_and_can_be_configured"
    ),
    mapping!(
        86,
        "crates/sockudo-adapter/tests/adapter/local_adapter_fallback_tests.rs",
        "test_local_adapter_presence_operations",
        "cargo test -p sockudo-adapter --test integration test_local_adapter_presence_operations"
    ),
    mapping!(
        87,
        "crates/sockudo-protocol/tests/protocol_compliance.rs",
        "test_ai_header_validation_rejects_limits_and_domains",
        "cargo test -p sockudo-protocol --test protocol_compliance test_ai_header_validation_rejects_limits_and_domains"
    ),
    mapping!(
        88,
        "crates/sockudo-server/src/http_handler/versioned_messages/mutations/tests.rs",
        "update_message_denies_authenticated_socket_without_mutation_capability",
        "cargo test -p sockudo --features ai-transport update_message_denies_authenticated_socket_without_mutation_capability"
    ),
    mapping!(
        89,
        "crates/sockudo-protocol/tests/protocol_compliance.rs",
        "test_v1_channel_event_json_snapshot_is_stable_without_extras",
        "cargo test -p sockudo-protocol --test protocol_compliance test_v1_channel_event_json_snapshot_is_stable_without_extras"
    ),
    mapping!(
        90,
        "crates/sockudo-ai-transport/tests/ait_s_conformance.rs",
        "ait_s053_054_ai_feature_and_runtime_channel_gate_defaults_are_closed",
        "cargo test -p sockudo-ai-transport --test ait_s_conformance ait_s053_054_ai_feature_and_runtime_channel_gate_defaults_are_closed"
    ),
    mapping!(
        91,
        "crates/sockudo-server/src/http_handler/ai/tests.rs",
        "append_message_rejects_ai_accumulated_content_over_cap",
        "cargo test -p sockudo --features ai-transport append_message_rejects_ai_accumulated_content_over_cap"
    ),
    mapping!(
        92,
        "crates/sockudo-server/src/http_handler/ai/tests.rs",
        "append_message_rejects_ai_append_count_over_cap",
        "cargo test -p sockudo --features ai-transport append_message_rejects_ai_append_count_over_cap"
    ),
    mapping!(
        93,
        "crates/sockudo-server/src/http_handler/ai/tests.rs",
        "events_rejects_ai_create_when_open_stream_cap_is_reached",
        "cargo test -p sockudo --features ai-transport events_rejects_ai_create_when_open_stream_cap_is_reached"
    ),
    mapping!(
        94,
        "crates/sockudo-server/src/http_handler/ai/tests.rs",
        "append_message_persists_terminal_status_in_latest_record",
        "cargo test -p sockudo --features ai-transport append_message_persists_terminal_status_in_latest_record"
    ),
    mapping!(
        95,
        "crates/sockudo-server/src/http_handler/ai/tests.rs",
        "ai_http_publish_returns_serial_ack_and_message_id_dedupes",
        "cargo test -p sockudo --features ai-transport ai_http_publish_returns_serial_ack_and_message_id_dedupes"
    ),
    mapping!(
        96,
        "crates/sockudo-server/src/http_handler/ai/tests.rs",
        "ai_http_publish_idempotency_key_header_returns_cached_serial_ack",
        "cargo test -p sockudo --features ai-transport ai_http_publish_idempotency_key_header_returns_cached_serial_ack"
    ),
    mapping!(
        97,
        "crates/sockudo-server/src/http_handler/ai/tests.rs",
        "channel_state_includes_ai_block_for_ai_transport_channels",
        "cargo test -p sockudo --features ai-transport channel_state_includes_ai_block_for_ai_transport_channels"
    ),
    mapping!(
        98,
        "crates/sockudo-server/src/http_handler/ai/tests.rs",
        "ai_batch_http_publish_returns_serial_acks_in_request_order",
        "cargo test -p sockudo --features ai-transport ai_batch_http_publish_returns_serial_acks_in_request_order"
    ),
    mapping!(
        99,
        "crates/sockudo-protocol/tests/protocol_compliance.rs",
        "test_ai_transport_model_key_accepted",
        "cargo test -p sockudo-protocol --test protocol_compliance test_ai_transport_model_key_accepted"
    ),
    mapping!(
        100,
        "crates/sockudo-protocol/tests/protocol_compliance.rs",
        "test_ai_transport_model_key_optional",
        "cargo test -p sockudo-protocol --test protocol_compliance test_ai_transport_model_key_optional"
    ),
];

#[test]
fn every_ait_s_assertion_maps_to_a_present_test_file_and_command() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut covered = BTreeMap::new();

    for mapping in COVERAGE {
        assert!(
            mapping.first <= mapping.last,
            "invalid mapping: {mapping:?}"
        );
        let path = root.join(mapping.file);
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("missing mapped test file {}: {error}", path.display()));
        assert!(
            source.contains(&format!("fn {}", mapping.test)),
            "mapped test {} is missing from {}",
            mapping.test,
            path.display()
        );
        assert!(
            mapping.command.contains(mapping.test),
            "mapped command must select test {}: {}",
            mapping.test,
            mapping.command
        );
        for id in mapping.first..=mapping.last {
            assert!(
                covered.insert(id, mapping).is_none(),
                "duplicate AIT-S{id} mapping"
            );
        }
    }

    let expected = (1..=100).collect::<Vec<_>>();
    let actual = covered.keys().copied().collect::<Vec<_>>();
    assert_eq!(
        actual, expected,
        "AIT-S coverage must be exact through AIT-S100"
    );
}
