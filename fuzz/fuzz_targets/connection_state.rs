#![no_main]

use libfuzzer_sys::fuzz_target;
use sockudo_core::websocket::{ConnectionState, ConnectionStatus};
use sockudo_protocol::{ProtocolVersion, WireFormat};
use std::collections::HashSet;
use std::time::Instant;

const MAX_INPUT_BYTES: usize = 64 * 1024;
const MAX_OPERATIONS: usize = 512;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_BYTES {
        return;
    }

    let protocol_version = if data.first().is_some_and(|byte| byte & 1 == 0) {
        ProtocolVersion::V1
    } else {
        ProtocolVersion::V2
    };
    let wire_format = match data.first().copied().unwrap_or_default() % 3 {
        0 => WireFormat::Json,
        1 => WireFormat::MessagePack,
        _ => WireFormat::Protobuf,
    };
    let mut state = ConnectionState::new()
        .with_protocol_version(protocol_version)
        .with_wire_format(wire_format);
    let mut expected_channels = HashSet::new();

    assert!(!state.is_presence());
    assert!(!state.is_authenticated());
    assert!(state.get_app_id().is_empty());
    assert!(state.get_app_key().is_empty());

    for operation in data.chunks(3).take(MAX_OPERATIONS) {
        let channel = format!(
            "channel-{}-{}",
            operation.get(1).copied().unwrap_or_default(),
            operation.get(2).copied().unwrap_or_default()
        );
        match operation[0] % 7 {
            0 => {
                state.add_subscription(channel.clone());
                expected_channels.insert(channel);
            }
            1 => {
                assert_eq!(
                    state.remove_subscription(&channel),
                    expected_channels.remove(&channel)
                );
            }
            2 => assert_eq!(
                state.is_subscribed(&channel),
                expected_channels.contains(&channel)
            ),
            3 => {
                state.add_subscription(channel.clone());
                state.add_subscription(channel.clone());
                expected_channels.insert(channel);
            }
            4 => state.update_ping(),
            5 => {
                let status = match operation.get(1).copied().unwrap_or_default() % 4 {
                    0 => ConnectionStatus::Active,
                    1 => ConnectionStatus::PingSent(Instant::now()),
                    2 => ConnectionStatus::Closing,
                    _ => ConnectionStatus::Closed,
                };
                state.status = status;
                assert_eq!(state.status, status);
            }
            _ => state.clear_timeouts(),
        }
    }

    let mut actual = state.get_subscribed_channels_list();
    actual.sort_unstable();
    let mut expected: Vec<_> = expected_channels.into_iter().collect();
    expected.sort_unstable();
    assert_eq!(actual, expected);
});
