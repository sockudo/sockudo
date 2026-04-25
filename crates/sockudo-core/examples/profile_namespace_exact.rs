use sockudo_core::namespace::Namespace;
use sockudo_core::websocket::SocketId;
use std::hint::black_box;

fn seed_namespace(
    exact_channel_count: usize,
    wildcard_channel_count: usize,
    sockets_per_channel: usize,
) -> Namespace {
    let namespace = Namespace::new("profile-app".to_string());

    for channel_idx in 0..exact_channel_count {
        let channel = format!("room-{channel_idx}");
        for _ in 0..sockets_per_channel {
            namespace.add_channel_to_socket(&channel, &SocketId::new());
        }
    }

    for channel_idx in 0..wildcard_channel_count {
        let channel = format!("room-{channel_idx}-*");
        for _ in 0..sockets_per_channel {
            namespace.add_channel_to_socket(&channel, &SocketId::new());
        }
    }

    namespace
}

fn main() {
    let namespace = seed_namespace(20_000, 500, 4);

    for _ in 0..2_000_000 {
        let matches = namespace.get_matching_channel_socket_ids_except("room-42", None);
        black_box(matches.len());
    }
}
