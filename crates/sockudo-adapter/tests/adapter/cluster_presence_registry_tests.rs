use crate::adapter::horizontal_adapter_helpers::{MockConfig, MockTransport};
use sockudo_adapter::ConnectionManager;
use sockudo_adapter::horizontal_adapter_base::HorizontalAdapterBase;
use sockudo_core::channel::PresenceMemberInfo;
use sonic_rs::json;
use tokio::task::JoinSet;

#[tokio::test]
async fn test_local_channel_members_merges_registry() {
    let adapter = HorizontalAdapterBase::<MockTransport>::new(MockConfig::default())
        .await
        .unwrap();

    let app_id = "app-merge";
    let channel = "presence-merge-ch";

    for (node, socket, user) in &[
        ("node-A", "sock-A", "alice"),
        ("node-B", "sock-B", "bob"),
        ("node-C", "sock-C", "carol"),
    ] {
        adapter
            .horizontal
            .add_presence_entry(
                node,
                channel,
                socket,
                user,
                app_id,
                Some(json!({"name": user})),
            )
            .await;
    }

    let members = adapter
        .get_local_channel_members(app_id, channel)
        .await
        .unwrap();

    assert!(
        members.contains_key("alice"),
        "alice (node-A) must be present"
    );
    assert!(members.contains_key("bob"), "bob (node-B) must be present");
    assert!(
        members.contains_key("carol"),
        "carol (node-C) must be present"
    );
    assert_eq!(
        members.len(),
        3,
        "exactly 3 distinct users should be merged; got {}",
        members.len()
    );

    let requests = adapter.transport.get_published_requests().await;
    assert!(
        requests.is_empty(),
        "get_local_channel_members must issue zero cluster requests; got {}",
        requests.len()
    );
}

#[tokio::test]
async fn test_local_channel_members_skips_own_node() {
    let adapter = HorizontalAdapterBase::<MockTransport>::new(MockConfig::default())
        .await
        .unwrap();

    let app_id = "app-skip";
    let channel = "presence-skip-ch";
    let self_node = adapter.node_id.clone();

    adapter
        .horizontal
        .add_presence_entry(&self_node, channel, "sock-self", "user-self", app_id, None)
        .await;

    adapter
        .horizontal
        .add_presence_entry(
            "remote-node-1",
            channel,
            "sock-remote",
            "user-remote",
            app_id,
            Some(json!({"role": "remote"})),
        )
        .await;

    let members = adapter
        .get_local_channel_members(app_id, channel)
        .await
        .unwrap();

    assert!(
        !members.contains_key("user-self"),
        "entry registered under own node_id must be excluded from registry scan"
    );
    assert!(
        members.contains_key("user-remote"),
        "entry from a remote node must be included"
    );
}

#[tokio::test]
async fn test_local_channel_members_filters_by_app_id() {
    let adapter = HorizontalAdapterBase::<MockTransport>::new(MockConfig::default())
        .await
        .unwrap();

    let target_app = "app-target";
    let other_app = "app-other";
    let channel = "presence-filter-ch";

    adapter
        .horizontal
        .add_presence_entry(
            "node-X",
            channel,
            "sock-X1",
            "user-target-1",
            target_app,
            Some(json!({"app": "target"})),
        )
        .await;
    adapter
        .horizontal
        .add_presence_entry(
            "node-X",
            channel,
            "sock-X2",
            "user-target-2",
            target_app,
            Some(json!({"app": "target"})),
        )
        .await;
    adapter
        .horizontal
        .add_presence_entry(
            "node-X",
            channel,
            "sock-Y",
            "user-other",
            other_app,
            Some(json!({"app": "other"})),
        )
        .await;

    let members = adapter
        .get_local_channel_members(target_app, channel)
        .await
        .unwrap();

    assert!(
        members.contains_key("user-target-1"),
        "user-target-1 must be included"
    );
    assert!(
        members.contains_key("user-target-2"),
        "user-target-2 must be included"
    );
    assert!(
        !members.contains_key("user-other"),
        "user-other (app-other) must be excluded when querying target_app"
    );
    assert_eq!(
        members.len(),
        2,
        "only target-app members should appear; got {}",
        members.len()
    );
}

#[tokio::test]
async fn test_concurrent_read_write_registry() {
    let adapter = HorizontalAdapterBase::<MockTransport>::new(MockConfig::default())
        .await
        .unwrap();

    let app_id = "app-concurrent";
    let channel = "presence-concurrent-ch";
    let own_node = adapter.node_id.clone();
    let horizontal = adapter.horizontal.clone();

    for i in 0..5_usize {
        horizontal
            .add_presence_entry(
                &format!("node-init-{i}"),
                channel,
                &format!("sock-init-{i}"),
                &format!("user-init-{i}"),
                app_id,
                Some(json!({"seq": i})),
            )
            .await;
    }

    let mut set = JoinSet::new();

    for _ in 0..8_usize {
        let h = horizontal.clone();
        let own = own_node.clone();
        set.spawn(async move {
            for _ in 0..20_usize {
                let mut collected: Vec<PresenceMemberInfo> = Vec::new();
                {
                    let registry = h.cluster_presence_registry.read().await;
                    for (node_id, node_data) in registry.iter() {
                        if node_id == &own {
                            continue;
                        }
                        if let Some(channel_sockets) = node_data.get(channel) {
                            for entry in channel_sockets.values() {
                                if entry.app_id != app_id {
                                    continue;
                                }
                                collected.push(PresenceMemberInfo {
                                    user_id: entry.user_id.clone(),
                                    user_info: entry
                                        .user_info
                                        .as_ref()
                                        .map(|info| (**info).clone()),
                                });
                            }
                        }
                    }
                }
                let _ = collected;
            }
        });
    }

    {
        let h = horizontal.clone();
        set.spawn(async move {
            for i in 0..20_usize {
                h.add_presence_entry(
                    "node-writer",
                    channel,
                    &format!("sock-writer-{i}"),
                    &format!("user-writer-{i}"),
                    app_id,
                    Some(json!({"writer": i})),
                )
                .await;
            }
        });
    }

    while let Some(res) = set.join_next().await {
        res.expect("task panicked during concurrent registry read/write");
    }

    let registry = horizontal.cluster_presence_registry.read().await;
    let writer_count = registry
        .get("node-writer")
        .and_then(|nd| nd.get(channel))
        .map(|c| c.len())
        .unwrap_or(0);
    assert_eq!(
        writer_count, 20,
        "all 20 writer entries must be present after concurrent writes; found {writer_count}"
    );
}
