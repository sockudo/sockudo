#![cfg(feature = "redis-cluster")]
use sockudo_cache::redis_cluster_cache_manager::RedisClusterCacheManager;
use sockudo_core::cache::CacheManager;
use std::{
    collections::BTreeMap,
    time::{SystemTime, UNIX_EPOCH},
};

#[tokio::test]
#[ignore = "requires SOCKUDO_CACHE_TEST_CLUSTER_NODES"]
async fn cluster_scan_visits_every_primary_and_returns_exact_values() {
    let nodes = std::env::var("SOCKUDO_CACHE_TEST_CLUSTER_NODES")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect();
    let prefix = format!(
        "s4-scan-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let cache = RedisClusterCacheManager::with_nodes(nodes, Some(&prefix))
        .await
        .unwrap();
    let mut expected = BTreeMap::new();
    for number in 0..1024 {
        let key = format!("rows:{number:04}");
        let value = format!("{number:04}:{}", "x".repeat(4096));
        cache.set(&key, &value, 300).await.unwrap();
        expected.insert(key, value);
    }
    for repeat in 0..3 {
        let mut cursor = None;
        let mut actual = BTreeMap::new();
        let mut pages = 0;
        loop {
            let page = cache.scan_prefix_page("rows:", cursor, 17).await.unwrap();
            pages += 1;
            for (key, value) in page.entries {
                assert!(
                    actual.insert(key, value).is_none(),
                    "duplicate in stable fixture"
                );
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
            assert!(pages < 500, "cursor must terminate");
        }
        assert_eq!(actual, expected);
        println!(
            "cluster_scan_repeat={repeat} exact_rows={} pages={pages}",
            actual.len()
        );
    }
    assert_eq!(cache.scan_prefix("rows:", 1024).await.unwrap().len(), 1024);
    for key in expected.keys() {
        cache.remove(key).await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires an isolated cluster and SOCKUDO_CACHE_TEST_ALLOW_RESHARD=1"]
async fn cluster_scan_rejects_cursor_after_interior_slot_reassignment() {
    assert_eq!(
        std::env::var("SOCKUDO_CACHE_TEST_ALLOW_RESHARD").as_deref(),
        Ok("1")
    );
    let nodes: Vec<String> = std::env::var("SOCKUDO_CACHE_TEST_CLUSTER_NODES")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect();
    let mut connections = Vec::new();
    for node in &nodes {
        let url = if node.contains("://") {
            node.clone()
        } else {
            format!("redis://{node}")
        };
        connections.push(
            redis::Client::open(url)
                .unwrap()
                .get_multiplexed_async_connection()
                .await
                .unwrap(),
        );
    }
    let topology: redis::Value = redis::cmd("CLUSTER")
        .arg("SLOTS")
        .query_async(&mut connections[0])
        .await
        .unwrap();
    let redis::Value::Array(ranges) = topology else {
        panic!("slot map");
    };
    let mut ranges: Vec<(u16, u16, String)> = ranges
        .into_iter()
        .map(|range| {
            let redis::Value::Array(fields) = range else {
                panic!("slot range");
            };
            let redis::Value::Array(primary) = &fields[2] else {
                panic!("primary");
            };
            (
                redis::from_redis_value(fields[0].clone()).unwrap(),
                redis::from_redis_value(fields[1].clone()).unwrap(),
                redis::from_redis_value(primary[2].clone()).unwrap(),
            )
        })
        .collect();
    ranges.sort();
    let (first, last, source) = ranges.last().unwrap();
    let target = &ranges.first().unwrap().2;
    assert_ne!(source, target, "requires at least two primaries");
    let mut empty_slot = None;
    for slot in first + 1..*last {
        let mut empty = true;
        for connection in &mut connections {
            let count: u64 = redis::cmd("CLUSTER")
                .arg("COUNTKEYSINSLOT")
                .arg(slot)
                .query_async(connection)
                .await
                .unwrap();
            empty &= count == 0;
        }
        if empty {
            empty_slot = Some(slot);
            break;
        }
    }
    let slot = empty_slot.expect("an empty interior slot in the synthetic fixture");
    let cache = RedisClusterCacheManager::with_nodes(nodes, Some("s4-reshard-audit"))
        .await
        .unwrap();
    let first = cache.scan_prefix_page("absent:", None, 1).await.unwrap();
    assert!(first.next_cursor.is_some());
    // Always restore the original owner before asserting the observed result.
    let reassigned = async {
        for connection in &mut connections {
            let _: () = redis::cmd("CLUSTER")
                .arg("SETSLOT")
                .arg(slot)
                .arg("NODE")
                .arg(target)
                .query_async(connection)
                .await?;
        }
        Ok::<_, redis::RedisError>(
            cache
                .scan_prefix_page("absent:", first.next_cursor, 1)
                .await,
        )
    }
    .await;
    for connection in &mut connections {
        let _: () = redis::cmd("CLUSTER")
            .arg("SETSLOT")
            .arg(slot)
            .arg("NODE")
            .arg(source)
            .query_async(connection)
            .await
            .unwrap();
    }
    assert!(
        reassigned.unwrap().is_err(),
        "changed topology must invalidate the old cursor"
    );
    println!("interior_slot={slot} stale_cursor_rejected=true original_owner_restored=true");
}
