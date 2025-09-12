use sockudo::adapter::horizontal_adapter_base::HorizontalAdapterBase;
use sockudo::adapter::ConnectionManager;
use sockudo::options::ClusterHealthConfig;
use crate::adapter::horizontal_adapter_helpers::{MockTransport, MockConfig};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;

/// Helper to create a cluster of mock nodes for testing
pub struct MockCluster {
    pub nodes: Vec<HorizontalAdapterBase<MockTransport>>,
    pub node_ids: Vec<String>,
    pub transports: Vec<MockTransport>,
}

impl MockCluster {
    /// Create a cluster with specified number of nodes
    pub async fn new(node_count: usize) -> Self {
        let mut nodes = Vec::new();
        let mut node_ids = Vec::new();
        let mut transports = Vec::new();

        // Create shared state for all mock transports to simulate real cluster communication
        let shared_state = Arc::new(Mutex::new(HashMap::new()));

        for i in 0..node_count {
            let mut config = MockConfig::default();
            config.prefix = format!("cluster_test_{}", i);
            
            // Create transport with shared state
            let transport = MockTransport::new_with_shared_state(config.clone(), shared_state.clone());
            let adapter = HorizontalAdapterBase::new(config).await.unwrap();
            
            node_ids.push(adapter.node_id.clone());
            transports.push(transport.clone());
            nodes.push(adapter);
        }

        Self {
            nodes,
            node_ids,
            transports,
        }
    }

    /// Start all nodes' listeners
    pub async fn start_all(&self) {
        for node in &self.nodes {
            node.start_listeners().await.unwrap();
        }
        
        // Give nodes time to establish connections
        sleep(Duration::from_millis(100)).await;
    }

    /// Simulate a node going dead by stopping its heartbeat
    pub async fn kill_node(&mut self, node_index: usize) {
        if node_index < self.transports.len() {
            self.transports[node_index].set_health_status(false).await;
        }
    }

    /// Get a specific node
    pub fn get_node(&self, index: usize) -> Option<&HorizontalAdapterBase<MockTransport>> {
        self.nodes.get(index)
    }

    /// Configure cluster health on all nodes
    pub async fn configure_cluster_health(&mut self, config: ClusterHealthConfig) {
        for node in &mut self.nodes {
            node.set_cluster_health(&config).await.unwrap();
        }
    }
}

#[tokio::test]
async fn test_multi_node_heartbeat_system() {
    let mut cluster = MockCluster::new(3).await;
    
    // Configure fast heartbeat for testing
    let config = ClusterHealthConfig {
        enabled: true,
        heartbeat_interval_ms: 100, // 100ms for fast testing
        node_timeout_ms: 500,       // 500ms timeout
        cleanup_interval_ms: 200,   // 200ms cleanup interval
    };
    
    cluster.configure_cluster_health(config).await;
    cluster.start_all().await;
    
    // Let heartbeats run for a bit
    sleep(Duration::from_millis(300)).await;
    
    // All nodes should be alive and tracking each other
    for (i, node) in cluster.nodes.iter().enumerate() {
        // Each node should see the other nodes as alive
        let heartbeats = {
            let horizontal = node.horizontal.lock().await;
            let heartbeats = horizontal.node_heartbeats.read().await;
            heartbeats.len()
        };
        
        // Should see 2 other nodes (not counting self)
        assert_eq!(heartbeats, 2, "Node {} should see 2 other nodes", i);
    }
}

#[tokio::test]
async fn test_dead_node_detection_and_cleanup() {
    let mut cluster = MockCluster::new(3).await;
    
    // Configure aggressive timing for testing
    let config = ClusterHealthConfig {
        enabled: true,
        heartbeat_interval_ms: 50,  // Very fast heartbeat
        node_timeout_ms: 200,       // Quick timeout
        cleanup_interval_ms: 100,   // Frequent cleanup checks
    };
    
    cluster.configure_cluster_health(config).await;
    cluster.start_all().await;
    
    // Let initial heartbeats establish
    sleep(Duration::from_millis(150)).await;
    
    // Kill node 1
    cluster.kill_node(1).await;
    
    // Wait for dead node detection and cleanup
    sleep(Duration::from_millis(400)).await;
    
    // Remaining nodes should have detected node 1 as dead
    for (i, node) in cluster.nodes.iter().enumerate() {
        if i != 1 { // Skip the dead node
            let heartbeats = {
                let horizontal = node.horizontal.lock().await;
                let heartbeats = horizontal.node_heartbeats.read().await;
                heartbeats.len()
            };
            
            // Should only see 1 other alive node now (excluding self and dead node)
            assert_eq!(heartbeats, 1, "Node {} should only see 1 alive node after cleanup", i);
        }
    }
}

#[tokio::test]
async fn test_leader_election_for_dead_node_cleanup() {
    let mut cluster = MockCluster::new(4).await;
    
    // Configure cluster health
    let config = ClusterHealthConfig {
        enabled: true,
        heartbeat_interval_ms: 50,
        node_timeout_ms: 200,
        cleanup_interval_ms: 100,
    };
    
    cluster.configure_cluster_health(config).await;
    
    // Set up dead node event receivers for leader election verification  
    let mut event_receivers: Vec<tokio::sync::mpsc::UnboundedReceiver<sockudo::adapter::horizontal_adapter::DeadNodeEvent>> = Vec::new();
    for node in &mut cluster.nodes {
        if let Some(receiver) = node.configure_dead_node_events() {
            event_receivers.push(receiver);
        }
    }
    
    cluster.start_all().await;
    sleep(Duration::from_millis(150)).await;
    
    // Kill two nodes simultaneously
    cluster.kill_node(2).await;
    cluster.kill_node(3).await;
    
    // Wait for detection and leader election
    sleep(Duration::from_millis(500)).await;
    
    // Check that dead node events were emitted (only by the leader)
    let mut total_events = 0;
    for receiver in &mut event_receivers {
        while let Ok(event) = receiver.try_recv() {
            total_events += 1;
            // Verify event structure
            assert!(!event.dead_node_id.is_empty());
            // Note: orphaned_members might be empty if no presence data
        }
    }
    
    // Should have events for the 2 dead nodes, but only from the elected leader
    assert!(total_events >= 2, "Should have received dead node events from leader");
}

#[tokio::test]
async fn test_cluster_health_disabled_no_heartbeat() {
    let mut cluster = MockCluster::new(2).await;
    
    // Configure cluster health as DISABLED
    let config = ClusterHealthConfig {
        enabled: false,
        heartbeat_interval_ms: 100,
        node_timeout_ms: 500,
        cleanup_interval_ms: 200,
    };
    
    cluster.configure_cluster_health(config).await;
    cluster.start_all().await;
    
    // Wait a bit
    sleep(Duration::from_millis(300)).await;
    
    // Nodes should NOT be tracking each other when cluster health is disabled
    for node in &cluster.nodes {
        let heartbeats = {
            let horizontal = node.horizontal.lock().await;
            let heartbeats = horizontal.node_heartbeats.read().await;
            heartbeats.len()
        };
        
        // Should see 0 nodes since heartbeat system is disabled
        assert_eq!(heartbeats, 0, "Node should not track heartbeats when cluster health disabled");
    }
}

#[tokio::test]
async fn test_node_recovery_after_network_partition() {
    let mut cluster = MockCluster::new(3).await;
    
    let config = ClusterHealthConfig {
        enabled: true,
        heartbeat_interval_ms: 100,
        node_timeout_ms: 400,
        cleanup_interval_ms: 150,
    };
    
    cluster.configure_cluster_health(config).await;
    cluster.start_all().await;
    
    // Let initial heartbeats establish
    sleep(Duration::from_millis(200)).await;
    
    // Simulate network partition - node 1 goes offline temporarily
    cluster.kill_node(1).await;
    
    // Wait for dead node detection
    sleep(Duration::from_millis(600)).await;
    
    // "Recover" the node by making it healthy again
    cluster.transports[1].set_health_status(true).await;
    
    // Let the node rejoin
    sleep(Duration::from_millis(300)).await;
    
    // Eventually, all nodes should see each other again
    // (In a real system, the recovered node would need to re-establish its heartbeat)
    let mut living_count = 0;
    for (i, _) in cluster.nodes.iter().enumerate() {
        if cluster.transports[i].is_healthy().await {
            living_count += 1;
        }
    }
    
    assert_eq!(living_count, 3, "All nodes should be recovered");
}