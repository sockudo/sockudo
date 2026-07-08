use std::path::Path;

use sockudo_simulator::{
    ActionWeights, FailureArtifact, FailureObservation, FailureRun, RunOutcome, SimulatorConfig,
    WorkloadActionCounts, WorkloadConfig, shrink_with_runner,
};

#[tokio::test]
async fn public_shrink_api_reduces_controlled_failure_artifact() {
    let config = SimulatorConfig {
        seed: 0x5eed,
        ticks: 24,
        nodes: 4,
        clients: 3,
        channels: 2,
        users: 5,
        workload: WorkloadConfig {
            weights: ActionWeights {
                publish_message: 8,
                create_versioned_message: 3,
                mutate_versioned_message: 2,
                presence_transition: 2,
                recovery_probe: 1,
                purge_history: 1,
                push_register_device: 1,
                push_delete_device: 1,
                push_subscribe: 1,
                push_unsubscribe: 1,
                push_publish: 1,
                push_scheduled_publish: 1,
                push_provider_feedback: 1,
                push_repair: 1,
                oracle_check: 1,
            },
        },
        ..SimulatorConfig::default()
    };

    let outcome = shrink_with_runner(config, 1, |config| async move { fixture_outcome(config) })
        .await
        .unwrap()
        .expect("fixture should keep failing after shrink");

    assert_eq!(outcome.config.ticks, 5);
    assert_eq!(outcome.config.max_operations, Some(2));
    assert_eq!(outcome.config.max_faults, Some(1));
    assert_eq!(outcome.config.nodes, 2);
    assert_eq!(outcome.config.clients, 1);

    let artifact = outcome.artifact(Path::new("/tmp/shrunk-fixture.json"), None);
    let encoded = serde_json::to_string(&artifact).unwrap();
    let decoded: FailureArtifact = serde_json::from_str(&encoded).unwrap();

    assert_eq!(decoded.config.ticks, 5);
    assert_eq!(decoded.config.max_operations, Some(2));
    assert_eq!(decoded.config.max_faults, Some(1));
    assert!(decoded.replay_command.contains("--corpus-file"));
    assert!(decoded.shrink.unwrap().steps.iter().any(|step| {
        step.field == "workload.weights.create_versioned_message" && step.after == "0"
    }));
}

fn fixture_outcome(config: SimulatorConfig) -> RunOutcome {
    if config.ticks < 5
        || config.max_operations.unwrap_or(config.ticks) < 2
        || config.max_faults.unwrap_or(2) < 1
        || config.nodes < 2
        || config.workload.weights.publish_message == 0
    {
        return RunOutcome::Passed;
    }

    RunOutcome::Failed(FailureRun {
        error: "controlled integration fixture failure".to_string(),
        observation: FailureObservation {
            tick: config.ticks,
            operations: config.max_operations.unwrap_or(2).min(2),
            fault_events: config.max_faults.unwrap_or(1).min(1),
            suppressed_fault_events: 0,
            nodes: config.nodes,
            clients: config.clients,
            channels: config.channels,
            users: config.users,
            workload_actions: WorkloadActionCounts {
                publish_message: 2,
                ..WorkloadActionCounts::default()
            },
        },
    })
}
