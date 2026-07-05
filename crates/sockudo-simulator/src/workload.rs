use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};

use crate::error::{SimulatorError, SimulatorResult};

const WORKLOAD_SEED_DOMAIN: u64 = 0x57f4_d04d_100d_f177;

/// One operation family the deterministic workload generator can emit.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkloadAction {
    PublishMessage,
    CreateVersionedMessage,
    MutateVersionedMessage,
    PresenceTransition,
    RecoveryProbe,
    PurgeHistory,
    PushRegisterDevice,
    PushDeleteDevice,
    PushSubscribe,
    PushUnsubscribe,
    PushPublish,
    PushScheduledPublish,
    PushProviderFeedback,
    PushRepair,
    OracleCheck,
}

impl WorkloadAction {
    pub const ALL: [Self; 15] = [
        Self::PublishMessage,
        Self::CreateVersionedMessage,
        Self::MutateVersionedMessage,
        Self::PresenceTransition,
        Self::RecoveryProbe,
        Self::PurgeHistory,
        Self::PushRegisterDevice,
        Self::PushDeleteDevice,
        Self::PushSubscribe,
        Self::PushUnsubscribe,
        Self::PushPublish,
        Self::PushScheduledPublish,
        Self::PushProviderFeedback,
        Self::PushRepair,
        Self::OracleCheck,
    ];

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PublishMessage => "publish_message",
            Self::CreateVersionedMessage => "create_versioned_message",
            Self::MutateVersionedMessage => "mutate_versioned_message",
            Self::PresenceTransition => "presence_transition",
            Self::RecoveryProbe => "recovery_probe",
            Self::PurgeHistory => "purge_history",
            Self::PushRegisterDevice => "push_register_device",
            Self::PushDeleteDevice => "push_delete_device",
            Self::PushSubscribe => "push_subscribe",
            Self::PushUnsubscribe => "push_unsubscribe",
            Self::PushPublish => "push_publish",
            Self::PushScheduledPublish => "push_scheduled_publish",
            Self::PushProviderFeedback => "push_provider_feedback",
            Self::PushRepair => "push_repair",
            Self::OracleCheck => "oracle_check",
        }
    }
}

/// Weighted workload profile.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActionWeights {
    pub publish_message: u32,
    pub create_versioned_message: u32,
    pub mutate_versioned_message: u32,
    pub presence_transition: u32,
    pub recovery_probe: u32,
    pub purge_history: u32,
    pub push_register_device: u32,
    pub push_delete_device: u32,
    pub push_subscribe: u32,
    pub push_unsubscribe: u32,
    pub push_publish: u32,
    pub push_scheduled_publish: u32,
    pub push_provider_feedback: u32,
    pub push_repair: u32,
    pub oracle_check: u32,
}

impl Default for ActionWeights {
    fn default() -> Self {
        Self {
            publish_message: 35,
            create_versioned_message: 15,
            mutate_versioned_message: 20,
            presence_transition: 20,
            recovery_probe: 7,
            purge_history: 2,
            push_register_device: 8,
            push_delete_device: 3,
            push_subscribe: 8,
            push_unsubscribe: 5,
            push_publish: 12,
            push_scheduled_publish: 4,
            push_provider_feedback: 2,
            push_repair: 2,
            oracle_check: 1,
        }
    }
}

impl ActionWeights {
    #[must_use]
    pub fn total(self) -> u32 {
        WorkloadAction::ALL
            .iter()
            .map(|&action| self.weight(action))
            .sum()
    }

    #[must_use]
    pub fn weight(self, action: WorkloadAction) -> u32 {
        match action {
            WorkloadAction::PublishMessage => self.publish_message,
            WorkloadAction::CreateVersionedMessage => self.create_versioned_message,
            WorkloadAction::MutateVersionedMessage => self.mutate_versioned_message,
            WorkloadAction::PresenceTransition => self.presence_transition,
            WorkloadAction::RecoveryProbe => self.recovery_probe,
            WorkloadAction::PurgeHistory => self.purge_history,
            WorkloadAction::PushRegisterDevice => self.push_register_device,
            WorkloadAction::PushDeleteDevice => self.push_delete_device,
            WorkloadAction::PushSubscribe => self.push_subscribe,
            WorkloadAction::PushUnsubscribe => self.push_unsubscribe,
            WorkloadAction::PushPublish => self.push_publish,
            WorkloadAction::PushScheduledPublish => self.push_scheduled_publish,
            WorkloadAction::PushProviderFeedback => self.push_provider_feedback,
            WorkloadAction::PushRepair => self.push_repair,
            WorkloadAction::OracleCheck => self.oracle_check,
        }
    }

    pub(crate) fn validate(self) -> SimulatorResult<()> {
        if self.total() == 0 {
            return Err(SimulatorError::Config(
                "at least one workload action weight must be greater than 0".into(),
            ));
        }
        Ok(())
    }
}

/// Workload generator configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkloadConfig {
    pub weights: ActionWeights,
}

impl WorkloadConfig {
    pub(crate) fn validate(&self) -> SimulatorResult<()> {
        self.weights.validate()
    }
}

/// Per-action sample counts emitted by the generator.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkloadActionCounts {
    pub publish_message: u64,
    pub create_versioned_message: u64,
    pub mutate_versioned_message: u64,
    pub presence_transition: u64,
    pub recovery_probe: u64,
    pub purge_history: u64,
    pub push_register_device: u64,
    pub push_delete_device: u64,
    pub push_subscribe: u64,
    pub push_unsubscribe: u64,
    pub push_publish: u64,
    pub push_scheduled_publish: u64,
    pub push_provider_feedback: u64,
    pub push_repair: u64,
    pub oracle_check: u64,
}

impl WorkloadActionCounts {
    fn increment(&mut self, action: WorkloadAction) {
        match action {
            WorkloadAction::PublishMessage => {
                self.publish_message = self.publish_message.saturating_add(1);
            }
            WorkloadAction::CreateVersionedMessage => {
                self.create_versioned_message = self.create_versioned_message.saturating_add(1);
            }
            WorkloadAction::MutateVersionedMessage => {
                self.mutate_versioned_message = self.mutate_versioned_message.saturating_add(1);
            }
            WorkloadAction::PresenceTransition => {
                self.presence_transition = self.presence_transition.saturating_add(1);
            }
            WorkloadAction::RecoveryProbe => {
                self.recovery_probe = self.recovery_probe.saturating_add(1);
            }
            WorkloadAction::PurgeHistory => {
                self.purge_history = self.purge_history.saturating_add(1);
            }
            WorkloadAction::PushRegisterDevice => {
                self.push_register_device = self.push_register_device.saturating_add(1);
            }
            WorkloadAction::PushDeleteDevice => {
                self.push_delete_device = self.push_delete_device.saturating_add(1);
            }
            WorkloadAction::PushSubscribe => {
                self.push_subscribe = self.push_subscribe.saturating_add(1);
            }
            WorkloadAction::PushUnsubscribe => {
                self.push_unsubscribe = self.push_unsubscribe.saturating_add(1);
            }
            WorkloadAction::PushPublish => {
                self.push_publish = self.push_publish.saturating_add(1);
            }
            WorkloadAction::PushScheduledPublish => {
                self.push_scheduled_publish = self.push_scheduled_publish.saturating_add(1);
            }
            WorkloadAction::PushProviderFeedback => {
                self.push_provider_feedback = self.push_provider_feedback.saturating_add(1);
            }
            WorkloadAction::PushRepair => {
                self.push_repair = self.push_repair.saturating_add(1);
            }
            WorkloadAction::OracleCheck => {
                self.oracle_check = self.oracle_check.saturating_add(1);
            }
        }
    }

    #[must_use]
    pub fn count(&self, action: WorkloadAction) -> u64 {
        match action {
            WorkloadAction::PublishMessage => self.publish_message,
            WorkloadAction::CreateVersionedMessage => self.create_versioned_message,
            WorkloadAction::MutateVersionedMessage => self.mutate_versioned_message,
            WorkloadAction::PresenceTransition => self.presence_transition,
            WorkloadAction::RecoveryProbe => self.recovery_probe,
            WorkloadAction::PurgeHistory => self.purge_history,
            WorkloadAction::PushRegisterDevice => self.push_register_device,
            WorkloadAction::PushDeleteDevice => self.push_delete_device,
            WorkloadAction::PushSubscribe => self.push_subscribe,
            WorkloadAction::PushUnsubscribe => self.push_unsubscribe,
            WorkloadAction::PushPublish => self.push_publish,
            WorkloadAction::PushScheduledPublish => self.push_scheduled_publish,
            WorkloadAction::PushProviderFeedback => self.push_provider_feedback,
            WorkloadAction::PushRepair => self.push_repair,
            WorkloadAction::OracleCheck => self.oracle_check,
        }
    }
}

/// Deterministic weighted workload generator.
#[derive(Debug, Clone)]
pub struct WorkloadGenerator {
    weights: ActionWeights,
    rng: StdRng,
    selected: WorkloadActionCounts,
}

impl WorkloadGenerator {
    pub(crate) fn new(seed: u64, config: WorkloadConfig) -> SimulatorResult<Self> {
        config.validate()?;
        Ok(Self {
            weights: config.weights,
            rng: StdRng::seed_from_u64(seed ^ WORKLOAD_SEED_DOMAIN),
            selected: WorkloadActionCounts::default(),
        })
    }

    pub(crate) fn next_action(&mut self) -> WorkloadAction {
        let mut roll = self.rng.random_range(0..self.weights.total());
        for action in WorkloadAction::ALL {
            let weight = self.weights.weight(action);
            if weight == 0 {
                continue;
            }
            if roll < weight {
                self.selected.increment(action);
                return action;
            }
            roll -= weight;
        }
        unreachable!("validated action weights must contain at least one non-zero entry")
    }

    #[must_use]
    pub fn weights(&self) -> ActionWeights {
        self.weights
    }

    #[must_use]
    pub fn selected(&self) -> &WorkloadActionCounts {
        &self.selected
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generator_is_seed_deterministic() {
        let config = WorkloadConfig::default();
        let mut left = WorkloadGenerator::new(42, config.clone()).unwrap();
        let mut right = WorkloadGenerator::new(42, config).unwrap();

        let left_actions = (0..100).map(|_| left.next_action()).collect::<Vec<_>>();
        let right_actions = (0..100).map(|_| right.next_action()).collect::<Vec<_>>();

        assert_eq!(left_actions, right_actions);
        assert_eq!(left.selected(), right.selected());
    }

    #[test]
    fn zero_weight_actions_are_never_selected() {
        let config = WorkloadConfig {
            weights: ActionWeights {
                publish_message: 0,
                create_versioned_message: 0,
                mutate_versioned_message: 0,
                presence_transition: 5,
                recovery_probe: 0,
                purge_history: 0,
                push_register_device: 0,
                push_delete_device: 0,
                push_subscribe: 0,
                push_unsubscribe: 0,
                push_publish: 0,
                push_scheduled_publish: 0,
                push_provider_feedback: 0,
                push_repair: 0,
                oracle_check: 0,
            },
        };
        let mut generator = WorkloadGenerator::new(7, config).unwrap();

        for _ in 0..100 {
            assert_eq!(generator.next_action(), WorkloadAction::PresenceTransition);
        }
        assert_eq!(
            generator
                .selected()
                .count(WorkloadAction::PresenceTransition),
            100
        );
    }

    #[test]
    fn rejects_empty_weight_profiles() {
        let error = WorkloadGenerator::new(
            7,
            WorkloadConfig {
                weights: ActionWeights {
                    publish_message: 0,
                    create_versioned_message: 0,
                    mutate_versioned_message: 0,
                    presence_transition: 0,
                    recovery_probe: 0,
                    purge_history: 0,
                    push_register_device: 0,
                    push_delete_device: 0,
                    push_subscribe: 0,
                    push_unsubscribe: 0,
                    push_publish: 0,
                    push_scheduled_publish: 0,
                    push_provider_feedback: 0,
                    push_repair: 0,
                    oracle_check: 0,
                },
            },
        )
        .unwrap_err();

        assert!(error.to_string().contains("at least one workload action"));
    }
}
