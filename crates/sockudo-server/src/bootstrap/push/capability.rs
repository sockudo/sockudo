use std::collections::{BTreeMap, BTreeSet};
use std::fs;

use sockudo_core::options::{PushQueueDriver, PushStorageDriver, ServerOptions};
use sockudo_push::{
    DynPushStore, FanoutRegime, ProviderCredential, ProviderCredentialMaterial, PublishTarget,
    PushProviderKind,
};
use sonic_rs::JsonValueTrait;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PushAdmissionSnapshot {
    storage_driver: PushStorageDriver,
    queue_driver: PushQueueDriver,
    allow_memory_drivers: bool,
    production_mode: bool,
    planner_worker_count: u32,
    shard_worker_count: u32,
    feedback_worker_count: u32,
    backpressure_lag_threshold_secs: u64,
    providers: BTreeMap<PushProviderKind, PushProviderCapability>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PushProviderCapability {
    status: PushProviderStatus,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum PushProviderStatus {
    Active,
    WorkerDisabled(&'static str),
    UnsupportedByFeature(&'static str),
    MissingCredentials(String),
    StoreUnavailable(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PushAdmissionRejection {
    PushDisabled,
    NoProviderWorkers,
    ProviderWorkerDisabled {
        provider: PushProviderKind,
        reason: &'static str,
    },
    ProviderUnsupported {
        provider: PushProviderKind,
        feature: &'static str,
    },
    ProviderMissingCredentials {
        provider: PushProviderKind,
        reason: String,
    },
    ProviderStoreUnavailable {
        provider: PushProviderKind,
        reason: String,
    },
    PipelineWorkerDisabled {
        reason: &'static str,
    },
    StoreUnsafeForProduction {
        reason: String,
    },
    QueueUnavailable {
        reason: String,
    },
}

impl PushAdmissionSnapshot {
    pub(crate) async fn from_config(config: &ServerOptions, store: &DynPushStore) -> Self {
        let mut providers = BTreeMap::new();
        for provider in all_providers() {
            providers.insert(
                provider,
                PushProviderCapability {
                    status: provider_status(config, store, provider).await,
                },
            );
        }

        Self {
            storage_driver: config.push.storage_driver.clone(),
            queue_driver: config.push.queue_driver.clone(),
            allow_memory_drivers: config.push.allow_memory_drivers,
            production_mode: config.mode.eq_ignore_ascii_case("production"),
            planner_worker_count: config.push.planner_worker_count,
            shard_worker_count: config.push.shard_worker_count,
            feedback_worker_count: config.push.feedback_worker_count,
            backpressure_lag_threshold_secs: config.push.backpressure_lag_threshold_secs,
            providers,
        }
    }

    #[cfg(test)]
    pub(crate) fn testing_active(providers: impl IntoIterator<Item = PushProviderKind>) -> Self {
        let active = providers.into_iter().collect::<BTreeSet<_>>();
        let providers = all_providers()
            .into_iter()
            .map(|provider| {
                let status = if active.contains(&provider) {
                    PushProviderStatus::Active
                } else {
                    PushProviderStatus::WorkerDisabled("provider worker intentionally disabled")
                };
                (provider, PushProviderCapability { status })
            })
            .collect();

        Self {
            storage_driver: PushStorageDriver::Memory,
            queue_driver: PushQueueDriver::Memory,
            allow_memory_drivers: true,
            production_mode: false,
            planner_worker_count: 1,
            shard_worker_count: 1,
            feedback_worker_count: 1,
            backpressure_lag_threshold_secs: 60,
            providers,
        }
    }

    #[cfg(test)]
    pub(crate) fn testing_with_memory_policy(
        production_mode: bool,
        allow_memory_drivers: bool,
    ) -> Self {
        Self {
            production_mode,
            allow_memory_drivers,
            ..Self::testing_active([PushProviderKind::Fcm])
        }
    }

    pub(crate) fn storage_driver(&self) -> &PushStorageDriver {
        &self.storage_driver
    }

    pub(crate) fn queue_driver(&self) -> &PushQueueDriver {
        &self.queue_driver
    }

    pub(crate) fn safe_for_admission(&self) -> bool {
        self.storage_rejection().is_none() && self.has_active_provider()
    }

    pub(crate) fn active_providers(&self) -> Vec<PushProviderKind> {
        self.providers
            .iter()
            .filter_map(|(provider, capability)| {
                matches!(capability.status, PushProviderStatus::Active).then_some(*provider)
            })
            .collect()
    }

    pub(crate) fn backpressure_lag_threshold_secs(&self) -> u64 {
        self.backpressure_lag_threshold_secs
    }

    pub(crate) fn rejection_for_targets(
        &self,
        targets: &[PublishTarget],
        fanout_regime: FanoutRegime,
    ) -> Option<PushAdmissionRejection> {
        if let Some(rejection) = self.storage_rejection() {
            return Some(rejection);
        }
        if let Some(rejection) = self.pipeline_worker_rejection(fanout_regime) {
            return Some(rejection);
        }

        let required_providers = required_direct_providers(targets);
        if required_providers.is_empty() {
            return self.planned_target_rejection();
        }

        for provider in required_providers {
            if let Some(rejection) = self.provider_rejection(provider) {
                return Some(rejection);
            }
        }

        None
    }

    fn pipeline_worker_rejection(
        &self,
        fanout_regime: FanoutRegime,
    ) -> Option<PushAdmissionRejection> {
        if self.planner_worker_count == 0 {
            return Some(PushAdmissionRejection::PipelineWorkerDisabled {
                reason: "planner_worker_count is zero",
            });
        }
        if self.feedback_worker_count == 0 {
            return Some(PushAdmissionRejection::PipelineWorkerDisabled {
                reason: "feedback_worker_count is zero",
            });
        }
        if fanout_regime == FanoutRegime::ShardPath && self.shard_worker_count == 0 {
            return Some(PushAdmissionRejection::PipelineWorkerDisabled {
                reason: "shard_worker_count is zero for shard-path publish",
            });
        }
        None
    }

    fn storage_rejection(&self) -> Option<PushAdmissionRejection> {
        if !self.production_mode || self.allow_memory_drivers {
            return None;
        }
        if self.storage_driver == PushStorageDriver::Memory {
            return Some(PushAdmissionRejection::StoreUnsafeForProduction {
                reason: "push.storage_driver=memory is node-local and unsafe in production"
                    .to_owned(),
            });
        }
        if self.queue_driver == PushQueueDriver::Memory {
            return Some(PushAdmissionRejection::StoreUnsafeForProduction {
                reason: "push.queue_driver=memory is node-local and unsafe in production"
                    .to_owned(),
            });
        }
        None
    }

    fn planned_target_rejection(&self) -> Option<PushAdmissionRejection> {
        if self.has_active_provider() {
            return None;
        }
        if self.providers.values().all(|capability| {
            matches!(
                capability.status,
                PushProviderStatus::WorkerDisabled("provider disabled in config")
            )
        }) {
            return Some(PushAdmissionRejection::PushDisabled);
        }
        Some(PushAdmissionRejection::NoProviderWorkers)
    }

    fn provider_rejection(&self, provider: PushProviderKind) -> Option<PushAdmissionRejection> {
        let capability = self.providers.get(&provider)?;
        match &capability.status {
            PushProviderStatus::Active => None,
            PushProviderStatus::WorkerDisabled(reason) => {
                Some(PushAdmissionRejection::ProviderWorkerDisabled { provider, reason })
            }
            PushProviderStatus::UnsupportedByFeature(feature) => {
                Some(PushAdmissionRejection::ProviderUnsupported { provider, feature })
            }
            PushProviderStatus::MissingCredentials(reason) => {
                Some(PushAdmissionRejection::ProviderMissingCredentials {
                    provider,
                    reason: reason.clone(),
                })
            }
            PushProviderStatus::StoreUnavailable(reason) => {
                Some(PushAdmissionRejection::ProviderStoreUnavailable {
                    provider,
                    reason: reason.clone(),
                })
            }
        }
    }

    fn has_active_provider(&self) -> bool {
        self.providers
            .values()
            .any(|capability| matches!(capability.status, PushProviderStatus::Active))
    }
}

impl PushAdmissionRejection {
    pub(crate) fn message(&self) -> String {
        match self {
            Self::PushDisabled => {
                "push admission unavailable: push providers are disabled".to_owned()
            }
            Self::NoProviderWorkers => {
                "push admission unavailable: no provider delivery worker is active".to_owned()
            }
            Self::ProviderWorkerDisabled { provider, reason } => format!(
                "push admission unavailable: {} provider worker is disabled ({reason})",
                provider_label(*provider)
            ),
            Self::ProviderUnsupported { provider, feature } => format!(
                "push admission unavailable: {} provider worker requires feature `{feature}`",
                provider_label(*provider)
            ),
            Self::ProviderMissingCredentials { provider, reason } => format!(
                "push admission unavailable: {} provider credentials are not ready ({reason})",
                provider_label(*provider)
            ),
            Self::ProviderStoreUnavailable { provider, reason } => format!(
                "push admission unavailable: {} provider credential store is unavailable ({reason})",
                provider_label(*provider)
            ),
            Self::PipelineWorkerDisabled { reason } => {
                format!(
                    "push admission unavailable: local push pipeline worker is disabled ({reason})"
                )
            }
            Self::StoreUnsafeForProduction { reason } => format!(
                "push admission unavailable: {reason}; set push.allow_memory_drivers=true only for local/dev acknowledgment"
            ),
            Self::QueueUnavailable { reason } => {
                format!("push admission unavailable: queue is not ready ({reason})")
            }
        }
    }
}

async fn provider_status(
    config: &ServerOptions,
    store: &DynPushStore,
    provider: PushProviderKind,
) -> PushProviderStatus {
    if !provider_enabled(config, provider) {
        return PushProviderStatus::WorkerDisabled("provider disabled in config");
    }
    if config.push.dispatch_worker_count == 0 {
        return PushProviderStatus::WorkerDisabled("dispatch_worker_count is zero");
    }
    if !cfg!(feature = "monolith") {
        return PushProviderStatus::UnsupportedByFeature("monolith");
    }
    if let Some(feature) = missing_provider_feature(provider) {
        return PushProviderStatus::UnsupportedByFeature(feature);
    }

    match provider {
        PushProviderKind::Fcm => fcm_status(store).await,
        PushProviderKind::Apns => apns_status(store).await,
        PushProviderKind::WebPush => webpush_status(),
        PushProviderKind::Hms => hms_status(),
        PushProviderKind::Wns => wns_status(),
        PushProviderKind::Realtime => PushProviderStatus::Active,
    }
}

fn provider_enabled(config: &ServerOptions, provider: PushProviderKind) -> bool {
    match provider {
        PushProviderKind::Fcm => config.push.fcm_enabled,
        PushProviderKind::Apns => config.push.apns_enabled,
        PushProviderKind::WebPush => config.push.webpush_enabled,
        PushProviderKind::Hms => config.push.hms_enabled,
        PushProviderKind::Wns => config.push.wns_enabled,
        PushProviderKind::Realtime => config.ably_compat.enabled,
    }
}

fn missing_provider_feature(provider: PushProviderKind) -> Option<&'static str> {
    match provider {
        PushProviderKind::Fcm if !cfg!(feature = "push-fcm") => Some("push-fcm"),
        PushProviderKind::Apns if !cfg!(feature = "push-apns") => Some("push-apns"),
        PushProviderKind::WebPush if !cfg!(feature = "push-webpush") => Some("push-webpush"),
        PushProviderKind::Hms if !cfg!(feature = "push-hms") => Some("push-hms"),
        PushProviderKind::Wns if !cfg!(feature = "push-wns") => Some("push-wns"),
        _ => None,
    }
}

async fn fcm_status(store: &DynPushStore) -> PushProviderStatus {
    let configured_project_id = match optional_env_value(&["FCM_PROJECT_ID", "PUSH_FCM_PROJECT_ID"])
    {
        Ok(value) => value,
        Err(reason) => return PushProviderStatus::MissingCredentials(reason),
    };

    match stored_credential(
        store,
        PushProviderKind::Fcm,
        &["FCM_APP_ID", "PUSH_FCM_APP_ID"],
        &["FCM_CREDENTIAL_ID", "PUSH_FCM_CREDENTIAL_ID"],
        "fcm",
    )
    .await
    {
        StoredCredentialStatus::Found(credential) => {
            return fcm_stored_credential_status(credential, configured_project_id.as_deref());
        }
        StoredCredentialStatus::StoreUnavailable(reason) => {
            return PushProviderStatus::StoreUnavailable(reason);
        }
        StoredCredentialStatus::Missing(reason) => {
            return PushProviderStatus::MissingCredentials(reason);
        }
        StoredCredentialStatus::NotConfigured => {}
    }

    match fcm_service_account_project_id(configured_project_id.as_deref()) {
        Ok(ServiceAccountProject::Ready) => PushProviderStatus::Active,
        Ok(ServiceAccountProject::NotConfigured) => {
            match env_present(&["FCM_PROVIDER_TOKEN", "PUSH_FCM_PROVIDER_TOKEN"]) {
                Ok(true) if configured_project_id.is_some() => PushProviderStatus::Active,
                Ok(true) => PushProviderStatus::MissingCredentials(
                    "FCM_PROJECT_ID/PUSH_FCM_PROJECT_ID is required with a static provider token"
                        .to_owned(),
                ),
                Ok(false) => PushProviderStatus::MissingCredentials(
                    "configure FCM_SERVICE_ACCOUNT_JSON_PATH/PUSH_FCM_SERVICE_ACCOUNT_JSON_PATH, FCM_SERVICE_ACCOUNT_JSON/PUSH_FCM_SERVICE_ACCOUNT_JSON, FCM_PROVIDER_TOKEN/PUSH_FCM_PROVIDER_TOKEN, or stored FCM credentials"
                        .to_owned(),
                ),
                Err(reason) => PushProviderStatus::MissingCredentials(reason),
            }
        }
        Err(reason) => PushProviderStatus::MissingCredentials(reason),
    }
}

fn fcm_stored_credential_status(
    credential: ProviderCredential,
    configured_project_id: Option<&str>,
) -> PushProviderStatus {
    let ProviderCredentialMaterial::Fcm {
        service_account_json,
    } = credential.material
    else {
        return PushProviderStatus::MissingCredentials(
            "stored credential material is not FCM".to_owned(),
        );
    };

    if configured_project_id.is_some() {
        return PushProviderStatus::Active;
    }

    let Ok(service_account_json) =
        crate::push_http::decrypt_credential_secret(&service_account_json)
    else {
        return PushProviderStatus::MissingCredentials(
            "stored FCM credential cannot be decrypted".to_owned(),
        );
    };
    match service_account_json_project_id(&service_account_json) {
        Ok(true) => PushProviderStatus::Active,
        Ok(false) => PushProviderStatus::MissingCredentials(
            "stored FCM service account JSON has no project_id and no FCM_PROJECT_ID/PUSH_FCM_PROJECT_ID is configured"
                .to_owned(),
        ),
        Err(reason) => PushProviderStatus::MissingCredentials(reason),
    }
}

async fn apns_status(store: &DynPushStore) -> PushProviderStatus {
    if let Err(reason) = env_present(&["APNS_TOPIC", "PUSH_APNS_TOPIC"]) {
        return PushProviderStatus::MissingCredentials(reason);
    }
    if !matches!(env_present(&["APNS_TOPIC", "PUSH_APNS_TOPIC"]), Ok(true)) {
        return PushProviderStatus::MissingCredentials(
            "APNS_TOPIC/PUSH_APNS_TOPIC is required".to_owned(),
        );
    }

    match stored_credential(
        store,
        PushProviderKind::Apns,
        &["APNS_APP_ID", "PUSH_APNS_APP_ID"],
        &["APNS_CREDENTIAL_ID", "PUSH_APNS_CREDENTIAL_ID"],
        "apns",
    )
    .await
    {
        StoredCredentialStatus::Found(credential) => {
            return apns_stored_credential_status(credential);
        }
        StoredCredentialStatus::StoreUnavailable(reason) => {
            return PushProviderStatus::StoreUnavailable(reason);
        }
        StoredCredentialStatus::Missing(reason) => {
            return PushProviderStatus::MissingCredentials(reason);
        }
        StoredCredentialStatus::NotConfigured => {}
    }

    match env_present(&["APNS_PROVIDER_TOKEN", "PUSH_APNS_PROVIDER_TOKEN"]) {
        Ok(true) => return PushProviderStatus::Active,
        Ok(false) => {}
        Err(reason) => return PushProviderStatus::MissingCredentials(reason),
    }

    let has_team_id = match env_present(&["APNS_TEAM_ID", "PUSH_APNS_TEAM_ID"]) {
        Ok(value) => value,
        Err(reason) => return PushProviderStatus::MissingCredentials(reason),
    };
    let has_key_id = match env_present(&["APNS_KEY_ID", "PUSH_APNS_KEY_ID"]) {
        Ok(value) => value,
        Err(reason) => return PushProviderStatus::MissingCredentials(reason),
    };
    let has_private_key = match env_present(&["APNS_PRIVATE_KEY", "PUSH_APNS_PRIVATE_KEY"]) {
        Ok(value) => value,
        Err(reason) => return PushProviderStatus::MissingCredentials(reason),
    };
    let has_private_key_path =
        match env_present(&["APNS_PRIVATE_KEY_PATH", "PUSH_APNS_PRIVATE_KEY_PATH"]) {
            Ok(value) => value,
            Err(reason) => return PushProviderStatus::MissingCredentials(reason),
        };
    if has_team_id && has_key_id && (has_private_key || has_private_key_path) {
        PushProviderStatus::Active
    } else {
        PushProviderStatus::MissingCredentials(
            "APNS_PROVIDER_TOKEN/PUSH_APNS_PROVIDER_TOKEN or APNS_TEAM_ID/PUSH_APNS_TEAM_ID + APNS_KEY_ID/PUSH_APNS_KEY_ID + APNS_PRIVATE_KEY/PUSH_APNS_PRIVATE_KEY is required"
                .to_owned(),
        )
    }
}

fn apns_stored_credential_status(credential: ProviderCredential) -> PushProviderStatus {
    let ProviderCredentialMaterial::Apns {
        p12,
        pem,
        team_id,
        key_id,
        private_key,
        ..
    } = credential.material
    else {
        return PushProviderStatus::MissingCredentials(
            "stored credential material is not APNs".to_owned(),
        );
    };

    if p12.is_some()
        || pem.is_some()
        || (team_id.is_some() && key_id.is_some() && private_key.is_some())
    {
        PushProviderStatus::Active
    } else {
        PushProviderStatus::MissingCredentials(
            "stored APNs credential requires p12, pem, or teamId/keyId/privateKey material"
                .to_owned(),
        )
    }
}

fn webpush_status() -> PushProviderStatus {
    match env_present(&["VAPID_PRIVATE_KEY", "PUSH_WEBPUSH_VAPID_PRIVATE_KEY"]) {
        Ok(true) => PushProviderStatus::Active,
        Ok(false) => PushProviderStatus::MissingCredentials(
            "VAPID_PRIVATE_KEY/PUSH_WEBPUSH_VAPID_PRIVATE_KEY is required".to_owned(),
        ),
        Err(reason) => PushProviderStatus::MissingCredentials(reason),
    }
}

fn hms_status() -> PushProviderStatus {
    let has_app_id = match env_present(&["HMS_APP_ID", "PUSH_HMS_APP_ID"]) {
        Ok(value) => value,
        Err(reason) => return PushProviderStatus::MissingCredentials(reason),
    };
    let has_token = match env_present(&["HMS_PROVIDER_TOKEN", "PUSH_HMS_PROVIDER_TOKEN"]) {
        Ok(value) => value,
        Err(reason) => return PushProviderStatus::MissingCredentials(reason),
    };
    if has_app_id && has_token {
        PushProviderStatus::Active
    } else {
        PushProviderStatus::MissingCredentials(
            "HMS_APP_ID/PUSH_HMS_APP_ID and HMS_PROVIDER_TOKEN/PUSH_HMS_PROVIDER_TOKEN are required"
                .to_owned(),
        )
    }
}

fn wns_status() -> PushProviderStatus {
    match env_present(&["WNS_PROVIDER_TOKEN", "PUSH_WNS_PROVIDER_TOKEN"]) {
        Ok(true) => PushProviderStatus::Active,
        Ok(false) => PushProviderStatus::MissingCredentials(
            "WNS_PROVIDER_TOKEN/PUSH_WNS_PROVIDER_TOKEN is required".to_owned(),
        ),
        Err(reason) => PushProviderStatus::MissingCredentials(reason),
    }
}

enum StoredCredentialStatus {
    Found(ProviderCredential),
    Missing(String),
    StoreUnavailable(String),
    NotConfigured,
}

async fn stored_credential(
    store: &DynPushStore,
    provider: PushProviderKind,
    app_env_names: &[&'static str],
    credential_env_names: &[&'static str],
    default_credential_id: &'static str,
) -> StoredCredentialStatus {
    let app_id = match optional_env_value(app_env_names) {
        Ok(Some(app_id)) => app_id,
        Ok(None) => return StoredCredentialStatus::NotConfigured,
        Err(reason) => return StoredCredentialStatus::Missing(reason),
    };
    let credential_id = match optional_env_value(credential_env_names) {
        Ok(Some(credential_id)) => credential_id,
        Ok(None) => default_credential_id.to_owned(),
        Err(reason) => return StoredCredentialStatus::Missing(reason),
    };

    match store.get_credential(&app_id, &credential_id).await {
        Ok(Some(credential)) if credential.provider == provider => {
            StoredCredentialStatus::Found(credential)
        }
        Ok(Some(_)) => StoredCredentialStatus::Missing(format!(
            "stored credential {credential_id:?} for configured app is not {}",
            provider_label(provider)
        )),
        Ok(None) => StoredCredentialStatus::Missing(format!(
            "stored credential {credential_id:?} was not found for configured app"
        )),
        Err(error) => StoredCredentialStatus::StoreUnavailable(error.to_string()),
    }
}

enum ServiceAccountProject {
    Ready,
    NotConfigured,
}

fn fcm_service_account_project_id(
    configured_project_id: Option<&str>,
) -> Result<ServiceAccountProject, String> {
    if let Some(json) =
        optional_env_value(&["FCM_SERVICE_ACCOUNT_JSON", "PUSH_FCM_SERVICE_ACCOUNT_JSON"])?
    {
        return service_account_json_project_id(&json).map(|has_project| {
            if has_project || configured_project_id.is_some() {
                ServiceAccountProject::Ready
            } else {
                ServiceAccountProject::NotConfigured
            }
        });
    }

    let path = optional_env_value(&[
        "FCM_SERVICE_ACCOUNT_JSON_PATH",
        "PUSH_FCM_SERVICE_ACCOUNT_JSON_PATH",
        "GOOGLE_APPLICATION_CREDENTIALS",
    ])?;
    let Some(path) = path else {
        return Ok(ServiceAccountProject::NotConfigured);
    };
    let json = fs::read_to_string(&path).map_err(|error| {
        format!("failed to read FCM service account JSON from configured path: {error}")
    })?;
    service_account_json_project_id(&json).map(|has_project| {
        if has_project || configured_project_id.is_some() {
            ServiceAccountProject::Ready
        } else {
            ServiceAccountProject::NotConfigured
        }
    })
}

fn service_account_json_project_id(json: &str) -> Result<bool, String> {
    let value: sonic_rs::Value = sonic_rs::from_str(json)
        .map_err(|error| format!("FCM service account JSON is invalid: {error}"))?;
    Ok(value["project_id"]
        .as_str()
        .is_some_and(|project_id| !project_id.trim().is_empty()))
}

fn env_present(names: &[&'static str]) -> Result<bool, String> {
    optional_env_value(names).map(|value| value.is_some())
}

fn optional_env_value(names: &[&'static str]) -> Result<Option<String>, String> {
    for name in names {
        if let Ok(value) = std::env::var(name) {
            if value.trim().is_empty() {
                return Err(format!("{} is set but empty", names.join("/")));
            }
            return Ok(Some(value));
        }
    }
    Ok(None)
}

fn required_direct_providers(targets: &[PublishTarget]) -> BTreeSet<PushProviderKind> {
    targets
        .iter()
        .filter_map(|target| match target {
            PublishTarget::Recipient { recipient } => Some(recipient.provider()),
            PublishTarget::ProviderTopic { provider, .. }
            | PublishTarget::ProviderCondition { provider, .. } => Some(*provider),
            _ => None,
        })
        .collect()
}

fn all_providers() -> Vec<PushProviderKind> {
    vec![
        PushProviderKind::Fcm,
        PushProviderKind::Apns,
        PushProviderKind::WebPush,
        PushProviderKind::Hms,
        PushProviderKind::Wns,
    ]
}

fn provider_label(provider: PushProviderKind) -> &'static str {
    match provider {
        PushProviderKind::Fcm => "FCM",
        PushProviderKind::Apns => "APNs",
        PushProviderKind::WebPush => "Web Push",
        PushProviderKind::Hms => "HMS",
        PushProviderKind::Wns => "WNS",
        PushProviderKind::Realtime => "Realtime",
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use sockudo_core::options::PushQueueDriver;
    use sockudo_push::{
        MemoryPushStore, PublishTarget, PushProviderKind, PushRecipient, SecretString,
    };

    use super::*;

    #[test]
    fn testing_snapshot_requires_raw_provider_capability() {
        let snapshot = PushAdmissionSnapshot::testing_active([PushProviderKind::Apns]);
        let rejection = snapshot
            .rejection_for_targets(
                &[PublishTarget::Recipient {
                    recipient: PushRecipient::Fcm {
                        registration_token: SecretString::new("token").unwrap(),
                    },
                }],
                FanoutRegime::FastPath,
            )
            .unwrap();

        assert!(matches!(
            rejection,
            PushAdmissionRejection::ProviderWorkerDisabled {
                provider: PushProviderKind::Fcm,
                ..
            }
        ));
    }

    #[test]
    fn planned_targets_require_any_active_provider_without_scanning_recipients() {
        let snapshot =
            PushAdmissionSnapshot::testing_active(std::iter::empty::<PushProviderKind>());
        let rejection = snapshot
            .rejection_for_targets(
                &[PublishTarget::Channel {
                    channel: "room".to_owned(),
                }],
                FanoutRegime::FastPath,
            )
            .unwrap();

        assert_eq!(rejection, PushAdmissionRejection::NoProviderWorkers);
    }

    #[test]
    fn production_memory_policy_blocks_admission_without_escape_hatch() {
        let snapshot = PushAdmissionSnapshot::testing_with_memory_policy(true, false);

        assert!(matches!(
            snapshot.rejection_for_targets(
                &[PublishTarget::Channel {
                    channel: "room".to_owned()
                }],
                FanoutRegime::FastPath
            ),
            Some(PushAdmissionRejection::StoreUnsafeForProduction { .. })
        ));
    }

    #[test]
    fn shard_path_requires_shard_worker_without_planned_target_scan() {
        let mut snapshot = PushAdmissionSnapshot::testing_active([PushProviderKind::Fcm]);
        snapshot.shard_worker_count = 0;

        assert!(
            snapshot
                .rejection_for_targets(
                    &[PublishTarget::Channel {
                        channel: "room".to_owned(),
                    }],
                    FanoutRegime::FastPath,
                )
                .is_none()
        );
        assert_eq!(
            snapshot.rejection_for_targets(
                &[PublishTarget::Channel {
                    channel: "room".to_owned(),
                }],
                FanoutRegime::ShardPath,
            ),
            Some(PushAdmissionRejection::PipelineWorkerDisabled {
                reason: "shard_worker_count is zero for shard-path publish",
            })
        );
    }

    #[tokio::test]
    async fn disabled_config_is_reported_as_push_disabled() {
        let mut config = ServerOptions {
            mode: "development".to_owned(),
            ..Default::default()
        };
        config.push.queue_driver = PushQueueDriver::Memory;
        let store: DynPushStore = Arc::new(MemoryPushStore::new());

        let snapshot = PushAdmissionSnapshot::from_config(&config, &store).await;
        assert_eq!(
            snapshot.rejection_for_targets(
                &[PublishTarget::Channel {
                    channel: "room".to_owned()
                }],
                FanoutRegime::FastPath
            ),
            Some(PushAdmissionRejection::PushDisabled)
        );
    }
}
