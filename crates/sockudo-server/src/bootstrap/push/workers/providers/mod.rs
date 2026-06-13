#[cfg(all(feature = "push", feature = "monolith", feature = "push-apns"))]
mod apns;
#[cfg(all(feature = "push", feature = "monolith", feature = "push-fcm"))]
mod fcm;
#[cfg(all(feature = "push", feature = "monolith", feature = "push-hms"))]
mod hms;
#[cfg(all(feature = "push", feature = "monolith", feature = "push-webpush"))]
mod webpush;
#[cfg(all(feature = "push", feature = "monolith", feature = "push-wns"))]
mod wns;

#[cfg(any(
    all(feature = "push", feature = "monolith", feature = "push-fcm"),
    all(feature = "push", feature = "monolith", feature = "push-hms"),
    all(feature = "push", feature = "monolith", feature = "push-wns"),
))]
use sockudo_core::error::{Error, Result};
#[cfg(any(
    all(feature = "push", feature = "monolith", feature = "push-fcm"),
    all(feature = "push", feature = "monolith", feature = "push-hms"),
    all(feature = "push", feature = "monolith", feature = "push-wns"),
))]
use std::{env, sync::Arc};

#[cfg(all(feature = "push", feature = "monolith", feature = "push-apns"))]
pub(super) use apns::start_apns_provider_workers;
#[cfg(all(feature = "push", feature = "monolith", feature = "push-fcm"))]
pub(super) use fcm::start_fcm_provider_workers;
#[cfg(all(feature = "push", feature = "monolith", feature = "push-hms"))]
pub(super) use hms::start_hms_provider_workers;
#[cfg(all(feature = "push", feature = "monolith", feature = "push-webpush"))]
pub(super) use webpush::start_webpush_provider_workers;
#[cfg(all(feature = "push", feature = "monolith", feature = "push-wns"))]
pub(super) use wns::start_wns_provider_workers;

#[cfg(all(feature = "push", feature = "monolith", not(feature = "push-apns")))]
pub(super) fn start_apns_provider_workers(
    _config: &sockudo_core::options::ServerOptions,
    _store: sockudo_push::DynPushStore,
    _queue: sockudo_push::DynPushQueue,
) {
    tracing::warn!(
        "push.apns_enabled is true but the binary was not compiled with the push-apns feature"
    );
}

#[cfg(all(feature = "push", feature = "monolith", not(feature = "push-fcm")))]
pub(super) fn start_fcm_provider_workers(
    _config: &sockudo_core::options::ServerOptions,
    _store: sockudo_push::DynPushStore,
    _queue: sockudo_push::DynPushQueue,
) {
    tracing::warn!(
        "push.fcm_enabled is true but the binary was not compiled with the push-fcm feature"
    );
}

#[cfg(all(feature = "push", feature = "monolith", not(feature = "push-hms")))]
pub(super) fn start_hms_provider_workers(
    _config: &sockudo_core::options::ServerOptions,
    _queue: sockudo_push::DynPushQueue,
) {
    tracing::warn!(
        "push.hms_enabled is true but the binary was not compiled with the push-hms feature"
    );
}

#[cfg(all(feature = "push", feature = "monolith", not(feature = "push-webpush")))]
pub(super) fn start_webpush_provider_workers(
    _config: &sockudo_core::options::ServerOptions,
    _queue: sockudo_push::DynPushQueue,
) {
    tracing::warn!(
        "push.webpush_enabled is true but the binary was not compiled with the push-webpush feature"
    );
}

#[cfg(all(feature = "push", feature = "monolith", not(feature = "push-wns")))]
pub(super) fn start_wns_provider_workers(
    _config: &sockudo_core::options::ServerOptions,
    _queue: sockudo_push::DynPushQueue,
) {
    tracing::warn!(
        "push.wns_enabled is true but the binary was not compiled with the push-wns feature"
    );
}

#[cfg(any(
    all(feature = "push", feature = "monolith", feature = "push-fcm"),
    all(feature = "push", feature = "monolith", feature = "push-hms"),
    all(feature = "push", feature = "monolith", feature = "push-wns"),
))]
pub(super) fn static_push_token_provider(
    provider: &'static str,
    env_names: &[&str],
) -> Result<sockudo_push::CachedTokenProvider> {
    for env_name in env_names {
        if let Ok(token) = env::var(env_name) {
            if token.trim().is_empty() {
                return Err(Error::Internal(format!("{env_name} is empty")));
            }
            return Ok(sockudo_push::CachedTokenProvider::new(Arc::new(
                sockudo_push::StaticTokenSource::new(
                    sockudo_push::SecretString::new(token).map_err(|error| {
                        Error::Internal(format!("invalid {provider} provider token: {error}"))
                    })?,
                    u64::MAX,
                ),
            )));
        }
    }
    Err(Error::Internal(format!(
        "{provider} dispatch requires one of {}",
        env_names.join("/")
    )))
}
