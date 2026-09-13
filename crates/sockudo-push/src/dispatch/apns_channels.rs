use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sonic_rs::prelude::*;
use sonic_rs::{Value, json};
use thiserror::Error;

use super::auth::{CachedTokenProvider, auth_error};
use super::http::{
    ProviderEndpointConfig, ProviderHttpClient, ProviderHttpMethod, ProviderHttpRequest,
    ProviderHttpResponse,
};
use crate::domain::{ApnsChannelStoragePolicy, ProviderError, SecretString, stable_hash};
use crate::pipeline::now_ms;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApnsBroadcastChannel {
    pub channel_id: SecretString,
    pub storage_policy: ApnsChannelStoragePolicy,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApnsBroadcastChannelList {
    pub channels: Vec<SecretString>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ApnsChannelManagerError {
    #[error("invalid APNs channel configuration: {0}")]
    InvalidConfiguration(String),
    #[error("APNs channel management transport failed")]
    Transport,
    #[error("APNs channel management rejected the request with status {status}: {reason}")]
    Provider {
        status: u16,
        reason: String,
        retry_after_ms: Option<u64>,
    },
}

#[derive(Clone)]
pub struct ApnsChannelManager {
    bundle_id: String,
    endpoint: ProviderEndpointConfig,
    token_provider: Option<CachedTokenProvider>,
    http: Arc<dyn ProviderHttpClient + Send + Sync>,
}

impl ApnsChannelManager {
    pub fn new(
        bundle_id: impl Into<String>,
        token_provider: CachedTokenProvider,
        http: Arc<dyn ProviderHttpClient + Send + Sync>,
    ) -> Result<Self, ApnsChannelManagerError> {
        let bundle_id = bundle_id.into();
        validate_bundle_id(&bundle_id)?;
        Ok(Self {
            bundle_id,
            endpoint: ProviderEndpointConfig {
                base_url: "https://api-manage-broadcast.push.apple.com:2196".to_owned(),
                credential_id: "apns".to_owned(),
            },
            token_provider: Some(token_provider),
            http,
        })
    }

    pub fn new_with_tls_identity(
        bundle_id: impl Into<String>,
        http: Arc<dyn ProviderHttpClient + Send + Sync>,
    ) -> Result<Self, ApnsChannelManagerError> {
        let bundle_id = bundle_id.into();
        validate_bundle_id(&bundle_id)?;
        Ok(Self {
            bundle_id,
            endpoint: ProviderEndpointConfig {
                base_url: "https://api-manage-broadcast.push.apple.com:2196".to_owned(),
                credential_id: "apns".to_owned(),
            },
            token_provider: None,
            http,
        })
    }

    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.endpoint.base_url = base_url.into();
        self
    }

    pub async fn create(
        &self,
        storage_policy: ApnsChannelStoragePolicy,
    ) -> Result<ApnsBroadcastChannel, ApnsChannelManagerError> {
        let body = sonic_rs::to_vec(&json!({
            "message-storage-policy": storage_policy.wire_value(),
            "push-type": "LiveActivity"
        }))
        .map_err(|_| {
            ApnsChannelManagerError::InvalidConfiguration(
                "channel request could not be serialized".to_owned(),
            )
        })?;
        let response = self
            .send_with_auth_retry(ProviderHttpMethod::Post, "channels", None, body)
            .await?;
        let channel_id = channel_id_from_response(&response).ok_or_else(|| {
            ApnsChannelManagerError::Provider {
                status: response.status,
                reason: "APNs response omitted apns-channel-id".to_owned(),
                retry_after_ms: None,
            }
        })?;
        Ok(ApnsBroadcastChannel {
            channel_id,
            storage_policy,
        })
    }

    pub async fn get(
        &self,
        channel_id: SecretString,
    ) -> Result<ApnsBroadcastChannel, ApnsChannelManagerError> {
        validate_channel_id(channel_id.expose_secret())?;
        let response = self
            .send_with_auth_retry(
                ProviderHttpMethod::Get,
                "channels",
                Some(&channel_id),
                Vec::new(),
            )
            .await?;
        let storage_policy = storage_policy_from_response(&response).ok_or_else(|| {
            ApnsChannelManagerError::Provider {
                status: response.status,
                reason: "APNs response omitted message-storage-policy".to_owned(),
                retry_after_ms: None,
            }
        })?;
        Ok(ApnsBroadcastChannel {
            channel_id: channel_id_from_response(&response).unwrap_or(channel_id),
            storage_policy,
        })
    }

    pub async fn list(&self) -> Result<ApnsBroadcastChannelList, ApnsChannelManagerError> {
        let response = self
            .send_with_auth_retry(ProviderHttpMethod::Get, "all-channels", None, Vec::new())
            .await?;
        let value: Value = sonic_rs::from_slice(&response.body).map_err(|_| {
            ApnsChannelManagerError::Provider {
                status: response.status,
                reason: "APNs response omitted channels".to_owned(),
                retry_after_ms: None,
            }
        })?;
        let channels = value
            .get("channels")
            .and_then(Value::as_array)
            .ok_or_else(|| ApnsChannelManagerError::Provider {
                status: response.status,
                reason: "APNs response omitted channels".to_owned(),
                retry_after_ms: None,
            })?
            .iter()
            .map(|value| {
                let channel = value
                    .as_str()
                    .ok_or_else(|| ApnsChannelManagerError::Provider {
                        status: response.status,
                        reason: "APNs response contained an invalid channel ID".to_owned(),
                        retry_after_ms: None,
                    })?;
                validate_channel_id(channel)?;
                SecretString::new(channel).map_err(|error| {
                    ApnsChannelManagerError::InvalidConfiguration(error.to_string())
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ApnsBroadcastChannelList { channels })
    }

    pub async fn delete(&self, channel_id: SecretString) -> Result<(), ApnsChannelManagerError> {
        validate_channel_id(channel_id.expose_secret())?;
        self.send_with_auth_retry(
            ProviderHttpMethod::Delete,
            "channels",
            Some(&channel_id),
            Vec::new(),
        )
        .await?;
        Ok(())
    }

    async fn send_with_auth_retry(
        &self,
        method: ProviderHttpMethod,
        resource: &str,
        channel_id: Option<&SecretString>,
        body: Vec<u8>,
    ) -> Result<ProviderHttpResponse, ApnsChannelManagerError> {
        let request_id = channel_request_id(&self.bundle_id, method);
        let mut response = self
            .send(method, resource, channel_id, body.clone(), &request_id)
            .await?;
        if self.token_provider.is_some() && is_expired_provider_token(&response) {
            if let Some(token_provider) = &self.token_provider {
                token_provider.invalidate().await;
            }
            response = self
                .send(method, resource, channel_id, body, &request_id)
                .await?;
        }
        if (200..300).contains(&response.status) {
            Ok(response)
        } else {
            Err(provider_response_error(&response))
        }
    }

    async fn send(
        &self,
        method: ProviderHttpMethod,
        resource: &str,
        channel_id: Option<&SecretString>,
        body: Vec<u8>,
        request_id: &str,
    ) -> Result<ProviderHttpResponse, ApnsChannelManagerError> {
        let token = if let Some(token_provider) = &self.token_provider {
            Some(
                token_provider
                    .bearer_token(now_ms())
                    .await
                    .map_err(auth_error)
                    .map_err(map_auth_error)?,
            )
        } else {
            None
        };
        let mut headers =
            BTreeMap::from([("content-type".to_owned(), "application/json".to_owned())]);
        headers.insert("apns-request-id".to_owned(), request_id.to_owned());
        if let Some(channel_id) = channel_id {
            headers.insert(
                "apns-channel-id".to_owned(),
                channel_id.expose_secret().to_owned(),
            );
        }
        self.http
            .send(ProviderHttpRequest {
                method,
                url: self
                    .endpoint
                    .joined_url(&format!("/1/apps/{}/{resource}", self.bundle_id)),
                headers,
                authorization: token,
                body,
            })
            .await
            .map_err(|_| ApnsChannelManagerError::Transport)
    }
}

fn channel_request_id(bundle_id: &str, method: ProviderHttpMethod) -> String {
    let method = match method {
        ProviderHttpMethod::Get => "get",
        ProviderHttpMethod::Post => "post",
        ProviderHttpMethod::Delete => "delete",
    };
    let nonce = rand::random::<u64>();
    let digest = stable_hash(format!("{bundle_id}:{method}:{}:{nonce}", now_ms()).as_bytes());
    format!(
        "{}-{}-{}-{}-{}",
        &digest[0..8],
        &digest[8..12],
        &digest[12..16],
        &digest[16..20],
        &digest[20..32]
    )
}

fn map_auth_error(error: ProviderError) -> ApnsChannelManagerError {
    ApnsChannelManagerError::Provider {
        status: 401,
        reason: error
            .reason
            .unwrap_or_else(|| "APNs provider authentication failed".to_owned()),
        retry_after_ms: error.retry_after_ms,
    }
}

fn validate_bundle_id(bundle_id: &str) -> Result<(), ApnsChannelManagerError> {
    if bundle_id.is_empty()
        || bundle_id.len() > 255
        || !bundle_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
    {
        return Err(ApnsChannelManagerError::InvalidConfiguration(
            "bundle ID has an invalid format".to_owned(),
        ));
    }
    Ok(())
}

fn validate_channel_id(channel_id: &str) -> Result<(), ApnsChannelManagerError> {
    if channel_id.is_empty()
        || channel_id.len() > 4_096
        || !channel_id.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'+' | b'/' | b'=')
        })
    {
        return Err(ApnsChannelManagerError::InvalidConfiguration(
            "channel ID has an invalid format".to_owned(),
        ));
    }
    Ok(())
}

fn channel_id_from_response(response: &ProviderHttpResponse) -> Option<SecretString> {
    response
        .headers
        .get("apns-channel-id")
        .and_then(|value| SecretString::new(value.clone()).ok())
}

fn storage_policy_from_response(
    response: &ProviderHttpResponse,
) -> Option<ApnsChannelStoragePolicy> {
    let value: Value = sonic_rs::from_slice(&response.body).ok()?;
    match value
        .get("message-storage-policy")
        .and_then(Value::as_u64)?
    {
        0 => Some(ApnsChannelStoragePolicy::NoStorage),
        1 => Some(ApnsChannelStoragePolicy::MostRecent),
        _ => None,
    }
}

fn provider_response_error(response: &ProviderHttpResponse) -> ApnsChannelManagerError {
    let reason = sonic_rs::from_slice::<Value>(&response.body)
        .ok()
        .and_then(|body| {
            body.get("reason")
                .or_else(|| body.get("error"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| "provider rejected channel request".to_owned());
    let retry_after_ms = response
        .headers
        .get("retry-after")
        .and_then(|value| value.parse::<u64>().ok())
        .map(|seconds| now_ms().saturating_add(seconds.saturating_mul(1_000)));
    ApnsChannelManagerError::Provider {
        status: response.status,
        reason,
        retry_after_ms,
    }
}

fn is_expired_provider_token(response: &ProviderHttpResponse) -> bool {
    response.status == 403
        && String::from_utf8_lossy(&response.body)
            .to_ascii_lowercase()
            .contains("expiredprovidertoken")
}
