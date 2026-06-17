use crate::{Result, Sockudo, SockudoError};
use reqwest::Response;
use serde::{Deserialize, Serialize};
use sonic_rs::{JsonValueTrait, Value, json};
use std::collections::{BTreeMap, HashMap};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PushCursorParams {
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

impl PushCursorParams {
    pub fn to_map(&self) -> BTreeMap<String, String> {
        let mut map = BTreeMap::new();
        if let Some(limit) = self.limit {
            map.insert("limit".to_string(), limit.to_string());
        }
        if let Some(cursor) = &self.cursor {
            map.insert("cursor".to_string(), cursor.clone());
        }
        map
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PushSubscriptionParams {
    pub limit: Option<u32>,
    pub cursor: Option<String>,
    pub channel: Option<String>,
    pub device_id: Option<String>,
}

impl PushSubscriptionParams {
    pub fn to_map(&self) -> BTreeMap<String, String> {
        let mut map = PushCursorParams {
            limit: self.limit,
            cursor: self.cursor.clone(),
        }
        .to_map();
        if let Some(channel) = &self.channel {
            map.insert("channel".to_string(), channel.clone());
        }
        if let Some(device_id) = &self.device_id {
            map.insert("deviceId".to_string(), device_id.clone());
        }
        map
    }
}

fn push_headers(capability: &str, device_identity_token: Option<&str>) -> HashMap<String, String> {
    let mut headers = HashMap::from([(
        "X-Sockudo-Push-Capability".to_string(),
        capability.to_string(),
    )]);
    if let Some(token) = device_identity_token {
        headers.insert(
            "X-Sockudo-Device-Identity-Token".to_string(),
            token.to_string(),
        );
    }
    headers
}

fn push_path(path: &str) -> String {
    format!("/push{path}")
}

impl Sockudo {
    pub async fn activate_device(&self, device: &Value) -> Result<Response> {
        self.post_with_headers(
            &push_path("/deviceRegistrations"),
            device,
            &push_headers("push-admin", None),
        )
        .await
    }

    pub async fn create_device_activation(&self, device: &Value) -> Result<Response> {
        self.activate_device(device).await
    }

    pub async fn update_device_registration(
        &self,
        device: &Value,
        device_identity_token: &str,
    ) -> Result<Response> {
        self.post_with_headers(
            &push_path("/deviceRegistrations"),
            device,
            &push_headers("push-subscribe", Some(device_identity_token)),
        )
        .await
    }

    pub async fn list_device_registrations(
        &self,
        params: Option<&PushCursorParams>,
    ) -> Result<Response> {
        let params = params.map(PushCursorParams::to_map);
        self.get_with_headers(
            &push_path("/deviceRegistrations"),
            params.as_ref(),
            &push_headers("push-admin", None),
        )
        .await
    }

    pub async fn get_device_registration(
        &self,
        device_id: &str,
        device_identity_token: Option<&str>,
    ) -> Result<Response> {
        let capability = if device_identity_token.is_some() {
            "push-subscribe"
        } else {
            "push-admin"
        };
        self.get_with_headers(
            &push_path(&format!("/deviceRegistrations/{device_id}")),
            None,
            &push_headers(capability, device_identity_token),
        )
        .await
    }

    pub async fn delete_device_registration(
        &self,
        device_id: &str,
        device_identity_token: Option<&str>,
    ) -> Result<Response> {
        let capability = if device_identity_token.is_some() {
            "push-subscribe"
        } else {
            "push-admin"
        };
        self.delete_with_headers(
            &push_path(&format!("/deviceRegistrations/{device_id}")),
            None,
            &push_headers(capability, device_identity_token),
        )
        .await
    }

    pub async fn remove_device_registrations_by_client(&self, client_id: &str) -> Result<Response> {
        let params = BTreeMap::from([("clientId".to_string(), client_id.to_string())]);
        self.delete_with_headers(
            &push_path("/deviceRegistrations"),
            Some(&params),
            &push_headers("push-admin", None),
        )
        .await
    }

    pub async fn upsert_channel_push_subscription(
        &self,
        subscription: &Value,
        device_identity_token: Option<&str>,
    ) -> Result<Response> {
        let capability = if device_identity_token.is_some() {
            "push-subscribe"
        } else {
            "push-admin"
        };
        self.post_with_headers(
            &push_path("/channelSubscriptions"),
            subscription,
            &push_headers(capability, device_identity_token),
        )
        .await
    }

    pub async fn list_channel_push_subscriptions(
        &self,
        params: Option<&PushSubscriptionParams>,
        device_identity_token: Option<&str>,
    ) -> Result<Response> {
        let capability = if device_identity_token.is_some() {
            "push-subscribe"
        } else {
            "push-admin"
        };
        let params = params.map(PushSubscriptionParams::to_map);
        self.get_with_headers(
            &push_path("/channelSubscriptions"),
            params.as_ref(),
            &push_headers(capability, device_identity_token),
        )
        .await
    }

    pub async fn delete_channel_push_subscriptions(
        &self,
        params: &PushSubscriptionParams,
        device_identity_token: Option<&str>,
    ) -> Result<Response> {
        let capability = if device_identity_token.is_some() {
            "push-subscribe"
        } else {
            "push-admin"
        };
        let params = params.to_map();
        self.delete_with_headers(
            &push_path("/channelSubscriptions"),
            Some(&params),
            &push_headers(capability, device_identity_token),
        )
        .await
    }

    pub async fn list_channel_push_subscription_channels(
        &self,
        params: Option<&PushCursorParams>,
    ) -> Result<Response> {
        let params = params.map(PushCursorParams::to_map);
        self.get_with_headers(
            &push_path("/channelSubscriptions/channels"),
            params.as_ref(),
            &push_headers("push-admin", None),
        )
        .await
    }

    pub async fn list_push_credentials(
        &self,
        params: Option<&PushCursorParams>,
    ) -> Result<Response> {
        let params = params.map(PushCursorParams::to_map);
        self.get_with_headers(
            &push_path("/credentials"),
            params.as_ref(),
            &push_headers("push-admin", None),
        )
        .await
    }

    pub async fn put_push_credential(
        &self,
        provider: &str,
        credential: &Value,
    ) -> Result<Response> {
        self.post_with_headers(
            &push_path(&format!("/credentials/{provider}")),
            credential,
            &push_headers("push-admin", None),
        )
        .await
    }

    pub async fn publish_push(&self, request: &Value) -> Result<Response> {
        let mut request = request.clone();
        request["sync"] = json!(false);
        self.post_with_headers(
            &push_path("/publish"),
            &request,
            &push_headers("push-admin", None),
        )
        .await
    }

    pub async fn publish_push_direct(&self, request: &Value) -> Result<Response> {
        self.publish_push(request).await
    }

    pub async fn publish_push_batch(&self, requests: &[Value]) -> Result<Response> {
        let requests: Vec<Value> = requests
            .iter()
            .map(|request| {
                let mut item = request.clone();
                item["sync"] = json!(false);
                item
            })
            .collect();
        let body = sonic_rs::to_value(&requests).map_err(SockudoError::Json)?;
        self.post_with_headers(
            &push_path("/batch/publish"),
            &body,
            &push_headers("push-admin", None),
        )
        .await
    }

    pub async fn schedule_push(&self, request: &Value) -> Result<Response> {
        if request.get("notBeforeMs").is_none() {
            return Err(SockudoError::Validation {
                message: "scheduled push requires notBeforeMs".to_string(),
            });
        }
        self.publish_push(request).await
    }

    pub async fn get_publish_status(&self, publish_id: &str) -> Result<Response> {
        self.get_with_headers(
            &push_path(&format!("/publish/{publish_id}/status")),
            None,
            &push_headers("push-admin", None),
        )
        .await
    }

    pub async fn cancel_scheduled_push(&self, publish_id: &str) -> Result<Response> {
        self.delete_with_headers(
            &push_path(&format!("/scheduled/{publish_id}")),
            None,
            &push_headers("push-admin", None),
        )
        .await
    }

    pub async fn post_push_delivery_status(&self, event: &Value) -> Result<Response> {
        self.post_with_headers(
            &push_path("/deliveryStatus"),
            event,
            &push_headers("push-admin", None),
        )
        .await
    }
}
