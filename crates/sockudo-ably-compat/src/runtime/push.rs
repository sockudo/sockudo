//! Push registration, subscriptions, admission, fanout, and REST handlers.

use super::*;

#[cfg(feature = "push")]
#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub(super) struct AblyPushQuery {
    key: Option<String>,
    #[serde(rename = "access_token", alias = "accessToken")]
    access_token: Option<String>,
    client_id: Option<String>,
    channel: Option<String>,
    device_id: Option<String>,
    limit: Option<usize>,
    #[serde(rename = "cursor")]
    _cursor: Option<String>,
    format: Option<String>,
}

#[cfg(feature = "push")]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct AblyPushDeviceRequest {
    id: Option<String>,
    client_id: Option<String>,
    device_secret: Option<String>,
    platform: String,
    form_factor: String,
    metadata: Option<WireValue>,
    push: AblyPushDeviceDetailsRequest,
}

#[cfg(feature = "push")]
#[derive(Debug, Deserialize)]
pub(super) struct AblyPushDeviceDetailsRequest {
    recipient: WireValue,
}

#[cfg(feature = "push")]
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub(super) struct AblyPushSubscription {
    channel: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    device_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_id: Option<String>,
}

#[cfg(feature = "push")]
#[derive(Debug, Deserialize)]
pub(super) struct AblyPushPublishRequest {
    pub(super) recipient: WireValue,
    #[serde(default)]
    pub(super) notification: Option<WireValue>,
    #[serde(default)]
    pub(super) data: Option<WireValue>,
}

#[cfg(feature = "push")]
pub(super) fn ably_push_store(hub: &AblyCompatHub) -> Result<DynPushStore, AppError> {
    hub.push_store.clone().ok_or_else(|| {
        AppError::InternalError("Ably push requires the native Sockudo push store".to_string())
    })
}

#[cfg(feature = "push")]
pub(super) fn ably_push_queue(hub: &AblyCompatHub) -> Result<DynPushQueue, AppError> {
    hub.push_queue.clone().ok_or_else(|| {
        AppError::InternalError("Ably push requires the native Sockudo push queue".to_string())
    })
}

#[cfg(feature = "push")]
pub(super) fn ably_push_payload(
    notification: Option<Value>,
    data: Option<Value>,
) -> Result<PushPayload, AppError> {
    let string_field = |name: &'static str| -> Result<Option<String>, AppError> {
        notification
            .as_ref()
            .and_then(|value| value.get(name))
            .map(|value| {
                value.as_str().map(str::to_string).ok_or_else(|| {
                    AppError::InvalidInput(format!("push notification {name} must be a string"))
                })
            })
            .transpose()
    };
    let payload = PushPayload {
        template_id: None,
        template_data: data.unwrap_or_else(Value::new_object),
        title: string_field("title")?,
        body: string_field("body")?,
        icon: string_field("icon")?,
        sound: string_field("sound")?,
        collapse_key: notification
            .as_ref()
            .and_then(|value| value.get("collapseKey"))
            .map(|value| {
                value.as_str().map(str::to_string).ok_or_else(|| {
                    AppError::InvalidInput(
                        "push notification collapseKey must be a string".to_string(),
                    )
                })
            })
            .transpose()?,
    };
    payload
        .validate()
        .map_err(|error| AppError::InvalidInput(error.to_string()))?;
    Ok(payload)
}

#[cfg(feature = "push")]
pub(super) fn ably_push_payload_value(value: &Value) -> Result<PushPayload, AppError> {
    if !value.is_object() {
        return Err(AppError::InvalidInput(
            "message extras.push must be an object".to_string(),
        ));
    }
    ably_push_payload(
        value.get("notification").cloned(),
        value.get("data").cloned(),
    )
}

#[cfg(feature = "push")]
pub(super) fn ably_push_pipeline_error(error: PushPipelineError) -> AppError {
    match error {
        PushPipelineError::Domain(error) => AppError::InvalidInput(error.to_string()),
        PushPipelineError::InvalidPayload(message) => AppError::InvalidInput(message),
        PushPipelineError::Backpressure(message) => AppError::Protocol {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: 50003,
            message,
        },
        PushPipelineError::Storage(error) => ably_push_error(error),
        PushPipelineError::Queue(error) => {
            AppError::InternalError(format!("native push queue failed: {error}"))
        }
        PushPipelineError::PublishStatusCasExhausted { .. } => AppError::Protocol {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: 50003,
            message: "native push status update contention exceeded its retry budget".to_string(),
        },
        PushPipelineError::UnexpectedPublishStatusCasOutcome { .. } => AppError::InternalError(
            "native push status update returned an invalid outcome".to_string(),
        ),
    }
}

#[cfg(feature = "push")]
pub(super) struct AblyPushIntentRequest {
    pub(super) app_id: String,
    pub(super) publish_id: String,
    pub(super) targets: Vec<PublishTarget>,
    pub(super) required_providers: BTreeSet<PushProviderKind>,
    pub(super) payload: PushPayload,
    pub(super) expected_recipients: u64,
}

#[cfg(feature = "push")]
pub(super) async fn accept_ably_push_intent(
    store: DynPushStore,
    queue: DynPushQueue,
    admission: Option<&dyn AblyPushAdmissionGuard>,
    request: AblyPushIntentRequest,
) -> Result<PushAcceptOutcome, AppError> {
    if let Some(message) = admission.and_then(|guard| {
        guard.rejection_for(
            &request.targets,
            &request.required_providers,
            request.expected_recipients,
        )
    }) {
        return Err(AppError::Protocol {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: 50003,
            message,
        });
    }
    PushPipeline::new(store, queue, FanoutConfig::default())
        .accept_publish(
            PushAcceptRequest {
                intent: PublishIntent {
                    app_id: request.app_id,
                    publish_id: request.publish_id,
                    targets: request.targets,
                    payload: request.payload,
                    provider_overrides: Vec::new(),
                    not_before_ms: None,
                    expires_at_ms: None,
                },
                expected_recipients: request.expected_recipients,
            },
            u64::try_from(now_ms()).unwrap_or_default(),
        )
        .await
        .map_err(ably_push_pipeline_error)
}

#[cfg(feature = "push")]
#[derive(Clone)]
pub(super) struct AblyRealtimePushDispatcher {
    handler: Weak<ConnectionHandler>,
}

#[cfg(feature = "push")]
impl AblyRealtimePushDispatcher {
    pub(super) fn new(handler: Weak<ConnectionHandler>) -> Self {
        Self { handler }
    }

    pub(super) fn is_bound(&self) -> bool {
        self.handler.strong_count() > 0
    }

    pub(super) fn result(
        job: sockudo_push::DeliveryJob,
        outcome: DeliveryOutcome,
        provider_message_id: Option<String>,
        error: Option<ProviderError>,
    ) -> DeliveryResult {
        DeliveryResult {
            app_id: job.app_id,
            publish_id: job.publish_id,
            provider: job.provider,
            batch_id: job.batch_id,
            device_id: job.device_id,
            outcome,
            provider_message_id,
            error,
            attempt: job.attempt,
        }
    }

    pub(super) fn unavailable(
        job: sockudo_push::DeliveryJob,
        reason: &'static str,
    ) -> DeliveryResult {
        Self::result(
            job,
            DeliveryOutcome::Retryable,
            None,
            Some(ProviderError {
                class: "unavailable".to_string(),
                failure_class: ProviderFailureClass::ProviderTransient,
                reason: Some(reason.to_string()),
                retry_after_ms: None,
            }),
        )
    }
}

#[cfg(feature = "push")]
pub(super) fn ably_realtime_delivery_id(job: &DeliveryJob) -> String {
    stable_hash(
        format!(
            "{}:{}:{}",
            job.app_id,
            job.publish_id,
            job.recipient.token_hash()
        )
        .as_bytes(),
    )
}

#[cfg(feature = "push")]
#[async_trait::async_trait]
impl PushDispatcher for AblyRealtimePushDispatcher {
    fn provider(&self) -> PushProviderKind {
        PushProviderKind::Realtime
    }

    async fn dispatch(&self, batch: DeliveryBatch) -> Vec<DeliveryResult> {
        let Some(handler) = self.handler.upgrade() else {
            return batch
                .jobs
                .into_iter()
                .map(|job| Self::unavailable(job, "realtime service is shutting down"))
                .collect();
        };
        let app = match handler.app_manager().find_by_id(&batch.app_id).await {
            Ok(Some(app)) => app,
            Ok(None) => {
                return batch
                    .jobs
                    .into_iter()
                    .map(|job| {
                        Self::result(
                            job,
                            DeliveryOutcome::Rejected,
                            None,
                            Some(ProviderError {
                                class: "invalid_target".to_string(),
                                failure_class: ProviderFailureClass::CallerPayload,
                                reason: Some("application is unavailable".to_string()),
                                retry_after_ms: None,
                            }),
                        )
                    })
                    .collect();
            }
            Err(_) => {
                return batch
                    .jobs
                    .into_iter()
                    .map(|job| Self::unavailable(job, "application lookup is unavailable"))
                    .collect();
            }
        };

        let mut results = Vec::with_capacity(batch.jobs.len());
        for job in batch.jobs {
            let PushRecipient::Realtime { channel } = &job.recipient else {
                results.push(Self::result(
                    job,
                    DeliveryOutcome::Rejected,
                    None,
                    Some(ProviderError {
                        class: "invalid_target".to_string(),
                        failure_class: ProviderFailureClass::CallerPayload,
                        reason: Some(
                            "realtime worker received another provider target".to_string(),
                        ),
                        retry_after_ms: None,
                    }),
                ));
                continue;
            };
            let Some(rendered) = job.rendered_payload.as_ref() else {
                results.push(Self::result(
                    job,
                    DeliveryOutcome::Rejected,
                    None,
                    Some(ProviderError {
                        class: "invalid_payload".to_string(),
                        failure_class: ProviderFailureClass::CallerPayload,
                        reason: Some("rendered realtime payload is missing".to_string()),
                        retry_after_ms: None,
                    }),
                ));
                continue;
            };
            let delivery_id = ably_realtime_delivery_id(&job);
            match publish_ably_push_event(&handler, &app, channel, &rendered.payload, &delivery_id)
                .await
            {
                Ok(provider_message_id) => results.push(Self::result(
                    job,
                    DeliveryOutcome::Accepted,
                    Some(provider_message_id),
                    None,
                )),
                Err(_) => results.push(Self::unavailable(job, "realtime fanout failed")),
            }
        }
        results
    }

    async fn health_check(&self) -> HealthStatus {
        HealthStatus {
            provider: PushProviderKind::Realtime,
            healthy: self.is_bound(),
            details: if self.is_bound() {
                "native realtime fanout is available".to_string()
            } else {
                "native realtime fanout is unavailable".to_string()
            },
        }
    }
}

#[cfg(feature = "push")]
pub(super) async fn resolve_ably_push_auth(
    hub: &AblyCompatHub,
    handler: &Arc<ConnectionHandler>,
    headers: &HeaderMap,
    query: &AblyPushQuery,
) -> Result<ResolvedAblyAuth, AblyAuthError> {
    let ordinary = resolve_ably_auth(
        hub,
        handler,
        headers,
        query.key.as_deref(),
        query.access_token.as_deref(),
        query.client_id.as_deref(),
    )
    .await;
    if ordinary.is_ok() {
        return ordinary;
    }

    let token = bearer_token(headers).or_else(|| query.access_token.clone());
    let Some(token) = token else {
        return ordinary;
    };
    let Some((app_id, device_id)) = parse_ably_device_identity_token(&token) else {
        return ordinary;
    };
    let store = hub
        .push_store
        .as_ref()
        .ok_or_else(|| AblyAuthError::forbidden("Native push identity storage is unavailable"))?;
    let Some(device) = store
        .get_device(&app_id, &device_id)
        .await
        .map_err(|_| AblyAuthError::forbidden("Device identity could not be verified"))?
    else {
        return Err(AblyAuthError::invalid_credentials());
    };
    if !verify_device_identity_token(&token, &device.device_secret) {
        return Err(AblyAuthError::invalid_credentials());
    }
    let app = handler
        .app_manager()
        .find_by_id(&app_id)
        .await
        .map_err(|_| AblyAuthError::forbidden("Device application could not be loaded"))?
        .ok_or_else(AblyAuthError::invalid_credentials)?;
    let mut capabilities = restricted_ably_capabilities();
    capabilities.push_subscribe = Some(vec!["*".to_string()]);
    Ok(ResolvedAblyAuth {
        app,
        client_id: device.client_id.clone(),
        connection_client_id: device.client_id,
        capabilities: Some(capabilities),
        issued_ms: now_ms(),
        expires_ms: None,
        credential_id: format!("push-device:{}", stable_hash(token.as_bytes())),
        revocable: false,
        revocation_key: None,
        #[cfg(feature = "push")]
        push_device_id: Some(device_id),
    })
}

#[cfg(feature = "push")]
pub(super) fn generate_ably_device_identity_token(app_id: &str, device_id: &str) -> SecretString {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;

    let app_id = URL_SAFE_NO_PAD.encode(app_id.as_bytes());
    let device_id = URL_SAFE_NO_PAD.encode(device_id.as_bytes());
    let random = generate_device_identity_token();
    SecretString::new(format!(
        "v1.{app_id}.{device_id}.{}",
        random.expose_secret()
    ))
    .expect("generated device identity components are non-empty")
}

#[cfg(feature = "push")]
pub(super) fn parse_ably_device_identity_token(token: &str) -> Option<(String, String)> {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;

    let mut parts = token.split('.');
    if parts.next()? != "v1" {
        return None;
    }
    let app_id = parts.next()?;
    let device_id = parts.next()?;
    let random = parts.next()?;
    if random.is_empty() || parts.next().is_some() {
        return None;
    }
    let app_id = String::from_utf8(URL_SAFE_NO_PAD.decode(app_id).ok()?).ok()?;
    let device_id = String::from_utf8(URL_SAFE_NO_PAD.decode(device_id).ok()?).ok()?;
    Some((app_id, device_id))
}

#[cfg(feature = "push")]
pub(super) fn ably_push_admin_allowed(
    capabilities: Option<&ConnectionCapabilities>,
    resource: Option<&str>,
) -> bool {
    let Some(capabilities) = capabilities else {
        return true;
    };
    resource.map_or_else(
        || {
            capabilities
                .push_admin
                .as_deref()
                .is_some_and(|patterns| !patterns.is_empty())
        },
        |resource| capabilities.allows_push_admin(resource),
    )
}

#[cfg(feature = "push")]
pub(super) fn ensure_ably_push_admin(
    resolved: &ResolvedAblyAuth,
    resource: Option<&str>,
) -> Result<(), AppError> {
    if ably_push_admin_allowed(resolved.capabilities.as_ref(), resource) {
        Ok(())
    } else {
        Err(AppError::Forbidden(
            "push-admin capability is required".to_string(),
        ))
    }
}

#[cfg(feature = "push")]
pub(super) fn ensure_ably_push_subscribe(
    resolved: &ResolvedAblyAuth,
    channel: Option<&str>,
) -> Result<(), AppError> {
    let Some(capabilities) = resolved.capabilities.as_ref() else {
        return Ok(());
    };
    let allowed = channel.map_or_else(
        || {
            capabilities
                .push_subscribe
                .as_deref()
                .is_some_and(|patterns| !patterns.is_empty())
        },
        |channel| {
            capabilities.allows_push_subscribe(channel) || capabilities.allows_push_admin(channel)
        },
    );
    if allowed {
        Ok(())
    } else {
        Err(AppError::Forbidden(
            "push-subscribe capability is required".to_string(),
        ))
    }
}

#[cfg(feature = "push")]
pub(super) fn ably_device_identity_header(headers: &HeaderMap) -> Result<Option<&str>, AppError> {
    headers
        .get("x-ably-devicetoken")
        .map(|value| {
            value
                .to_str()
                .map_err(|_| AppError::Forbidden("Invalid device identity token".to_string()))
        })
        .transpose()
}

#[cfg(feature = "push")]
pub(super) fn ensure_ably_device_owner(
    resolved: &ResolvedAblyAuth,
    headers: &HeaderMap,
    device: &DeviceDetails,
) -> Result<(), AppError> {
    if resolved.push_device_id.as_deref() == Some(device.id.as_str()) {
        return Ok(());
    }
    let Some(token) = ably_device_identity_header(headers)? else {
        return Err(AppError::Forbidden(
            "A matching device identity token is required".to_string(),
        ));
    };
    if verify_device_identity_token(token, &device.device_secret) {
        Ok(())
    } else {
        Err(AppError::Forbidden(
            "Invalid device identity token".to_string(),
        ))
    }
}

#[cfg(feature = "push")]
pub(super) fn ably_push_error(error: impl std::fmt::Display) -> AppError {
    AppError::InternalError(format!("native push store failed: {error}"))
}

#[cfg(feature = "push")]
pub(super) fn ably_push_wire_value(value: WireValue) -> Result<Value, AppError> {
    sonic_rs::to_value(&value)
        .map_err(|error| AppError::InvalidInput(format!("Invalid push payload value: {error}")))
}

#[cfg(feature = "push")]
pub(super) fn ably_push_platform(value: &str) -> Result<Platform, AppError> {
    match value.to_ascii_lowercase().as_str() {
        "android" => Ok(Platform::Android),
        "ios" => Ok(Platform::Ios),
        "browser" => Ok(Platform::Browser),
        "windows" => Ok(Platform::Windows),
        "macos" => Ok(Platform::Macos),
        "watchos" => Ok(Platform::Watchos),
        "tvos" => Ok(Platform::Tvos),
        "other" => Ok(Platform::Other),
        _ => Err(AppError::InvalidInput(
            "Invalid push device platform".to_string(),
        )),
    }
}

#[cfg(feature = "push")]
pub(super) fn ably_push_form_factor(value: &str) -> Result<FormFactor, AppError> {
    match value.to_ascii_lowercase().as_str() {
        "phone" => Ok(FormFactor::Phone),
        "tablet" => Ok(FormFactor::Tablet),
        "desktop" => Ok(FormFactor::Desktop),
        "tv" => Ok(FormFactor::Tv),
        "car" => Ok(FormFactor::Car),
        "watch" => Ok(FormFactor::Watch),
        "embedded" => Ok(FormFactor::Embedded),
        "other" => Ok(FormFactor::Other),
        _ => Err(AppError::InvalidInput(
            "Invalid push device formFactor".to_string(),
        )),
    }
}

#[cfg(feature = "push")]
pub(super) fn ably_push_recipient(value: &Value) -> Result<PushRecipient, AppError> {
    let transport = value
        .get("transportType")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            AppError::InvalidInput("push recipient requires transportType".to_string())
        })?;
    let required_secret = |name: &'static str| {
        value
            .get(name)
            .and_then(Value::as_str)
            .ok_or_else(|| AppError::InvalidInput(format!("push recipient requires {name}")))
            .and_then(|value| {
                SecretString::new(value).map_err(|error| AppError::InvalidInput(error.to_string()))
            })
    };
    match transport {
        "ablyChannel" => Ok(PushRecipient::Realtime {
            channel: value
                .get("channel")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    AppError::InvalidInput("ablyChannel recipient requires channel".to_string())
                })?
                .to_string(),
        }),
        "gcm" => Ok(PushRecipient::Fcm {
            registration_token: required_secret("registrationToken")?,
        }),
        "apns" => Ok(PushRecipient::Apns {
            device_token: required_secret("deviceToken")?,
        }),
        "hms" => Ok(PushRecipient::Hms {
            registration_token: required_secret("registrationToken")?,
        }),
        "wns" => Ok(PushRecipient::Wns {
            channel_uri: required_secret("channelUri")?,
        }),
        "web" => Ok(PushRecipient::Web {
            endpoint: required_secret("endpoint")?,
            p256dh: required_secret("p256dh")?,
            auth: required_secret("auth")?,
        }),
        _ => Err(AppError::InvalidInput(format!(
            "Unsupported push transportType '{transport}'"
        ))),
    }
}

#[cfg(feature = "push")]
pub(super) fn ably_push_recipient_value(recipient: &PushRecipient) -> Value {
    match recipient {
        PushRecipient::Fcm { registration_token } => json!({
            "transportType": "gcm",
            "registrationToken": registration_token.expose_secret()
        }),
        PushRecipient::Apns { device_token } => json!({
            "transportType": "apns",
            "deviceToken": device_token.expose_secret()
        }),
        PushRecipient::Web {
            endpoint,
            p256dh,
            auth,
        } => json!({
            "transportType": "web",
            "endpoint": endpoint.expose_secret(),
            "p256dh": p256dh.expose_secret(),
            "auth": auth.expose_secret()
        }),
        PushRecipient::Hms { registration_token } => json!({
            "transportType": "hms",
            "registrationToken": registration_token.expose_secret()
        }),
        PushRecipient::Wns { channel_uri } => json!({
            "transportType": "wns",
            "channelUri": channel_uri.expose_secret()
        }),
        PushRecipient::Realtime { channel } => json!({
            "transportType": "ablyChannel",
            "channel": channel
        }),
    }
}

#[cfg(feature = "push")]
pub(super) fn ably_push_device_value(device: &DeviceDetails, issued_token: Option<&str>) -> Value {
    const DEVICE_SECRET_KEY: &str = "sockudo_ably_device_secret";
    let mut value = Value::new_object();
    let object = value.as_object_mut().expect("fresh object");
    object.insert("id", Value::from(device.id.as_str()));
    if let Some(client_id) = device.client_id.as_ref() {
        object.insert("clientId", Value::from(client_id.as_str()));
    }
    object.insert(
        "platform",
        Value::from(ably_push_platform_label(&device.platform)),
    );
    object.insert(
        "formFactor",
        Value::from(ably_push_form_factor_label(&device.form_factor)),
    );
    let mut metadata = device.metadata.clone();
    let device_secret = metadata
        .as_object()
        .and_then(|object| object.get(&DEVICE_SECRET_KEY))
        .and_then(Value::as_str)
        .map(str::to_string);
    if let Some(object) = metadata.as_object_mut() {
        object.remove(&DEVICE_SECRET_KEY);
    }
    object.insert("metadata", metadata);
    if let Some(device_secret) = device_secret.as_deref() {
        object.insert("deviceSecret", Value::from(device_secret));
    }
    let mut push = Value::new_object();
    let push_object = push.as_object_mut().expect("fresh object");
    push_object.insert(
        "recipient",
        ably_push_recipient_value(&device.push.recipient),
    );
    push_object.insert("state", Value::from("ACTIVE"));
    object.insert("push", push);
    if let Some(token) = issued_token
        && let Some(object) = value.as_object_mut()
    {
        let issued = now_ms();
        let mut details = Value::new_object();
        let details_object = details.as_object_mut().expect("fresh object");
        details_object.insert("token", Value::from(token));
        details_object.insert("issued", Value::from(issued));
        details_object.insert(
            "expires",
            Value::from(issued.saturating_add(365 * 24 * 60 * 60 * 1000)),
        );
        details_object.insert("capability", Value::from("{}"));
        if let Some(client_id) = device.client_id.as_ref() {
            details_object.insert("clientId", Value::from(client_id.as_str()));
        }
        object.insert("deviceIdentityToken", details);
    }
    value
}

#[cfg(feature = "push")]
pub(super) fn ably_push_platform_label(platform: &Platform) -> &'static str {
    match platform {
        Platform::Android => "android",
        Platform::Ios => "ios",
        Platform::Browser => "browser",
        Platform::Windows => "windows",
        Platform::Macos => "macos",
        Platform::Watchos => "watchos",
        Platform::Tvos => "tvos",
        Platform::Other => "other",
    }
}

#[cfg(feature = "push")]
pub(super) fn ably_push_form_factor_label(form_factor: &FormFactor) -> &'static str {
    match form_factor {
        FormFactor::Phone => "phone",
        FormFactor::Tablet => "tablet",
        FormFactor::Desktop => "desktop",
        FormFactor::Tv => "tv",
        FormFactor::Car => "car",
        FormFactor::Watch => "watch",
        FormFactor::Embedded => "embedded",
        FormFactor::Other => "other",
    }
}

#[cfg(feature = "push")]
pub(super) async fn save_ably_push_device(
    hub: &AblyCompatHub,
    app_id: &str,
    path_id: Option<String>,
    request: AblyPushDeviceRequest,
) -> Result<Value, AppError> {
    let store = ably_push_store(hub)?;
    if let (Some(path_id), Some(body_id)) = (path_id.as_deref(), request.id.as_deref())
        && path_id != body_id
    {
        return Err(AppError::InvalidInput(
            "push device path id does not match body id".to_string(),
        ));
    }
    let id = path_id.or(request.id).ok_or_else(|| {
        AppError::InvalidInput("push device registration requires id".to_string())
    })?;
    const DEVICE_SECRET_KEY: &str = "sockudo_ably_device_secret";
    let existing = store
        .get_device(app_id, &id)
        .await
        .map_err(ably_push_error)?;
    let issued_identity = existing
        .is_none()
        .then(|| generate_ably_device_identity_token(app_id, &id));
    let recipient = ably_push_wire_value(request.push.recipient)?;
    let mut metadata = request
        .metadata
        .map(ably_push_wire_value)
        .transpose()?
        .or_else(|| existing.as_ref().map(|device| device.metadata.clone()))
        .unwrap_or_else(|| json!({}));
    if !metadata.is_object() {
        metadata = json!({ "value": metadata });
    }
    if let Some(device_secret) = request.device_secret.as_deref()
        && let Some(object) = metadata.as_object_mut()
    {
        object.insert(DEVICE_SECRET_KEY, Value::from(device_secret));
    }
    let identity_hash = existing
        .as_ref()
        .map(|device| device.device_secret.clone())
        .unwrap_or_else(|| {
            hash_device_identity_token(
                issued_identity
                    .as_ref()
                    .expect("new registrations issue an identity token"),
            )
        });
    let device = DeviceDetails {
        app_id: app_id.to_string(),
        id,
        client_id: request.client_id,
        form_factor: ably_push_form_factor(&request.form_factor)?,
        platform: ably_push_platform(&request.platform)?,
        metadata,
        device_secret: identity_hash,
        timezone: "UTC".to_string(),
        locale: "en".to_string(),
        last_active_at_ms: u64::try_from(now_ms()).unwrap_or_default(),
        push: DevicePushDetails {
            recipient: ably_push_recipient(&recipient)?,
            state: DevicePushState::Active,
            failure_count: 0,
            error_reason: None,
        },
        push_rate_policy: None,
    };
    store
        .upsert_device(device.clone())
        .await
        .map_err(ably_push_error)?;
    Ok(ably_push_device_value(
        &device,
        issued_identity.as_ref().map(SecretString::expose_secret),
    ))
}

#[cfg(feature = "push")]
pub(super) async fn ably_push_put_device(
    Path(device_id): Path<String>,
    Query(query): Query<AblyPushQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
    body: Bytes,
) -> Response {
    ably_push_save_device_response(Some(device_id), query, headers, runtime, handler, body).await
}

#[cfg(feature = "push")]
pub(super) async fn ably_push_save_device(
    Query(query): Query<AblyPushQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
    body: Bytes,
) -> Response {
    ably_push_save_device_response(None, query, headers, runtime, handler, body).await
}

#[cfg(feature = "push")]
pub(super) async fn ably_push_save_device_response(
    path_id: Option<String>,
    query: AblyPushQuery,
    headers: HeaderMap,
    runtime: Arc<AblyCompatRuntime>,
    handler: Arc<ConnectionHandler>,
    body: Bytes,
) -> Response {
    let request_format = ably_rest_request_format(&headers);
    let format = ably_rest_response_format(&headers, query.format.as_deref(), request_format);
    let request = match decode_value::<AblyPushDeviceRequest>(&body, request_format) {
        Ok(request) => request,
        Err(error) => {
            return ably_error_response_format(StatusCode::BAD_REQUEST, 40000, error, format);
        }
    };
    let requested_id = path_id.as_deref().or(request.id.as_deref());
    let resolved = match resolve_ably_push_auth(&runtime.hub, &handler, &headers, &query).await {
        Ok(resolved) => resolved,
        Err(error) => {
            return ably_error_response_format(error.status, error.code, error.message, format);
        }
    };
    if ensure_ably_push_admin(&resolved, None).is_err() {
        if let Err(error) = ensure_ably_push_subscribe(&resolved, None) {
            return ably_app_error_response_format(error, format);
        }
        let Some(device_id) = requested_id else {
            return ably_error_response_format(
                StatusCode::BAD_REQUEST,
                40000,
                "push device registration requires id",
                format,
            );
        };
        let store = match ably_push_store(&runtime.hub) {
            Ok(store) => store,
            Err(error) => return ably_app_error_response_format(error, format),
        };
        let device = match store.get_device(&resolved.app.id, device_id).await {
            Ok(Some(device)) => device,
            Ok(None) => {
                return ably_app_error_response_format(
                    AppError::Forbidden("push-admin capability is required".to_string()),
                    format,
                );
            }
            Err(error) => return ably_app_error_response_format(ably_push_error(error), format),
        };
        if let Err(error) = ensure_ably_device_owner(&resolved, &headers, &device) {
            return ably_app_error_response_format(error, format);
        }
    }
    match save_ably_push_device(&runtime.hub, &resolved.app.id, path_id, request).await {
        Ok(device) => encode_ably_rest_response(StatusCode::CREATED, format, &device)
            .unwrap_or_else(ably_app_error_response),
        Err(error) => ably_app_error_response_format(error, format),
    }
}

#[cfg(feature = "push")]
pub(super) async fn ably_push_get_device(
    Path(device_id): Path<String>,
    Query(query): Query<AblyPushQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    let format = ably_rest_response_format(&headers, query.format.as_deref(), AblyFormat::Json);
    let resolved = match resolve_ably_push_auth(&runtime.hub, &handler, &headers, &query).await {
        Ok(resolved) => resolved,
        Err(error) => {
            return ably_error_response_format(error.status, error.code, error.message, format);
        }
    };
    let store = match ably_push_store(&runtime.hub) {
        Ok(store) => store,
        Err(error) => return ably_app_error_response_format(error, format),
    };
    match store.get_device(&resolved.app.id, &device_id).await {
        Ok(Some(device)) => {
            if ensure_ably_push_admin(&resolved, None).is_err()
                && let Err(error) = ensure_ably_push_subscribe(&resolved, None)
                    .and_then(|()| ensure_ably_device_owner(&resolved, &headers, &device))
            {
                return ably_app_error_response_format(error, format);
            }
            encode_ably_rest_response(
                StatusCode::OK,
                format,
                &ably_push_device_value(&device, None),
            )
            .unwrap_or_else(ably_app_error_response)
        }
        Ok(None) => {
            ably_error_response_format(StatusCode::NOT_FOUND, 40400, "Device not found", format)
        }
        Err(error) => ably_app_error_response_format(ably_push_error(error), format),
    }
}

#[cfg(feature = "push")]
pub(super) async fn ably_push_list_devices(
    Query(query): Query<AblyPushQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    let format = ably_rest_response_format(&headers, query.format.as_deref(), AblyFormat::Json);
    let resolved = match resolve_ably_push_auth(&runtime.hub, &handler, &headers, &query).await {
        Ok(resolved) => resolved,
        Err(error) => {
            return ably_error_response_format(error.status, error.code, error.message, format);
        }
    };
    if let Err(error) = ensure_ably_push_admin(&resolved, None) {
        return ably_app_error_response_format(error, format);
    }
    let store = match ably_push_store(&runtime.hub) {
        Ok(store) => store,
        Err(error) => return ably_app_error_response_format(error, format),
    };
    let limit = query.limit.unwrap_or(1_000).clamp(1, 1_000);
    match store.list_devices(&resolved.app.id, limit, None).await {
        Ok(page) => {
            let devices = page
                .items
                .into_iter()
                .filter(|device| {
                    query
                        .client_id
                        .as_ref()
                        .is_none_or(|client_id| device.client_id.as_ref() == Some(client_id))
                })
                .map(|device| ably_push_device_value(&device, None))
                .collect::<Vec<_>>();
            encode_ably_rest_response(StatusCode::OK, format, &devices)
                .unwrap_or_else(ably_app_error_response)
        }
        Err(error) => ably_app_error_response_format(ably_push_error(error), format),
    }
}

#[cfg(feature = "push")]
pub(super) async fn ably_push_delete_device(
    Path(device_id): Path<String>,
    Query(query): Query<AblyPushQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    ably_push_delete_devices_inner(Some(device_id), query, headers, runtime, handler).await
}

#[cfg(feature = "push")]
pub(super) async fn ably_push_delete_devices(
    Query(query): Query<AblyPushQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    ably_push_delete_devices_inner(None, query, headers, runtime, handler).await
}

#[cfg(feature = "push")]
pub(super) async fn ably_push_delete_devices_inner(
    path_id: Option<String>,
    query: AblyPushQuery,
    headers: HeaderMap,
    runtime: Arc<AblyCompatRuntime>,
    handler: Arc<ConnectionHandler>,
) -> Response {
    let format = ably_rest_response_format(&headers, query.format.as_deref(), AblyFormat::Json);
    let resolved = match resolve_ably_push_auth(&runtime.hub, &handler, &headers, &query).await {
        Ok(resolved) => resolved,
        Err(error) => {
            return ably_error_response_format(error.status, error.code, error.message, format);
        }
    };
    let store = match ably_push_store(&runtime.hub) {
        Ok(store) => store,
        Err(error) => return ably_app_error_response_format(error, format),
    };
    let result = if let Some(device_id) = path_id.or(query.device_id) {
        if ensure_ably_push_admin(&resolved, None).is_err() {
            if let Err(error) = ensure_ably_push_subscribe(&resolved, None) {
                return ably_app_error_response_format(error, format);
            }
            let device = match store.get_device(&resolved.app.id, &device_id).await {
                Ok(Some(device)) => device,
                Ok(None) => {
                    return ably_error_response_format(
                        StatusCode::NOT_FOUND,
                        40400,
                        "Device not found",
                        format,
                    );
                }
                Err(error) => {
                    return ably_app_error_response_format(ably_push_error(error), format);
                }
            };
            if let Err(error) = ensure_ably_device_owner(&resolved, &headers, &device) {
                return ably_app_error_response_format(error, format);
            }
        }
        store
            .delete_device(&resolved.app.id, &device_id)
            .await
            .map(|_| ())
    } else if let Some(client_id) = query.client_id {
        if let Err(error) = ensure_ably_push_admin(&resolved, None) {
            return ably_app_error_response_format(error, format);
        }
        store
            .delete_devices_by_client(&resolved.app.id, &client_id)
            .await
            .map(|_| ())
    } else {
        return ably_error_response_format(
            StatusCode::BAD_REQUEST,
            40000,
            "Device delete requires deviceId or clientId",
            format,
        );
    };
    match result {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => ably_app_error_response_format(ably_push_error(error), format),
    }
}

#[cfg(feature = "push")]
pub(super) fn ably_push_subscription_value(
    subscription: &ChannelSubscription,
) -> AblyPushSubscription {
    let logical_client = subscription.scoped_client_id().is_some();
    AblyPushSubscription {
        channel: subscription.channel.clone(),
        device_id: (!logical_client).then(|| subscription.device_id.clone()),
        client_id: subscription.client_id.clone(),
    }
}

#[cfg(feature = "push")]
pub(super) async fn ably_push_save_subscription(
    Query(query): Query<AblyPushQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
    body: Bytes,
) -> Response {
    let request_format = ably_rest_request_format(&headers);
    let format = ably_rest_response_format(&headers, query.format.as_deref(), request_format);
    let request = match decode_value::<AblyPushSubscription>(&body, request_format) {
        Ok(request) => request,
        Err(error) => {
            return ably_error_response_format(StatusCode::BAD_REQUEST, 40000, error, format);
        }
    };
    let resolved = match resolve_ably_push_auth(&runtime.hub, &handler, &headers, &query).await {
        Ok(resolved) => resolved,
        Err(error) => {
            return ably_error_response_format(error.status, error.code, error.message, format);
        }
    };
    let store = match ably_push_store(&runtime.hub) {
        Ok(store) => store,
        Err(error) => return ably_app_error_response_format(error, format),
    };
    let subscription = if let Some(device_id) = request.device_id.as_deref() {
        match store.get_device(&resolved.app.id, device_id).await {
            Ok(Some(device)) => {
                if ensure_ably_push_admin(&resolved, Some(&request.channel)).is_err()
                    && let Err(error) =
                        ensure_ably_push_subscribe(&resolved, Some(&request.channel))
                            .and_then(|()| ensure_ably_device_owner(&resolved, &headers, &device))
                {
                    return ably_app_error_response_format(error, format);
                }
                ChannelSubscription::from_device(request.channel.clone(), &device)
            }
            Ok(None) => {
                return ably_error_response_format(
                    StatusCode::NOT_FOUND,
                    40400,
                    "Device not found",
                    format,
                );
            }
            Err(error) => return ably_app_error_response_format(ably_push_error(error), format),
        }
    } else if let Some(client_id) = request.client_id.as_deref() {
        if ensure_ably_push_admin(&resolved, Some(&request.channel)).is_err() {
            if let Err(error) = ensure_ably_push_subscribe(&resolved, Some(&request.channel)) {
                return ably_app_error_response_format(error, format);
            }
            if resolved.client_id.as_deref() != Some(client_id) {
                return ably_app_error_response_format(
                    AppError::Forbidden(
                        "push-subscribe clientId must match authenticated clientId".to_string(),
                    ),
                    format,
                );
            }
        }
        ChannelSubscription::from_client(
            resolved.app.id.clone(),
            request.channel.clone(),
            client_id,
        )
    } else {
        return ably_error_response_format(
            StatusCode::BAD_REQUEST,
            40000,
            "Subscription requires deviceId or clientId",
            format,
        );
    };
    match store.upsert_subscription(subscription).await {
        Ok(()) => encode_ably_rest_response(StatusCode::CREATED, format, &request)
            .unwrap_or_else(ably_app_error_response),
        Err(error) => ably_app_error_response_format(ably_push_error(error), format),
    }
}

#[cfg(feature = "push")]
pub(super) async fn all_ably_push_subscriptions(
    store: &DynPushStore,
    app_id: &str,
) -> Result<Vec<ChannelSubscription>, AppError> {
    store
        .list_subscriptions(app_id, 1_000, None)
        .await
        .map(|page| page.items)
        .map_err(ably_push_error)
}

#[cfg(feature = "push")]
pub(super) async fn ably_push_list_subscriptions(
    Query(query): Query<AblyPushQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    let format = ably_rest_response_format(&headers, query.format.as_deref(), AblyFormat::Json);
    let resolved = match resolve_ably_push_auth(&runtime.hub, &handler, &headers, &query).await {
        Ok(resolved) => resolved,
        Err(error) => {
            return ably_error_response_format(error.status, error.code, error.message, format);
        }
    };
    let is_admin = ensure_ably_push_admin(&resolved, None).is_ok();
    if !is_admin {
        let Some(channel) = query.channel.as_deref() else {
            return ably_app_error_response_format(
                AppError::Forbidden(
                    "push-admin is required to list subscriptions across channels".to_string(),
                ),
                format,
            );
        };
        if let Err(error) = ensure_ably_push_subscribe(&resolved, Some(channel)) {
            return ably_app_error_response_format(error, format);
        }
        if query.device_id.is_none()
            && query.client_id.is_none()
            && resolved.push_device_id.is_none()
            && resolved.client_id.is_none()
        {
            return ably_app_error_response_format(
                AppError::Forbidden("push-subscribe can only list owned subscriptions".to_string()),
                format,
            );
        }
    }
    let store = match ably_push_store(&runtime.hub) {
        Ok(store) => store,
        Err(error) => return ably_app_error_response_format(error, format),
    };
    match all_ably_push_subscriptions(&store, &resolved.app.id).await {
        Ok(subscriptions) => {
            let subscriptions = subscriptions
                .into_iter()
                .filter(|subscription| {
                    query
                        .channel
                        .as_ref()
                        .is_none_or(|channel| &subscription.channel == channel)
                })
                .filter(|subscription| {
                    query
                        .device_id
                        .as_ref()
                        .is_none_or(|device_id| &subscription.device_id == device_id)
                })
                .filter(|subscription| {
                    query
                        .client_id
                        .as_ref()
                        .is_none_or(|client_id| subscription.client_id.as_ref() == Some(client_id))
                })
                .filter(|subscription| {
                    is_admin
                        || resolved
                            .push_device_id
                            .as_ref()
                            .is_some_and(|device_id| &subscription.device_id == device_id)
                        || resolved.client_id.as_ref().is_some_and(|client_id| {
                            subscription.client_id.as_ref() == Some(client_id)
                        })
                })
                .map(|subscription| ably_push_subscription_value(&subscription))
                .collect::<Vec<_>>();
            encode_ably_rest_response(StatusCode::OK, format, &subscriptions)
                .unwrap_or_else(ably_app_error_response)
        }
        Err(error) => ably_app_error_response_format(error, format),
    }
}

#[cfg(feature = "push")]
pub(super) async fn ably_push_delete_subscriptions(
    Query(query): Query<AblyPushQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    let format = ably_rest_response_format(&headers, query.format.as_deref(), AblyFormat::Json);
    let resolved = match resolve_ably_push_auth(&runtime.hub, &handler, &headers, &query).await {
        Ok(resolved) => resolved,
        Err(error) => {
            return ably_error_response_format(error.status, error.code, error.message, format);
        }
    };
    let store = match ably_push_store(&runtime.hub) {
        Ok(store) => store,
        Err(error) => return ably_app_error_response_format(error, format),
    };
    if ensure_ably_push_admin(&resolved, query.channel.as_deref()).is_err() {
        let Some(channel) = query.channel.as_deref() else {
            return ably_app_error_response_format(
                AppError::Forbidden(
                    "push-subscribe can only remove subscriptions from one channel".to_string(),
                ),
                format,
            );
        };
        if let Err(error) = ensure_ably_push_subscribe(&resolved, Some(channel)) {
            return ably_app_error_response_format(error, format);
        }
        if let Some(device_id) = query.device_id.as_deref() {
            let device = match store.get_device(&resolved.app.id, device_id).await {
                Ok(Some(device)) => device,
                Ok(None) => {
                    return ably_error_response_format(
                        StatusCode::NOT_FOUND,
                        40400,
                        "Device not found",
                        format,
                    );
                }
                Err(error) => {
                    return ably_app_error_response_format(ably_push_error(error), format);
                }
            };
            if let Err(error) = ensure_ably_device_owner(&resolved, &headers, &device) {
                return ably_app_error_response_format(error, format);
            }
        } else if let Some(client_id) = query.client_id.as_deref() {
            if resolved.client_id.as_deref() != Some(client_id) {
                return ably_app_error_response_format(
                    AppError::Forbidden(
                        "push-subscribe clientId must match authenticated clientId".to_string(),
                    ),
                    format,
                );
            }
        } else {
            return ably_app_error_response_format(
                AppError::Forbidden(
                    "push-subscribe can only remove owned subscriptions".to_string(),
                ),
                format,
            );
        }
    }
    let mut cursor = None;
    loop {
        let page = match store
            .list_subscriptions(&resolved.app.id, 1_000, cursor)
            .await
        {
            Ok(page) => page,
            Err(error) => return ably_app_error_response_format(ably_push_error(error), format),
        };
        let next_cursor = page.next_cursor;
        for subscription in page.items {
            if query
                .channel
                .as_ref()
                .is_none_or(|channel| &subscription.channel == channel)
                && query
                    .device_id
                    .as_ref()
                    .is_none_or(|device_id| &subscription.device_id == device_id)
                && query
                    .client_id
                    .as_ref()
                    .is_none_or(|client_id| subscription.client_id.as_ref() == Some(client_id))
                && let Err(error) = store
                    .delete_subscription(
                        &resolved.app.id,
                        &subscription.channel,
                        &subscription.device_id,
                    )
                    .await
            {
                return ably_app_error_response_format(ably_push_error(error), format);
            }
        }
        cursor = next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    StatusCode::NO_CONTENT.into_response()
}

#[cfg(feature = "push")]
pub(super) async fn ably_push_list_channels(
    Query(query): Query<AblyPushQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    let format = ably_rest_response_format(&headers, query.format.as_deref(), AblyFormat::Json);
    let resolved = match resolve_ably_push_auth(&runtime.hub, &handler, &headers, &query).await {
        Ok(resolved) => resolved,
        Err(error) => {
            return ably_error_response_format(error.status, error.code, error.message, format);
        }
    };
    if let Err(error) = ensure_ably_push_admin(&resolved, None) {
        return ably_app_error_response_format(error, format);
    }
    let store = match ably_push_store(&runtime.hub) {
        Ok(store) => store,
        Err(error) => return ably_app_error_response_format(error, format),
    };
    match store
        .list_subscription_channels(
            &resolved.app.id,
            query.limit.unwrap_or(1_000).clamp(1, 1_000),
            None,
        )
        .await
    {
        Ok(page) => encode_ably_rest_response(StatusCode::OK, format, &page.items)
            .unwrap_or_else(ably_app_error_response),
        Err(error) => ably_app_error_response_format(ably_push_error(error), format),
    }
}

#[cfg(feature = "push")]
pub(super) async fn publish_ably_push_event(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    channel: &str,
    payload: &Value,
    delivery_id: &str,
) -> Result<String, AppError> {
    let data = sonic_rs::to_string(payload).map_err(|error| {
        AppError::InternalError(format!("push payload serialization failed: {error}"))
    })?;
    let result = MessageService::new(Arc::clone(handler))
        .publish_message(
            app,
            channel,
            PusherMessage {
                event: Some("__ably_push__".to_string()),
                channel: Some(channel.to_string()),
                data: Some(MessageData::String(data)),
                name: None,
                user_id: None,
                tags: None,
                sequence: None,
                conflation_key: None,
                message_id: Some(delivery_id.to_string()),
                stream_id: None,
                serial: None,
                idempotency_key: Some(delivery_id.to_string()),
                extras: None,
                delta_sequence: None,
                delta_conflation_key: None,
            },
            PublishContext::default(),
        )
        .await?;
    Ok(result.receipt.acknowledgement_id)
}

#[cfg(feature = "push")]
#[derive(Default)]
pub(super) struct AblyPushRecipientSummary {
    pub(super) count: u64,
    pub(super) providers: BTreeSet<PushProviderKind>,
}

#[cfg(feature = "push")]
pub(super) async fn summarize_ably_push_devices_for_client(
    store: &DynPushStore,
    app_id: &str,
    client_id: &str,
) -> Result<AblyPushRecipientSummary, AppError> {
    let mut cursor = None;
    let mut summary = AblyPushRecipientSummary::default();
    loop {
        let page = store
            .list_devices(app_id, 1_000, cursor)
            .await
            .map_err(ably_push_error)?;
        for device in page
            .items
            .iter()
            .filter(|device| device.client_id.as_deref() == Some(client_id))
        {
            summary.count = summary.count.saturating_add(1);
            summary.providers.insert(device.push.recipient.provider());
        }
        cursor = page.next_cursor;
        if cursor.is_none() {
            return Ok(summary);
        }
    }
}

#[cfg(feature = "push")]
pub(super) async fn summarize_ably_push_channel_recipients(
    store: &DynPushStore,
    app_id: &str,
    channel: &str,
) -> Result<AblyPushRecipientSummary, AppError> {
    let mut cursor = None;
    let mut summary = AblyPushRecipientSummary::default();
    loop {
        let page = store
            .list_channel_subscribers(app_id, channel, 1_000, cursor)
            .await
            .map_err(ably_push_error)?;
        for subscription in page.items {
            if let Some(client_id) = subscription.scoped_client_id() {
                let client =
                    summarize_ably_push_devices_for_client(store, app_id, client_id).await?;
                summary.count = summary.count.saturating_add(client.count);
                summary.providers.extend(client.providers);
            } else if let Some(device) = store
                .get_device(app_id, &subscription.device_id)
                .await
                .map_err(ably_push_error)?
            {
                summary.count = summary.count.saturating_add(1);
                summary.providers.insert(device.push.recipient.provider());
            }
        }
        cursor = page.next_cursor;
        if cursor.is_none() {
            return Ok(summary);
        }
    }
}

#[cfg(feature = "push")]
pub(super) fn ably_push_publish_id(headers: &HeaderMap) -> String {
    headers
        .get("idempotency-key")
        .or_else(|| headers.get("x-idempotency-key"))
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| Uuid::new_v4().to_string())
}

#[cfg(feature = "push")]
pub(super) async fn ably_push_publish(
    Query(query): Query<AblyPushQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
    body: Bytes,
) -> Response {
    let request_bytes = u64::try_from(body.len()).unwrap_or(u64::MAX);
    let request_format = ably_rest_request_format(&headers);
    let format = ably_rest_response_format(&headers, query.format.as_deref(), request_format);
    let resolved = match resolve_ably_push_auth(&runtime.hub, &handler, &headers, &query).await {
        Ok(resolved) => resolved,
        Err(error) => {
            return ably_error_response_format(error.status, error.code, error.message, format);
        }
    };
    let request = match decode_value::<AblyPushPublishRequest>(&body, request_format) {
        Ok(request) => request,
        Err(error) => {
            return ably_error_response_format(StatusCode::BAD_REQUEST, 40000, error, format);
        }
    };
    let store = match ably_push_store(&runtime.hub) {
        Ok(store) => store,
        Err(error) => return ably_app_error_response_format(error, format),
    };
    let recipient = match ably_push_wire_value(request.recipient) {
        Ok(recipient) => recipient,
        Err(error) => return ably_app_error_response_format(error, format),
    };
    let admin_resource = recipient
        .get("transportType")
        .and_then(Value::as_str)
        .filter(|transport| *transport == "ablyChannel")
        .and_then(|_| recipient.get("channel"))
        .and_then(Value::as_str);
    if let Err(error) = ensure_ably_push_admin(&resolved, admin_resource) {
        return ably_app_error_response_format(error, format);
    }
    let notification = match request.notification.map(ably_push_wire_value).transpose() {
        Ok(notification) => notification,
        Err(error) => return ably_app_error_response_format(error, format),
    };
    let data = match request.data.map(ably_push_wire_value).transpose() {
        Ok(data) => data,
        Err(error) => return ably_app_error_response_format(error, format),
    };
    let payload = match ably_push_payload(notification, data) {
        Ok(payload) => payload,
        Err(error) => return ably_app_error_response_format(error, format),
    };
    let (target, summary) = if recipient.get("transportType").and_then(Value::as_str)
        == Some("ablyChannel")
    {
        match ably_push_recipient(&recipient) {
            Ok(recipient) => {
                let provider = recipient.provider();
                (
                    PublishTarget::Recipient { recipient },
                    AblyPushRecipientSummary {
                        count: 1,
                        providers: BTreeSet::from([provider]),
                    },
                )
            }
            Err(error) => return ably_app_error_response_format(error, format),
        }
    } else if let Some(device_id) = recipient.get("deviceId").and_then(Value::as_str) {
        match store.get_device(&resolved.app.id, device_id).await {
            Ok(Some(device)) => (
                PublishTarget::Device {
                    device_id: device_id.to_string(),
                },
                AblyPushRecipientSummary {
                    count: 1,
                    providers: BTreeSet::from([device.push.recipient.provider()]),
                },
            ),
            Ok(None) => {
                return ably_error_response_format(
                    StatusCode::NOT_FOUND,
                    40400,
                    "Device not found",
                    format,
                );
            }
            Err(error) => return ably_app_error_response_format(ably_push_error(error), format),
        }
    } else if let Some(client_id) = recipient.get("clientId").and_then(Value::as_str) {
        match summarize_ably_push_devices_for_client(&store, &resolved.app.id, client_id).await {
            Ok(summary) => (
                PublishTarget::Client {
                    client_id: client_id.to_string(),
                },
                summary,
            ),
            Err(error) => return ably_app_error_response_format(error, format),
        }
    } else {
        return ably_error_response_format(
            StatusCode::BAD_REQUEST,
            40000,
            "Unsupported push recipient",
            format,
        );
    };
    let queue = match ably_push_queue(&runtime.hub) {
        Ok(queue) => queue,
        Err(error) => return ably_app_error_response_format(error, format),
    };
    if let Err(error) = accept_ably_push_intent(
        store,
        queue,
        runtime.hub.push_admission.as_deref(),
        AblyPushIntentRequest {
            app_id: resolved.app.id.clone(),
            publish_id: ably_push_publish_id(&headers),
            targets: vec![target],
            required_providers: summary.providers,
            payload,
            expected_recipients: summary.count,
        },
    )
    .await
    {
        return ably_app_error_response_format(error, format);
    }
    if let Ok(observation) = StatsObservation::push(
        &resolved.app.id,
        now_ms(),
        true,
        summary.count,
        request_bytes,
    ) && let Err(error) = runtime.hub.stats.record(observation).await
    {
        return ably_app_error_response_format(stats_app_error(error), format);
    }
    encode_ably_rest_response(StatusCode::CREATED, format, &json!({}))
        .unwrap_or_else(ably_app_error_response)
}
