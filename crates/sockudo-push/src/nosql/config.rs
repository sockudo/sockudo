#![allow(unused_imports)]
use super::constants::*;
use super::document::{DocumentBackend, DocumentPushStore};
use super::helpers::*;
use crate::domain::{
    ChannelSubscription, DeleteDeviceOutcome, DeliveryEvent, DeviceDetails, NotificationTemplate,
    ProviderCredential, PublishLogEvent, PublishStatus, PushCursor, PushCursorKind, ShardJob,
};
use crate::storage::{
    DeviceRegistrationChange, DeviceRegistrationOutcome, IdempotencyRecord,
    OperatorInvalidationEvent, Page, PushCredentialStore, PushDeliveryEventStore, PushDeviceStore,
    PushFanoutShardStore, PushIdempotencyStore, PushOperatorEventStore, PushPublishLogStore,
    PushPublishStatusStore, PushScheduleStore, PushSchedulerLockStore, PushStorageResult,
    PushSubscriptionStore, PushTemplateStore, ScheduledPushJob, SchedulerLock,
};
use async_trait::async_trait;

#[async_trait]
impl<B> PushCredentialStore for DocumentPushStore<B>
where
    B: DocumentBackend,
{
    async fn put_credential(&self, credential: ProviderCredential) -> PushStorageResult<()> {
        credential.validate()?;
        self.put_json(
            FAMILY_CREDENTIAL,
            &credential.app_id,
            &credential.credential_id,
            DEFAULT_SK,
            &credential,
        )
        .await
    }

    async fn get_credential(
        &self,
        app_id: &str,
        credential_id: &str,
    ) -> PushStorageResult<Option<ProviderCredential>> {
        self.get_json(FAMILY_CREDENTIAL, app_id, credential_id, DEFAULT_SK)
            .await
    }

    async fn list_credentials(
        &self,
        app_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<ProviderCredential>> {
        let start = cursor_position(cursor, app_id)?;
        let rows = self
            .scan_json::<ProviderCredential>(FAMILY_CREDENTIAL, app_id)
            .await?
            .into_iter()
            .map(|(credential_id, _, credential)| (credential_id, credential))
            .collect();
        Ok(page_from_rows(
            app_id,
            PushCursorKind::Credential,
            rows,
            limit,
            start,
        ))
    }
}

#[async_trait]
impl<B> PushTemplateStore for DocumentPushStore<B>
where
    B: DocumentBackend,
{
    async fn put_template(&self, template: NotificationTemplate) -> PushStorageResult<()> {
        template.validate()?;
        self.put_json(
            FAMILY_TEMPLATE,
            &template.app_id,
            &template.template_id,
            DEFAULT_SK,
            &template,
        )
        .await
    }

    async fn get_template(
        &self,
        app_id: &str,
        template_id: &str,
    ) -> PushStorageResult<Option<NotificationTemplate>> {
        self.get_json(FAMILY_TEMPLATE, app_id, template_id, DEFAULT_SK)
            .await
    }

    async fn list_templates(
        &self,
        app_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<NotificationTemplate>> {
        let start = cursor_position(cursor, app_id)?;
        let rows = self
            .scan_json::<NotificationTemplate>(FAMILY_TEMPLATE, app_id)
            .await?
            .into_iter()
            .map(|(template_id, _, template)| (template_id, template))
            .collect();
        Ok(page_from_rows(
            app_id,
            PushCursorKind::Template,
            rows,
            limit,
            start,
        ))
    }

    async fn delete_template(
        &self,
        app_id: &str,
        template_id: &str,
    ) -> PushStorageResult<DeleteDeviceOutcome> {
        self.delete_json(FAMILY_TEMPLATE, app_id, template_id, DEFAULT_SK)
            .await
    }
}
