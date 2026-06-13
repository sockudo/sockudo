use super::DynamoDbPushStore;
use super::constants::*;
use super::document::{DocumentBackend, StoredDocument};
use super::helpers::*;
use crate::storage::PushStorageResult;
use async_trait::async_trait;

#[cfg(feature = "dynamodb")]
#[derive(Clone)]
pub struct DynamoDbDocumentBackend {
    client: aws_sdk_dynamodb::Client,
    table: String,
}

#[cfg(feature = "dynamodb")]
impl DynamoDbPushStore {
    pub async fn new(
        client: aws_sdk_dynamodb::Client,
        table: impl Into<String>,
    ) -> PushStorageResult<Self> {
        let backend = DynamoDbDocumentBackend {
            client,
            table: table.into(),
        };
        backend.ensure_table().await?;
        Ok(Self::with_backend(backend))
    }
}

#[cfg(feature = "dynamodb")]
impl DynamoDbDocumentBackend {
    async fn ensure_table(&self) -> PushStorageResult<()> {
        use aws_sdk_dynamodb::types::{
            AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
        };

        if self
            .client
            .describe_table()
            .table_name(&self.table)
            .send()
            .await
            .is_ok()
        {
            return Ok(());
        }

        self.client
            .create_table()
            .table_name(&self.table)
            .billing_mode(BillingMode::PayPerRequest)
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("family_app")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .map_err(dynamodb_build_error)?,
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("doc_key")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .map_err(dynamodb_build_error)?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("family_app")
                    .key_type(KeyType::Hash)
                    .build()
                    .map_err(dynamodb_build_error)?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("doc_key")
                    .key_type(KeyType::Range)
                    .build()
                    .map_err(dynamodb_build_error)?,
            )
            .send()
            .await
            .map_err(dynamodb_error)?;
        Ok(())
    }

    fn key(family: &'static str, app_id: &str, pk: &str, sk: &str) -> (String, String) {
        (family_app(family, app_id), document_key(pk, sk))
    }
}

#[cfg(feature = "dynamodb")]
#[async_trait]
impl DocumentBackend for DynamoDbDocumentBackend {
    async fn put(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
        data: String,
    ) -> PushStorageResult<()> {
        use aws_sdk_dynamodb::types::AttributeValue;
        let (family_app, doc_key) = Self::key(family, app_id, pk, sk);
        self.client
            .put_item()
            .table_name(&self.table)
            .item("family_app", AttributeValue::S(family_app))
            .item("doc_key", AttributeValue::S(doc_key))
            .item("app_id", AttributeValue::S(app_id.to_owned()))
            .item("pk", AttributeValue::S(pk.to_owned()))
            .item("sk", AttributeValue::S(sk.to_owned()))
            .item("data", AttributeValue::S(data))
            .send()
            .await
            .map_err(dynamodb_error)?;
        Ok(())
    }

    async fn put_if_absent(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
        data: String,
    ) -> PushStorageResult<bool> {
        use aws_sdk_dynamodb::types::AttributeValue;
        let (family_app, doc_key) = Self::key(family, app_id, pk, sk);
        let result = self
            .client
            .put_item()
            .table_name(&self.table)
            .condition_expression("attribute_not_exists(family_app)")
            .item("family_app", AttributeValue::S(family_app))
            .item("doc_key", AttributeValue::S(doc_key))
            .item("app_id", AttributeValue::S(app_id.to_owned()))
            .item("pk", AttributeValue::S(pk.to_owned()))
            .item("sk", AttributeValue::S(sk.to_owned()))
            .item("data", AttributeValue::S(data))
            .send()
            .await;
        match result {
            Ok(_) => Ok(true),
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(|error| error.is_conditional_check_failed_exception()) =>
            {
                Ok(false)
            }
            Err(error) => Err(dynamodb_error(error)),
        }
    }

    async fn get(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
    ) -> PushStorageResult<Option<String>> {
        use aws_sdk_dynamodb::types::AttributeValue;
        let (family_app, doc_key) = Self::key(family, app_id, pk, sk);
        Ok(self
            .client
            .get_item()
            .table_name(&self.table)
            .key("family_app", AttributeValue::S(family_app))
            .key("doc_key", AttributeValue::S(doc_key))
            .send()
            .await
            .map_err(dynamodb_error)?
            .item
            .and_then(|mut item| item.remove("data"))
            .and_then(|value| value.as_s().ok().cloned()))
    }

    async fn delete(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
    ) -> PushStorageResult<bool> {
        use aws_sdk_dynamodb::types::{AttributeValue, ReturnValue};
        let (family_app, doc_key) = Self::key(family, app_id, pk, sk);
        Ok(self
            .client
            .delete_item()
            .table_name(&self.table)
            .key("family_app", AttributeValue::S(family_app))
            .key("doc_key", AttributeValue::S(doc_key))
            .return_values(ReturnValue::AllOld)
            .send()
            .await
            .map_err(dynamodb_error)?
            .attributes
            .is_some_and(|attributes| !attributes.is_empty()))
    }

    async fn scan_app(
        &self,
        family: &'static str,
        app_id: &str,
    ) -> PushStorageResult<Vec<StoredDocument>> {
        use aws_sdk_dynamodb::types::AttributeValue;
        let family_app = family_app(family, app_id);
        let rows = self
            .client
            .query()
            .table_name(&self.table)
            .key_condition_expression("family_app = :family_app")
            .expression_attribute_values(":family_app", AttributeValue::S(family_app))
            .send()
            .await
            .map_err(dynamodb_error)?
            .items
            .unwrap_or_default()
            .into_iter()
            .map(|mut item| {
                let pk = take_dynamodb_string(&mut item, "pk")?;
                let sk = take_dynamodb_string(&mut item, "sk")?;
                let data = take_dynamodb_string(&mut item, "data")?;
                Ok(StoredDocument { pk, sk, data })
            })
            .collect::<PushStorageResult<Vec<_>>>()?;
        Ok(rows)
    }

    async fn scan_pk(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
    ) -> PushStorageResult<Vec<StoredDocument>> {
        use aws_sdk_dynamodb::types::AttributeValue;
        let family_app = family_app(family, app_id);
        let prefix = format!("{pk}\0");
        let rows = self
            .client
            .query()
            .table_name(&self.table)
            .key_condition_expression("family_app = :family_app AND begins_with(doc_key, :prefix)")
            .expression_attribute_values(":family_app", AttributeValue::S(family_app))
            .expression_attribute_values(":prefix", AttributeValue::S(prefix))
            .send()
            .await
            .map_err(dynamodb_error)?
            .items
            .unwrap_or_default()
            .into_iter()
            .map(|mut item| {
                let pk = take_dynamodb_string(&mut item, "pk")?;
                let sk = take_dynamodb_string(&mut item, "sk")?;
                let data = take_dynamodb_string(&mut item, "data")?;
                Ok(StoredDocument { pk, sk, data })
            })
            .collect::<PushStorageResult<Vec<_>>>()?;
        Ok(rows)
    }

    async fn scan_pk_page(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> PushStorageResult<Vec<StoredDocument>> {
        use aws_sdk_dynamodb::types::AttributeValue;
        let family_app = family_app(family, app_id);
        let prefix = format!("{pk}\0");
        let mut query = self
            .client
            .query()
            .table_name(&self.table)
            .key_condition_expression("family_app = :family_app AND begins_with(doc_key, :prefix)")
            .expression_attribute_values(":family_app", AttributeValue::S(family_app.clone()))
            .expression_attribute_values(":prefix", AttributeValue::S(prefix))
            .limit(limit.max(1) as i32);
        if let Some(start_after) = start_after {
            query = query
                .exclusive_start_key("family_app", AttributeValue::S(family_app))
                .exclusive_start_key("doc_key", AttributeValue::S(document_key(pk, start_after)));
        }
        let rows = query
            .send()
            .await
            .map_err(dynamodb_error)?
            .items
            .unwrap_or_default()
            .into_iter()
            .map(|mut item| {
                let pk = take_dynamodb_string(&mut item, "pk")?;
                let sk = take_dynamodb_string(&mut item, "sk")?;
                let data = take_dynamodb_string(&mut item, "data")?;
                Ok(StoredDocument { pk, sk, data })
            })
            .collect::<PushStorageResult<Vec<_>>>()?;
        Ok(rows)
    }

    async fn scan_app_page_by_pk(
        &self,
        family: &'static str,
        app_id: &str,
        start_after_pk: Option<&str>,
        limit: usize,
    ) -> PushStorageResult<Vec<StoredDocument>> {
        use aws_sdk_dynamodb::types::AttributeValue;
        let family_app = family_app(family, app_id);
        let mut query = self
            .client
            .query()
            .table_name(&self.table)
            .key_condition_expression("family_app = :family_app")
            .expression_attribute_values(":family_app", AttributeValue::S(family_app.clone()))
            .limit(limit.max(1) as i32);
        if let Some(start_after_pk) = start_after_pk {
            query = query
                .exclusive_start_key("family_app", AttributeValue::S(family_app))
                .exclusive_start_key(
                    "doc_key",
                    AttributeValue::S(document_key(start_after_pk, DEFAULT_SK)),
                );
        }
        let rows = query
            .send()
            .await
            .map_err(dynamodb_error)?
            .items
            .unwrap_or_default()
            .into_iter()
            .map(|mut item| {
                let pk = take_dynamodb_string(&mut item, "pk")?;
                let sk = take_dynamodb_string(&mut item, "sk")?;
                let data = take_dynamodb_string(&mut item, "data")?;
                Ok(StoredDocument { pk, sk, data })
            })
            .collect::<PushStorageResult<Vec<_>>>()?;
        Ok(rows)
    }

    async fn delete_many(
        &self,
        family: &'static str,
        app_id: &str,
        keys: &[(String, String)],
    ) -> PushStorageResult<u64> {
        use aws_sdk_dynamodb::types::{AttributeValue, DeleteRequest, WriteRequest};
        let mut deleted = 0_u64;
        for chunk in keys.chunks(25) {
            let mut requests = Vec::with_capacity(chunk.len());
            for (pk, sk) in chunk {
                let (family_app, doc_key) = Self::key(family, app_id, pk, sk);
                let delete = DeleteRequest::builder()
                    .key("family_app", AttributeValue::S(family_app))
                    .key("doc_key", AttributeValue::S(doc_key))
                    .build()
                    .map_err(dynamodb_build_error)?;
                requests.push(WriteRequest::builder().delete_request(delete).build());
            }
            if requests.is_empty() {
                continue;
            }
            self.client
                .batch_write_item()
                .request_items(self.table.clone(), requests)
                .send()
                .await
                .map_err(dynamodb_error)?;
            deleted += chunk.len() as u64;
        }
        Ok(deleted)
    }
}
