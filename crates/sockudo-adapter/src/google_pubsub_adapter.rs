#[cfg(feature = "google-pubsub")]
pub use inner::*;

#[cfg(feature = "google-pubsub")]
mod inner {
    use crate::horizontal_adapter_base::HorizontalAdapterBase;
    use crate::transports::GooglePubSubTransport;
    use sockudo_core::error::Result;
    pub(crate) use sockudo_core::options::GooglePubSubAdapterConfig;

    /// Google Cloud Pub/Sub adapter for horizontal scaling.
    pub type GooglePubSubAdapter = HorizontalAdapterBase<GooglePubSubTransport>;

    impl GooglePubSubAdapter {
        pub async fn with_project_id(project_id: impl Into<String>) -> Result<Self> {
            let config = GooglePubSubAdapterConfig {
                project_id: project_id.into(),
                ..Default::default()
            };
            HorizontalAdapterBase::new(config).await
        }
    }
}
