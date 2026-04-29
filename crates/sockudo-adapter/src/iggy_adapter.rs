#[cfg(feature = "iggy")]
pub use inner::*;

#[cfg(feature = "iggy")]
mod inner {
    use crate::horizontal_adapter_base::HorizontalAdapterBase;
    use crate::transports::IggyTransport;
    use sockudo_core::error::Result;
    pub(crate) use sockudo_core::options::IggyConfig;

    /// Apache Iggy adapter for horizontal scaling.
    pub type IggyAdapter = HorizontalAdapterBase<IggyTransport>;

    impl IggyAdapter {
        pub async fn with_connection_string(connection_string: String) -> Result<Self> {
            let config = IggyConfig {
                connection_string,
                ..Default::default()
            };
            HorizontalAdapterBase::new(config).await
        }
    }
}
