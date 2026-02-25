use async_trait::async_trait;
use sockudo_types::app::App;

/// Core app repository/store contract.
#[async_trait]
pub trait AppStore: Send + Sync + 'static {
    type Error: Send + Sync + 'static;

    async fn init(&self) -> Result<(), Self::Error>;
    async fn create_app(&self, config: App) -> Result<(), Self::Error>;
    async fn update_app(&self, config: App) -> Result<(), Self::Error>;
    async fn delete_app(&self, app_id: &str) -> Result<(), Self::Error>;
    async fn get_apps(&self) -> Result<Vec<App>, Self::Error>;
    async fn find_by_key(&self, key: &str) -> Result<Option<App>, Self::Error>;
    async fn find_by_id(&self, app_id: &str) -> Result<Option<App>, Self::Error>;
    async fn check_health(&self) -> Result<(), Self::Error>;
}
