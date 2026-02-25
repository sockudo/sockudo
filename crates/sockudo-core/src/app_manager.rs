use crate::app_store::AppStore;
use crate::error::{Error, Result};
use async_trait::async_trait;
use sockudo_types::app::App;

/// Trait defining operations that all AppManager implementations must support
#[async_trait]
pub trait AppManager: Send + Sync + 'static {
    /// Initialize the App Manager
    async fn init(&self) -> Result<()>;

    /// Register a new application
    async fn create_app(&self, config: App) -> Result<()>;

    /// Update an existing application
    async fn update_app(&self, config: App) -> Result<()>;

    /// Remove an application
    async fn delete_app(&self, app_id: &str) -> Result<()>;

    /// Get all registered applications
    async fn get_apps(&self) -> Result<Vec<App>>;

    /// Get an app by its key
    async fn find_by_key(&self, key: &str) -> Result<Option<App>>;

    /// Get an app by its ID
    async fn find_by_id(&self, app_id: &str) -> Result<Option<App>>;

    /// Health check for the app manager
    async fn check_health(&self) -> Result<()>;
}

#[async_trait]
impl AppStore for dyn AppManager + Send + Sync {
    type Error = Error;

    async fn init(&self) -> std::result::Result<(), Self::Error> {
        AppManager::init(self).await
    }

    async fn create_app(&self, config: App) -> std::result::Result<(), Self::Error> {
        AppManager::create_app(self, config).await
    }

    async fn update_app(&self, config: App) -> std::result::Result<(), Self::Error> {
        AppManager::update_app(self, config).await
    }

    async fn delete_app(&self, app_id: &str) -> std::result::Result<(), Self::Error> {
        AppManager::delete_app(self, app_id).await
    }

    async fn get_apps(&self) -> std::result::Result<Vec<App>, Self::Error> {
        AppManager::get_apps(self).await
    }

    async fn find_by_key(&self, key: &str) -> std::result::Result<Option<App>, Self::Error> {
        AppManager::find_by_key(self, key).await
    }

    async fn find_by_id(&self, app_id: &str) -> std::result::Result<Option<App>, Self::Error> {
        AppManager::find_by_id(self, app_id).await
    }

    async fn check_health(&self) -> std::result::Result<(), Self::Error> {
        AppManager::check_health(self).await
    }
}
