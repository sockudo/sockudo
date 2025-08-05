// src/app/traits.rs
use crate::app::config::App;
use crate::error::Result;
use async_trait::async_trait;

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
