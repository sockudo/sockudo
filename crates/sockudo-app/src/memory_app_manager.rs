use async_trait::async_trait;
use sockudo_core::app::{App, AppManager};
use sockudo_core::error::Result;
use std::collections::HashMap;
use std::sync::RwLock;

#[derive(Default)]
struct MemoryAppState {
    apps_by_id: HashMap<String, App>,
    app_id_by_key: HashMap<String, String>,
}

pub struct MemoryAppManager {
    state: RwLock<MemoryAppState>,
}

impl Default for MemoryAppManager {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryAppManager {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(MemoryAppState::default()),
        }
    }
}

#[async_trait]
impl AppManager for MemoryAppManager {
    async fn init(&self) -> Result<()> {
        Ok(())
    }

    async fn create_app(&self, config: App) -> Result<()> {
        let mut state = self.state.write().unwrap();
        if let Some(previous) = state.apps_by_id.insert(config.id.clone(), config.clone()) {
            state.app_id_by_key.remove(&previous.key);
        }
        state
            .app_id_by_key
            .insert(config.key.clone(), config.id.clone());
        Ok(())
    }

    async fn update_app(&self, config: App) -> Result<()> {
        let mut state = self.state.write().unwrap();
        if let Some(previous) = state.apps_by_id.insert(config.id.clone(), config.clone()) {
            state.app_id_by_key.remove(&previous.key);
        }
        state
            .app_id_by_key
            .insert(config.key.clone(), config.id.clone());
        Ok(())
    }

    async fn delete_app(&self, app_id: &str) -> Result<()> {
        let mut state = self.state.write().unwrap();
        if let Some(previous) = state.apps_by_id.remove(app_id) {
            state.app_id_by_key.remove(&previous.key);
        }
        Ok(())
    }

    async fn get_apps(&self) -> Result<Vec<App>> {
        let state = self.state.read().unwrap();
        Ok(state.apps_by_id.values().cloned().collect())
    }

    async fn find_by_key(&self, key: &str) -> Result<Option<App>> {
        let state = self.state.read().unwrap();
        Ok(state
            .app_id_by_key
            .get(key)
            .and_then(|app_id| state.apps_by_id.get(app_id))
            .cloned())
    }

    async fn find_by_id(&self, app_id: &str) -> Result<Option<App>> {
        let state = self.state.read().unwrap();
        Ok(state.apps_by_id.get(app_id).cloned())
    }

    async fn check_health(&self) -> Result<()> {
        Ok(())
    }
}
