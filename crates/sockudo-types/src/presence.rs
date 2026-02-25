use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresenceMemberInfo {
    pub user_id: String,
    pub user_info: Option<sonic_rs::Value>,
}
