use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct EventQuery {
    #[serde(default)]
    pub auth_key: String,
    #[serde(default)]
    pub auth_timestamp: String,
    #[serde(default)]
    pub auth_version: String,
    #[serde(default)]
    pub body_md5: String,
    #[serde(default)]
    pub auth_signature: String,
}
