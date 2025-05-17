use super::manager::AppManager;
use crate::app::config::App;
use crate::error::Error;
use crate::token::{secure_compare, Token};
use crate::websocket::SocketId;
use serde::Deserialize;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
pub struct ChannelAuth {
    pub channel_name: String,
    pub socket_id: String,
    #[serde(default)]
    pub user_data: Option<String>,
}

pub struct AuthValidator {
    app_manager: Arc<dyn AppManager>,
}

#[derive(Debug)]
pub struct AuthValidationResult {
    pub is_valid: bool,
    pub signature: String,
}

impl AuthValidator {
    pub fn new(app_manager: Arc<dyn AppManager>) -> Self {
        AuthValidator { app_manager }
    }

    pub async fn validate_channel_auth(
        &self,
        socket_id: SocketId,
        app_key: &str,
        user_data: &str,
        auth: &str,
    ) -> Result<bool, Error> {
        let app = self.app_manager.find_by_id(app_key).await?;
        if app.is_none() {
            return Err(Error::InvalidAppKey);
        }
        let is_valid =
            self.sign_in_token_is_valid(socket_id.0.as_str(), user_data, auth, app.unwrap());
        Ok(is_valid)
    }

    pub fn sign_in_token_is_valid(
        &self,
        socket_id: &str,
        user_data: &str,
        expected_signature: &str,
        app_config: App,
    ) -> bool {
        let signature = self.sing_in_token_for_user_data(socket_id, user_data, app_config);
        secure_compare(&signature, expected_signature)
    }

    pub fn sing_in_token_for_user_data(
        &self,
        socket_id: &str,
        user_data: &str,
        app_config: App,
    ) -> String {
        let decoded_string = format!("{}::user::{}", socket_id, user_data);
        let signature = Token::new(app_config.key, app_config.secret);
        signature.sign(&decoded_string)
    }
}
