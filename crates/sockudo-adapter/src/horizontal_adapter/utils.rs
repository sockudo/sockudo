use uuid::Uuid;

/// Generate unique request ID
pub fn generate_request_id() -> String {
    Uuid::new_v4().to_string()
}

/// Get current timestamp in seconds
pub fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
