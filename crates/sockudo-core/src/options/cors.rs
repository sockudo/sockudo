use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CorsConfig {
    pub credentials: bool,
    #[serde(deserialize_with = "deserialize_and_validate_cors_origins")]
    pub origin: Vec<String>,
    pub methods: Vec<String>,
    pub allowed_headers: Vec<String>,
}

fn deserialize_and_validate_cors_origins<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    let origins = Vec::<String>::deserialize(deserializer)?;

    if let Err(e) = crate::origin_validation::OriginValidator::validate_patterns(&origins) {
        return Err(D::Error::custom(format!(
            "CORS origin pattern validation failed: {}",
            e
        )));
    }

    Ok(origins)
}

impl Default for CorsConfig {
    fn default() -> Self {
        Self {
            credentials: true,
            origin: vec!["*".to_string()],
            methods: vec![
                "GET".to_string(),
                "POST".to_string(),
                "DELETE".to_string(),
                "OPTIONS".to_string(),
            ],
            allowed_headers: vec![
                "Authorization".to_string(),
                "Content-Type".to_string(),
                "X-Requested-With".to_string(),
                "Accept".to_string(),
            ],
        }
    }
}

#[cfg(test)]
mod cors_config_tests {
    use super::CorsConfig;

    fn cors_from_json(json: &str) -> sonic_rs::Result<CorsConfig> {
        sonic_rs::from_str(json)
    }

    #[test]
    fn test_deserialize_valid_exact_origins() {
        let config =
            cors_from_json(r#"{"origin": ["https://example.com", "https://other.com"]}"#).unwrap();
        assert_eq!(config.origin.len(), 2);
    }

    #[test]
    fn test_deserialize_valid_wildcard_patterns() {
        let config =
            cors_from_json(r#"{"origin": ["*.example.com", "https://*.staging.example.com"]}"#)
                .unwrap();
        assert_eq!(config.origin.len(), 2);
    }

    #[test]
    fn test_deserialize_allows_special_markers() {
        assert!(cors_from_json(r#"{"origin": ["*"]}"#).is_ok());
        assert!(cors_from_json(r#"{"origin": ["Any"]}"#).is_ok());
        assert!(cors_from_json(r#"{"origin": ["any"]}"#).is_ok());
        assert!(cors_from_json(r#"{"origin": ["*", "https://example.com"]}"#).is_ok());
    }

    #[test]
    fn test_deserialize_rejects_invalid_patterns() {
        assert!(cors_from_json(r#"{"origin": ["*.*bad"]}"#).is_err());
        assert!(cors_from_json(r#"{"origin": ["*."]}"#).is_err());
        assert!(cors_from_json(r#"{"origin": [""]}"#).is_err());
        assert!(cors_from_json(r#"{"origin": ["https://"]}"#).is_err());
    }

    #[test]
    fn test_deserialize_rejects_mixed_valid_and_invalid() {
        assert!(cors_from_json(r#"{"origin": ["https://good.com", "*.*bad"]}"#).is_err());
    }
}
