//! Typed accessors over a tool's JSON argument object.

use rmcp::model::JsonObject;
use serde_json::{Map, Value};

use crate::error::ToolError;

/// Borrowed view over tool arguments with validating accessors.
#[derive(Debug, Clone, Copy)]
pub struct Args<'a>(&'a JsonObject);

impl<'a> Args<'a> {
    /// Wrap an argument object.
    pub fn new(object: &'a JsonObject) -> Self {
        Self(object)
    }

    /// Underlying object.
    pub fn as_object(&self) -> &'a JsonObject {
        self.0
    }

    /// Raw value.
    pub fn get(&self, key: &str) -> Option<&'a Value> {
        self.0.get(key).filter(|value| !value.is_null())
    }

    /// Whether the key is present and not null.
    pub fn has(&self, key: &str) -> bool {
        self.get(key).is_some()
    }

    /// Required non-empty string.
    pub fn str(&self, key: &str) -> Result<&'a str, ToolError> {
        match self.opt_str(key)? {
            Some(value) if !value.trim().is_empty() => Ok(value),
            Some(_) => Err(ToolError::invalid(format!("'{key}' must not be empty"))),
            None => Err(ToolError::invalid(format!("'{key}' is required"))),
        }
    }

    /// Required `app_id`.
    pub fn app_id(&self) -> Result<String, ToolError> {
        self.str("app_id").map(str::to_string)
    }

    /// Required `channel`.
    pub fn channel(&self) -> Result<String, ToolError> {
        self.str("channel").map(str::to_string)
    }

    /// Optional string.
    pub fn opt_str(&self, key: &str) -> Result<Option<&'a str>, ToolError> {
        match self.get(key) {
            None => Ok(None),
            Some(Value::String(value)) => Ok(Some(value.as_str())),
            Some(_) => Err(ToolError::invalid(format!("'{key}' must be a string"))),
        }
    }

    /// Optional owned string.
    pub fn opt_string(&self, key: &str) -> Result<Option<String>, ToolError> {
        Ok(self.opt_str(key)?.map(str::to_string))
    }

    /// Optional unsigned integer.
    pub fn opt_u64(&self, key: &str) -> Result<Option<u64>, ToolError> {
        match self.get(key) {
            None => Ok(None),
            Some(Value::Number(number)) => number.as_u64().map(Some).ok_or_else(|| {
                ToolError::invalid(format!("'{key}' must be a non-negative integer"))
            }),
            Some(_) => Err(ToolError::invalid(format!("'{key}' must be an integer"))),
        }
    }

    /// Optional signed integer.
    pub fn opt_i64(&self, key: &str) -> Result<Option<i64>, ToolError> {
        match self.get(key) {
            None => Ok(None),
            Some(Value::Number(number)) => number
                .as_i64()
                .map(Some)
                .ok_or_else(|| ToolError::invalid(format!("'{key}' must be an integer"))),
            Some(_) => Err(ToolError::invalid(format!("'{key}' must be an integer"))),
        }
    }

    /// Optional boolean.
    pub fn opt_bool(&self, key: &str) -> Result<Option<bool>, ToolError> {
        match self.get(key) {
            None => Ok(None),
            Some(Value::Bool(value)) => Ok(Some(*value)),
            Some(_) => Err(ToolError::invalid(format!("'{key}' must be a boolean"))),
        }
    }

    /// Boolean defaulting to `false`.
    pub fn flag(&self, key: &str) -> Result<bool, ToolError> {
        Ok(self.opt_bool(key)?.unwrap_or(false))
    }

    /// Optional array of non-empty strings.
    pub fn opt_str_array(&self, key: &str) -> Result<Option<Vec<String>>, ToolError> {
        match self.get(key) {
            None => Ok(None),
            Some(Value::Array(items)) => items
                .iter()
                .map(|item| match item {
                    Value::String(value) if !value.is_empty() => Ok(value.clone()),
                    _ => Err(ToolError::invalid(format!(
                        "'{key}' must be an array of non-empty strings"
                    ))),
                })
                .collect::<Result<Vec<_>, _>>()
                .map(Some),
            Some(_) => Err(ToolError::invalid(format!("'{key}' must be an array"))),
        }
    }

    /// Optional JSON object.
    pub fn opt_object(&self, key: &str) -> Result<Option<&'a Map<String, Value>>, ToolError> {
        match self.get(key) {
            None => Ok(None),
            Some(Value::Object(object)) => Ok(Some(object)),
            Some(_) => Err(ToolError::invalid(format!("'{key}' must be an object"))),
        }
    }

    /// Required JSON object.
    pub fn object(&self, key: &str) -> Result<&'a Map<String, Value>, ToolError> {
        self.opt_object(key)?
            .ok_or_else(|| ToolError::invalid(format!("'{key}' is required")))
    }

    /// Optional array.
    pub fn opt_array(&self, key: &str) -> Result<Option<&'a Vec<Value>>, ToolError> {
        match self.get(key) {
            None => Ok(None),
            Some(Value::Array(items)) => Ok(Some(items)),
            Some(_) => Err(ToolError::invalid(format!("'{key}' must be an array"))),
        }
    }

    /// Copy every argument except `skip` into a new JSON object. Used for
    /// tools whose body mirrors the HTTP request body one-to-one.
    pub fn passthrough(&self, skip: &[&str]) -> Map<String, Value> {
        self.0
            .iter()
            .filter(|(key, value)| !skip.contains(&key.as_str()) && !value.is_null())
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }
}

/// Validate a page-size style limit.
pub fn validate_limit(limit: Option<u64>, key: &str) -> Result<Option<u64>, ToolError> {
    match limit {
        Some(0) => Err(ToolError::invalid(format!(
            "'{key}' must be greater than 0"
        ))),
        Some(value) if value > 10_000 => {
            Err(ToolError::invalid(format!("'{key}' must be at most 10000")))
        }
        other => Ok(other),
    }
}

/// Validate a history direction value.
pub fn validate_direction(direction: Option<&str>) -> Result<Option<&str>, ToolError> {
    match direction {
        None => Ok(None),
        Some(value)
            if matches!(
                value,
                "newest_first" | "oldest_first" | "backwards" | "forwards" | "forward" | "reverse"
            ) =>
        {
            Ok(Some(value))
        }
        Some(other) => Err(ToolError::invalid(format!(
            "'direction' must be newest_first or oldest_first, got '{other}'"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn accessors_validate_types() {
        let object = json!({
            "app_id": "a", "n": 3, "neg": -1, "flag": true,
            "list": ["x", "y"], "obj": {"k": 1}, "nil": null
        });
        let args = Args::new(object.as_object().unwrap());
        assert_eq!(args.app_id().unwrap(), "a");
        assert_eq!(args.opt_u64("n").unwrap(), Some(3));
        assert!(args.opt_u64("neg").is_err());
        assert_eq!(args.opt_i64("neg").unwrap(), Some(-1));
        assert!(args.flag("flag").unwrap());
        assert!(!args.flag("missing").unwrap());
        assert_eq!(args.opt_str_array("list").unwrap().unwrap().len(), 2);
        assert!(args.object("obj").is_ok());
        assert!(!args.has("nil"));
        assert!(args.str("nil").is_err());
        assert!(args.opt_str("n").is_err());
        let passthrough = args.passthrough(&["app_id"]);
        assert!(!passthrough.contains_key("app_id"));
        assert!(!passthrough.contains_key("nil"));
        assert!(passthrough.contains_key("n"));
    }

    #[test]
    fn limit_and_direction_validation() {
        assert!(validate_limit(Some(0), "limit").is_err());
        assert!(validate_limit(Some(20_000), "limit").is_err());
        assert_eq!(validate_limit(Some(10), "limit").unwrap(), Some(10));
        assert!(validate_direction(Some("sideways")).is_err());
        assert_eq!(
            validate_direction(Some("oldest_first")).unwrap(),
            Some("oldest_first")
        );
    }
}
