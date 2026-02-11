use serde::{Deserialize, Deserializer, Serialize};
use sonic_rs::Value;
use sonic_rs::prelude::*;

use super::ops::{CompareOp, LogicalOp};

/// Custom deserializer that accepts both strings and numbers for the val field
fn deserialize_string_or_number<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value: Option<Value> = Option::deserialize(deserializer)?;
    Ok(value.map(|v| {
        if let Some(s) = v.as_str() {
            s.to_string()
        } else if let Some(n) = v.as_number() {
            n.to_string()
        } else if let Some(b) = v.as_bool() {
            b.to_string()
        } else {
            v.to_string()
        }
    }))
}

/// Custom deserializer that accepts both strings and numbers for the vals array
fn deserialize_string_or_number_vec<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let values: Vec<Value> = Vec::deserialize(deserializer)?;
    Ok(values
        .into_iter()
        .map(|v| {
            if let Some(s) = v.as_str() {
                s.to_string()
            } else if let Some(n) = v.as_number() {
                n.to_string()
            } else if let Some(b) = v.as_bool() {
                b.to_string()
            } else {
                v.to_string()
            }
        })
        .collect())
}

fn default_empty_vec() -> Vec<String> {
    Vec::new()
}

/// A filter node that can be a leaf (comparison) or a branch (logical operation).
///
/// This structure is designed to be serialized/deserialized from JSON and can be
/// constructed programmatically using the builder pattern.
///
/// Design goals:
/// - Zero allocations during evaluation
/// - Easy serialization to/from JSON
/// - Programmatic construction via builder
/// - Tree structure for complex filters
///
/// Performance optimizations:
/// - HashSet cache for IN/NIN operators with many values (O(1) lookup)
/// - Arc-wrapped HashSet shared across clones (zero-copy for socket subscriptions)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterNode {
    /// Logical operator for branch nodes: "and", "or", "not"
    /// If None or empty, this is a leaf node (comparison)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub op: Option<String>,

    /// Key for comparison (leaf nodes only)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,

    /// Comparison operator for leaf nodes
    /// "eq", "neq", "in", "nin", "ex", "nex", "sw", "ew", "ct", "gt", "gte", "lt", "lte"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cmp: Option<String>,

    /// Single value for most comparisons
    #[serde(
        skip_serializing_if = "Option::is_none",
        default,
        deserialize_with = "deserialize_string_or_number"
    )]
    pub val: Option<String>,

    /// Multiple values for set operations (in, nin)
    #[serde(
        skip_serializing_if = "Vec::is_empty",
        default = "default_empty_vec",
        deserialize_with = "deserialize_string_or_number_vec"
    )]
    pub vals: Vec<String>,

    /// Child nodes for logical operations
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub nodes: Vec<FilterNode>,

    /// Flag indicating if the vals vector has been sorted for fast binary search
    #[serde(skip)]
    is_sorted: bool,
}

// Manual PartialEq to ignore is_sorted cache field
impl PartialEq for FilterNode {
    fn eq(&self, other: &Self) -> bool {
        self.op == other.op
            && self.key == other.key
            && self.cmp == other.cmp
            && self.val == other.val
            && self.vals == other.vals
            && self.nodes == other.nodes
    }
}

impl FilterNode {
    /// Creates a new leaf node with a comparison operation.
    pub fn new_comparison(key: String, cmp: CompareOp, val: String) -> Self {
        Self {
            op: None,
            key: Some(key),
            cmp: Some(cmp.to_string()),
            val: Some(val),
            vals: Vec::new(),
            nodes: Vec::new(),
            is_sorted: false,
        }
    }

    /// Creates a new leaf node with a set comparison operation (in/nin).
    pub fn new_set_comparison(key: String, cmp: CompareOp, vals: Vec<String>) -> Self {
        Self {
            op: None,
            key: Some(key),
            cmp: Some(cmp.to_string()),
            val: None,
            vals,
            nodes: Vec::new(),
            is_sorted: false,
        }
    }

    /// Creates a new leaf node with an existence check.
    pub fn new_existence(key: String, cmp: CompareOp) -> Self {
        Self {
            op: None,
            key: Some(key),
            cmp: Some(cmp.to_string()),
            val: None,
            vals: Vec::new(),
            nodes: Vec::new(),
            is_sorted: false,
        }
    }

    /// Creates a new branch node with a logical operation.
    pub fn new_logical(op: LogicalOp, nodes: Vec<FilterNode>) -> Self {
        Self {
            op: Some(op.to_string()),
            key: None,
            cmp: None,
            val: None,
            vals: Vec::new(),
            nodes,
            is_sorted: false,
        }
    }

    /// Returns true if the vals vector is sorted and ready for binary search.
    #[inline]
    pub fn is_sorted(&self) -> bool {
        self.is_sorted
    }

    /// Optimizes the filter node by sorting vectors for binary search.
    /// This avoids the overhead of building HashSets (allocations) while still
    /// providing O(log n) lookup performance.
    pub fn optimize(&mut self) {
        // Sort and deduplicate values for binary search
        if !self.vals.is_empty() && !self.is_sorted {
            self.vals.sort();
            self.vals.dedup();
            self.is_sorted = true;
        }

        // Recursively optimize child nodes
        for node in &mut self.nodes {
            node.optimize();
        }
    }

    /// Returns the logical operator if this is a branch node.
    #[inline]
    pub fn logical_op(&self) -> Option<LogicalOp> {
        self.op.as_ref().and_then(|s| LogicalOp::parse(s))
    }

    /// Returns the comparison operator if this is a leaf node.
    #[inline]
    pub fn compare_op(&self) -> CompareOp {
        self.cmp
            .as_ref()
            .and_then(|s| CompareOp::parse(s))
            .unwrap_or(CompareOp::Equal)
    }

    /// Returns the key for leaf node comparisons.
    #[inline]
    pub fn key(&self) -> &str {
        self.key.as_deref().unwrap_or("")
    }

    /// Returns the single value for leaf node comparisons.
    #[inline]
    pub fn val(&self) -> &str {
        self.val.as_deref().unwrap_or("")
    }

    /// Returns the multiple values for set comparisons.
    #[inline]
    pub fn vals(&self) -> &[String] {
        &self.vals
    }

    /// Returns the child nodes for logical operations.
    #[inline]
    pub fn nodes(&self) -> &[FilterNode] {
        &self.nodes
    }

    /// Validates the filter node structure.
    ///
    /// Returns an error message if the node is invalid, None otherwise.
    pub fn validate(&self) -> Option<String> {
        if let Some(ref op) = self.op {
            // This is a logical node
            let logical_op = LogicalOp::parse(op)?;

            match logical_op {
                LogicalOp::And | LogicalOp::Or => {
                    if self.nodes.is_empty() {
                        return Some(format!("{op} operation requires at least one child node"));
                    }
                }
                LogicalOp::Not => {
                    if self.nodes.len() != 1 {
                        return Some(format!(
                            "not operation requires exactly one child node, got {}",
                            self.nodes.len()
                        ));
                    }
                }
            }

            // Validate all children recursively
            for (i, child) in self.nodes.iter().enumerate() {
                if let Some(err) = child.validate() {
                    return Some(format!("Child node {i}: {err}"));
                }
            }
        } else {
            // This is a leaf node
            if self.key.is_none() || self.key.as_ref().is_none_or(|k| k.is_empty()) {
                return Some("Leaf node requires a non-empty key".to_string());
            }

            let cmp = self.cmp.as_ref()?;
            let compare_op = CompareOp::parse(cmp)?;

            match compare_op {
                CompareOp::In | CompareOp::NotIn => {
                    if self.vals.is_empty() {
                        return Some(format!(
                            "{cmp} operation requires at least one value in vals"
                        ));
                    }
                }
                CompareOp::Exists | CompareOp::NotExists => {
                    // No value needed
                }
                _ => {
                    if self.val.is_none() || self.val.as_ref().is_none_or(|v| v.is_empty()) {
                        return Some(format!("{cmp} operation requires a non-empty val"));
                    }
                }
            }
        }

        None
    }
}

/// Builder for constructing filter nodes programmatically.
///
/// This provides a clean API for creating filters without dealing with the
/// internal structure directly.
pub struct FilterNodeBuilder;

impl FilterNodeBuilder {
    /// Creates an equality comparison: key == val
    pub fn eq(key: impl Into<String>, val: impl Into<String>) -> FilterNode {
        FilterNode::new_comparison(key.into(), CompareOp::Equal, val.into())
    }

    /// Creates an inequality comparison: key != val
    pub fn neq(key: impl Into<String>, val: impl Into<String>) -> FilterNode {
        FilterNode::new_comparison(key.into(), CompareOp::NotEqual, val.into())
    }

    /// Creates a set membership comparison: key in [vals...]
    pub fn in_set(key: impl Into<String>, vals: &[impl ToString]) -> FilterNode {
        FilterNode::new_set_comparison(
            key.into(),
            CompareOp::In,
            vals.iter().map(|v| v.to_string()).collect(),
        )
    }

    /// Creates a set non-membership comparison: key not in [vals...]
    pub fn nin(key: impl Into<String>, vals: &[impl ToString]) -> FilterNode {
        FilterNode::new_set_comparison(
            key.into(),
            CompareOp::NotIn,
            vals.iter().map(|v| v.to_string()).collect(),
        )
    }

    /// Creates an existence check: key exists
    pub fn exists(key: impl Into<String>) -> FilterNode {
        FilterNode::new_existence(key.into(), CompareOp::Exists)
    }

    /// Creates a non-existence check: key does not exist
    pub fn not_exists(key: impl Into<String>) -> FilterNode {
        FilterNode::new_existence(key.into(), CompareOp::NotExists)
    }

    /// Creates a starts-with comparison: key starts with val
    pub fn starts_with(key: impl Into<String>, val: impl Into<String>) -> FilterNode {
        FilterNode::new_comparison(key.into(), CompareOp::StartsWith, val.into())
    }

    /// Creates an ends-with comparison: key ends with val
    pub fn ends_with(key: impl Into<String>, val: impl Into<String>) -> FilterNode {
        FilterNode::new_comparison(key.into(), CompareOp::EndsWith, val.into())
    }

    /// Creates a contains comparison: key contains val
    pub fn contains(key: impl Into<String>, val: impl Into<String>) -> FilterNode {
        FilterNode::new_comparison(key.into(), CompareOp::Contains, val.into())
    }

    /// Creates a greater-than comparison: key > val (numeric)
    pub fn gt(key: impl Into<String>, val: impl Into<String>) -> FilterNode {
        FilterNode::new_comparison(key.into(), CompareOp::GreaterThan, val.into())
    }

    /// Creates a greater-than-or-equal comparison: key >= val (numeric)
    pub fn gte(key: impl Into<String>, val: impl Into<String>) -> FilterNode {
        FilterNode::new_comparison(key.into(), CompareOp::GreaterThanOrEqual, val.into())
    }

    /// Creates a less-than comparison: key < val (numeric)
    pub fn lt(key: impl Into<String>, val: impl Into<String>) -> FilterNode {
        FilterNode::new_comparison(key.into(), CompareOp::LessThan, val.into())
    }

    /// Creates a less-than-or-equal comparison: key <= val (numeric)
    pub fn lte(key: impl Into<String>, val: impl Into<String>) -> FilterNode {
        FilterNode::new_comparison(key.into(), CompareOp::LessThanOrEqual, val.into())
    }

    /// Creates an AND logical operation: all children must match
    pub fn and(nodes: Vec<FilterNode>) -> FilterNode {
        FilterNode::new_logical(LogicalOp::And, nodes)
    }

    /// Creates an OR logical operation: at least one child must match
    pub fn or(nodes: Vec<FilterNode>) -> FilterNode {
        FilterNode::new_logical(LogicalOp::Or, nodes)
    }

    /// Creates a NOT logical operation: negates the child
    pub fn not(node: FilterNode) -> FilterNode {
        FilterNode::new_logical(LogicalOp::Not, vec![node])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_simple_filter() {
        let filter = FilterNodeBuilder::eq("event_type", "goal");
        let json = sonic_rs::to_string(&filter).unwrap();
        let parsed: FilterNode = sonic_rs::from_str(&json).unwrap();
        assert_eq!(filter, parsed);
    }

    #[test]
    fn test_serialize_complex_filter() {
        let filter = FilterNodeBuilder::or(vec![
            FilterNodeBuilder::eq("event_type", "goal"),
            FilterNodeBuilder::and(vec![
                FilterNodeBuilder::eq("event_type", "shot"),
                FilterNodeBuilder::gte("xG", "0.8"),
            ]),
        ]);

        let json = sonic_rs::to_string(&filter).unwrap();
        let parsed: FilterNode = sonic_rs::from_str(&json).unwrap();
        assert_eq!(filter, parsed);
    }

    #[test]
    fn test_validate_valid_leaf() {
        let filter = FilterNodeBuilder::eq("key", "value");
        assert_eq!(filter.validate(), None);
    }

    #[test]
    fn test_validate_invalid_leaf_missing_key() {
        let filter = FilterNode {
            op: None,
            key: None,
            cmp: Some("eq".to_string()),
            val: Some("value".to_string()),
            vals: Vec::new(),
            nodes: Vec::new(),
            is_sorted: false,
        };
        assert!(filter.validate().is_some());
    }

    #[test]
    fn test_validate_invalid_leaf_missing_value() {
        let filter = FilterNode {
            op: None,
            key: Some("key".to_string()),
            cmp: Some("eq".to_string()),
            val: None,
            vals: Vec::new(),
            nodes: Vec::new(),
            is_sorted: false,
        };
        assert!(filter.validate().is_some());
    }

    #[test]
    fn test_validate_valid_set_operation() {
        let filter = FilterNodeBuilder::in_set("key", &["a", "b", "c"]);
        assert_eq!(filter.validate(), None);
    }

    #[test]
    fn test_validate_invalid_set_operation_empty_vals() {
        let filter = FilterNode {
            op: None,
            key: Some("key".to_string()),
            cmp: Some("in".to_string()),
            val: None,
            vals: Vec::new(),
            nodes: Vec::new(),
            is_sorted: false,
        };
        assert!(filter.validate().is_some());
    }

    #[test]
    fn test_validate_valid_and() {
        let filter = FilterNodeBuilder::and(vec![
            FilterNodeBuilder::eq("a", "1"),
            FilterNodeBuilder::eq("b", "2"),
        ]);
        assert_eq!(filter.validate(), None);
    }

    #[test]
    fn test_validate_invalid_and_no_children() {
        let filter = FilterNode {
            op: Some("and".to_string()),
            key: None,
            cmp: None,
            val: None,
            vals: Vec::new(),
            nodes: Vec::new(),
            is_sorted: false,
        };
        assert!(filter.validate().is_some());
    }

    #[test]
    fn test_validate_valid_not() {
        let filter = FilterNodeBuilder::not(FilterNodeBuilder::eq("key", "value"));
        assert_eq!(filter.validate(), None);
    }

    #[test]
    fn test_validate_invalid_not_multiple_children() {
        let filter = FilterNode {
            op: Some("not".to_string()),
            key: None,
            cmp: None,
            val: None,
            vals: Vec::new(),
            nodes: vec![
                FilterNodeBuilder::eq("a", "1"),
                FilterNodeBuilder::eq("b", "2"),
            ],
            is_sorted: false,
        };
        assert!(filter.validate().is_some());
    }

    #[test]
    fn test_validate_existence_checks() {
        let exists = FilterNodeBuilder::exists("key");
        let not_exists = FilterNodeBuilder::not_exists("key");
        assert_eq!(exists.validate(), None);
        assert_eq!(not_exists.validate(), None);
    }

    #[test]
    fn test_deserialize_numeric_val() {
        // Test that numeric values in JSON are properly converted to strings
        let json = r#"{"key":"category_id","cmp":"eq","val":501}"#;
        let filter: FilterNode = sonic_rs::from_str(json).unwrap();
        assert_eq!(filter.key, Some("category_id".to_string()));
        assert_eq!(filter.cmp, Some("eq".to_string()));
        assert_eq!(filter.val, Some("501".to_string()));
    }

    #[test]
    fn test_deserialize_numeric_vals() {
        // Test that numeric values in vals array are properly converted to strings
        let json = r#"{"key":"category_id","cmp":"in","vals":[501,1,56]}"#;
        let filter: FilterNode = sonic_rs::from_str(json).unwrap();
        assert_eq!(filter.key, Some("category_id".to_string()));
        assert_eq!(filter.cmp, Some("in".to_string()));
        assert_eq!(filter.vals, vec!["501", "1", "56"]);
    }

    #[test]
    fn test_deserialize_and_with_numeric_values() {
        // Test the exact user scenario: AND filter with numeric values
        let json = r#"{
            "op": "and",
            "nodes": [
                {
                    "key": "category_id",
                    "cmp": "eq",
                    "val": 501
                },
                {
                    "key": "item_id",
                    "cmp": "eq",
                    "val": "item-abc-001"
                }
            ]
        }"#;
        let filter: FilterNode = sonic_rs::from_str(json).unwrap();

        // Validate structure
        assert_eq!(filter.op, Some("and".to_string()));
        assert_eq!(filter.nodes.len(), 2);

        // Validate first node (numeric value)
        assert_eq!(filter.nodes[0].key, Some("category_id".to_string()));
        assert_eq!(filter.nodes[0].cmp, Some("eq".to_string()));
        assert_eq!(filter.nodes[0].val, Some("501".to_string()));

        // Validate second node (string value)
        assert_eq!(filter.nodes[1].key, Some("item_id".to_string()));
        assert_eq!(filter.nodes[1].cmp, Some("eq".to_string()));
        assert_eq!(
            filter.nodes[1].val,
            Some("item-abc-001".to_string())
        );

        // Validate the filter
        assert_eq!(filter.validate(), None);
    }

    #[test]
    fn test_deserialize_mixed_types() {
        // Test that booleans and other types are also converted
        let json = r#"{"key":"active","cmp":"eq","val":true}"#;
        let filter: FilterNode = sonic_rs::from_str(json).unwrap();
        assert_eq!(filter.val, Some("true".to_string()));
    }
}
