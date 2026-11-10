//! Server-side tag filters (Protocol V2).
//!
//! Filters are evaluated by the server against publication tags so only
//! matching messages are delivered to a subscription.
//!
//! ```
//! use sockudo_client::Filter;
//!
//! let filter = Filter::and([
//!     Filter::eq("market", "spot"),
//!     Filter::gt("spread", "0"),
//! ]);
//! assert!(filter.validate().is_ok());
//! ```

use serde::{Deserialize, Serialize};

/// A filter tree node. Leaf nodes compare a tag; branch nodes combine children.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct FilterNode {
    /// Logical operator: `and`, `or` or `not`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub op: Option<String>,
    /// Tag key (leaf).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// Comparison operator (leaf).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cmp: Option<String>,
    /// Comparison value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub val: Option<String>,
    /// Comparison values for `in` / `nin`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vals: Option<Vec<String>>,
    /// Child nodes (branch).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nodes: Option<Vec<FilterNode>>,
}

/// Filter constructors.
#[derive(Debug, Clone, Copy)]
pub struct Filter;

const COMPARISONS: [&str; 13] = [
    "eq", "neq", "in", "nin", "ex", "nex", "sw", "ew", "ct", "gt", "gte", "lt", "lte",
];

fn leaf(key: impl Into<String>, cmp: &str, val: Option<String>) -> FilterNode {
    FilterNode {
        key: Some(key.into()),
        cmp: Some(cmp.to_owned()),
        val,
        ..FilterNode::default()
    }
}

fn branch(op: &str, nodes: Vec<FilterNode>) -> FilterNode {
    FilterNode {
        op: Some(op.to_owned()),
        nodes: Some(nodes),
        ..FilterNode::default()
    }
}

impl Filter {
    /// `key == value`
    pub fn eq(key: impl Into<String>, value: impl Into<String>) -> FilterNode {
        leaf(key, "eq", Some(value.into()))
    }
    /// `key != value`
    pub fn neq(key: impl Into<String>, value: impl Into<String>) -> FilterNode {
        leaf(key, "neq", Some(value.into()))
    }
    /// `key` is one of `values`.
    pub fn in_set<I, S>(key: impl Into<String>, values: I) -> FilterNode
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        FilterNode {
            vals: Some(values.into_iter().map(Into::into).collect()),
            ..leaf(key, "in", None)
        }
    }
    /// `key` is none of `values`.
    pub fn not_in<I, S>(key: impl Into<String>, values: I) -> FilterNode
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        FilterNode {
            vals: Some(values.into_iter().map(Into::into).collect()),
            ..leaf(key, "nin", None)
        }
    }
    /// `key` is present.
    pub fn exists(key: impl Into<String>) -> FilterNode {
        leaf(key, "ex", None)
    }
    /// `key` is absent.
    pub fn not_exists(key: impl Into<String>) -> FilterNode {
        leaf(key, "nex", None)
    }
    /// `key` starts with `value`.
    pub fn starts_with(key: impl Into<String>, value: impl Into<String>) -> FilterNode {
        leaf(key, "sw", Some(value.into()))
    }
    /// `key` ends with `value`.
    pub fn ends_with(key: impl Into<String>, value: impl Into<String>) -> FilterNode {
        leaf(key, "ew", Some(value.into()))
    }
    /// `key` contains `value`.
    pub fn contains(key: impl Into<String>, value: impl Into<String>) -> FilterNode {
        leaf(key, "ct", Some(value.into()))
    }
    /// `key > value` (numeric comparison on the server).
    pub fn gt(key: impl Into<String>, value: impl Into<String>) -> FilterNode {
        leaf(key, "gt", Some(value.into()))
    }
    /// `key >= value`
    pub fn gte(key: impl Into<String>, value: impl Into<String>) -> FilterNode {
        leaf(key, "gte", Some(value.into()))
    }
    /// `key < value`
    pub fn lt(key: impl Into<String>, value: impl Into<String>) -> FilterNode {
        leaf(key, "lt", Some(value.into()))
    }
    /// `key <= value`
    pub fn lte(key: impl Into<String>, value: impl Into<String>) -> FilterNode {
        leaf(key, "lte", Some(value.into()))
    }
    /// All children match.
    pub fn and(nodes: impl IntoIterator<Item = FilterNode>) -> FilterNode {
        branch("and", nodes.into_iter().collect())
    }
    /// Any child matches.
    pub fn or(nodes: impl IntoIterator<Item = FilterNode>) -> FilterNode {
        branch("or", nodes.into_iter().collect())
    }
    /// The child does not match.
    #[allow(clippy::should_implement_trait)]
    pub fn not(node: FilterNode) -> FilterNode {
        branch("not", vec![node])
    }
}

impl FilterNode {
    /// Validate the tree with the same rules the server applies.
    pub fn validate(&self) -> Result<(), String> {
        if let Some(op) = self.op.as_deref() {
            if !matches!(op, "and" | "or" | "not") {
                return Err(format!("Invalid logical operator: {op}"));
            }
            let Some(nodes) = self.nodes.as_ref() else {
                return Err(format!("Logical operation '{op}' requires nodes array"));
            };
            if op == "not" && nodes.len() != 1 {
                return Err(format!(
                    "NOT operation requires exactly one child node, got {}",
                    nodes.len()
                ));
            }
            if nodes.is_empty() {
                return Err(format!(
                    "{} operation requires at least one child node",
                    op.to_ascii_uppercase()
                ));
            }
            for (index, child) in nodes.iter().enumerate() {
                child
                    .validate()
                    .map_err(|error| format!("Child node {index}: {error}"))?;
            }
            return Ok(());
        }
        if self.key.as_deref().is_none_or(str::is_empty) {
            return Err("Leaf node requires a key".to_owned());
        }
        let Some(cmp) = self.cmp.as_deref() else {
            return Err("Leaf node requires a comparison operator".to_owned());
        };
        if !COMPARISONS.contains(&cmp) {
            return Err(format!("Invalid comparison operator: {cmp}"));
        }
        match cmp {
            "in" | "nin" => {
                if self.vals.as_ref().is_none_or(Vec::is_empty) {
                    return Err(format!("{cmp} operation requires non-empty vals array"));
                }
            }
            "ex" | "nex" => {}
            _ => {
                if self.val.as_deref().is_none_or(str::is_empty) {
                    return Err(format!("{cmp} operation requires a val"));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_without_empty_fields() {
        let filter = Filter::and([Filter::eq("a", "1"), Filter::in_set("b", ["x", "y"])]);
        let json = sonic_rs::to_string(&filter).unwrap();
        assert_eq!(
            json,
            r#"{"op":"and","nodes":[{"key":"a","cmp":"eq","val":"1"},{"key":"b","cmp":"in","vals":["x","y"]}]}"#
        );
    }

    #[test]
    fn validates_like_the_server() {
        assert!(Filter::eq("a", "b").validate().is_ok());
        assert!(Filter::exists("a").validate().is_ok());
        assert!(Filter::not(Filter::exists("a")).validate().is_ok());
        assert_eq!(
            Filter::eq("a", "").validate().unwrap_err(),
            "eq operation requires a val"
        );
        assert!(
            Filter::in_set("a", Vec::<String>::new())
                .validate()
                .is_err()
        );
        assert!(Filter::and([]).validate().is_err());
        let bad_not = FilterNode {
            op: Some("not".into()),
            nodes: Some(vec![Filter::exists("a"), Filter::exists("b")]),
            ..FilterNode::default()
        };
        assert!(bad_not.validate().unwrap_err().contains("exactly one"));
        let nested = Filter::or([Filter::eq("a", "1"), FilterNode::default()]);
        assert!(nested.validate().unwrap_err().starts_with("Child node 1"));
        let bad_cmp = FilterNode {
            key: Some("a".into()),
            cmp: Some("regex".into()),
            ..FilterNode::default()
        };
        assert!(bad_cmp.validate().is_err());
    }
}
