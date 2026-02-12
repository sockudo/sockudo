pub mod index;
pub mod node;
mod ops;

pub use node::FilterNode;
pub use ops::{CompareOp, LogicalOp};

use ahash::AHashMap as HashMap;
use memchr::memmem;
use std::collections::BTreeMap; // SIMD-accelerated substring search

/// Trait for map types that can be used with filter matching
/// Supports both HashMap and BTreeMap
pub trait TagMap {
    fn get_tag(&self, key: &str) -> Option<&String>;
    fn contains_tag(&self, key: &str) -> bool;
}

impl TagMap for HashMap<String, String> {
    #[inline]
    fn get_tag(&self, key: &str) -> Option<&String> {
        self.get(key)
    }

    #[inline]
    fn contains_tag(&self, key: &str) -> bool {
        self.contains_key(key)
    }
}

impl TagMap for BTreeMap<String, String> {
    #[inline]
    fn get_tag(&self, key: &str) -> Option<&String> {
        self.get(key)
    }

    #[inline]
    fn contains_tag(&self, key: &str) -> bool {
        self.contains_key(key)
    }
}

/// Matches a filter node against publication tags with zero allocations.
///
/// This function is designed to be called in the hot path during message broadcasting,
/// so it must not allocate any memory during evaluation.
///
/// # Arguments
/// * `filter` - The filter node to evaluate
/// * `tags` - Publication tags to match against (supports HashMap or BTreeMap)
///
/// # Returns
/// `true` if the filter matches, `false` otherwise
#[inline]
pub fn matches<T: TagMap>(filter: &FilterNode, tags: &T) -> bool {
    match filter.logical_op() {
        Some(LogicalOp::And) => {
            // Early exit on first false
            for child in filter.nodes() {
                if !matches(child, tags) {
                    return false;
                }
            }
            true
        }
        Some(LogicalOp::Or) => {
            // Early exit on first true
            for child in filter.nodes() {
                if matches(child, tags) {
                    return true;
                }
            }
            false
        }
        Some(LogicalOp::Not) => {
            // NOT should have exactly one child
            if let Some(child) = filter.nodes().first() {
                !matches(child, tags)
            } else {
                false
            }
        }
        None => {
            // Leaf node - perform comparison
            evaluate_comparison(filter, tags)
        }
    }
}

/// Evaluates a comparison operation (leaf node) with zero allocations.
#[inline]
fn evaluate_comparison<T: TagMap>(filter: &FilterNode, tags: &T) -> bool {
    let key = filter.key();
    let tag_value = tags.get_tag(key);

    match filter.compare_op() {
        CompareOp::Equal => {
            if let Some(val) = tag_value {
                val == filter.val()
            } else {
                false
            }
        }
        CompareOp::NotEqual => {
            if let Some(val) = tag_value {
                val != filter.val()
            } else {
                true
            }
        }
        CompareOp::In => {
            if let Some(val) = tag_value {
                // Use binary search for O(log n) lookup if sorted (large value sets)
                // Otherwise fall back to linear search for small sets
                if filter.is_sorted() {
                    filter.vals().binary_search(val).is_ok()
                } else {
                    filter.vals().iter().any(|v| v == val)
                }
            } else {
                false
            }
        }
        CompareOp::NotIn => {
            if let Some(val) = tag_value {
                // Use binary search for O(log n) lookup if sorted (large value sets)
                if filter.is_sorted() {
                    filter.vals().binary_search(val).is_err()
                } else {
                    !filter.vals().iter().any(|v| v == val)
                }
            } else {
                true
            }
        }
        CompareOp::Exists => tag_value.is_some(),
        CompareOp::NotExists => tag_value.is_none(),
        CompareOp::StartsWith => {
            if let Some(val) = tag_value {
                // Fast path: Use built-in for short strings (CPU cache friendly)
                // SIMD path: Use memchr for longer strings (>16 bytes)
                let filter_val = filter.val();
                if filter_val.len() <= 16 {
                    val.starts_with(filter_val)
                } else {
                    val.as_bytes().starts_with(filter_val.as_bytes())
                }
            } else {
                false
            }
        }
        CompareOp::EndsWith => {
            if let Some(val) = tag_value {
                // Fast path: Use built-in for short strings
                let filter_val = filter.val();
                if filter_val.len() <= 16 {
                    val.ends_with(filter_val)
                } else {
                    val.as_bytes().ends_with(filter_val.as_bytes())
                }
            } else {
                false
            }
        }
        CompareOp::Contains => {
            if let Some(val) = tag_value {
                // SIMD-optimized substring search using memchr (up to 10x faster)
                let filter_val = filter.val();
                if filter_val.is_empty() {
                    true
                } else if filter_val.len() == 1 {
                    // Single byte search (fastest SIMD path)
                    memchr::memchr(filter_val.as_bytes()[0], val.as_bytes()).is_some()
                } else {
                    // Multi-byte substring search with SIMD
                    memmem::find(val.as_bytes(), filter_val.as_bytes()).is_some()
                }
            } else {
                false
            }
        }
        CompareOp::GreaterThan => compare_numeric(tag_value, filter.val(), |a, b| a > b),
        CompareOp::GreaterThanOrEqual => compare_numeric(tag_value, filter.val(), |a, b| a >= b),
        CompareOp::LessThan => compare_numeric(tag_value, filter.val(), |a, b| a < b),
        CompareOp::LessThanOrEqual => compare_numeric(tag_value, filter.val(), |a, b| a <= b),
    }
}

/// Compares two string values as numbers with zero allocations.
/// Uses a simple decimal comparison that handles both integers and decimals.
#[inline]
fn compare_numeric<F>(tag_value: Option<&String>, filter_val: &str, cmp: F) -> bool
where
    F: Fn(f64, f64) -> bool,
{
    if let Some(val) = tag_value {
        // Fast path: try to parse both as f64
        if let (Ok(a), Ok(b)) = (val.parse::<f64>(), filter_val.parse::<f64>()) {
            cmp(a, b)
        } else {
            false
        }
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use crate::filter::node::FilterNodeBuilder;

    use super::*;

    fn tags(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn test_equal() {
        let filter = FilterNodeBuilder::eq("event_type", "goal");
        let matching_tags = tags(&[("event_type", "goal")]);
        let non_matching_tags = tags(&[("event_type", "shot")]);

        assert!(matches(&filter, &matching_tags));
        assert!(!matches(&filter, &non_matching_tags));
    }

    #[test]
    fn test_not_equal() {
        let filter = FilterNodeBuilder::neq("event_type", "goal");
        let matching_tags = tags(&[("event_type", "shot")]);
        let non_matching_tags = tags(&[("event_type", "goal")]);

        assert!(matches(&filter, &matching_tags));
        assert!(!matches(&filter, &non_matching_tags));
    }

    #[test]
    fn test_in() {
        let filter = FilterNodeBuilder::in_set("event_type", &["goal", "shot"]);
        let matching_goal = tags(&[("event_type", "goal")]);
        let matching_shot = tags(&[("event_type", "shot")]);
        let non_matching = tags(&[("event_type", "pass")]);

        assert!(matches(&filter, &matching_goal));
        assert!(matches(&filter, &matching_shot));
        assert!(!matches(&filter, &non_matching));
    }

    #[test]
    fn test_not_in() {
        let filter = FilterNodeBuilder::nin("event_type", &["goal", "shot"]);
        let matching = tags(&[("event_type", "pass")]);
        let non_matching = tags(&[("event_type", "goal")]);

        assert!(matches(&filter, &matching));
        assert!(!matches(&filter, &non_matching));
    }

    #[test]
    fn test_exists() {
        let filter = FilterNodeBuilder::exists("event_type");
        let matching = tags(&[("event_type", "goal")]);
        let non_matching = tags(&[("other_key", "value")]);

        assert!(matches(&filter, &matching));
        assert!(!matches(&filter, &non_matching));
    }

    #[test]
    fn test_not_exists() {
        let filter = FilterNodeBuilder::not_exists("event_type");
        let matching = tags(&[("other_key", "value")]);
        let non_matching = tags(&[("event_type", "goal")]);

        assert!(matches(&filter, &matching));
        assert!(!matches(&filter, &non_matching));
    }

    #[test]
    fn test_starts_with() {
        let filter = FilterNodeBuilder::starts_with("ticker", "GOO");
        let matching = tags(&[("ticker", "GOOGLE")]);
        let non_matching = tags(&[("ticker", "AAPL")]);

        assert!(matches(&filter, &matching));
        assert!(!matches(&filter, &non_matching));
    }

    #[test]
    fn test_ends_with() {
        let filter = FilterNodeBuilder::ends_with("ticker", "LE");
        let matching = tags(&[("ticker", "GOOGLE")]);
        let non_matching = tags(&[("ticker", "AAPL")]);

        assert!(matches(&filter, &matching));
        assert!(!matches(&filter, &non_matching));
    }

    #[test]
    fn test_contains() {
        let filter = FilterNodeBuilder::contains("ticker", "OOG");
        let matching = tags(&[("ticker", "GOOGLE")]);
        let non_matching = tags(&[("ticker", "AAPL")]);

        assert!(matches(&filter, &matching));
        assert!(!matches(&filter, &non_matching));
    }

    #[test]
    fn test_greater_than() {
        let filter = FilterNodeBuilder::gt("xG", "0.5");
        let matching = tags(&[("xG", "0.85")]);
        let non_matching = tags(&[("xG", "0.3")]);
        let equal = tags(&[("xG", "0.5")]);

        assert!(matches(&filter, &matching));
        assert!(!matches(&filter, &non_matching));
        assert!(!matches(&filter, &equal));
    }

    #[test]
    fn test_greater_than_or_equal() {
        let filter = FilterNodeBuilder::gte("xG", "0.5");
        let matching_greater = tags(&[("xG", "0.85")]);
        let matching_equal = tags(&[("xG", "0.5")]);
        let non_matching = tags(&[("xG", "0.3")]);

        assert!(matches(&filter, &matching_greater));
        assert!(matches(&filter, &matching_equal));
        assert!(!matches(&filter, &non_matching));
    }

    #[test]
    fn test_less_than() {
        let filter = FilterNodeBuilder::lt("xG", "0.5");
        let matching = tags(&[("xG", "0.3")]);
        let non_matching = tags(&[("xG", "0.85")]);
        let equal = tags(&[("xG", "0.5")]);

        assert!(matches(&filter, &matching));
        assert!(!matches(&filter, &non_matching));
        assert!(!matches(&filter, &equal));
    }

    #[test]
    fn test_less_than_or_equal() {
        let filter = FilterNodeBuilder::lte("xG", "0.5");
        let matching_less = tags(&[("xG", "0.3")]);
        let matching_equal = tags(&[("xG", "0.5")]);
        let non_matching = tags(&[("xG", "0.85")]);

        assert!(matches(&filter, &matching_less));
        assert!(matches(&filter, &matching_equal));
        assert!(!matches(&filter, &non_matching));
    }

    #[test]
    fn test_and() {
        let filter = FilterNodeBuilder::and(vec![
            FilterNodeBuilder::eq("event_type", "shot"),
            FilterNodeBuilder::gte("xG", "0.8"),
        ]);

        let matching = tags(&[("event_type", "shot"), ("xG", "0.85")]);
        let non_matching_type = tags(&[("event_type", "pass"), ("xG", "0.85")]);
        let non_matching_value = tags(&[("event_type", "shot"), ("xG", "0.3")]);

        assert!(matches(&filter, &matching));
        assert!(!matches(&filter, &non_matching_type));
        assert!(!matches(&filter, &non_matching_value));
    }

    #[test]
    fn test_or() {
        let filter = FilterNodeBuilder::or(vec![
            FilterNodeBuilder::eq("event_type", "goal"),
            FilterNodeBuilder::and(vec![
                FilterNodeBuilder::eq("event_type", "shot"),
                FilterNodeBuilder::gte("xG", "0.8"),
            ]),
        ]);

        let matching_goal = tags(&[("event_type", "goal")]);
        let matching_shot = tags(&[("event_type", "shot"), ("xG", "0.85")]);
        let non_matching = tags(&[("event_type", "shot"), ("xG", "0.3")]);

        assert!(matches(&filter, &matching_goal));
        assert!(matches(&filter, &matching_shot));
        assert!(!matches(&filter, &non_matching));
    }

    #[test]
    fn test_not() {
        let filter = FilterNodeBuilder::not(FilterNodeBuilder::eq("event_type", "goal"));

        let matching = tags(&[("event_type", "shot")]);
        let non_matching = tags(&[("event_type", "goal")]);

        assert!(matches(&filter, &matching));
        assert!(!matches(&filter, &non_matching));
    }

    #[test]
    fn test_complex_nested() {
        // (event_type == "goal") OR ((event_type == "shot") AND (xG >= "0.8") AND (outcome != "saved"))
        let filter = FilterNodeBuilder::or(vec![
            FilterNodeBuilder::eq("event_type", "goal"),
            FilterNodeBuilder::and(vec![
                FilterNodeBuilder::eq("event_type", "shot"),
                FilterNodeBuilder::gte("xG", "0.8"),
                FilterNodeBuilder::neq("outcome", "saved"),
            ]),
        ]);

        let matching_goal = tags(&[("event_type", "goal")]);
        let matching_dangerous_shot =
            tags(&[("event_type", "shot"), ("xG", "0.85"), ("outcome", "goal")]);
        let non_matching_saved_shot =
            tags(&[("event_type", "shot"), ("xG", "0.85"), ("outcome", "saved")]);
        let non_matching_low_xg = tags(&[("event_type", "shot"), ("xG", "0.3")]);

        assert!(matches(&filter, &matching_goal));
        assert!(matches(&filter, &matching_dangerous_shot));
        assert!(!matches(&filter, &non_matching_saved_shot));
        assert!(!matches(&filter, &non_matching_low_xg));
    }

    #[test]
    fn test_numeric_comparison_integers() {
        let filter = FilterNodeBuilder::gt("count", "42");
        let matching = tags(&[("count", "100")]);
        let non_matching = tags(&[("count", "10")]);

        assert!(matches(&filter, &matching));
        assert!(!matches(&filter, &non_matching));
    }

    #[test]
    fn test_numeric_comparison_decimals() {
        let filter = FilterNodeBuilder::gte("price", "99.5");
        let matching_greater = tags(&[("price", "150.25")]);
        let matching_equal = tags(&[("price", "99.5")]);
        let non_matching = tags(&[("price", "50.0")]);

        assert!(matches(&filter, &matching_greater));
        assert!(matches(&filter, &matching_equal));
        assert!(!matches(&filter, &non_matching));
    }

    #[test]
    fn test_missing_key_in_comparison() {
        let filter = FilterNodeBuilder::eq("event_type", "goal");
        let empty_tags = tags(&[]);

        assert!(!matches(&filter, &empty_tags));
    }

    #[test]
    fn test_missing_key_in_numeric_comparison() {
        let filter = FilterNodeBuilder::gt("count", "42");
        let empty_tags = tags(&[]);

        assert!(!matches(&filter, &empty_tags));
    }

    #[test]
    fn test_and_filter_with_numeric_deserialization() {
        // Test AND filter with numeric values in JSON
        // Verifies that numeric values are properly converted to strings during deserialization
        let json = r#"{
            "op": "and",
            "nodes": [
                {
                    "key": "user_id",
                    "cmp": "eq",
                    "val": 12345
                },
                {
                    "key": "status",
                    "cmp": "eq",
                    "val": "premium"
                }
            ]
        }"#;

        let filter: FilterNode = sonic_rs::from_str(json).unwrap();

        // Tags that should match both conditions
        let matching_tags = tags(&[("user_id", "12345"), ("status", "premium")]);

        // Tags that don't match (wrong user_id)
        let non_matching_user = tags(&[("user_id", "99999"), ("status", "premium")]);

        // Tags that don't match (wrong status)
        let non_matching_status = tags(&[("user_id", "12345"), ("status", "basic")]);

        assert!(
            matches(&filter, &matching_tags),
            "Should match when both conditions are met"
        );
        assert!(
            !matches(&filter, &non_matching_user),
            "Should not match with wrong user_id"
        );
        assert!(
            !matches(&filter, &non_matching_status),
            "Should not match with wrong status"
        );
    }

    #[test]
    fn test_numeric_value_conversion_verification() {
        // This test explicitly verifies the fix for numeric values in filters
        // Issue: Filters with "val": 123 (number) were failing because serde
        // doesn't auto-convert numbers to strings, leaving val as None
        // Fix: Custom deserializer converts numbers/bools to strings

        let json = r#"{"key":"count","cmp":"eq","val":100}"#;
        let filter: FilterNode = sonic_rs::from_str(json).unwrap();

        // Verify the numeric 100 was converted to string "100"
        assert_eq!(
            filter.val,
            Some("100".to_string()),
            "Numeric val should be converted to string"
        );

        // Verify it matches against string tag values
        let test_tags = tags(&[("count", "100")]);
        assert!(
            matches(&filter, &test_tags),
            "Filter with numeric val should match string tag"
        );

        // Test with float
        let json_float = r#"{"key":"temperature","cmp":"eq","val":98.6}"#;
        let filter_float: FilterNode = sonic_rs::from_str(json_float).unwrap();
        assert_eq!(filter_float.val, Some("98.6".to_string()));

        let temp_tags = tags(&[("temperature", "98.6")]);
        assert!(matches(&filter_float, &temp_tags));
    }
}
