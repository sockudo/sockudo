//! Bounded, compile-once subscription predicates.

use std::{
    hash::{Hash, Hasher},
    io::{self, Write},
    sync::Arc,
};

use jmespath::ast::Ast;
use jmespath::{Rcvar, ToJmespath};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{CompareOp, FilterNode, LogicalOp, TagMap};

/// Default resource limits applied while compiling an untrusted subscription view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PredicateLimits {
    pub max_events: usize,
    pub max_event_bytes: usize,
    pub max_tag_nodes: usize,
    pub max_tag_depth: usize,
    pub max_tag_key_bytes: usize,
    pub max_tag_value_bytes: usize,
    pub max_tag_values_per_node: usize,
    pub max_expression_bytes: usize,
    pub max_expression_nodes: usize,
    pub max_expression_depth: usize,
}

impl Default for PredicateLimits {
    fn default() -> Self {
        Self {
            max_events: 64,
            max_event_bytes: 256,
            max_tag_nodes: 128,
            max_tag_depth: 16,
            max_tag_key_bytes: 256,
            max_tag_value_bytes: 1_024,
            max_tag_values_per_node: 128,
            max_expression_bytes: 4_096,
            max_expression_nodes: 256,
            max_expression_depth: 32,
        }
    }
}

/// Protocol-neutral, serializable input accepted at a subscription boundary.
///
/// Non-empty components are combined with logical AND.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SubscriptionView {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<FilterNode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expression: Option<String>,
}

/// Error returned when an untrusted subscription view cannot be compiled safely.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum PredicateCompileError {
    #[error("event list exceeds the limit of {limit}")]
    TooManyEvents { limit: usize },
    #[error("event at index {index} is empty")]
    EmptyEvent { index: usize },
    #[error("event at index {index} exceeds the byte limit of {limit}")]
    EventTooLong { index: usize, limit: usize },
    #[error("tag filter exceeds the node limit of {limit}")]
    TooManyTagNodes { limit: usize },
    #[error("tag filter exceeds the depth limit of {limit}")]
    TagTreeTooDeep { limit: usize },
    #[error("tag filter key exceeds the byte limit of {limit}")]
    TagKeyTooLong { limit: usize },
    #[error("tag filter value exceeds the byte limit of {limit}")]
    TagValueTooLong { limit: usize },
    #[error("tag filter value list exceeds the limit of {limit}")]
    TooManyTagValues { limit: usize },
    #[error("invalid tag filter: {0}")]
    InvalidTagFilter(String),
    #[error("JMESPath expression is empty")]
    EmptyExpression,
    #[error("JMESPath expression exceeds the byte limit of {limit}")]
    ExpressionTooLong { limit: usize },
    #[error("invalid JMESPath expression: {0}")]
    InvalidExpression(String),
    #[error("JMESPath expression exceeds the AST node limit of {limit}")]
    TooManyExpressionNodes { limit: usize },
    #[error("JMESPath expression exceeds the AST depth limit of {limit}")]
    ExpressionTooDeep { limit: usize },
}

/// Error returned when evaluating a previously compiled expression.
#[derive(Debug, Error)]
#[error("JMESPath evaluation failed: {message}")]
pub struct PredicateEvaluationError {
    message: String,
}

/// Stable, non-secret identity of a canonical compiled predicate.
///
/// This identifier is intended for bounded caches, grouping and diagnostics, not authorization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PredicateId(u64);

impl PredicateId {
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

struct PredicateIdHasher(u64);

impl Default for PredicateIdHasher {
    fn default() -> Self {
        // FNV-1a is deliberately stable and fast. Collision resistance is not a security boundary.
        Self(0xcbf2_9ce4_8422_2325)
    }
}

impl Hasher for PredicateIdHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
}

/// A message projection converted once to JMESPath's immutable value representation.
#[derive(Debug, Clone)]
pub struct ProjectedDocument {
    value: Rcvar,
}

impl ProjectedDocument {
    /// Projects a protocol-neutral serializable document once for reuse by many predicates.
    ///
    /// # Errors
    ///
    /// Returns an error when the input contains a value JMESPath cannot represent.
    pub fn new<D: Serialize>(document: &D) -> Result<Self, PredicateEvaluationError> {
        document
            .to_jmespath()
            .map(|value| Self { value })
            .map_err(PredicateEvaluationError::from_jmespath)
    }

    /// Projects a document through a hard-bounded JSON buffer.
    ///
    /// This is the preferred entry point for untrusted or independently configurable native
    /// payloads. Serialization stops before the buffer can grow past `max_bytes`.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization exceeds `max_bytes`, serialization fails, or JMESPath
    /// cannot represent the resulting JSON value.
    pub fn new_bounded<D: Serialize>(
        document: &D,
        max_bytes: usize,
    ) -> Result<Self, PredicateEvaluationError> {
        let mut writer = BoundedWriter::new(max_bytes);
        let serialization = {
            let mut serializer = serde_json::Serializer::new(&mut writer);
            document.serialize(&mut serializer)
        };
        if writer.exceeded {
            return Err(PredicateEvaluationError {
                message: format!("projected document exceeds the byte limit of {max_bytes}"),
            });
        }
        serialization.map_err(|error| PredicateEvaluationError {
            message: format!("document projection failed: {error}"),
        })?;
        let json =
            std::str::from_utf8(&writer.bytes).map_err(|error| PredicateEvaluationError {
                message: format!("document projection produced invalid UTF-8: {error}"),
            })?;
        jmespath::Variable::from_json(json)
            .map(Arc::new)
            .map(|value| Self { value })
            .map_err(|error| PredicateEvaluationError {
                message: format!("document projection is invalid JSON: {error}"),
            })
    }
}

struct BoundedWriter {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}

impl BoundedWriter {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(limit.min(4_096)),
            limit,
            exceeded: false,
        }
    }
}

impl Write for BoundedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let Some(end) = self.bytes.len().checked_add(bytes.len()) else {
            self.exceeded = true;
            return Err(io::Error::other("projection byte limit exceeded"));
        };
        if end > self.limit {
            self.exceeded = true;
            return Err(io::Error::other("projection byte limit exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl ToJmespath for &ProjectedDocument {
    fn to_jmespath(self) -> Result<Rcvar, jmespath::JmespathError> {
        Ok(Arc::clone(&self.value))
    }
}

impl PredicateEvaluationError {
    fn from_jmespath(error: jmespath::JmespathError) -> Self {
        Self {
            message: error.to_string(),
        }
    }

    fn missing_document() -> Self {
        Self {
            message: "predicate requires a projected document".to_string(),
        }
    }
}

#[derive(Debug, Clone, Hash)]
enum CompiledTagNode {
    Logical {
        op: LogicalOp,
        nodes: Box<[CompiledTagNode]>,
    },
    Comparison {
        key: Box<str>,
        op: CompareOp,
        val: Option<Box<str>>,
        vals: Box<[String]>,
    },
}

/// An immutable, compile-once predicate suitable for sharing across subscriptions.
#[derive(Clone)]
pub struct MessagePredicate {
    events: Arc<[String]>,
    tags: Option<Arc<CompiledTagNode>>,
    expression: Option<Arc<jmespath::Expression<'static>>>,
    id: PredicateId,
}

impl std::fmt::Debug for MessagePredicate {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MessagePredicate")
            .field("events", &self.events)
            .field("tags", &self.tags)
            .field(
                "expression",
                &self.expression.as_ref().map(|value| value.as_str()),
            )
            .finish()
    }
}

impl MessagePredicate {
    /// Compiles a view using [`PredicateLimits::default`].
    ///
    /// # Errors
    ///
    /// Returns an error for malformed or over-limit input.
    pub fn compile(view: SubscriptionView) -> Result<Self, PredicateCompileError> {
        Self::compile_with_limits(view, PredicateLimits::default())
    }

    /// Compiles and bounds all parts of a subscription view.
    ///
    /// JMESPath is parsed exactly once. The resulting predicate is immutable and cheap to clone.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed or over-limit input.
    pub fn compile_with_limits(
        mut view: SubscriptionView,
        limits: PredicateLimits,
    ) -> Result<Self, PredicateCompileError> {
        validate_events(&view.events, limits)?;
        view.events.sort_unstable();
        view.events.dedup();

        let tags = if let Some(tags) = view.tags {
            validate_tags(&tags, limits)?;
            Some(Arc::new(compile_tags(tags)?))
        } else {
            None
        };

        let expression = view
            .expression
            .map(|source| compile_expression(&source, limits).map(Arc::new))
            .transpose()?;

        let mut hasher = PredicateIdHasher::default();
        view.events.hash(&mut hasher);
        tags.hash(&mut hasher);
        expression
            .as_ref()
            .map(|value| value.as_str())
            .hash(&mut hasher);
        let id = PredicateId(hasher.finish());

        Ok(Self {
            events: view.events.into(),
            tags,
            expression,
            id,
        })
    }

    /// Returns whether this predicate accepts a projected message.
    ///
    /// Event and tag checks run before JMESPath to avoid serializing the document when a cheaper
    /// condition already rejects it. Evaluation errors are returned so callers can fail closed.
    ///
    /// # Errors
    ///
    /// Returns an error if the document cannot be projected to JMESPath or evaluation fails.
    #[inline]
    pub fn matches<T, D>(
        &self,
        event: Option<&str>,
        tags: &T,
        document: &D,
    ) -> Result<bool, PredicateEvaluationError>
    where
        T: TagMap,
        D: Serialize,
    {
        if !self.matches_cheap(event, tags) {
            return Ok(false);
        }
        let Some(expression) = &self.expression else {
            return Ok(true);
        };
        let projected = ProjectedDocument::new(document)?;
        expression
            .search(&projected)
            .map(|result| result.is_truthy())
            .map_err(PredicateEvaluationError::from_jmespath)
    }

    /// Evaluates against a document projected once for reuse across predicates.
    ///
    /// Pass `None` without allocation when [`Self::requires_document`] is false.
    /// Missing required input is an error, allowing callers to fail closed.
    ///
    /// # Errors
    ///
    /// Returns an error if a JMESPath predicate needs a document or evaluation fails.
    #[inline]
    pub fn matches_projected<T: TagMap>(
        &self,
        event: Option<&str>,
        tags: &T,
        document: Option<&ProjectedDocument>,
    ) -> Result<bool, PredicateEvaluationError> {
        if !self.matches_cheap(event, tags) {
            return Ok(false);
        }
        let Some(expression) = &self.expression else {
            return Ok(true);
        };
        let document = document.ok_or_else(PredicateEvaluationError::missing_document)?;
        expression
            .search(document)
            .map(|result| result.is_truthy())
            .map_err(PredicateEvaluationError::from_jmespath)
    }

    #[inline]
    fn matches_cheap<T: TagMap>(&self, event: Option<&str>, tags: &T) -> bool {
        if !self.events.is_empty()
            && event.is_none_or(|event| {
                self.events
                    .binary_search_by(|candidate| candidate.as_str().cmp(event))
                    .is_err()
            })
        {
            return false;
        }
        !self
            .tags
            .as_ref()
            .is_some_and(|filter| !filter.matches(tags))
    }

    #[must_use]
    pub fn is_match_all(&self) -> bool {
        self.events.is_empty() && self.tags.is_none() && self.expression.is_none()
    }

    #[must_use]
    pub fn requires_document(&self) -> bool {
        self.expression.is_some()
    }

    #[must_use]
    pub const fn id(&self) -> PredicateId {
        self.id
    }

    #[must_use]
    pub fn expression_source(&self) -> Option<&str> {
        self.expression.as_ref().map(|value| value.as_str())
    }
}

impl CompiledTagNode {
    fn matches<T: TagMap>(&self, tags: &T) -> bool {
        match self {
            Self::Logical { op, nodes } => match op {
                LogicalOp::And => nodes.iter().all(|node| node.matches(tags)),
                LogicalOp::Or => nodes.iter().any(|node| node.matches(tags)),
                LogicalOp::Not => nodes.first().is_some_and(|node| !node.matches(tags)),
            },
            Self::Comparison { key, op, val, vals } => {
                let actual = tags.get_tag(key);
                let expected = val.as_deref().unwrap_or_default();
                match op {
                    CompareOp::Equal => actual.is_some_and(|value| value == expected),
                    CompareOp::NotEqual => actual.is_none_or(|value| value != expected),
                    CompareOp::In => actual.is_some_and(|value| vals.binary_search(value).is_ok()),
                    CompareOp::NotIn => {
                        actual.is_none_or(|value| vals.binary_search(value).is_err())
                    }
                    CompareOp::Exists => actual.is_some(),
                    CompareOp::NotExists => actual.is_none(),
                    CompareOp::StartsWith => {
                        actual.is_some_and(|value| value.starts_with(expected))
                    }
                    CompareOp::EndsWith => actual.is_some_and(|value| value.ends_with(expected)),
                    CompareOp::Contains => actual.is_some_and(|value| {
                        if expected.len() == 1 {
                            memchr::memchr(expected.as_bytes()[0], value.as_bytes()).is_some()
                        } else {
                            memchr::memmem::find(value.as_bytes(), expected.as_bytes()).is_some()
                        }
                    }),
                    CompareOp::GreaterThan => compare_number(actual, expected, |a, b| a > b),
                    CompareOp::GreaterThanOrEqual => {
                        compare_number(actual, expected, |a, b| a >= b)
                    }
                    CompareOp::LessThan => compare_number(actual, expected, |a, b| a < b),
                    CompareOp::LessThanOrEqual => compare_number(actual, expected, |a, b| a <= b),
                }
            }
        }
    }
}

fn compare_number<F>(actual: Option<&String>, expected: &str, compare: F) -> bool
where
    F: FnOnce(f64, f64) -> bool,
{
    actual
        .and_then(|value| value.parse::<f64>().ok())
        .zip(expected.parse::<f64>().ok())
        .is_some_and(|(actual, expected)| compare(actual, expected))
}

fn compile_tags(node: FilterNode) -> Result<CompiledTagNode, PredicateCompileError> {
    if let Some(op) = node.op {
        let op = LogicalOp::parse(&op).ok_or_else(|| {
            PredicateCompileError::InvalidTagFilter("unknown logical operator".into())
        })?;
        let nodes = node
            .nodes
            .into_iter()
            .map(compile_tags)
            .collect::<Result<Vec<_>, _>>()?
            .into_boxed_slice();
        return Ok(CompiledTagNode::Logical { op, nodes });
    }

    let key = node.key.ok_or_else(|| {
        PredicateCompileError::InvalidTagFilter("leaf node requires a key".into())
    })?;
    let op_source = node.cmp.ok_or_else(|| {
        PredicateCompileError::InvalidTagFilter("leaf node requires an operator".into())
    })?;
    let op = CompareOp::parse(&op_source).ok_or_else(|| {
        PredicateCompileError::InvalidTagFilter("unknown comparison operator".into())
    })?;
    let mut vals = node.vals;
    vals.sort_unstable();
    vals.dedup();
    Ok(CompiledTagNode::Comparison {
        key: key.into_boxed_str(),
        op,
        val: node.val.map(String::into_boxed_str),
        vals: vals.into_boxed_slice(),
    })
}

fn validate_events(
    events: &[String],
    limits: PredicateLimits,
) -> Result<(), PredicateCompileError> {
    if events.len() > limits.max_events {
        return Err(PredicateCompileError::TooManyEvents {
            limit: limits.max_events,
        });
    }
    for (index, event) in events.iter().enumerate() {
        if event.is_empty() {
            return Err(PredicateCompileError::EmptyEvent { index });
        }
        if event.len() > limits.max_event_bytes {
            return Err(PredicateCompileError::EventTooLong {
                index,
                limit: limits.max_event_bytes,
            });
        }
    }
    Ok(())
}

fn validate_tags(root: &FilterNode, limits: PredicateLimits) -> Result<(), PredicateCompileError> {
    let mut stack = Vec::with_capacity(limits.max_tag_depth.min(32));
    stack.push((root, 1_usize));
    let mut count = 0_usize;

    while let Some((node, depth)) = stack.pop() {
        count = count.saturating_add(1);
        if count > limits.max_tag_nodes {
            return Err(PredicateCompileError::TooManyTagNodes {
                limit: limits.max_tag_nodes,
            });
        }
        if depth > limits.max_tag_depth {
            return Err(PredicateCompileError::TagTreeTooDeep {
                limit: limits.max_tag_depth,
            });
        }
        if node
            .key
            .as_ref()
            .is_some_and(|key| key.len() > limits.max_tag_key_bytes)
        {
            return Err(PredicateCompileError::TagKeyTooLong {
                limit: limits.max_tag_key_bytes,
            });
        }
        if node
            .val
            .as_ref()
            .is_some_and(|value| value.len() > limits.max_tag_value_bytes)
            || node
                .vals
                .iter()
                .any(|value| value.len() > limits.max_tag_value_bytes)
        {
            return Err(PredicateCompileError::TagValueTooLong {
                limit: limits.max_tag_value_bytes,
            });
        }
        if node.vals.len() > limits.max_tag_values_per_node {
            return Err(PredicateCompileError::TooManyTagValues {
                limit: limits.max_tag_values_per_node,
            });
        }
        validate_tag_shape(node)?;
        stack.extend(
            node.nodes
                .iter()
                .map(|child| (child, depth.saturating_add(1))),
        );
    }

    root.validate().map_or(Ok(()), |error| {
        Err(PredicateCompileError::InvalidTagFilter(error))
    })
}

fn validate_tag_shape(node: &FilterNode) -> Result<(), PredicateCompileError> {
    if let Some(op) = &node.op {
        let logical = LogicalOp::parse(op).ok_or_else(|| {
            PredicateCompileError::InvalidTagFilter(format!("unknown logical operator: {op}"))
        })?;
        if node.key.is_some() || node.cmp.is_some() || node.val.is_some() || !node.vals.is_empty() {
            return Err(PredicateCompileError::InvalidTagFilter(
                "logical node contains comparison fields".to_string(),
            ));
        }
        let valid_arity = match logical {
            LogicalOp::And | LogicalOp::Or => !node.nodes.is_empty(),
            LogicalOp::Not => node.nodes.len() == 1,
        };
        if !valid_arity {
            return Err(PredicateCompileError::InvalidTagFilter(format!(
                "invalid child count for {op}"
            )));
        }
        return Ok(());
    }

    if !node.nodes.is_empty() {
        return Err(PredicateCompileError::InvalidTagFilter(
            "comparison node contains child nodes".to_string(),
        ));
    }
    let key = node.key.as_deref().ok_or_else(|| {
        PredicateCompileError::InvalidTagFilter("leaf node requires a key".to_string())
    })?;
    if key.is_empty() {
        return Err(PredicateCompileError::InvalidTagFilter(
            "leaf node requires a non-empty key".to_string(),
        ));
    }
    let op_source = node.cmp.as_deref().ok_or_else(|| {
        PredicateCompileError::InvalidTagFilter("leaf node requires an operator".to_string())
    })?;
    let op = CompareOp::parse(op_source).ok_or_else(|| {
        PredicateCompileError::InvalidTagFilter(format!("unknown comparison operator: {op_source}"))
    })?;
    match op {
        CompareOp::In | CompareOp::NotIn if node.vals.is_empty() => Err(
            PredicateCompileError::InvalidTagFilter("set comparison requires values".to_string()),
        ),
        CompareOp::Exists | CompareOp::NotExists => Ok(()),
        CompareOp::In | CompareOp::NotIn => Ok(()),
        _ if node.val.as_deref().is_none_or(str::is_empty) => Err(
            PredicateCompileError::InvalidTagFilter("comparison requires a value".to_string()),
        ),
        _ => Ok(()),
    }
}

fn compile_expression(
    source: &str,
    limits: PredicateLimits,
) -> Result<jmespath::Expression<'static>, PredicateCompileError> {
    if source.is_empty() {
        return Err(PredicateCompileError::EmptyExpression);
    }
    if source.len() > limits.max_expression_bytes {
        return Err(PredicateCompileError::ExpressionTooLong {
            limit: limits.max_expression_bytes,
        });
    }
    let expression = jmespath::compile(source)
        .map_err(|error| PredicateCompileError::InvalidExpression(error.to_string()))?;
    validate_expression_ast(expression.as_ast(), limits)?;
    Ok(expression)
}

fn validate_expression_ast(
    root: &Ast,
    limits: PredicateLimits,
) -> Result<(), PredicateCompileError> {
    let mut stack = Vec::with_capacity(limits.max_expression_depth.min(32));
    stack.push((root, 1_usize));
    let mut count = 0_usize;

    while let Some((node, depth)) = stack.pop() {
        count = count.saturating_add(1);
        if count > limits.max_expression_nodes {
            return Err(PredicateCompileError::TooManyExpressionNodes {
                limit: limits.max_expression_nodes,
            });
        }
        if depth > limits.max_expression_depth {
            return Err(PredicateCompileError::ExpressionTooDeep {
                limit: limits.max_expression_depth,
            });
        }
        let child_depth = depth.saturating_add(1);
        match node {
            Ast::Comparison { lhs, rhs, .. }
            | Ast::Projection { lhs, rhs, .. }
            | Ast::And { lhs, rhs, .. }
            | Ast::Or { lhs, rhs, .. }
            | Ast::Subexpr { lhs, rhs, .. } => {
                stack.push((lhs, child_depth));
                stack.push((rhs, child_depth));
            }
            Ast::Condition {
                predicate, then, ..
            } => {
                stack.push((predicate, child_depth));
                stack.push((then, child_depth));
            }
            Ast::Expref { ast, .. } => stack.push((ast, child_depth)),
            Ast::Flatten { node, .. } | Ast::Not { node, .. } | Ast::ObjectValues { node, .. } => {
                stack.push((node, child_depth))
            }
            Ast::Function { args, .. } => {
                stack.extend(args.iter().map(|child| (child, child_depth)));
            }
            Ast::MultiList { elements, .. } => {
                stack.extend(elements.iter().map(|child| (child, child_depth)));
            }
            Ast::MultiHash { elements, .. } => {
                stack.extend(elements.iter().map(|pair| (&pair.value, child_depth)));
            }
            Ast::Identity { .. }
            | Ast::Field { .. }
            | Ast::Index { .. }
            | Ast::Literal { .. }
            | Ast::Slice { .. } => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use proptest::prelude::*;
    use sonic_rs::json;

    use super::*;
    use crate::node::FilterNodeBuilder;

    #[test]
    fn combines_events_tags_and_expression() {
        let predicate = MessagePredicate::compile(SubscriptionView {
            events: vec!["updated".to_string()],
            tags: Some(FilterNodeBuilder::eq("tenant", "acme")),
            expression: Some("data.total > `100`".to_string()),
        })
        .expect("bounded predicate compiles");
        let tags = BTreeMap::from([("tenant".to_string(), "acme".to_string())]);

        assert!(
            predicate
                .matches(Some("updated"), &tags, &json!({"data": {"total": 101}}))
                .expect("document evaluates")
        );
        assert!(
            !predicate
                .matches(Some("created"), &tags, &json!({"data": {"total": 101}}))
                .expect("cheap event rejection does not evaluate JMESPath")
        );
        assert!(
            !predicate
                .matches(Some("updated"), &tags, &json!({"data": {"total": 99}}))
                .expect("document evaluates")
        );
    }

    #[test]
    fn rejects_unknown_operators_and_all_bounded_dimensions() {
        let malformed: FilterNode =
            sonic_rs::from_str(r#"{"key":"kind","cmp":"regex","val":".*"}"#)
                .expect("shape is valid before semantic compilation");
        assert!(matches!(
            MessagePredicate::compile(SubscriptionView {
                tags: Some(malformed),
                ..SubscriptionView::default()
            }),
            Err(PredicateCompileError::InvalidTagFilter(_))
        ));

        let limits = PredicateLimits {
            max_events: 1,
            max_event_bytes: 2,
            max_tag_nodes: 1,
            max_tag_depth: 1,
            max_tag_key_bytes: 2,
            max_tag_value_bytes: 2,
            max_tag_values_per_node: 1,
            max_expression_bytes: 2,
            max_expression_nodes: 1,
            max_expression_depth: 1,
        };
        assert!(matches!(
            MessagePredicate::compile_with_limits(
                SubscriptionView {
                    events: vec!["a".into(), "b".into()],
                    ..Default::default()
                },
                limits,
            ),
            Err(PredicateCompileError::TooManyEvents { .. })
        ));
        assert!(matches!(
            MessagePredicate::compile_with_limits(
                SubscriptionView {
                    expression: Some("foo".into()),
                    ..Default::default()
                },
                limits,
            ),
            Err(PredicateCompileError::ExpressionTooLong { .. })
        ));

        let cases = [
            (
                FilterNodeBuilder::and(vec![
                    FilterNodeBuilder::eq("a", "1"),
                    FilterNodeBuilder::eq("b", "2"),
                ]),
                PredicateLimits {
                    max_tag_nodes: 1,
                    ..PredicateLimits::default()
                },
                PredicateCompileError::TooManyTagNodes { limit: 1 },
            ),
            (
                FilterNodeBuilder::not(FilterNodeBuilder::eq("a", "1")),
                PredicateLimits {
                    max_tag_depth: 1,
                    ..PredicateLimits::default()
                },
                PredicateCompileError::TagTreeTooDeep { limit: 1 },
            ),
            (
                FilterNodeBuilder::eq("long", "1"),
                PredicateLimits {
                    max_tag_key_bytes: 2,
                    ..PredicateLimits::default()
                },
                PredicateCompileError::TagKeyTooLong { limit: 2 },
            ),
            (
                FilterNodeBuilder::eq("a", "long"),
                PredicateLimits {
                    max_tag_value_bytes: 2,
                    ..PredicateLimits::default()
                },
                PredicateCompileError::TagValueTooLong { limit: 2 },
            ),
            (
                FilterNodeBuilder::in_set("a", &["1", "2"]),
                PredicateLimits {
                    max_tag_values_per_node: 1,
                    ..PredicateLimits::default()
                },
                PredicateCompileError::TooManyTagValues { limit: 1 },
            ),
        ];
        for (tags, case_limits, expected) in cases {
            let error = MessagePredicate::compile_with_limits(
                SubscriptionView {
                    tags: Some(tags),
                    ..SubscriptionView::default()
                },
                case_limits,
            )
            .expect_err("over-limit tag input must fail closed");
            assert_eq!(error, expected);
        }
    }

    #[test]
    fn rejects_ambiguous_tag_node_shapes() {
        let ambiguous: FilterNode = sonic_rs::from_str(
            r#"{"op":"and","key":"also-a-leaf","nodes":[{"key":"a","cmp":"eq","val":"1"}]}"#,
        )
        .expect("JSON shape deserializes");
        assert!(matches!(
            MessagePredicate::compile(SubscriptionView {
                tags: Some(ambiguous),
                ..SubscriptionView::default()
            }),
            Err(PredicateCompileError::InvalidTagFilter(_))
        ));
    }

    #[test]
    fn expression_ast_limits_are_enforced_after_parsing() {
        let limits = PredicateLimits {
            max_expression_nodes: 2,
            ..PredicateLimits::default()
        };
        let result = MessagePredicate::compile_with_limits(
            SubscriptionView {
                expression: Some("a.b.c".to_string()),
                ..SubscriptionView::default()
            },
            limits,
        );
        assert!(matches!(
            result,
            Err(PredicateCompileError::TooManyExpressionNodes { .. })
        ));
    }

    #[test]
    fn projected_document_is_reusable_and_identity_is_canonical() {
        let view = SubscriptionView {
            events: vec!["b".to_string(), "a".to_string(), "a".to_string()],
            expression: Some("enabled".to_string()),
            ..SubscriptionView::default()
        };
        let first = MessagePredicate::compile(view.clone()).expect("predicate compiles");
        let second = MessagePredicate::compile(view).expect("predicate compiles");
        let document =
            ProjectedDocument::new(&json!({"enabled": true})).expect("document projects once");
        let tags = BTreeMap::new();

        assert!(first.requires_document());
        assert_eq!(first.id(), second.id());
        assert!(
            first
                .matches_projected(Some("a"), &tags, Some(&document))
                .expect("first evaluation succeeds")
        );
        assert!(
            second
                .matches_projected(Some("b"), &tags, Some(&document))
                .expect("reused projection succeeds")
        );
        assert!(first.matches_projected(Some("a"), &tags, None).is_err());
    }

    #[test]
    fn bounded_projection_stops_at_the_configured_limit() {
        let document = json!({"data": "0123456789"});
        assert!(ProjectedDocument::new_bounded(&document, 8).is_err());
        assert!(ProjectedDocument::new_bounded(&document, 64).is_ok());
    }

    proptest! {
        #[test]
        fn arbitrary_event_inputs_never_bypass_limits(
            events in prop::collection::vec(".*", 0..80),
        ) {
            let result = MessagePredicate::compile(SubscriptionView {
                events: events.clone(),
                ..SubscriptionView::default()
            });
            let valid = events.len() <= PredicateLimits::default().max_events
                && events.iter().all(|event| !event.is_empty() && event.len() <= PredicateLimits::default().max_event_bytes);
            prop_assert_eq!(result.is_ok(), valid);
        }

        #[test]
        fn arbitrary_jmespath_source_never_panics(source in ".{0,5000}") {
            let result = MessagePredicate::compile(SubscriptionView {
                expression: Some(source.clone()),
                ..SubscriptionView::default()
            });
            if source.is_empty() || source.len() > PredicateLimits::default().max_expression_bytes {
                prop_assert!(result.is_err());
            }
        }
    }
}
