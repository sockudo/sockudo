//! Tool catalog: declarative specs mapped onto the Sockudo HTTP API.
//!
//! Most tools are thin, validated projections of one API route (`ToolKind::Api`);
//! a few need local computation or the credential source (`ToolKind::Custom`).
//! The catalog builds the MCP `Tool` definitions once and caches the filtered
//! `tools/list` for every scope combination, so listing is an `Arc` clone.

pub mod args;
pub mod catalog;

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};

use rmcp::model::{CallToolResult, JsonObject, Tool, ToolAnnotations};
use serde_json::Value;

pub use args::Args;

use crate::api::ApiRequest;
use crate::auth::{Principal, Scope, ScopeSet};
use crate::error::ToolError;
use crate::handler::McpCore;

/// Execution context handed to custom tools.
#[derive(Debug, Clone, Copy)]
pub struct ToolContext<'a> {
    /// Shared server state (API client, introspection, config).
    pub core: &'a McpCore,
    /// Authenticated caller.
    pub principal: &'a Principal,
}

/// Boxed future returned by custom tool handlers.
pub type ToolFuture<'a> =
    Pin<Box<dyn Future<Output = Result<CallToolResult, ToolError>> + Send + 'a>>;

/// Custom tool implementation.
pub type CustomHandler = for<'a> fn(ToolContext<'a>, Args<'a>) -> ToolFuture<'a>;

/// Builder that turns validated arguments into one API request.
pub type ApiBuilder = fn(&Args<'_>) -> Result<ApiRequest, ToolError>;

/// How a tool executes.
#[derive(Clone, Copy)]
pub enum ToolKind {
    /// One signed (or operational) Sockudo API call; the JSON response is the
    /// tool result.
    Api(ApiBuilder),
    /// Local computation or multi-step logic.
    Custom(CustomHandler),
}

impl std::fmt::Debug for ToolKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ToolKind::Api(_) => f.write_str("Api"),
            ToolKind::Custom(_) => f.write_str("Custom"),
        }
    }
}

/// Static description of one tool.
#[derive(Debug, Clone)]
pub struct ToolSpec {
    /// Unique tool name (`sockudo_*`).
    pub name: &'static str,
    /// Human title.
    pub title: &'static str,
    /// Model-facing description.
    pub description: &'static str,
    /// Minimum scope.
    pub scope: Scope,
    /// MCP behavior hints.
    pub read_only: bool,
    pub destructive: bool,
    pub idempotent: bool,
    /// JSON Schema for `arguments`.
    pub schema: Value,
    /// Execution strategy.
    pub kind: ToolKind,
}

impl ToolSpec {
    fn to_tool(&self) -> Tool {
        let schema: JsonObject = match &self.schema {
            Value::Object(object) => object.clone(),
            other => panic!("tool '{}' schema must be an object, got {other}", self.name),
        };
        Tool::new(self.name, self.description, Arc::new(schema))
            .with_title(self.title)
            .with_annotations(
                ToolAnnotations::with_title(self.title)
                    .read_only(self.read_only)
                    .destructive(self.destructive)
                    .idempotent(self.idempotent)
                    .open_world(false),
            )
    }
}

/// All tools, indexed by name, with per-scope `tools/list` caches.
pub struct Catalog {
    specs: Vec<ToolSpec>,
    tools: Vec<Tool>,
    by_name: HashMap<&'static str, usize>,
    per_scope: [OnceLock<Arc<Vec<Tool>>>; ScopeSet::CARDINALITY],
}

impl std::fmt::Debug for Catalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Catalog")
            .field("tools", &self.specs.len())
            .finish()
    }
}

impl Catalog {
    /// Build the catalog, omitting any tool named in `disabled`.
    pub fn new(disabled: &HashSet<String>) -> Self {
        let specs: Vec<ToolSpec> = catalog::specs()
            .into_iter()
            .filter(|spec| !disabled.contains(spec.name))
            .collect();
        let tools = specs.iter().map(ToolSpec::to_tool).collect();
        let by_name = specs
            .iter()
            .enumerate()
            .map(|(index, spec)| (spec.name, index))
            .collect();
        Self {
            specs,
            tools,
            by_name,
            per_scope: Default::default(),
        }
    }

    /// Number of enabled tools.
    pub fn len(&self) -> usize {
        self.specs.len()
    }

    /// Whether no tools are enabled.
    pub fn is_empty(&self) -> bool {
        self.specs.is_empty()
    }

    /// Look up a tool by name.
    pub fn get(&self, name: &str) -> Option<&ToolSpec> {
        self.by_name.get(name).map(|index| &self.specs[*index])
    }

    /// Every enabled spec.
    pub fn specs(&self) -> &[ToolSpec] {
        &self.specs
    }

    /// Tools visible to a principal with `scopes`, cached per scope set.
    pub fn list_for(&self, scopes: ScopeSet) -> Arc<Vec<Tool>> {
        self.per_scope[scopes.index()]
            .get_or_init(|| {
                Arc::new(
                    self.specs
                        .iter()
                        .zip(&self.tools)
                        .filter(|(spec, _)| scopes.allows(spec.scope))
                        .map(|(_, tool)| tool.clone())
                        .collect(),
                )
            })
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_builds_and_filters_by_scope() {
        let catalog = Catalog::new(&HashSet::new());
        assert!(catalog.len() > 40);
        let read = catalog.list_for(ScopeSet::READ);
        let all = catalog.list_for(ScopeSet::ALL);
        assert!(read.len() < all.len());
        assert_eq!(all.len(), catalog.len());
        assert!(read.iter().all(|tool| {
            let spec = catalog.get(&tool.name).unwrap();
            spec.scope == Scope::Read && spec.read_only
        }));
        assert!(catalog.list_for(ScopeSet::EMPTY).is_empty());
        // Cached: same Arc.
        assert!(Arc::ptr_eq(&read, &catalog.list_for(ScopeSet::READ)));
    }

    #[test]
    fn names_are_unique_and_prefixed() {
        let catalog = Catalog::new(&HashSet::new());
        let mut names: Vec<_> = catalog.specs().iter().map(|spec| spec.name).collect();
        let before = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), before);
        assert!(names.iter().all(|name| name.starts_with("sockudo_")));
    }

    #[test]
    fn disabled_tools_are_removed() {
        let disabled = HashSet::from(["sockudo_trigger_event".to_string()]);
        let catalog = Catalog::new(&disabled);
        assert!(catalog.get("sockudo_trigger_event").is_none());
        assert!(catalog.get("sockudo_list_channels").is_some());
    }

    #[test]
    fn schemas_are_objects_with_properties() {
        for spec in Catalog::new(&HashSet::new()).specs() {
            let object = spec.schema.as_object().unwrap();
            assert_eq!(object["type"], "object", "{}", spec.name);
            assert!(object.contains_key("properties"), "{}", spec.name);
            if spec.destructive {
                assert_eq!(
                    spec.scope,
                    Scope::Admin,
                    "{} destructive tools are admin",
                    spec.name
                );
            }
        }
    }
}
