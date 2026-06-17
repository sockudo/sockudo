// sockudo-js/src/core/channels/filter.ts

/**
 * Filter node structure for publication filtering by tags.
 *
 * This structure matches the server-side FilterNode format for zero-allocation
 * evaluation during message broadcast.
 */
export interface FilterNode {
  /** Logical operator: "and", "or", "not" (if present, this is a branch node) */
  op?: string;

  /** Key for comparison (leaf nodes only) */
  key?: string;

  /** Comparison operator: "eq", "neq", "in", "nin", "ex", "nex", "sw", "ew", "ct", "gt", "gte", "lt", "lte" */
  cmp?: string;

  /** Single value for most comparisons */
  val?: string;

  /** Multiple values for set operations (in, nin) */
  vals?: string[];

  /** Child nodes for logical operations */
  nodes?: FilterNode[];
}

/**
 * Builder for constructing filter nodes programmatically.
 *
 * Provides a clean API for creating complex filters without dealing with
 * the internal FilterNode structure directly.
 *
 * @example
 * ```typescript
 * // Simple equality filter
 * const filter = Filter.eq("event_type", "goal");
 *
 * // Complex nested filter
 * const filter = Filter.or(
 *   Filter.eq("event_type", "goal"),
 *   Filter.and(
 *     Filter.eq("event_type", "shot"),
 *     Filter.gte("xG", "0.8")
 *   )
 * );
 * ```
 */
export const Filter = {
  // Comparison operators

  /**
   * Creates an equality comparison: key == val
   * @param key The tag key to compare
   * @param val The value to compare against
   * @returns FilterNode for equality comparison
   */
  eq: (key: string, val: string): FilterNode => ({
    key,
    cmp: "eq",
    val,
  }),

  /**
   * Creates an inequality comparison: key != val
   * @param key The tag key to compare
   * @param val The value to compare against
   * @returns FilterNode for inequality comparison
   */
  neq: (key: string, val: string): FilterNode => ({
    key,
    cmp: "neq",
    val,
  }),

  /**
   * Creates a set membership comparison: key in [vals...]
   * @param key The tag key to compare
   * @param vals Array of values for set membership
   * @returns FilterNode for set membership comparison
   */
  in: (key: string, vals: string[]): FilterNode => ({
    key,
    cmp: "in",
    vals,
  }),

  /**
   * Creates a set non-membership comparison: key not in [vals...]
   * @param key The tag key to compare
   * @param vals Array of values for set non-membership
   * @returns FilterNode for set non-membership comparison
   */
  nin: (key: string, vals: string[]): FilterNode => ({
    key,
    cmp: "nin",
    vals,
  }),

  /**
   * Creates an existence check: key exists
   * @param key The tag key to check for existence
   * @returns FilterNode for existence check
   */
  exists: (key: string): FilterNode => ({
    key,
    cmp: "ex",
  }),

  /**
   * Creates a non-existence check: key does not exist
   * @param key The tag key to check for non-existence
   * @returns FilterNode for non-existence check
   */
  notExists: (key: string): FilterNode => ({
    key,
    cmp: "nex",
  }),

  /**
   * Creates a starts-with comparison: key starts with val
   * @param key The tag key to compare
   * @param val The prefix to match
   * @returns FilterNode for starts-with comparison
   */
  startsWith: (key: string, val: string): FilterNode => ({
    key,
    cmp: "sw",
    val,
  }),

  /**
   * Creates an ends-with comparison: key ends with val
   * @param key The tag key to compare
   * @param val The suffix to match
   * @returns FilterNode for ends-with comparison
   */
  endsWith: (key: string, val: string): FilterNode => ({
    key,
    cmp: "ew",
    val,
  }),

  /**
   * Creates a contains comparison: key contains val
   * @param key The tag key to compare
   * @param val The substring to match
   * @returns FilterNode for contains comparison
   */
  contains: (key: string, val: string): FilterNode => ({
    key,
    cmp: "ct",
    val,
  }),

  /**
   * Creates a greater-than comparison: key > val (numeric)
   * @param key The tag key to compare
   * @param val The numeric value to compare against (as string)
   * @returns FilterNode for greater-than comparison
   */
  gt: (key: string, val: string): FilterNode => ({
    key,
    cmp: "gt",
    val,
  }),

  /**
   * Creates a greater-than-or-equal comparison: key >= val (numeric)
   * @param key The tag key to compare
   * @param val The numeric value to compare against (as string)
   * @returns FilterNode for greater-than-or-equal comparison
   */
  gte: (key: string, val: string): FilterNode => ({
    key,
    cmp: "gte",
    val,
  }),

  /**
   * Creates a less-than comparison: key < val (numeric)
   * @param key The tag key to compare
   * @param val The numeric value to compare against (as string)
   * @returns FilterNode for less-than comparison
   */
  lt: (key: string, val: string): FilterNode => ({
    key,
    cmp: "lt",
    val,
  }),

  /**
   * Creates a less-than-or-equal comparison: key <= val (numeric)
   * @param key The tag key to compare
   * @param val The numeric value to compare against (as string)
   * @returns FilterNode for less-than-or-equal comparison
   */
  lte: (key: string, val: string): FilterNode => ({
    key,
    cmp: "lte",
    val,
  }),

  // Logical operators

  /**
   * Creates an AND logical operation: all children must match
   * @param nodes Child filter nodes to combine with AND
   * @returns FilterNode for AND operation
   */
  and: (...nodes: FilterNode[]): FilterNode => ({
    op: "and",
    nodes,
  }),

  /**
   * Creates an OR logical operation: at least one child must match
   * @param nodes Child filter nodes to combine with OR
   * @returns FilterNode for OR operation
   */
  or: (...nodes: FilterNode[]): FilterNode => ({
    op: "or",
    nodes,
  }),

  /**
   * Creates a NOT logical operation: negates the child
   * @param node Child filter node to negate
   * @returns FilterNode for NOT operation
   */
  not: (node: FilterNode): FilterNode => ({
    op: "not",
    nodes: [node],
  }),
};

/**
 * Validates a filter node structure.
 *
 * @param filter The filter node to validate
 * @returns Error message if invalid, null if valid
 */
export function validateFilter(filter: FilterNode): string | null {
  if (filter.op) {
    // Branch node - logical operation
    const op = filter.op;

    if (!["and", "or", "not"].includes(op)) {
      return `Invalid logical operator: ${op}`;
    }

    if (!filter.nodes || !Array.isArray(filter.nodes)) {
      return `Logical operation '${op}' requires nodes array`;
    }

    if (op === "not" && filter.nodes.length !== 1) {
      return `NOT operation requires exactly one child node, got ${filter.nodes.length}`;
    }

    if ((op === "and" || op === "or") && filter.nodes.length === 0) {
      return `${op.toUpperCase()} operation requires at least one child node`;
    }

    // Validate all children recursively
    for (let i = 0; i < filter.nodes.length; i++) {
      const childError = validateFilter(filter.nodes[i]);
      if (childError) {
        return `Child node ${i}: ${childError}`;
      }
    }
  } else {
    // Leaf node - comparison
    if (!filter.key) {
      return "Leaf node requires a key";
    }

    if (!filter.cmp) {
      return "Leaf node requires a comparison operator";
    }

    const validOps = [
      "eq",
      "neq",
      "in",
      "nin",
      "ex",
      "nex",
      "sw",
      "ew",
      "ct",
      "gt",
      "gte",
      "lt",
      "lte",
    ];
    if (!validOps.includes(filter.cmp)) {
      return `Invalid comparison operator: ${filter.cmp}`;
    }

    // Check value requirements based on operator
    if (filter.cmp === "in" || filter.cmp === "nin") {
      if (
        !filter.vals ||
        !Array.isArray(filter.vals) ||
        filter.vals.length === 0
      ) {
        return `${filter.cmp} operation requires non-empty vals array`;
      }
    } else if (filter.cmp === "ex" || filter.cmp === "nex") {
      // Existence checks don't need values
    } else {
      if (!filter.val) {
        return `${filter.cmp} operation requires a val`;
      }
    }
  }

  return null;
}

/**
 * Example filter patterns for common use cases
 */
export const FilterExamples = {
  /**
   * Filter for a specific event type
   * @example Filter.eq("event_type", "goal")
   */
  eventType: (type: string) => Filter.eq("event_type", type),

  /**
   * Filter for events in a set of types
   * @example FilterExamples.eventTypes(["goal", "shot"])
   */
  eventTypes: (types: string[]) => Filter.in("event_type", types),

  /**
   * Filter for numeric range
   * @example FilterExamples.range("xG", "0.5", "0.9")
   */
  range: (key: string, min: string, max: string) =>
    Filter.and(Filter.gte(key, min), Filter.lte(key, max)),

  /**
   * Filter for goals OR high-probability shots
   * @example FilterExamples.importantEvents("0.8")
   */
  importantEvents: (xGThreshold: string) =>
    Filter.or(
      Filter.eq("event_type", "goal"),
      Filter.and(
        Filter.eq("event_type", "shot"),
        Filter.gte("xG", xGThreshold),
      ),
    ),
};
