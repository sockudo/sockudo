import Foundation

public struct FilterNode: Sendable, Codable, Equatable {
  public var op: String?
  public var key: String?
  public var cmp: String?
  public var val: String?
  public var vals: [String]?
  public var nodes: [FilterNode]?

  public init(
    op: String? = nil,
    key: String? = nil,
    cmp: String? = nil,
    val: String? = nil,
    vals: [String]? = nil,
    nodes: [FilterNode]? = nil
  ) {
    self.op = op
    self.key = key
    self.cmp = cmp
    self.val = val
    self.vals = vals
    self.nodes = nodes
  }
}

public enum Filter {
  public static func eq(_ key: String, _ value: String) -> FilterNode {
    .init(key: key, cmp: "eq", val: value)
  }
  public static func neq(_ key: String, _ value: String) -> FilterNode {
    .init(key: key, cmp: "neq", val: value)
  }
  public static func `in`(_ key: String, _ values: [String]) -> FilterNode {
    .init(key: key, cmp: "in", vals: values)
  }
  public static func nin(_ key: String, _ values: [String]) -> FilterNode {
    .init(key: key, cmp: "nin", vals: values)
  }
  public static func exists(_ key: String) -> FilterNode { .init(key: key, cmp: "ex") }
  public static func notExists(_ key: String) -> FilterNode { .init(key: key, cmp: "nex") }
  public static func startsWith(_ key: String, _ value: String) -> FilterNode {
    .init(key: key, cmp: "sw", val: value)
  }
  public static func endsWith(_ key: String, _ value: String) -> FilterNode {
    .init(key: key, cmp: "ew", val: value)
  }
  public static func contains(_ key: String, _ value: String) -> FilterNode {
    .init(key: key, cmp: "ct", val: value)
  }
  public static func gt(_ key: String, _ value: String) -> FilterNode {
    .init(key: key, cmp: "gt", val: value)
  }
  public static func gte(_ key: String, _ value: String) -> FilterNode {
    .init(key: key, cmp: "gte", val: value)
  }
  public static func lt(_ key: String, _ value: String) -> FilterNode {
    .init(key: key, cmp: "lt", val: value)
  }
  public static func lte(_ key: String, _ value: String) -> FilterNode {
    .init(key: key, cmp: "lte", val: value)
  }
  public static func and(_ nodes: FilterNode...) -> FilterNode { .init(op: "and", nodes: nodes) }
  public static func or(_ nodes: FilterNode...) -> FilterNode { .init(op: "or", nodes: nodes) }
  public static func not(_ node: FilterNode) -> FilterNode { .init(op: "not", nodes: [node]) }
}

public func validateFilter(_ filter: FilterNode) -> String? {
  if let op = filter.op {
    guard ["and", "or", "not"].contains(op) else {
      return "Invalid logical operator: \(op)"
    }
    guard let nodes = filter.nodes else {
      return "Logical operation '\(op)' requires nodes array"
    }
    if op == "not", nodes.count != 1 {
      return "NOT operation requires exactly one child node, got \(nodes.count)"
    }
    if ["and", "or"].contains(op), nodes.isEmpty {
      return "\(op.uppercased()) operation requires at least one child node"
    }
    for (index, child) in nodes.enumerated() {
      if let error = validateFilter(child) {
        return "Child node \(index): \(error)"
      }
    }
    return nil
  }

  guard let key = filter.key, key.isEmpty == false else {
    return "Leaf node requires a key"
  }
  _ = key
  guard let cmp = filter.cmp else {
    return "Leaf node requires a comparison operator"
  }
  let validOps = [
    "eq", "neq", "in", "nin", "ex", "nex", "sw", "ew", "ct", "gt", "gte", "lt", "lte",
  ]
  guard validOps.contains(cmp) else {
    return "Invalid comparison operator: \(cmp)"
  }
  if cmp == "in" || cmp == "nin" {
    guard let values = filter.vals, values.isEmpty == false else {
      return "\(cmp) operation requires non-empty vals array"
    }
    _ = values
  } else if cmp != "ex" && cmp != "nex" {
    guard let value = filter.val, value.isEmpty == false else {
      return "\(cmp) operation requires a val"
    }
    _ = value
  }
  return nil
}
