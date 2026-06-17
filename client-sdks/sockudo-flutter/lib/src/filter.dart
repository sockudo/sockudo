import 'dart:convert';

class FilterNode {
  const FilterNode({
    this.op,
    this.key,
    this.cmp,
    this.val,
    this.vals,
    this.nodes,
  });

  final String? op;
  final String? key;
  final String? cmp;
  final String? val;
  final List<String>? vals;
  final List<FilterNode>? nodes;

  Map<String, dynamic> toJson() => <String, dynamic>{
    if (op != null) 'op': op,
    if (key != null) 'key': key,
    if (cmp != null) 'cmp': cmp,
    if (val != null) 'val': val,
    if (vals != null) 'vals': vals,
    if (nodes != null) 'nodes': nodes!.map((node) => node.toJson()).toList(),
  };

  @override
  String toString() => jsonEncode(toJson());
}

abstract final class Filter {
  static FilterNode eq(String key, String value) =>
      FilterNode(key: key, cmp: 'eq', val: value);
  static FilterNode neq(String key, String value) =>
      FilterNode(key: key, cmp: 'neq', val: value);
  static FilterNode inside(String key, List<String> values) =>
      FilterNode(key: key, cmp: 'in', vals: values);
  static FilterNode notIn(String key, List<String> values) =>
      FilterNode(key: key, cmp: 'nin', vals: values);
  static FilterNode exists(String key) => FilterNode(key: key, cmp: 'ex');
  static FilterNode notExists(String key) => FilterNode(key: key, cmp: 'nex');
  static FilterNode startsWith(String key, String value) =>
      FilterNode(key: key, cmp: 'sw', val: value);
  static FilterNode endsWith(String key, String value) =>
      FilterNode(key: key, cmp: 'ew', val: value);
  static FilterNode contains(String key, String value) =>
      FilterNode(key: key, cmp: 'ct', val: value);
  static FilterNode gt(String key, String value) =>
      FilterNode(key: key, cmp: 'gt', val: value);
  static FilterNode gte(String key, String value) =>
      FilterNode(key: key, cmp: 'gte', val: value);
  static FilterNode lt(String key, String value) =>
      FilterNode(key: key, cmp: 'lt', val: value);
  static FilterNode lte(String key, String value) =>
      FilterNode(key: key, cmp: 'lte', val: value);
  static FilterNode and(List<FilterNode> nodes) =>
      FilterNode(op: 'and', nodes: nodes);
  static FilterNode or(List<FilterNode> nodes) =>
      FilterNode(op: 'or', nodes: nodes);
  static FilterNode not(FilterNode node) =>
      FilterNode(op: 'not', nodes: <FilterNode>[node]);
}

String? validateFilter(FilterNode filter) {
  if (filter.op != null) {
    if (!<String>{'and', 'or', 'not'}.contains(filter.op)) {
      return 'Invalid logical operator: ${filter.op}';
    }
    final nodes = filter.nodes;
    if (nodes == null) {
      return "Logical operation '${filter.op}' requires nodes array";
    }
    if (filter.op == 'not' && nodes.length != 1) {
      return 'NOT operation requires exactly one child node, got ${nodes.length}';
    }
    if ((filter.op == 'and' || filter.op == 'or') && nodes.isEmpty) {
      return '${filter.op!.toUpperCase()} operation requires at least one child node';
    }
    for (var index = 0; index < nodes.length; index += 1) {
      final childError = validateFilter(nodes[index]);
      if (childError != null) {
        return 'Child node $index: $childError';
      }
    }
    return null;
  }

  if (filter.key == null || filter.key!.isEmpty) {
    return 'Leaf node requires a key';
  }
  if (filter.cmp == null || filter.cmp!.isEmpty) {
    return 'Leaf node requires a comparison operator';
  }

  const validOps = <String>{
    'eq',
    'neq',
    'in',
    'nin',
    'ex',
    'nex',
    'sw',
    'ew',
    'ct',
    'gt',
    'gte',
    'lt',
    'lte',
  };
  if (!validOps.contains(filter.cmp)) {
    return 'Invalid comparison operator: ${filter.cmp}';
  }
  if (filter.cmp == 'in' || filter.cmp == 'nin') {
    if (filter.vals == null || filter.vals!.isEmpty) {
      return '${filter.cmp} operation requires non-empty vals array';
    }
  } else if (filter.cmp != 'ex' && filter.cmp != 'nex') {
    if (filter.val == null || filter.val!.isEmpty) {
      return '${filter.cmp} operation requires a val';
    }
  }
  return null;
}
