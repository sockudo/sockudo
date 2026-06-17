using System.Text.Json.Serialization;

namespace Sockudo.Client;

public sealed record FilterNode(
    [property: JsonPropertyName("op")] string? Op = null,
    [property: JsonPropertyName("key")] string? Key = null,
    [property: JsonPropertyName("cmp")] string? Cmp = null,
    [property: JsonPropertyName("val")] string? Val = null,
    [property: JsonPropertyName("vals")] IReadOnlyList<string>? Vals = null,
    [property: JsonPropertyName("nodes")] IReadOnlyList<FilterNode>? Nodes = null
);

public static class Filter
{
    public static FilterNode Eq(string key, string value) => new(Key: key, Cmp: "eq", Val: value);
    public static FilterNode Neq(string key, string value) => new(Key: key, Cmp: "neq", Val: value);
    public static FilterNode Inside(string key, params string[] values) => new(Key: key, Cmp: "in", Vals: values);
    public static FilterNode NotIn(string key, params string[] values) => new(Key: key, Cmp: "nin", Vals: values);
    public static FilterNode Exists(string key) => new(Key: key, Cmp: "ex");
    public static FilterNode NotExists(string key) => new(Key: key, Cmp: "nex");
    public static FilterNode StartsWith(string key, string value) => new(Key: key, Cmp: "sw", Val: value);
    public static FilterNode EndsWith(string key, string value) => new(Key: key, Cmp: "ew", Val: value);
    public static FilterNode Contains(string key, string value) => new(Key: key, Cmp: "ct", Val: value);
    public static FilterNode Gt(string key, string value) => new(Key: key, Cmp: "gt", Val: value);
    public static FilterNode Gte(string key, string value) => new(Key: key, Cmp: "gte", Val: value);
    public static FilterNode Lt(string key, string value) => new(Key: key, Cmp: "lt", Val: value);
    public static FilterNode Lte(string key, string value) => new(Key: key, Cmp: "lte", Val: value);
    public static FilterNode And(params FilterNode[] nodes) => new(Op: "and", Nodes: nodes);
    public static FilterNode Or(params FilterNode[] nodes) => new(Op: "or", Nodes: nodes);
    public static FilterNode Not(FilterNode node) => new(Op: "not", Nodes: new[] { node });
}

public static class FilterValidator
{
    public static string? Validate(FilterNode filter)
    {
        if (filter.Op is not null)
        {
            if (filter.Op is not ("and" or "or" or "not"))
            {
                return $"Invalid logical operator: {filter.Op}";
            }

            if (filter.Nodes is null)
            {
                return $"Logical operation '{filter.Op}' requires nodes array";
            }

            if (filter.Op == "not" && filter.Nodes.Count != 1)
            {
                return $"NOT operation requires exactly one child node, got {filter.Nodes.Count}";
            }

            if ((filter.Op == "and" || filter.Op == "or") && filter.Nodes.Count == 0)
            {
                return $"{filter.Op.ToUpperInvariant()} operation requires at least one child node";
            }

            for (var index = 0; index < filter.Nodes.Count; index++)
            {
                var childError = Validate(filter.Nodes[index]);
                if (childError is not null)
                {
                    return $"Child node {index}: {childError}";
                }
            }

            return null;
        }

        if (string.IsNullOrWhiteSpace(filter.Key))
        {
            return "Leaf node requires a key";
        }

        if (string.IsNullOrWhiteSpace(filter.Cmp))
        {
            return "Leaf node requires a comparison operator";
        }

        if (filter.Cmp is not ("eq" or "neq" or "in" or "nin" or "ex" or "nex" or "sw" or "ew" or "ct" or "gt" or "gte" or "lt" or "lte"))
        {
            return $"Invalid comparison operator: {filter.Cmp}";
        }

        if (filter.Cmp is "in" or "nin")
        {
            if (filter.Vals is null || filter.Vals.Count == 0)
            {
                return $"{filter.Cmp} operation requires non-empty vals array";
            }
        }
        else if (filter.Cmp is not ("ex" or "nex") && string.IsNullOrWhiteSpace(filter.Val))
        {
            return $"{filter.Cmp} operation requires a val";
        }

        return null;
    }
}
