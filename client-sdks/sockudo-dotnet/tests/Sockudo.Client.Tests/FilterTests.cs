using Xunit;

namespace Sockudo.Client.Tests;

public sealed class FilterTests
{
    [Fact]
    public void ValidatesNestedFilters()
    {
        var filter = Filter.Or(
            Filter.Eq("sport", "football"),
            Filter.And(
                Filter.Eq("type", "goal"),
                Filter.Gte("xg", "0.8")
            )
        );

        Assert.Null(FilterValidator.Validate(filter));
    }

    [Fact]
    public void RejectsInvalidNotNode()
    {
        Assert.Equal(
            "NOT operation requires exactly one child node, got 0",
            FilterValidator.Validate(new FilterNode(Op: "not", Nodes: Array.Empty<FilterNode>()))
        );
    }
}
