from sockudo_python import Filter, FilterNode, validate_filter


def test_validates_nested_filters() -> None:
    filter_node = Filter.or_(
        Filter.eq("sport", "football"),
        Filter.and_(
            Filter.eq("type", "goal"),
            Filter.gte("xg", "0.8"),
        ),
    )

    assert validate_filter(filter_node) is None


def test_rejects_invalid_not_node() -> None:
    assert (
        validate_filter(FilterNode(op="not", nodes=[]))
        == "NOT operation requires exactly one child node, got 0"
    )
