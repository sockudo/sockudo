package io.sockudo.client

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class FilterTest {
    @Test
    fun validatesNestedFilters() {
        val filter = Filter.or(
            Filter.eq("sport", "football"),
            Filter.and(
                Filter.eq("type", "goal"),
                Filter.gte("xg", "0.8"),
            ),
        )

        assertNull(validateFilter(filter))
    }

    @Test
    fun rejectsInvalidNotNode() {
        assertEquals(
            "NOT operation requires exactly one child node, got 0",
            validateFilter(FilterNode(op = "not", nodes = emptyList())),
        )
    }
}
