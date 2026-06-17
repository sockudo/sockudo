package io.sockudo.client

import kotlinx.serialization.Serializable

@Serializable
data class FilterNode(
    val op: String? = null,
    val key: String? = null,
    val cmp: String? = null,
    val `val`: String? = null,
    val vals: List<String>? = null,
    val nodes: List<FilterNode>? = null,
)

object Filter {
    fun eq(key: String, value: String) = FilterNode(key = key, cmp = "eq", `val` = value)
    fun neq(key: String, value: String) = FilterNode(key = key, cmp = "neq", `val` = value)
    fun inside(key: String, values: List<String>) = FilterNode(key = key, cmp = "in", vals = values)
    fun notIn(key: String, values: List<String>) = FilterNode(key = key, cmp = "nin", vals = values)
    fun exists(key: String) = FilterNode(key = key, cmp = "ex")
    fun notExists(key: String) = FilterNode(key = key, cmp = "nex")
    fun startsWith(key: String, value: String) = FilterNode(key = key, cmp = "sw", `val` = value)
    fun endsWith(key: String, value: String) = FilterNode(key = key, cmp = "ew", `val` = value)
    fun contains(key: String, value: String) = FilterNode(key = key, cmp = "ct", `val` = value)
    fun gt(key: String, value: String) = FilterNode(key = key, cmp = "gt", `val` = value)
    fun gte(key: String, value: String) = FilterNode(key = key, cmp = "gte", `val` = value)
    fun lt(key: String, value: String) = FilterNode(key = key, cmp = "lt", `val` = value)
    fun lte(key: String, value: String) = FilterNode(key = key, cmp = "lte", `val` = value)
    fun and(vararg nodes: FilterNode) = FilterNode(op = "and", nodes = nodes.toList())
    fun or(vararg nodes: FilterNode) = FilterNode(op = "or", nodes = nodes.toList())
    fun not(node: FilterNode) = FilterNode(op = "not", nodes = listOf(node))
}

fun validateFilter(filter: FilterNode): String? {
    filter.op?.let { op ->
        if (op !in setOf("and", "or", "not")) {
            return "Invalid logical operator: $op"
        }
        val nodes = filter.nodes ?: return "Logical operation '$op' requires nodes array"
        if (op == "not" && nodes.size != 1) {
            return "NOT operation requires exactly one child node, got ${nodes.size}"
        }
        if ((op == "and" || op == "or") && nodes.isEmpty()) {
            return "${op.uppercase()} operation requires at least one child node"
        }
        nodes.forEachIndexed { index, child ->
            validateFilter(child)?.let { return "Child node $index: $it" }
        }
        return null
    }

    if (filter.key.isNullOrBlank()) {
        return "Leaf node requires a key"
    }
    if (filter.cmp.isNullOrBlank()) {
        return "Leaf node requires a comparison operator"
    }
    if (filter.cmp !in setOf("eq", "neq", "in", "nin", "ex", "nex", "sw", "ew", "ct", "gt", "gte", "lt", "lte")) {
        return "Invalid comparison operator: ${filter.cmp}"
    }
    if (filter.cmp == "in" || filter.cmp == "nin") {
        if (filter.vals.isNullOrEmpty()) {
            return "${filter.cmp} operation requires non-empty vals array"
        }
    } else if (filter.cmp != "ex" && filter.cmp != "nex" && filter.`val`.isNullOrBlank()) {
        return "${filter.cmp} operation requires a val"
    }
    return null
}
