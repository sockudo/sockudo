package io.sockudo.client

/**
 * LRU-based message deduplication using a [LinkedHashMap] with access-order eviction.
 * When the cache exceeds [capacity], the least-recently-used entry is evicted.
 */
internal class MessageDeduplicator(private val capacity: Int = 1000) {

    private val seen: LinkedHashMap<String, Boolean> = object : LinkedHashMap<String, Boolean>(
        /* initialCapacity = */ (capacity * 4 / 3) + 1,
        /* loadFactor = */ 0.75f,
        /* accessOrder = */ true,
    ) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, Boolean>?): Boolean =
            size > capacity
    }

    /**
     * Returns `true` if [messageId] has already been seen (i.e. is a duplicate).
     */
    fun isDuplicate(messageId: String): Boolean = seen.containsKey(messageId)

    /**
     * Records [messageId] so future calls to [isDuplicate] will return `true`.
     */
    fun track(messageId: String) {
        seen[messageId] = true
    }
}
