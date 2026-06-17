package io.sockudo.client

import com.davidehrmann.vcdiff.VCDiffDecoderBuilder
import java.io.ByteArrayOutputStream
import java.util.Base64

private data class CachedMessage(
    val content: String,
    val sequence: Int,
)

private class ChannelState(
    val channelName: String,
) {
    var conflationKey: String? = null
    var maxMessagesPerKey: Int = 30
    val conflationCaches = linkedMapOf<String, MutableList<CachedMessage>>()
    var baseMessage: String? = null
    var baseSequence: Int? = null
    var lastSequence: Int? = null
    var deltaCount: Int = 0
    var fullMessageCount: Int = 0

    fun initialize(syncData: Map<String, Any?>) {
        conflationKey = syncData["conflation_key"] as? String
        maxMessagesPerKey = maxOf(((syncData["max_messages_per_key"] as? Number)?.toInt() ?: 10), 30)
        conflationCaches.clear()
        val states = syncData["states"] as? Map<*, *> ?: return
        states.forEach { (key, value) ->
            val cache =
                (value as? List<*>)?.mapNotNull { raw ->
                    val item = raw as? Map<*, *> ?: return@mapNotNull null
                    val content = item["content"] as? String ?: return@mapNotNull null
                    val sequence = (item["seq"] as? Number)?.toInt() ?: return@mapNotNull null
                    CachedMessage(content, sequence)
                }?.toMutableList() ?: mutableListOf()
            conflationCaches[key.toString()] = cache
        }
    }

    fun baseMessage(conflationValue: String?, baseIndex: Int?): String? {
        if (conflationKey == null) {
            return baseMessage
        }
        val index = baseIndex ?: return null
        return conflationCaches[conflationValue.orEmpty()]?.firstOrNull { it.sequence == index }?.content
    }

    fun update(message: String, sequence: Int, conflationValue: String?) {
        if (conflationKey != null || conflationValue != null) {
            val key = conflationValue.orEmpty()
            val cache = conflationCaches.getOrPut(key) { mutableListOf() }
            cache += CachedMessage(message, sequence)
            while (cache.size > maxMessagesPerKey) {
                cache.removeFirst()
            }
            if (conflationKey == null) {
                conflationKey = "enabled"
            }
        } else {
            baseMessage = message
            baseSequence = sequence
        }
        lastSequence = sequence
    }

    fun stats(): ChannelDeltaStats =
        ChannelDeltaStats(
            channelName = channelName,
            conflationKey = conflationKey,
            conflationGroupCount = conflationCaches.size,
            deltaCount = deltaCount,
            fullMessageCount = fullMessageCount,
            totalMessages = deltaCount + fullMessageCount,
        )
}

internal class DeltaCompressionManager(
    private val options: DeltaOptions,
    private val sendEvent: (String, Any) -> Boolean,
    private val prefix: ProtocolPrefix = ProtocolPrefix(2),
) {
    private val channelStates = linkedMapOf<String, ChannelState>()
    private var enabled = false
    private var defaultAlgorithm = DeltaAlgorithm.fossil
    private var totals = Totals()

    fun enable() {
        if (enabled) {
            return
        }
        sendEvent(
            prefix.event("enable_delta_compression"),
            mapOf("algorithms" to options.algorithms.map { it.name }),
        )
    }

    fun disable() {
        enabled = false
    }

    fun handleEnabled(data: Any?) {
        val payload = data as? Map<*, *>
        enabled = payload?.get("enabled") as? Boolean ?: true
        defaultAlgorithm =
            (payload?.get("algorithm") as? String)?.let {
                runCatching { DeltaAlgorithm.valueOf(it) }.getOrNull()
            } ?: defaultAlgorithm
    }

    fun handleCacheSync(channel: String, data: Any?) {
        val payload = data as? Map<String, Any?> ?: return
        val state = channelStates.getOrPut(channel) { ChannelState(channel) }
        state.initialize(payload)
    }

    fun handleDeltaMessage(channel: String, data: Any?): SockudoEvent? {
        val payload = data as? Map<*, *> ?: return null
        val eventName = payload["event"] as? String ?: return null
        val delta = payload["delta"] as? String ?: return null
        val sequence = (payload["seq"] as? Number)?.toInt() ?: return null
        val algorithm =
            (payload["algorithm"] as? String)?.let { runCatching { DeltaAlgorithm.valueOf(it) }.getOrNull() }
                ?: defaultAlgorithm
        val conflationKey = payload["conflation_key"] as? String
        val baseIndex = (payload["base_index"] as? Number)?.toInt()

        val state = channelStates[channel] ?: return null
        val baseMessage = state.baseMessage(conflationKey, baseIndex) ?: run {
            requestResync(channel)
            return null
        }

        return runCatching {
            val deltaBytes = Base64.getDecoder().decode(delta)
            val reconstructed =
                when (algorithm) {
                    DeltaAlgorithm.fossil -> FossilDelta.apply(baseMessage.encodeToByteArray(), deltaBytes)
                        .decodeToString()

                    DeltaAlgorithm.xdelta3 -> decodeVcdiff(baseMessage.encodeToByteArray(), deltaBytes).decodeToString()
                }

            state.update(reconstructed, sequence, conflationKey)
            state.deltaCount += 1
            totals = totals.recordDelta(reconstructed.length, deltaBytes.size)
            emitStats()

            val parsed =
                runCatching { JsonSupport.fromJsonElement(JsonSupport.decode(reconstructed)) }.getOrElse { reconstructed }
            val eventData =
                if (parsed is Map<*, *> && parsed.containsKey("data")) {
                    parsed["data"]
                } else {
                    parsed
                }

            SockudoEvent(
                event = eventName,
                channel = channel,
                data = eventData,
                userId = null,
                rawMessage = reconstructed,
                sequence = sequence,
                conflationKey = conflationKey,
            )
        }.onFailure(::recordError).getOrNull()
    }

    fun handleFullMessage(channel: String, rawMessage: String, sequence: Int?, conflationKey: String?) {
        val resolvedSequence = sequence ?: return
        val state = channelStates.getOrPut(channel) { ChannelState(channel) }
        state.update(rawMessage, resolvedSequence, conflationKey)
        state.fullMessageCount += 1
        totals = totals.recordFull(rawMessage.length)
        emitStats()
    }

    fun getStats(): DeltaStats {
        val saved = totals.totalBytesWithoutCompression - totals.totalBytesWithCompression
        val percentage =
            if (totals.totalBytesWithoutCompression > 0) {
                saved.toDouble() / totals.totalBytesWithoutCompression.toDouble() * 100.0
            } else {
                0.0
            }
        return DeltaStats(
            totalMessages = totals.totalMessages,
            deltaMessages = totals.deltaMessages,
            fullMessages = totals.fullMessages,
            totalBytesWithoutCompression = totals.totalBytesWithoutCompression,
            totalBytesWithCompression = totals.totalBytesWithCompression,
            bandwidthSaved = saved,
            bandwidthSavedPercent = percentage,
            errors = totals.errors,
            channelCount = channelStates.size,
            channels = channelStates.values.map { it.stats() }.sortedBy { it.channelName },
        )
    }

    fun resetStats() {
        totals = Totals()
        emitStats()
    }

    fun clearChannelState(channel: String? = null) {
        if (channel == null) {
            channelStates.clear()
        } else {
            channelStates.remove(channel)
        }
    }

    private fun decodeVcdiff(base: ByteArray, delta: ByteArray): ByteArray {
        val output = ByteArrayOutputStream()
        VCDiffDecoderBuilder.builder().buildSimple().decode(base, delta, output)
        return output.toByteArray()
    }

    private fun requestResync(channel: String) {
        sendEvent(prefix.event("delta_sync_error"), mapOf("channel" to channel))
        channelStates.remove(channel)
    }

    private fun recordError(error: Throwable) {
        totals = totals.copy(errors = totals.errors + 1)
        options.onError?.invoke(error)
    }

    private fun emitStats() {
        options.onStats?.invoke(getStats())
    }

    private data class Totals(
        val totalMessages: Int = 0,
        val deltaMessages: Int = 0,
        val fullMessages: Int = 0,
        val totalBytesWithoutCompression: Int = 0,
        val totalBytesWithCompression: Int = 0,
        val errors: Int = 0,
    ) {
        fun recordDelta(rawBytes: Int, wireBytes: Int): Totals =
            copy(
                totalMessages = totalMessages + 1,
                deltaMessages = deltaMessages + 1,
                totalBytesWithoutCompression = totalBytesWithoutCompression + rawBytes,
                totalBytesWithCompression = totalBytesWithCompression + wireBytes,
            )

        fun recordFull(size: Int): Totals =
            copy(
                totalMessages = totalMessages + 1,
                fullMessages = fullMessages + 1,
                totalBytesWithoutCompression = totalBytesWithoutCompression + size,
                totalBytesWithCompression = totalBytesWithCompression + size,
            )
    }
}
