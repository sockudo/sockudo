package io.sockudo.client

import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

enum class SockudoTransport { ws, wss }

enum class SockudoWireFormat(
    val queryValue: String,
    val isBinary: Boolean,
) {
    json("json", false),
    messagepack("messagepack", true),
    protobuf("protobuf", true),
}

enum class ConnectionState {
    INITIALIZED,
    CONNECTING,
    CONNECTED,
    DISCONNECTED,
    UNAVAILABLE,
    FAILED,
}

enum class DeltaAlgorithm { fossil, xdelta3 }

sealed class AuthValue {
    abstract val stringValue: String

    data class Text(val value: String) : AuthValue() {
        override val stringValue: String = value
    }

    data class Integer(val value: Int) : AuthValue() {
        override val stringValue: String = value.toString()
    }

    data class Decimal(val value: Double) : AuthValue() {
        override val stringValue: String = value.toString()
    }

    data class Flag(val value: Boolean) : AuthValue() {
        override val stringValue: String = if (value) "true" else "false"
    }
}

data class ChannelDeltaSettings(
    val enabled: Boolean? = null,
    val algorithm: DeltaAlgorithm? = null,
) {
    fun subscriptionValue(): Any =
        when {
            enabled == null && algorithm != null -> algorithm.name
            enabled == false && algorithm == null -> false
            enabled == true && algorithm == null -> true
            else -> buildMap {
                enabled?.let { put("enabled", it) }
                algorithm?.let { put("algorithm", it.name) }
            }
        }
}

data class MessageExtras(
    val headers: Map<String, Any>? = null,
    val ephemeral: Boolean? = null,
    val idempotencyKey: String? = null,
    val echo: Boolean? = null,
)

data class SubscriptionOptions(
    val filter: FilterNode? = null,
    val delta: ChannelDeltaSettings? = null,
    val events: List<String>? = null,
    val rewind: SubscriptionRewind? = null,
    val annotationSubscribe: Boolean = false,
)

sealed class SubscriptionRewind {
    abstract fun subscriptionValue(): Any

    data class Count(val count: Int) : SubscriptionRewind() {
        override fun subscriptionValue(): Any = count
    }

    data class Seconds(val seconds: Int) : SubscriptionRewind() {
        override fun subscriptionValue(): Any = linkedMapOf("seconds" to seconds)
    }
}

data class RecoveryPosition(
    val streamId: String? = null,
    val serial: Long,
    val lastMessageId: String? = null,
)

data class ResumeRecoveredChannel(
    val channel: String,
    val source: String,
    val replayed: Int,
)

data class ResumeFailedChannel(
    val channel: String,
    val code: String,
    val reason: String,
    val expectedStreamId: String? = null,
    val currentStreamId: String? = null,
    val oldestAvailableSerial: Long? = null,
    val newestAvailableSerial: Long? = null,
)

data class ResumeSuccessData(
    val recovered: List<ResumeRecoveredChannel>,
    val failed: List<ResumeFailedChannel>,
)

data class RewindCompleteData(
    val historicalCount: Int,
    val liveCount: Int,
    val complete: Boolean,
    val truncatedByRetention: Boolean,
    val truncatedByLimit: Boolean,
)

data class DeltaOptions(
    val enabled: Boolean? = null,
    val algorithms: List<DeltaAlgorithm> = listOf(DeltaAlgorithm.fossil, DeltaAlgorithm.xdelta3),
    val debug: Boolean = false,
    val onStats: ((DeltaStats) -> Unit)? = null,
    val onError: ((Throwable) -> Unit)? = null,
)

data class ChannelDeltaStats(
    val channelName: String,
    val conflationKey: String?,
    val conflationGroupCount: Int,
    val deltaCount: Int,
    val fullMessageCount: Int,
    val totalMessages: Int,
)

data class DeltaStats(
    val totalMessages: Int,
    val deltaMessages: Int,
    val fullMessages: Int,
    val totalBytesWithoutCompression: Int,
    val totalBytesWithCompression: Int,
    val bandwidthSaved: Int,
    val bandwidthSavedPercent: Double,
    val errors: Int,
    val channelCount: Int,
    val channels: List<ChannelDeltaStats>,
)

data class PresenceMember(
    val id: String,
    val info: Any?,
)

data class PresenceHistoryOptions(
    val endpoint: String,
    val headers: Map<String, String> = emptyMap(),
    val headersProvider: (() -> Map<String, String>)? = null,
)

data class VersionedMessagesOptions(
    val endpoint: String,
    val headers: Map<String, String> = emptyMap(),
    val headersProvider: (() -> Map<String, String>)? = null,
)

data class PresenceHistoryParams(
    val direction: String? = null,
    val limit: Int? = null,
    val cursor: String? = null,
    val startSerial: Long? = null,
    val endSerial: Long? = null,
    val startTimeMs: Long? = null,
    val endTimeMs: Long? = null,
    val start: Long? = null,
    val end: Long? = null,
) {
    fun toPayload(): Map<String, Any> =
        buildMap {
            direction?.let { put("direction", it) }
            limit?.let { put("limit", it) }
            cursor?.let { put("cursor", it) }
            startSerial?.let { put("start_serial", it) }
            endSerial?.let { put("end_serial", it) }
            when {
                startTimeMs != null -> put("start_time_ms", startTimeMs)
                start != null -> put("start_time_ms", start)
            }
            when {
                endTimeMs != null -> put("end_time_ms", endTimeMs)
                end != null -> put("end_time_ms", end)
            }
        }
}

data class PresenceSnapshotParams(
    val atTimeMs: Long? = null,
    val at: Long? = null,
    val atSerial: Long? = null,
) {
    fun toPayload(): Map<String, Any> =
        buildMap {
            when {
                atTimeMs != null -> put("at_time_ms", atTimeMs)
                at != null -> put("at_time_ms", at)
            }
            atSerial?.let { put("at_serial", it) }
        }
}

data class PresenceHistoryItem(
    val streamId: String,
    val serial: Long,
    val publishedAtMs: Long,
    val event: String,
    val cause: String,
    val userId: String,
    val connectionId: String?,
    val deadNodeId: String?,
    val payloadSizeBytes: Int,
    val presenceEvent: Map<String, Any?>,
)

data class PresenceHistoryBounds(
    val startSerial: Long?,
    val endSerial: Long?,
    val startTimeMs: Long?,
    val endTimeMs: Long?,
)

data class PresenceHistoryContinuity(
    val streamId: String?,
    val oldestAvailableSerial: Long?,
    val newestAvailableSerial: Long?,
    val oldestAvailablePublishedAtMs: Long?,
    val newestAvailablePublishedAtMs: Long?,
    val retainedEvents: Long,
    val retainedBytes: Long,
    val degraded: Boolean,
    val complete: Boolean,
    val truncatedByRetention: Boolean,
)

data class PresenceSnapshotMember(
    val userId: String,
    val lastEvent: String,
    val lastEventSerial: Long,
    val lastEventAtMs: Long,
)

data class PresenceSnapshot(
    val channel: String,
    val members: List<PresenceSnapshotMember>,
    val memberCount: Int,
    val eventsReplayed: Long,
    val snapshotSerial: Long?,
    val snapshotTimeMs: Long?,
    val continuity: PresenceHistoryContinuity,
)

data class ChannelHistoryParams(
    val direction: String? = null,
    val limit: Int? = null,
    val cursor: String? = null,
    val startSerial: Long? = null,
    val endSerial: Long? = null,
    val startTimeMs: Long? = null,
    val endTimeMs: Long? = null,
) {
    fun toPayload(): Map<String, Any> =
        buildMap {
            direction?.let { put("direction", it) }
            limit?.let { put("limit", it) }
            cursor?.let { put("cursor", it) }
            startSerial?.let { put("start_serial", it) }
            endSerial?.let { put("end_serial", it) }
            startTimeMs?.let { put("start_time_ms", it) }
            endTimeMs?.let { put("end_time_ms", it) }
        }
}

class ChannelHistoryPage internal constructor(
    val items: List<Map<String, Any?>>,
    val direction: String,
    val limit: Int,
    val hasMore: Boolean,
    val nextCursor: String?,
    val bounds: Map<String, Any?>,
    val continuity: Map<String, Any?>,
    private val fetchNext: (suspend (String) -> ChannelHistoryPage)? = null,
) {
    fun hasNext(): Boolean = hasMore && nextCursor != null

    suspend fun next(): ChannelHistoryPage {
        val cursor = nextCursor ?: throw IllegalStateException("No more pages available")
        val callback = fetchNext ?: throw IllegalStateException("No page fetcher configured")
        return callback(cursor)
    }
}

data class MessageVersionsParams(
    val direction: String? = null,
    val limit: Int? = null,
    val cursor: String? = null,
) {
    fun toPayload(): Map<String, Any> =
        buildMap {
            direction?.let { put("direction", it) }
            limit?.let { put("limit", it) }
            cursor?.let { put("cursor", it) }
        }
}

class MessageVersionsPage internal constructor(
    val channel: String,
    val items: List<Map<String, Any?>>,
    val direction: String,
    val limit: Int,
    val hasMore: Boolean,
    val nextCursor: String?,
    private val fetchNext: (suspend (String) -> MessageVersionsPage)? = null,
) {
    fun hasNext(): Boolean = hasMore && nextCursor != null

    suspend fun next(): MessageVersionsPage {
        val cursor = nextCursor ?: throw IllegalStateException("No more pages available")
        val callback = fetchNext ?: throw IllegalStateException("No page fetcher configured")
        return callback(cursor)
    }
}

data class PublishAnnotationRequest(
    val type: String,
    val name: String? = null,
    val clientId: String? = null,
    val socketId: String? = null,
    val count: Long? = null,
    val data: Any? = null,
    val encoding: String? = null,
) {
    fun toPayload(): Map<String, Any?> =
        buildMap {
            put("type", type)
            name?.let { put("name", it) }
            clientId?.let { put("clientId", it) }
            socketId?.let { put("socketId", it) }
            count?.let { put("count", it) }
            data?.let { put("data", it) }
            encoding?.let { put("encoding", it) }
        }
}

data class PublishAnnotationResponse(val annotationSerial: String)

data class DeleteAnnotationResponse(
    val annotationSerial: String,
    val deletedAnnotationSerial: String,
)

data class AnnotationEventsParams(
    val type: String? = null,
    val limit: Int? = null,
    val fromSerial: String? = null,
    val socketId: String? = null,
) {
    fun toPayload(): Map<String, Any> =
        buildMap {
            type?.let { put("type", it) }
            limit?.let { put("limit", it) }
            fromSerial?.let { put("fromSerial", it) }
            socketId?.let { put("socketId", it) }
        }
}

class AnnotationEventsPage internal constructor(
    val channel: String,
    val messageSerial: String,
    val limit: Int,
    val hasMore: Boolean,
    val nextCursor: String?,
    val items: List<Map<String, Any?>>,
    private val fetchNext: (suspend (String) -> AnnotationEventsPage)? = null,
) {
    fun hasNext(): Boolean = hasMore && nextCursor != null

    suspend fun next(): AnnotationEventsPage {
        val cursor = nextCursor ?: throw IllegalStateException("No more pages available")
        val callback = fetchNext ?: throw IllegalStateException("No page fetcher configured")
        return callback(cursor)
    }
}

class PresenceHistoryPage internal constructor(
    val items: List<PresenceHistoryItem>,
    val direction: String,
    val limit: Int,
    val hasMore: Boolean,
    val nextCursor: String?,
    val bounds: PresenceHistoryBounds,
    val continuity: PresenceHistoryContinuity,
    private val fetchNext: (suspend (String) -> PresenceHistoryPage)? = null,
) {
    fun hasNext(): Boolean = hasMore && nextCursor != null

    suspend fun next(): PresenceHistoryPage {
        val cursor = nextCursor
        val fetcher = fetchNext
        if (!hasNext() || cursor == null || fetcher == null) {
            throw SockudoException.InvalidOptions("No more pages available")
        }
        return fetcher(cursor)
    }
}

data class SockudoOptions(
    val cluster: String,
    val protocolVersion: Int = 2,
    val activityTimeout: Duration = 120.seconds,
    val forceTls: Boolean? = null,
    val enabledTransports: List<SockudoTransport>? = null,
    val disabledTransports: List<SockudoTransport>? = null,
    val wsHost: String? = null,
    val wsPort: Int = 80,
    val wssPort: Int = 443,
    val wsPath: String = "",
    val httpHost: String? = null,
    val httpPort: Int = 80,
    val httpsPort: Int = 443,
    val httpPath: String = "/sockudo",
    val pongTimeout: Duration = 30.seconds,
    val unavailableTimeout: Duration = 10.seconds,
    val enableStats: Boolean = false,
    val statsHost: String = "stats.sockudo.io",
    val timelineParams: Map<String, AuthValue> = emptyMap(),
    val channelAuthorization: ChannelAuthorizationOptions = ChannelAuthorizationOptions(),
    val userAuthentication: UserAuthenticationOptions = UserAuthenticationOptions(),
    val presenceHistory: PresenceHistoryOptions? = null,
    val versionedMessages: VersionedMessagesOptions? = null,
    val deltaCompression: DeltaOptions? = null,
    val messageDeduplication: Boolean = true,
    val messageDeduplicationCapacity: Int = 1000,
    val connectionRecovery: Boolean = false,
    val echoMessages: Boolean = true,
    val wireFormat: SockudoWireFormat = SockudoWireFormat.json,
)
