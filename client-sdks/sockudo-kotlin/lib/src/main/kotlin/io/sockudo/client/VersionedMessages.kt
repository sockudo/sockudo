package io.sockudo.client

enum class MutableMessageAction(val value: String) {
    CREATE("message.create"),
    UPDATE("message.update"),
    DELETE("message.delete"),
    APPEND("message.append");

    companion object {
        fun fromValue(value: String): MutableMessageAction? =
            entries.firstOrNull { it.value == value }
    }
}

data class MutableMessageVersionInfo(
    val action: MutableMessageAction,
    val event: String,
    val messageSerial: String,
    val versionSerial: String? = null,
    val historySerial: Long? = null,
    val versionTimestampMs: Long? = null,
)

data class MutableMessageState(
    val messageSerial: String,
    val action: MutableMessageAction,
    val data: Any?,
    val event: String,
    val serial: Long? = null,
    val streamId: String? = null,
    val messageId: String? = null,
    val versionSerial: String? = null,
    val historySerial: Long? = null,
    val versionTimestampMs: Long? = null,
)

data class VersionedMessageCreateRequest(
    val eventName: String,
    val data: Any?,
    val extras: MessageExtras? = null,
    val socketId: String? = null,
    val idempotencyKey: String? = null,
) {
    internal fun toPayload(): Map<String, Any?> =
        linkedMapOf<String, Any?>(
            "event" to eventName,
            "data" to data,
            "extras" to extras?.toWireMap(),
            "socket_id" to socketId,
            "idempotency_key" to idempotencyKey,
        ).filterValues { it != null }
}

data class VersionedMessageAppendRequest(
    val messageSerial: String,
    val data: Any?,
    val extras: MessageExtras? = null,
    val socketId: String? = null,
    val idempotencyKey: String? = null,
    val expectedVersionSerial: String? = null,
) {
    internal fun toPayload(): Map<String, Any?> =
        linkedMapOf<String, Any?>(
            "message_serial" to messageSerial,
            "data" to data,
            "extras" to extras?.toWireMap(),
            "socket_id" to socketId,
            "idempotency_key" to idempotencyKey,
            "expected_version_serial" to expectedVersionSerial,
        ).filterValues { it != null }
}

data class VersionedMessageUpdateRequest(
    val messageSerial: String,
    val data: Any?,
    val extras: MessageExtras? = null,
    val socketId: String? = null,
    val idempotencyKey: String? = null,
    val expectedVersionSerial: String? = null,
) {
    internal fun toPayload(): Map<String, Any?> =
        linkedMapOf<String, Any?>(
            "message_serial" to messageSerial,
            "data" to data,
            "extras" to extras?.toWireMap(),
            "socket_id" to socketId,
            "idempotency_key" to idempotencyKey,
            "expected_version_serial" to expectedVersionSerial,
        ).filterValues { it != null }
}

data class VersionedMessageDeleteRequest(
    val messageSerial: String,
    val data: Any? = null,
    val extras: MessageExtras? = null,
    val socketId: String? = null,
    val idempotencyKey: String? = null,
    val expectedVersionSerial: String? = null,
) {
    internal fun toPayload(): Map<String, Any?> =
        linkedMapOf<String, Any?>(
            "message_serial" to messageSerial,
            "data" to data,
            "extras" to extras?.toWireMap(),
            "socket_id" to socketId,
            "idempotency_key" to idempotencyKey,
            "expected_version_serial" to expectedVersionSerial,
        ).filterValues { it != null }
}

data class VersionedMessageCreateAck(
    val messageSerial: String,
    val versionSerial: String? = null,
    val historySerial: Long? = null,
    val deliverySerial: Long? = null,
    val eventName: String? = null,
    val raw: Map<String, Any?> = emptyMap(),
)

data class VersionedMessageAppendAck(
    val messageSerial: String,
    val versionSerial: String? = null,
    val historySerial: Long? = null,
    val deliverySerial: Long? = null,
    val eventName: String? = null,
    val raw: Map<String, Any?> = emptyMap(),
)

data class VersionedMessageUpdateAck(
    val messageSerial: String,
    val versionSerial: String? = null,
    val historySerial: Long? = null,
    val deliverySerial: Long? = null,
    val eventName: String? = null,
    val raw: Map<String, Any?> = emptyMap(),
)

data class VersionedMessageDeleteAck(
    val messageSerial: String,
    val versionSerial: String? = null,
    val historySerial: Long? = null,
    val deliverySerial: Long? = null,
    val eventName: String? = null,
    val raw: Map<String, Any?> = emptyMap(),
)

internal data class VersionedMessageAckFields(
    val messageSerial: String,
    val versionSerial: String?,
    val historySerial: Long?,
    val deliverySerial: Long?,
    val eventName: String?,
    val raw: Map<String, Any?>,
)

internal fun decodeVersionedMessageAckFields(payload: Map<String, Any?>): VersionedMessageAckFields {
    val ack =
        (payload["ack"] as? Map<*, *>)
            ?: (payload["item"] as? Map<*, *>)
            ?: payload
    val normalized = ack.mapKeys { it.key.toString() }
    return VersionedMessageAckFields(
        messageSerial =
            normalized["message_serial"] as? String
                ?: normalized["messageSerial"] as? String
                ?: "",
        versionSerial =
            normalized["version_serial"] as? String
                ?: normalized["versionSerial"] as? String,
        historySerial =
            parseSockudoLong(normalized["history_serial"])
                ?: parseSockudoLong(normalized["historySerial"]),
        deliverySerial =
            parseSockudoLong(normalized["delivery_serial"])
                ?: parseSockudoLong(normalized["deliverySerial"]),
        eventName =
            normalized["event"] as? String
                ?: normalized["eventName"] as? String
                ?: normalized["name"] as? String,
        raw = normalized,
    )
}

private fun parseNumericHeader(value: Any?): Long? {
    return parseSockudoLong(value)
}

fun isMutableMessageEvent(event: SockudoEvent): Boolean =
    getMutableMessageInfo(event) != null

fun getMutableMessageInfo(event: SockudoEvent): MutableMessageVersionInfo? {
    val headers = event.extras?.headers ?: return null
    val actionRaw = headers["sockudo_action"] as? String ?: return null
    val messageSerial = headers["sockudo_message_serial"] as? String ?: return null
    val action = MutableMessageAction.fromValue(actionRaw) ?: return null

    val versionSerial = headers["sockudo_version_serial"] as? String
    val historySerial = parseNumericHeader(headers["sockudo_history_serial"])
    val versionTimestampMs = parseNumericHeader(headers["sockudo_version_timestamp_ms"])

    return MutableMessageVersionInfo(
        action = action,
        event = event.event,
        messageSerial = messageSerial,
        versionSerial = versionSerial,
        historySerial = historySerial,
        versionTimestampMs = versionTimestampMs,
    )
}

fun reduceMutableMessageEvent(
    current: MutableMessageState?,
    event: SockudoEvent,
): MutableMessageState {
    val info = getMutableMessageInfo(event)
        ?: error("Event is not a mutable-message event")

    if (current != null && current.messageSerial != info.messageSerial) {
        error(
            "Mutable-message reducer expected message_serial '${current.messageSerial}'" +
                " but received '${info.messageSerial}'"
        )
    }

    val nextData: Any? = when (info.action) {
        MutableMessageAction.APPEND -> {
            val base = current?.data as? String
                ?: error(
                    "message.append requires an existing string base;" +
                        " seed state from a create/update payload or latest-view history first"
                )
            val fragment = event.data as? String
                ?: error(
                    "message.append payload must be a string fragment when applying" +
                        " client-side concatenation"
                )
            base + fragment
        }
        MutableMessageAction.DELETE,
        MutableMessageAction.CREATE,
        MutableMessageAction.UPDATE -> event.data
    }

    return MutableMessageState(
        messageSerial = info.messageSerial,
        action = info.action,
        data = nextData,
        event = info.event,
        serial = event.serial,
        streamId = event.streamId,
        messageId = event.messageId,
        versionSerial = info.versionSerial,
        historySerial = info.historySerial,
        versionTimestampMs = info.versionTimestampMs,
    )
}

fun reduceMutableMessageEvents(events: List<SockudoEvent>): MutableMessageState? {
    var state: MutableMessageState? = null
    for (event in events) {
        state = reduceMutableMessageEvent(state, event)
    }
    return state
}
