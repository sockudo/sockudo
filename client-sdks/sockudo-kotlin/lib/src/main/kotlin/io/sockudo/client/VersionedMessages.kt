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

private fun parseNumericHeader(value: Any?): Long? {
    return when (value) {
        is Long -> value
        is Int -> value.toLong()
        is Double -> if (value == kotlin.math.floor(value)) value.toLong() else null
        is String -> value.trim().toDoubleOrNull()?.let { d ->
            if (d == kotlin.math.floor(d)) d.toLong() else null
        }
        else -> null
    }
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
