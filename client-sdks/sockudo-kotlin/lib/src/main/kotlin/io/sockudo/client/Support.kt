package io.sockudo.client

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.util.UUID

data class EventMetadata(val userId: String? = null)

data class EventBindingToken internal constructor(
    internal val id: String = UUID.randomUUID().toString(),
)

sealed class SockudoException(message: String, cause: Throwable? = null) : RuntimeException(message, cause) {
    data object InvalidAppKey : SockudoException("You must pass your app key when you instantiate SockudoClient.")
    class InvalidOptions(message: String) : SockudoException(message)
    class UnsupportedFeature(message: String) : SockudoException(message)
    class BadEventName(message: String) : SockudoException(message)
    class BadChannelName(message: String) : SockudoException(message)
    class MessageParseError(message: String) : SockudoException(message)
    class AuthFailure(val statusCode: Int?, message: String) : SockudoException(message)
    data object InvalidHandshake : SockudoException("Invalid handshake")
    class DecryptionFailure(message: String) : SockudoException(message)
    class DeltaFailure(message: String) : SockudoException(message)
    class InvalidUrl(message: String) : SockudoException(message)
    data object ConnectionUnavailable : SockudoException("Connection unavailable")
}

object JsonSupport {
    val json: Json =
        Json {
            ignoreUnknownKeys = true
            isLenient = true
        }

    fun encode(value: Any?): String = json.encodeToString(JsonElement.serializer(), toJsonElement(value))

    fun decode(text: String): JsonElement = json.parseToJsonElement(text)

    fun toJsonElement(value: Any?): JsonElement =
        when (value) {
            null -> JsonNull
            is JsonElement -> value
            is String -> JsonPrimitive(value)
            is Number -> JsonPrimitive(value)
            is Boolean -> JsonPrimitive(value)
            is Map<*, *> ->
                JsonObject(
                    value.entries.associate { (key, entryValue) ->
                        key.toString() to toJsonElement(entryValue)
                    },
                )

            is Iterable<*> -> JsonArray(value.map(::toJsonElement))
            is Array<*> -> JsonArray(value.map(::toJsonElement))
            is AuthValue -> JsonPrimitive(value.stringValue)
            else -> JsonPrimitive(value.toString())
        }

    fun fromJsonElement(value: JsonElement): Any? =
        when (value) {
            JsonNull -> null
            is JsonObject -> value.mapValues { fromJsonElement(it.value) }
            is JsonArray -> value.map(::fromJsonElement)
            is JsonPrimitive ->
                when {
                    value.isString -> value.content
                    value.content == "true" || value.content == "false" -> value.content.toBoolean()
                    value.content.toLongOrNull() != null -> value.content.toLong()
                    value.content.toDoubleOrNull() != null -> value.content.toDouble()
                    else -> value.content
                }
        }
}

object QueryString {
    fun encode(params: Map<String, AuthValue>): String =
        params.entries
            .sortedBy { it.key }
            .joinToString("&") { (key, value) ->
                "${percentEncode(key)}=${percentEncode(value.stringValue)}"
            }

    private fun percentEncode(value: String): String = URLEncoder.encode(value, StandardCharsets.UTF_8)
}

internal class EventDispatcher(
    private val failThrough: ((String, Any?) -> Unit)? = null,
) {
    private val callbacks = linkedMapOf<String, LinkedHashMap<EventBindingToken, (Any?, EventMetadata?) -> Unit>>()
    private val globalCallbacks = linkedMapOf<EventBindingToken, (String, Any?) -> Unit>()

    fun bind(eventName: String, callback: (Any?, EventMetadata?) -> Unit): EventBindingToken {
        val token = EventBindingToken()
        callbacks.getOrPut(eventName) { linkedMapOf() }[token] = callback
        return token
    }

    fun bindGlobal(callback: (String, Any?) -> Unit): EventBindingToken {
        val token = EventBindingToken()
        globalCallbacks[token] = callback
        return token
    }

    fun unbind(eventName: String? = null, token: EventBindingToken? = null) {
        when {
            eventName != null && token == null -> callbacks.remove(eventName)
            eventName != null && token != null -> {
                callbacks[eventName]?.remove(token)
                if (callbacks[eventName].isNullOrEmpty()) {
                    callbacks.remove(eventName)
                }
            }

            token != null -> {
                callbacks.values.forEach { it.remove(token) }
                callbacks.entries.removeIf { it.value.isEmpty() }
                globalCallbacks.remove(token)
            }

            else -> {
                callbacks.clear()
                globalCallbacks.clear()
            }
        }
    }

    fun emit(eventName: String, data: Any?, metadata: EventMetadata? = null) {
        globalCallbacks.values.forEach { it(eventName, data) }
        val listeners = callbacks[eventName]
        if (listeners.isNullOrEmpty()) {
            failThrough?.invoke(eventName, data)
            return
        }
        listeners.values.forEach { it(data, metadata) }
    }
}

object SockudoLogger {
    @Volatile
    var logToConsole: Boolean = false

    @Volatile
    var customLog: ((String) -> Unit)? = null

    fun debug(vararg parts: Any?) = log("DEBUG", *parts)

    fun warn(vararg parts: Any?) = log("WARN", *parts)

    fun error(vararg parts: Any?) = log("ERROR", *parts)

    private fun log(level: String, vararg parts: Any?) {
        val message = buildString {
            append("[Sockudo:$level] ")
            append(parts.joinToString(" ") { it?.toString().orEmpty() })
        }
        customLog?.invoke(message)
        if (logToConsole) {
            println(message)
        }
    }
}

data class SockudoEvent(
    val event: String,
    val channel: String? = null,
    val data: Any? = null,
    val userId: String? = null,
    val streamId: String? = null,
    val messageId: String? = null,
    val rawMessage: String,
    val sequence: Int? = null,
    val conflationKey: String? = null,
    val serial: Long? = null,
    val extras: MessageExtras? = null,
)
