package io.sockudo.client

import com.google.protobuf.CodedInputStream
import com.google.protobuf.CodedOutputStream
import com.google.protobuf.WireFormat
import kotlinx.serialization.json.JsonObject
import org.msgpack.core.MessagePack

internal data class DecodedEnvelope(
    val envelope: Map<String, Any?>,
    val rawMessage: String,
)

private val messagePackEnvelopeFields =
    listOf(
        "event",
        "channel",
        "data",
        "name",
        "user_id",
        "tags",
        "sequence",
        "conflation_key",
        "message_id",
        "stream_id",
        "serial",
        "idempotency_key",
        "extras",
        "__delta_seq",
        "__conflation_key",
    )

internal object ProtocolCodec {
    fun encodeEnvelope(
        envelope: Map<String, Any?>,
        format: SockudoWireFormat,
    ): Any =
        when (format) {
            SockudoWireFormat.json -> JsonSupport.encode(envelope)
            SockudoWireFormat.messagepack -> encodeMessagePack(envelope)
            SockudoWireFormat.protobuf -> encodeProtobuf(envelope)
        }

    fun decodeEnvelope(
        rawMessage: Any,
        format: SockudoWireFormat,
    ): DecodedEnvelope =
        when (format) {
            SockudoWireFormat.json -> {
                val rawText =
                    when (rawMessage) {
                        is String -> rawMessage
                        is okio.ByteString -> rawMessage.utf8()
                        else -> throw SockudoException.MessageParseError("Unsupported socket payload type: ${rawMessage::class.simpleName}")
                    }
                val envelope = JsonSupport.fromJsonElement(JsonSupport.decode(rawText)) as? Map<*, *>
                    ?: throw SockudoException.MessageParseError("Unable to decode event envelope")
                DecodedEnvelope(normalizeMap(envelope), rawText)
            }

            SockudoWireFormat.messagepack -> {
                val envelope = decodeMessagePack(rawMessage)
                DecodedEnvelope(envelope, JsonSupport.encode(envelope))
            }

            SockudoWireFormat.protobuf -> {
                val envelope = decodeProtobuf(rawMessage)
                DecodedEnvelope(envelope, JsonSupport.encode(envelope))
            }
        }

    fun decodeEvent(
        rawMessage: Any,
        format: SockudoWireFormat,
    ): SockudoEvent {
        val decoded = decodeEnvelope(rawMessage, format)
        val envelope = decoded.envelope
        val eventName = envelope["event"] as? String
            ?: throw SockudoException.MessageParseError("Unable to decode event envelope")
        val rawData = envelope["data"]
        val data =
            when (rawData) {
                is String -> runCatching {
                    JsonSupport.fromJsonElement(JsonSupport.decode(rawData))
                }.getOrElse { rawData }
                else -> rawData
            }
        return SockudoEvent(
            event = eventName,
            channel = envelope["channel"] as? String,
            data = data,
            userId = envelope["user_id"] as? String,
            streamId = envelope["stream_id"] as? String,
            messageId = envelope["message_id"] as? String,
            rawMessage = decoded.rawMessage,
            sequence = (envelope["__delta_seq"] ?: envelope["sequence"]).asInt(),
            conflationKey = (envelope["__conflation_key"] ?: envelope["conflation_key"]) as? String,
            serial = envelope["serial"].asLong(),
            extras = decodeExtras(envelope["extras"]),
        )
    }

    private fun encodeMessagePack(envelope: Map<String, Any?>): ByteArray {
        val packer = MessagePack.newDefaultBufferPacker()
        packValue(
            packer,
            listOf(
                envelope["event"],
                envelope["channel"],
                encodeMessagePackData(envelope["data"]),
                envelope["name"],
                envelope["user_id"],
                envelope["tags"],
                envelope["sequence"],
                envelope["conflation_key"],
                envelope["message_id"],
                envelope["stream_id"],
                envelope["serial"],
                envelope["idempotency_key"],
                encodeMessagePackExtras(envelope["extras"]),
                envelope["__delta_seq"],
                envelope["__conflation_key"],
            ),
        )
        packer.close()
        return packer.toByteArray()
    }

    private fun decodeMessagePack(rawMessage: Any): Map<String, Any?> {
        val unpacker = MessagePack.newDefaultUnpacker(rawMessage.asByteArray())
        val value = unpackValue(unpacker)
        unpacker.close()
        return when (value) {
            is List<*> -> {
                buildMap {
                    messagePackEnvelopeFields.forEachIndexed { index, field ->
                        if (index < value.size) {
                            decodeMessagePackValue(value[index])?.let { put(field, it) }
                        }
                    }
                }
            }
            is Map<*, *> ->
                normalizeMap(
                    decodeMessagePackValue(value) as? Map<*, *>
                        ?: throw SockudoException.MessageParseError("Unable to decode event envelope"),
                )
            else -> throw SockudoException.MessageParseError("Unable to decode event envelope")
        }
    }

    private fun encodeMessagePackData(value: Any?): Any? =
        when (value) {
            null -> null
            is String -> listOf("string", value)
            else -> listOf("json", JsonSupport.encode(value))
        }

    private fun encodeMessagePackExtras(rawExtras: Any?): Any? {
        val extras = decodeExtras(rawExtras) ?: return null
        return buildMap<String, Any?> {
            extras.headers?.let { headers ->
                put(
                    "headers",
                    headers.mapValues { (_, value) ->
                        when (value) {
                            is Number -> listOf("number", value.toDouble())
                            is Boolean -> listOf("bool", value)
                            else -> listOf("string", value.toString())
                        }
                    },
                )
            }
            extras.ephemeral?.let { put("ephemeral", it) }
            extras.idempotencyKey?.let { put("idempotency_key", it) }
            extras.echo?.let { put("echo", it) }
        }
    }

    private fun decodeMessagePackValue(value: Any?): Any? =
        when (value) {
            is List<*> ->
                if (value.size == 2 && value.firstOrNull() is String) {
                    when (value.first() as String) {
                        "string", "json", "number", "bool" -> value[1]
                        "structured" -> decodeMessagePackValue(value[1])
                        else -> value.map { decodeMessagePackValue(it) }
                    }
                } else {
                    value.map { decodeMessagePackValue(it) }
                }
            is Map<*, *> -> value.entries.associate { (key, entryValue) -> key.toString() to decodeMessagePackValue(entryValue) }
            else -> value
        }

    private fun encodeProtobuf(envelope: Map<String, Any?>): ByteArray {
        val output = com.google.protobuf.ByteString.newOutput()
        val writer = CodedOutputStream.newInstance(output)

        (envelope["event"] as? String)?.let {
            writer.writeString(1, it)
        }
        (envelope["channel"] as? String)?.let {
            writer.writeString(2, it)
        }
        envelope["data"]?.let { data ->
            writer.writeTag(3, WireFormat.WIRETYPE_LENGTH_DELIMITED)
            val nested = com.google.protobuf.ByteString.newOutput()
            val nestedWriter = CodedOutputStream.newInstance(nested)
            if (data is String) {
                nestedWriter.writeString(1, data)
            } else {
                nestedWriter.writeString(3, JsonSupport.encode(data))
            }
            nestedWriter.flush()
            val bytes = nested.toByteString()
            writer.writeUInt32NoTag(bytes.size())
            writer.writeRawBytes(bytes)
        }
        (envelope["user_id"] as? String)?.let {
            writer.writeString(5, it)
        }
        envelope["sequence"].asLong()?.let {
            writer.writeUInt64(7, it)
        }
        (envelope["conflation_key"] as? String)?.let {
            writer.writeString(8, it)
        }
        (envelope["message_id"] as? String)?.let {
            writer.writeString(9, it)
        }
        (envelope["stream_id"] as? String)?.let {
            writer.writeString(15, it)
        }
        envelope["serial"].asLong()?.let {
            writer.writeUInt64(10, it)
        }
        encodeExtras(envelope["extras"])?.let { extras ->
            writer.writeTag(12, WireFormat.WIRETYPE_LENGTH_DELIMITED)
            writer.writeUInt32NoTag(extras.size)
            writer.writeRawBytes(extras)
        }
        envelope["__delta_seq"].asLong()?.let {
            writer.writeUInt64(13, it)
        }
        (envelope["__conflation_key"] as? String)?.let {
            writer.writeString(14, it)
        }

        writer.flush()
        return output.toByteString().toByteArray()
    }

    private fun decodeProtobuf(rawMessage: Any): Map<String, Any?> {
        val input = CodedInputStream.newInstance(rawMessage.asByteArray())
        val envelope = linkedMapOf<String, Any?>()
        while (!input.isAtEnd) {
            when (val tag = input.readTag()) {
                0 -> break
                10 -> envelope["event"] = input.readString()
                18 -> envelope["channel"] = input.readString()
                26 -> envelope["data"] = decodeProtoData(input.readByteArray())
                42 -> envelope["user_id"] = input.readString()
                56 -> envelope["sequence"] = input.readUInt64().toLong()
                66 -> envelope["conflation_key"] = input.readString()
                74 -> envelope["message_id"] = input.readString()
                80 -> envelope["serial"] = input.readUInt64().toLong()
                98 -> envelope["extras"] = decodeProtoExtras(input.readByteArray())
                104 -> envelope["__delta_seq"] = input.readUInt64().toLong()
                114 -> envelope["__conflation_key"] = input.readString()
                122 -> envelope["stream_id"] = input.readString()
                else -> input.skipField(tag)
            }
        }
        return envelope
    }

    private fun decodeProtoData(bytes: ByteArray): Any? {
        val input = CodedInputStream.newInstance(bytes)
        while (!input.isAtEnd) {
            when (val tag = input.readTag()) {
                0 -> break
                10 -> return input.readString()
                18 -> return decodeStructuredData(input.readByteArray())
                26 -> {
                    val raw = input.readString()
                    return runCatching {
                        JsonSupport.fromJsonElement(JsonSupport.decode(raw))
                    }.getOrElse { raw }
                }
                else -> input.skipField(tag)
            }
        }
        return null
    }

    private fun decodeStructuredData(bytes: ByteArray): Map<String, Any?> {
        val input = CodedInputStream.newInstance(bytes)
        val structured = linkedMapOf<String, Any?>()
        while (!input.isAtEnd) {
            when (val tag = input.readTag()) {
                0 -> break
                10 -> structured["channel_data"] = input.readString()
                18 -> structured["channel"] = input.readString()
                26 -> structured["user_data"] = input.readString()
                34 -> {
                    val entryInput = CodedInputStream.newInstance(input.readByteArray())
                    var key = ""
                    var value = ""
                    while (!entryInput.isAtEnd) {
                        when (val entryTag = entryInput.readTag()) {
                            0 -> break
                            10 -> key = entryInput.readString()
                            18 -> value = entryInput.readString()
                            else -> entryInput.skipField(entryTag)
                        }
                    }
                    val extra = (structured["extra"] as? MutableMap<String, String>) ?: linkedMapOf<String, String>().also {
                        structured["extra"] = it
                    }
                    extra[key] = value
                }
                else -> input.skipField(tag)
            }
        }
        return structured
    }

    private fun encodeExtras(rawExtras: Any?): ByteArray? {
        val extras = decodeExtras(rawExtras) ?: return null
        val output = com.google.protobuf.ByteString.newOutput()
        val writer = CodedOutputStream.newInstance(output)
        extras.headers?.forEach { (key, value) ->
            writer.writeTag(1, WireFormat.WIRETYPE_LENGTH_DELIMITED)
            val entryOutput = com.google.protobuf.ByteString.newOutput()
            val entryWriter = CodedOutputStream.newInstance(entryOutput)
            entryWriter.writeString(1, key)
            entryWriter.writeTag(2, WireFormat.WIRETYPE_LENGTH_DELIMITED)
            val valueBytes = encodeExtrasValue(value)
            entryWriter.writeUInt32NoTag(valueBytes.size)
            entryWriter.writeRawBytes(valueBytes)
            entryWriter.flush()
            val entryBytes = entryOutput.toByteString()
            writer.writeUInt32NoTag(entryBytes.size())
            writer.writeRawBytes(entryBytes)
        }
        extras.ephemeral?.let { writer.writeBool(2, it) }
        extras.idempotencyKey?.let { writer.writeString(3, it) }
        extras.echo?.let { writer.writeBool(4, it) }
        writer.flush()
        return output.toByteString().toByteArray()
    }

    private fun decodeProtoExtras(bytes: ByteArray): Map<String, Any?> {
        val input = CodedInputStream.newInstance(bytes)
        val extras = linkedMapOf<String, Any?>()
        val headers = linkedMapOf<String, Any>()
        while (!input.isAtEnd) {
            when (val tag = input.readTag()) {
                0 -> break
                10 -> {
                    val entryInput = CodedInputStream.newInstance(input.readByteArray())
                    var key = ""
                    var value: Any = ""
                    while (!entryInput.isAtEnd) {
                        when (val entryTag = entryInput.readTag()) {
                            0 -> break
                            10 -> key = entryInput.readString()
                            18 -> value = decodeExtrasValue(entryInput.readByteArray())
                            else -> entryInput.skipField(entryTag)
                        }
                    }
                    headers[key] = value
                }
                16 -> extras["ephemeral"] = input.readBool()
                26 -> extras["idempotency_key"] = input.readString()
                32 -> extras["echo"] = input.readBool()
                else -> input.skipField(tag)
            }
        }
        if (headers.isNotEmpty()) {
            extras["headers"] = headers
        }
        return extras
    }

    private fun encodeExtrasValue(value: Any): ByteArray {
        val output = com.google.protobuf.ByteString.newOutput()
        val writer = CodedOutputStream.newInstance(output)
        when (value) {
            is Number -> writer.writeDouble(2, value.toDouble())
            is Boolean -> writer.writeBool(3, value)
            else -> writer.writeString(1, value.toString())
        }
        writer.flush()
        return output.toByteString().toByteArray()
    }

    private fun decodeExtrasValue(bytes: ByteArray): Any {
        val input = CodedInputStream.newInstance(bytes)
        while (!input.isAtEnd) {
            when (val tag = input.readTag()) {
                0 -> break
                10 -> return input.readString()
                17 -> return input.readDouble()
                24 -> return input.readBool()
                else -> input.skipField(tag)
            }
        }
        return ""
    }

    private fun decodeExtras(rawExtras: Any?): MessageExtras? {
        val extrasMap =
            when (rawExtras) {
                null -> return null
                is MessageExtras -> return rawExtras
                is Map<*, *> -> normalizeMap(rawExtras)
                else -> return null
            }
        val headers =
            (extrasMap["headers"] as? Map<*, *>)?.mapNotNull { (key, value) ->
                val keyString = key as? String ?: return@mapNotNull null
                if (value == null) {
                    null
                } else {
                    keyString to value
                }
            }?.toMap()
        return MessageExtras(
            headers = headers,
            ephemeral = extrasMap["ephemeral"] as? Boolean,
            idempotencyKey = (extrasMap["idempotency_key"] ?: extrasMap["idempotencyKey"]) as? String,
            echo = extrasMap["echo"] as? Boolean,
        )
    }

    private fun packValue(
        packer: org.msgpack.core.MessagePacker,
        value: Any?,
    ) {
        when (value) {
            null -> packer.packNil()
            is String -> packer.packString(value)
            is Boolean -> packer.packBoolean(value)
            is Byte, is Short, is Int, is Long -> packer.packLong((value as Number).toLong())
            is Float, is Double -> packer.packDouble((value as Number).toDouble())
            is MessageExtras ->
                packValue(
                    packer,
                    linkedMapOf<String, Any?>().apply {
                        value.headers?.let { put("headers", it) }
                        value.ephemeral?.let { put("ephemeral", it) }
                        value.idempotencyKey?.let { put("idempotency_key", it) }
                        value.echo?.let { put("echo", it) }
                    },
                )
            is Map<*, *> -> {
                packer.packMapHeader(value.size)
                value.forEach { (key, entryValue) ->
                    packer.packString(key.toString())
                    packValue(packer, entryValue)
                }
            }
            is Iterable<*> -> {
                val items = value.toList()
                packer.packArrayHeader(items.size)
                items.forEach { packValue(packer, it) }
            }
            is Array<*> -> {
                packer.packArrayHeader(value.size)
                value.forEach { packValue(packer, it) }
            }
            is AuthValue -> packer.packString(value.stringValue)
            else -> packer.packString(value.toString())
        }
    }

    private fun unpackValue(unpacker: org.msgpack.core.MessageUnpacker): Any? {
        return when (unpacker.nextFormat.valueType) {
            org.msgpack.value.ValueType.NIL -> {
                unpacker.unpackNil()
                null
            }
            org.msgpack.value.ValueType.BOOLEAN -> unpacker.unpackBoolean()
            org.msgpack.value.ValueType.INTEGER -> unpacker.unpackLong()
            org.msgpack.value.ValueType.FLOAT -> unpacker.unpackDouble()
            org.msgpack.value.ValueType.STRING -> unpacker.unpackString()
            org.msgpack.value.ValueType.BINARY -> unpacker.readPayload(unpacker.unpackBinaryHeader())
            org.msgpack.value.ValueType.ARRAY -> {
                val size = unpacker.unpackArrayHeader()
                List(size) { unpackValue(unpacker) }
            }
            org.msgpack.value.ValueType.MAP -> {
                val size = unpacker.unpackMapHeader()
                linkedMapOf<String, Any?>().apply {
                    repeat(size) {
                        put(unpackValue(unpacker).toString(), unpackValue(unpacker))
                    }
                }
            }
            else -> throw SockudoException.MessageParseError("Unsupported MessagePack value")
        }
    }

    private fun normalizeMap(value: Map<*, *>): Map<String, Any?> =
        value.mapKeys { it.key.toString() }.mapValues { (_, entryValue) ->
            when (entryValue) {
                is Map<*, *> -> normalizeMap(entryValue)
                is List<*> -> entryValue.map { item -> if (item is Map<*, *>) normalizeMap(item) else item }
                is JsonObject -> JsonSupport.fromJsonElement(entryValue)
                else -> entryValue
            }
        }

    private fun Any.asByteArray(): ByteArray =
        when (this) {
            is ByteArray -> this
            is okio.ByteString -> toByteArray()
            is String -> encodeToByteArray()
            else -> throw SockudoException.MessageParseError("Unsupported socket payload type: ${this::class.simpleName}")
        }

    private fun Any?.asInt(): Int? =
        when (this) {
            is Int -> this
            is Long -> toInt()
            is Number -> toInt()
            else -> null
        }

    private fun Any?.asLong(): Long? =
        when (this) {
            is Long -> this
            is Int -> toLong()
            is Number -> toLong()
            else -> null
        }
}
