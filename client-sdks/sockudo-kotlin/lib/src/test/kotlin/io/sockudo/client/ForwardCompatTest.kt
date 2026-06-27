package io.sockudo.client

import java.lang.reflect.InvocationTargetException
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ForwardCompatTest {
    @Test
    fun replaysForwardCompatFixturesThroughClientDispatchWithoutCorruptingChannelState() {
        val client = testClient(connectionRecovery = true)
        val channel = client.subscribe("private-ai-forward")
        val knownEvents = mutableListOf<Any?>()
        val globalEvents = mutableListOf<Pair<String, Any?>>()
        val errors = mutableListOf<Any?>()

        channel.bind("app-known") { data, _ -> knownEvents += data }
        channel.bindGlobal { eventName, data -> globalEvents += eventName to data }
        client.bind("error") { data, _ -> errors += data }

        replayRaw(client, """{"event":"app-known","channel":"private-ai-forward","data":{"before":true}}""")
        val beforeState =
            Triple(
                channel.isSubscribed,
                channel.subscriptionPending,
                channel.subscriptionCancelled,
            )

        fixtureNames.forEach { name -> replayRaw(client, fixtureRaw(name)) }

        replayRaw(client, """{"event":"app-known","channel":"private-ai-forward","data":{"after":true}}""")

        assertEquals(2, knownEvents.size)
        assertEquals(
            beforeState,
            Triple(
                channel.isSubscribed,
                channel.subscriptionPending,
                channel.subscriptionCancelled,
            ),
        )
        assertEquals(9_007_199_254_740_993L, client.getRecoveryPosition("private-ai-forward")?.serial)
        assertTrue(globalEvents.any { it.first == "sockudo:future_event" })
        assertTrue(globalEvents.any { it.first == "sockudo:message.future" })
        assertTrue(globalEvents.any { it.first == "ai-output" })
        assertEquals(1, errors.count { it is SockudoException.MessageParseError })
    }

    @Test
    fun preservesAiAndUnknownExtrasFromForwardCompatFixture() {
        val decoded = ProtocolCodec.decodeEvent(fixtureRaw("unknown-ai-extras.json"), SockudoWireFormat.json)
        val transport = decoded.extras?.ai?.get("transport") as? Map<*, *>
        val codec = decoded.extras?.ai?.get("codec") as? Map<*, *>

        assertEquals("turn-1", transport?.get("turn-id"))
        assertEquals("streaming", transport?.get("status"))
        assertEquals("opaque", codec?.get("provider-future-key"))
        assertEquals(true, decoded.extras?.raw?.get("futureExtrasField"))
        assertEquals(true, decoded.extras?.get("futureExtrasField"))
    }

    @Test
    fun decodesSerialBoundariesWithoutIntOrDoubleTruncation() {
        val overInt = 2_147_483_648L
        val overSafe = 9_007_199_254_740_993L

        val jsonOverInt =
            ProtocolCodec.decodeEvent(
                """{"event":"sockudo:test","channel":"private-ai-forward","serial":$overInt}""",
                SockudoWireFormat.json,
            )
        val jsonOverSafe =
            ProtocolCodec.decodeEvent(
                """{"event":"sockudo:test","channel":"private-ai-forward","serial":"$overSafe"}""",
                SockudoWireFormat.json,
            )
        val msgpackOverSafe =
            ProtocolCodec.decodeEvent(
                ProtocolCodec.encodeEnvelope(
                    linkedMapOf(
                        "event" to "sockudo:test",
                        "channel" to "private-ai-forward",
                        "serial" to overSafe.toString(),
                    ),
                    SockudoWireFormat.messagepack,
                ),
                SockudoWireFormat.messagepack,
            )
        val protobufOverSafe =
            ProtocolCodec.decodeEvent(
                ProtocolCodec.encodeEnvelope(
                    linkedMapOf(
                        "event" to "sockudo:test",
                        "channel" to "private-ai-forward",
                        "stream_id" to "stream-1",
                        "serial" to overSafe,
                    ),
                    SockudoWireFormat.protobuf,
                ),
                SockudoWireFormat.protobuf,
            )

        assertEquals(overInt, jsonOverInt.serial)
        assertEquals(overSafe, jsonOverSafe.serial)
        assertEquals(overSafe, msgpackOverSafe.serial)
        assertEquals(overSafe, protobufOverSafe.serial)
        assertEquals("stream-1", protobufOverSafe.streamId)
    }

    @Test
    fun parsesVersionedIntegerStringHeadersExactlyAndIgnoresUnknownActions() {
        val event =
            SockudoEvent(
                event = "sockudo:message.update",
                rawMessage = "{}",
                extras =
                    MessageExtras(
                        headers =
                            mapOf(
                                "sockudo_action" to "message.update",
                                "sockudo_message_serial" to "msg-1",
                                "sockudo_history_serial" to "9007199254740993",
                                "sockudo_version_timestamp_ms" to "1710000000000",
                            ),
                    ),
            )
        val futureAction =
            event.copy(
                event = "sockudo:message.future",
                extras =
                    MessageExtras(
                        headers =
                            mapOf(
                                "sockudo_action" to "message.future",
                                "sockudo_message_serial" to "msg-1",
                                "sockudo_history_serial" to "9007199254740993",
                            ),
                    ),
            )

        val info = getMutableMessageInfo(event)

        assertEquals(MutableMessageAction.UPDATE, info?.action)
        assertEquals(9_007_199_254_740_993L, info?.historySerial)
        assertEquals(1_710_000_000_000L, info?.versionTimestampMs)
        assertEquals(null, getMutableMessageInfo(futureAction))
    }

    @Test
    fun presenceUpdatesApplyWithoutLettingMalformedMemberPayloadsMutateMembers() {
        val client = testClient()
        val channel = client.subscribe("presence-ai-forward") as PresenceChannel
        val emittedEvents = mutableListOf<Pair<String, Any?>>()
        channel.bindGlobal { eventName, data -> emittedEvents += eventName to data }
        channel.members.rememberMyId("user-1")
        channel.handle(
            SockudoEvent(
                event = client.p.internal_("subscription_succeeded"),
                channel = "presence-ai-forward",
                data =
                    mapOf(
                        "presence" to
                            mapOf(
                                "hash" to mapOf("user-1" to mapOf("role" to "reader")),
                                "count" to 1,
                            ),
                    ),
                rawMessage = "{}",
            ),
        )

        channel.handle(
            SockudoEvent(
                event = client.p.internal_("presence_update"),
                channel = "presence-ai-forward",
                data = mapOf("user_id" to "user-1", "user_info" to mapOf("role" to "writer")),
                rawMessage = "{}",
            ),
        )
        channel.handle(
            SockudoEvent(
                event = client.p.internal_("member_added"),
                channel = "presence-ai-forward",
                data = mapOf("user_info" to mapOf("malformed" to true)),
                rawMessage = "{}",
            ),
        )
        channel.handle(
            SockudoEvent(
                event = client.p.internal_("member_removed"),
                channel = "presence-ai-forward",
                data = listOf("malformed"),
                rawMessage = "{}",
            ),
        )

        assertEquals(1, channel.members.count)
        assertEquals(mapOf("role" to "writer"), channel.members.member("user-1")?.info)
        assertEquals(null, channel.members.member("undefined"))
        assertTrue(emittedEvents.any { it.first == client.p.event("presence_update") })
    }

    private fun testClient(connectionRecovery: Boolean = false): SockudoClient =
        SockudoClient(
            "app-key",
            SockudoOptions(
                cluster = "local",
                forceTls = false,
                enabledTransports = listOf(SockudoTransport.ws),
                wsHost = "127.0.0.1",
                wsPort = 6001,
                connectionRecovery = connectionRecovery,
            ),
        )

    private fun replayRaw(
        client: SockudoClient,
        raw: String,
    ) {
        val method = SockudoClient::class.java.getDeclaredMethod("handleRawMessage", Any::class.java)
        method.isAccessible = true
        try {
            method.invoke(client, raw)
        } catch (error: InvocationTargetException) {
            throw error.targetException
        }
    }

    private fun fixtureRaw(name: String): String {
        val candidates =
            listOf(
                Path.of("..", "..", "tests", "ai-conformance", "fixtures", "forward-compat", name),
                Path.of("..", "..", "..", "tests", "ai-conformance", "fixtures", "forward-compat", name),
                Path.of("tests", "ai-conformance", "fixtures", "forward-compat", name),
            )
        val path = candidates.firstOrNull { Files.exists(it) }
            ?: error("Unable to locate forward-compat fixture '$name'")
        return Files.readString(path)
    }

    private companion object {
        val fixtureNames =
            listOf(
                "future-v2-frame.json",
                "future-versioned-action.json",
                "future-webhook-events.json",
                "unknown-ai-extras.json",
            )
    }
}
