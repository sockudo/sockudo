package io.sockudo.client

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ProtocolWireFormatTest {
    @Test
    fun encodesWebsocketUrlWithV2FormatQuery() {
        val client =
            SockudoClient(
                "app-key",
                SockudoOptions(
                    cluster = "local",
                    forceTls = false,
                    enabledTransports = listOf(SockudoTransport.ws),
                    wsHost = "ws.example.com",
                    wsPort = 6001,
                    wssPort = 6002,
                    wireFormat = SockudoWireFormat.messagepack,
                    appendMode = SockudoAppendMode.full,
                    appendRollupWindow = 100,
                    authToken = "capability-token",
                ),
            )

        val method = SockudoClient::class.java.getDeclaredMethod("socketUrl", SockudoTransport::class.java)
        method.isAccessible = true
        val url = method.invoke(client, SockudoTransport.ws) as String

        val query = queryParams(url)

        assertEquals("2", query["protocol"])
        assertEquals("messagepack", query["format"])
        assertEquals("full", query["append_mode"])
        assertEquals("100", query["append_rollup_window"])
        assertEquals("capability-token", query["token"])
    }

    @Test
    fun validatesAppendRollupWindowAndOmitsV2OnlyParamsForProtocolV1() {
        assertFailsWith<SockudoException.InvalidOptions> {
            SockudoOptions(
                cluster = "local",
                appendRollupWindow = 21,
            )
        }

        val client =
            SockudoClient(
                "app-key",
                SockudoOptions(
                    cluster = "local",
                    protocolVersion = 1,
                    forceTls = false,
                    enabledTransports = listOf(SockudoTransport.ws),
                    wsHost = "ws.example.com",
                    wsPort = 6001,
                    authToken = "ignored-token",
                    appendRollupWindow = 100,
                ),
            )
        val method = SockudoClient::class.java.getDeclaredMethod("socketUrl", SockudoTransport::class.java)
        method.isAccessible = true
        val query = queryParams(method.invoke(client, SockudoTransport.ws) as String)

        assertEquals("1", query["protocol"])
        assertEquals(null, query["format"])
        assertEquals(null, query["append_rollup_window"])
        assertEquals(null, query["token"])
    }

    @Test
    fun roundTripsMessagepack() {
        val payload =
            ProtocolCodec.encodeEnvelope(
                linkedMapOf(
                    "event" to "sockudo:test",
                    "channel" to "chat:room-1",
                    "data" to linkedMapOf("hello" to "world", "count" to 3),
                    "stream_id" to "stream-1",
                    "message_id" to "msg-1",
                    "serial" to 7,
                    "__delta_seq" to 7,
                    "__conflation_key" to "room",
                ),
                SockudoWireFormat.messagepack,
            )

        val decoded = ProtocolCodec.decodeEvent(payload, SockudoWireFormat.messagepack)

        assertEquals("sockudo:test", decoded.event)
        assertEquals("chat:room-1", decoded.channel)
        assertEquals(mapOf("hello" to "world", "count" to 3L), decoded.data)
        assertEquals("stream-1", decoded.streamId)
        assertEquals("msg-1", decoded.messageId)
        assertEquals(7L, decoded.serial)
        assertEquals(7, decoded.sequence)
        assertEquals("room", decoded.conflationKey)
    }

    @Test
    fun roundTripsProtobuf() {
        val payload =
            ProtocolCodec.encodeEnvelope(
                linkedMapOf(
                    "event" to "sockudo:test",
                    "channel" to "chat:room-1",
                    "data" to linkedMapOf("hello" to "world"),
                    "stream_id" to "stream-2",
                    "message_id" to "msg-2",
                    "serial" to 9,
                    "__delta_seq" to 11,
                    "__conflation_key" to "btc",
                    "extras" to
                        linkedMapOf(
                            "headers" to linkedMapOf("region" to "eu", "ttl" to 5, "replay" to true),
                            "ai" to
                                linkedMapOf(
                                    "transport" to linkedMapOf("turn-id" to "turn-1", "status" to "streaming"),
                                    "codec" to linkedMapOf("x-custom" to "opaque"),
                                ),
                            "echo" to false,
                        ),
                ),
                SockudoWireFormat.protobuf,
            )

        val decoded = ProtocolCodec.decodeEvent(payload, SockudoWireFormat.protobuf)

        assertEquals("sockudo:test", decoded.event)
        assertEquals("chat:room-1", decoded.channel)
        assertEquals(mapOf("hello" to "world"), decoded.data)
        assertEquals("stream-2", decoded.streamId)
        assertEquals("msg-2", decoded.messageId)
        assertEquals(9L, decoded.serial)
        assertEquals(11, decoded.sequence)
        assertEquals("btc", decoded.conflationKey)
        assertEquals("eu", decoded.extras?.headers?.get("region"))
        assertEquals(5.0, decoded.extras?.headers?.get("ttl"))
        assertEquals(true, decoded.extras?.headers?.get("replay"))
        assertEquals("turn-1", (decoded.extras?.ai?.get("transport") as? Map<*, *>)?.get("turn-id"))
        assertEquals("opaque", (decoded.extras?.ai?.get("codec") as? Map<*, *>)?.get("x-custom"))
        assertFalse(decoded.extras?.echo ?: true)
    }

    @Test
    fun subscriptionRewindSerializesAsExpected() {
        assertEquals(10, SubscriptionRewind.Count(10).subscriptionValue())
        assertEquals(
            mapOf("seconds" to 30),
            SubscriptionRewind.Seconds(30).subscriptionValue(),
        )
    }

    @Test
    fun presenceHistoryParamsNormalizeAblyAliases() {
        assertEquals(
            mapOf(
                "direction" to "newest_first",
                "limit" to 50,
                "start_time_ms" to 1000L,
                "end_time_ms" to 2000L,
            ),
            PresenceHistoryParams(
                direction = "newest_first",
                limit = 50,
                start = 1000,
                end = 2000,
            ).toPayload(),
        )
    }

    @Test
    fun presenceHistoryPageNextUsesNextCursor() {
        var capturedCursor: String? = null
        val page =
            PresenceHistoryPage(
                items = emptyList(),
                direction = "newest_first",
                limit = 50,
                hasMore = true,
                nextCursor = "cursor-2",
                bounds = PresenceHistoryBounds(null, null, null, null),
                continuity = PresenceHistoryContinuity(null, null, null, null, null, 0, 0, false, true, false),
                fetchNext = { cursor ->
                    capturedCursor = cursor
                    PresenceHistoryPage(
                        items = emptyList(),
                        direction = "newest_first",
                        limit = 50,
                        hasMore = false,
                        nextCursor = null,
                        bounds = PresenceHistoryBounds(null, null, null, null),
                        continuity = PresenceHistoryContinuity(null, null, null, null, null, 0, 0, false, true, false),
                    )
                },
            )

        assertTrue(page.hasNext())
        kotlinx.coroutines.runBlocking { page.next() }
        assertEquals("cursor-2", capturedCursor)
    }

    private fun queryParams(url: String): Map<String, String> =
        java.net.URI(url).query.split("&").associate {
            val (key, value) = it.split("=", limit = 2)
            key to value
        }
}
