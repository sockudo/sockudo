package io.sockudo.client

import java.lang.reflect.InvocationTargetException
import java.nio.charset.StandardCharsets
import java.util.Base64
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import okhttp3.Request
import okhttp3.WebSocket
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okio.ByteString

class PlatformPrimitiveTest {
    @Test
    fun presenceUpdateAppliesMemberDataAndExposesAttachSerial() {
        val client = testClient()
        val channel = client.subscribe("presence-room") as PresenceChannel
        val updates = mutableListOf<PresenceMember>()

        channel.bind("sockudo:presence_update") { data, _ ->
            (data as? PresenceMember)?.let { updates += it }
        }
        channel.members.rememberMyId("user-1")
        channel.handle(
            SockudoEvent(
                event = client.p.internal_("subscription_succeeded"),
                channel = "presence-room",
                data =
                    mapOf(
                        "attach_serial" to "9007199254740993",
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
                channel = "presence-room",
                data = mapOf("user_id" to "user-1", "user_info" to mapOf("role" to "writer")),
                rawMessage = "{}",
            ),
        )

        assertEquals(9_007_199_254_740_993L, channel.attachSerial)
        assertEquals(mapOf("role" to "writer"), channel.members.member("user-1")?.info)
        assertEquals(PresenceMember("user-1", mapOf("role" to "writer")), updates.single())
    }

    @Test
    fun presenceUpdateSendsV2PlatformEvent() {
        val client = testClient()
        val socket = CapturingWebSocket()
        setWebSocket(client, socket)
        val channel = client.subscribe("presence-room") as PresenceChannel

        assertTrue(channel.update(mapOf("mood" to "focused")))

        val event = ProtocolCodec.decodeEvent(socket.textFrames.single(), SockudoWireFormat.json)
        assertEquals("sockudo:presence_update", event.event)
        assertEquals("presence-room", event.channel)
        assertEquals(mapOf("mood" to "focused"), event.data)
    }

    @Test
    fun tokenExpiredRefreshesWithProviderOnClientScope() =
        runBlocking {
            val requests = mutableListOf<ClientAuthTokenRequest>()
            val client =
                testClient(
                    authTokenProvider =
                        ClientAuthTokenProvider { request ->
                            requests += request
                            "token-${request.reason.name.lowercase()}"
                        },
                )
            val socket = CapturingWebSocket()
            setWebSocket(client, socket)

            replayRaw(
                client,
                """{"event":"sockudo:connection_established","data":{"socket_id":"1.2","activity_timeout":120}}""",
            )
            replayRaw(client, """{"event":"sockudo:token_expired","data":{"code":40160}}""")

            waitFor { socket.textFrames.isNotEmpty() }
            val refresh = ProtocolCodec.decodeEvent(socket.textFrames.single(), SockudoWireFormat.json)

            assertEquals(ClientAuthTokenReason.EXPIRED, requests.single().reason)
            assertEquals("1.2", requests.single().socketId)
            assertEquals("sockudo:auth", refresh.event)
            assertEquals(mapOf("token" to "token-expired"), refresh.data)
        }

    @Test
    fun jwtProviderTokensComputeEightyPercentRefreshDelay() {
        val nowMillis = 100_000L
        val withIat = unsignedJwt(mapOf("iat" to 90, "exp" to 110))
        val withoutIat = unsignedJwt(mapOf("exp" to 110))

        assertEquals(6_000L, clientAuthTokenRefreshDelayMillis(withIat, nowMillis))
        assertEquals(8_000L, clientAuthTokenRefreshDelayMillis(withoutIat, nowMillis))
        assertEquals(null, clientAuthTokenRefreshDelayMillis("opaque-token", nowMillis))
    }

    @Test
    fun jwtProviderTokenSchedulesProactiveRefresh() =
        runBlocking {
            val requests = CopyOnWriteArrayList<ClientAuthTokenRequest>()
            val client =
                testClient(
                    authTokenProvider =
                        ClientAuthTokenProvider { request ->
                            requests += request
                            val exp = currentEpochSeconds() + if (request.reason == ClientAuthTokenReason.EXPIRED) 1 else 60
                            unsignedJwt(mapOf("exp" to exp))
                        },
                )
            val socket = CapturingWebSocket()
            setWebSocket(client, socket)

            replayRaw(
                client,
                """{"event":"sockudo:connection_established","data":{"socket_id":"1.2","activity_timeout":120}}""",
            )
            replayRaw(client, """{"event":"sockudo:token_expired","data":{"code":40160}}""")

            waitFor { socket.textFrames.size >= 2 }

            assertEquals(
                listOf(ClientAuthTokenReason.EXPIRED, ClientAuthTokenReason.REFRESH),
                requests.map { it.reason },
            )
            assertEquals("sockudo:auth", ProtocolCodec.decodeEvent(socket.textFrames[0], SockudoWireFormat.json).event)
            assertEquals("sockudo:auth", ProtocolCodec.decodeEvent(socket.textFrames[1], SockudoWireFormat.json).event)
            client.disconnect()
        }

    @Test
    fun disconnectCancelsProactiveJwtRefresh() =
        runBlocking {
            val requests = CopyOnWriteArrayList<ClientAuthTokenRequest>()
            val client =
                testClient(
                    authTokenProvider =
                        ClientAuthTokenProvider { request ->
                            requests += request
                            unsignedJwt(mapOf("exp" to currentEpochSeconds() + 2))
                        },
                )
            val socket = CapturingWebSocket()
            setWebSocket(client, socket)

            replayRaw(
                client,
                """{"event":"sockudo:connection_established","data":{"socket_id":"1.2","activity_timeout":120}}""",
            )
            replayRaw(client, """{"event":"sockudo:token_expired","data":{"code":40160}}""")
            waitFor { socket.textFrames.size == 1 }

            client.disconnect()
            delay(2_200)

            assertEquals(listOf(ClientAuthTokenReason.EXPIRED), requests.map { it.reason })
            assertEquals(1, socket.textFrames.size)
        }

    @Test
    fun channelHistoryParamsCarryUntilAttach() {
        assertEquals(
            mapOf(
                "limit" to 10,
                "until_attach" to true,
            ),
            ChannelHistoryParams(limit = 10, untilAttach = true).toPayload(),
        )
    }

    @Test
    fun versionedMessageHelpersUseBackendProxyAndTypedAcks() =
        runBlocking {
            val server = MockWebServer()
            repeat(4) {
                server.enqueue(
                    MockResponse()
                        .setResponseCode(200)
                        .addHeader("Content-Type", "application/json")
                        .setBody(
                            """{"ack":{"message_serial":"msg-1","version_serial":"v-$it","history_serial":"9007199254740993","delivery_serial":12,"event":"chat-message"}}""",
                        ),
                )
            }
            server.use {
                val client =
                    testClient(
                        versionedMessages =
                            VersionedMessagesOptions(
                                endpoint = server.url("/versioned").toString(),
                                headers = mapOf("Authorization" to "Bearer session"),
                            ),
                    )
                val channel = client.subscribe("chat-room")

                val createAck = channel.createMessage("chat-message", mapOf("text" to "hello"))
                val appendAck = channel.appendMessage("msg-1", " world")
                val updateAck = channel.updateMessage("msg-1", mapOf("text" to "edited"))
                val deleteAck = channel.deleteMessage("msg-1")
                val actions =
                    (0 until 4).map {
                        val request = server.takeRequest(1, TimeUnit.SECONDS) ?: error("missing request")
                        assertEquals("/versioned", request.path)
                        assertEquals("Bearer session", request.getHeader("Authorization"))
                        val body = JsonSupport.fromJsonElement(JsonSupport.decode(request.body.readUtf8())) as Map<String, Any?>
                        body["action"] as String
                    }

                assertEquals("msg-1", createAck.messageSerial)
                assertEquals("v-1", appendAck.versionSerial)
                assertEquals(9_007_199_254_740_993L, updateAck.historySerial)
                assertEquals(12L, deleteAck.deliverySerial)
                assertEquals(
                    listOf("message_create", "message_append", "message_update", "message_delete"),
                    actions,
                )
            }
        }

    private fun testClient(
        authTokenProvider: ClientAuthTokenProvider? = null,
        versionedMessages: VersionedMessagesOptions? = null,
    ): SockudoClient =
        SockudoClient(
            "app-key",
            SockudoOptions(
                cluster = "local",
                forceTls = false,
                enabledTransports = listOf(SockudoTransport.ws),
                wsHost = "127.0.0.1",
                wsPort = 6001,
                authTokenProvider = authTokenProvider,
                versionedMessages = versionedMessages,
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

    private suspend fun waitFor(condition: () -> Boolean) {
        val deadline = System.currentTimeMillis() + 2_000
        while (System.currentTimeMillis() < deadline) {
            if (condition()) {
                return
            }
            delay(20)
        }
        error("Timed out waiting for condition")
    }

    private fun setWebSocket(
        client: SockudoClient,
        socket: WebSocket,
    ) {
        val field = SockudoClient::class.java.getDeclaredField("webSocket")
        field.isAccessible = true
        field.set(client, socket)
    }

    private fun unsignedJwt(payload: Map<String, Any?>): String {
        val header = mapOf("alg" to "none", "typ" to "JWT")
        return "${base64Url(header)}.${base64Url(payload)}.signature"
    }

    private fun base64Url(value: Map<String, Any?>): String =
        Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(JsonSupport.encode(value).toByteArray(StandardCharsets.UTF_8))

    private fun currentEpochSeconds(): Long = System.currentTimeMillis() / 1000L

    private class CapturingWebSocket : WebSocket {
        val textFrames = CopyOnWriteArrayList<String>()
        val binaryFrames = CopyOnWriteArrayList<ByteString>()

        override fun request(): Request = Request.Builder().url("ws://127.0.0.1/app/app-key").build()

        override fun queueSize(): Long = 0L

        override fun send(text: String): Boolean {
            textFrames += text
            return true
        }

        override fun send(bytes: ByteString): Boolean {
            binaryFrames += bytes
            return true
        }

        override fun close(
            code: Int,
            reason: String?,
        ): Boolean = true

        override fun cancel() = Unit
    }
}
