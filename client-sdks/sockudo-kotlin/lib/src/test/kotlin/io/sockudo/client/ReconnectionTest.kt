package io.sockudo.client

import java.lang.reflect.InvocationTargetException
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking

class ReconnectionTest {

    @Test
    fun `ConnectionState contains RECONNECTING`() {
        val states = ConnectionState.entries.map { it.name }
        assertTrue("RECONNECTING" in states)
    }

    @Test
    fun `RECONNECTING name lowercases to reconnecting`() {
        assertEquals("reconnecting", ConnectionState.RECONNECTING.name.lowercase())
    }

    @Test
    fun `SockudoOptions maxReconnectAttempts defaults to 6`() {
        val options = SockudoOptions(cluster = "test")
        assertEquals(6, options.maxReconnectAttempts)
    }

    @Test
    fun `SockudoOptions maxReconnectGapInSeconds defaults to 120`() {
        val options = SockudoOptions(cluster = "test")
        assertEquals(120.0, options.maxReconnectGapInSeconds)
    }

    @Test
    fun `SockudoOptions maxReconnectAttempts accepts null for unlimited`() {
        val options = SockudoOptions(cluster = "test", maxReconnectAttempts = null)
        assertNull(options.maxReconnectAttempts)
    }

    @Test
    fun `state_change emits reconnecting during retry`() =
        runBlocking {
            val stateChanges = CopyOnWriteArrayList<String>()
            val client = testClient()
            client.bind("state_change") { data, _ ->
                val map = data as? Map<*, *>
                val current = map?.get("current") as? String
                if (current != null) stateChanges += current
            }

            replayRaw(
                client,
                """{"event":"sockudo:connection_established","data":{"socket_id":"1.1","activity_timeout":120}}""",
            )

            invokeHandleSocketClosed(client, 4100, null)

            waitFor { "reconnecting" in stateChanges }

            assertTrue("reconnecting" in stateChanges, "Expected 'reconnecting' in state_change events, got: $stateChanges")
        }

    @Test
    fun `reconnect stops after maxReconnectAttempts exceeded`() =
        runBlocking {
            val stateChanges = CopyOnWriteArrayList<String>()
            val client = testClient(maxReconnectAttempts = 2)
            client.bind("state_change") { data, _ ->
                val map = data as? Map<*, *>
                val current = map?.get("current") as? String
                if (current != null) stateChanges += current
            }

            replayRaw(
                client,
                """{"event":"sockudo:connection_established","data":{"socket_id":"1.1","activity_timeout":120}}""",
            )

            repeat(3) {
                invokeHandleSocketClosed(client, 4100, null)
                delay(50)
            }

            waitFor { "disconnected" in stateChanges }
            assertTrue("disconnected" in stateChanges, "Expected DISCONNECTED after max attempts, got: $stateChanges")
        }

    @Test
    fun `connect resets reconnectAttempts`() =
        runBlocking {
            val client = testClient(maxReconnectAttempts = 1)

            replayRaw(
                client,
                """{"event":"sockudo:connection_established","data":{"socket_id":"1.1","activity_timeout":120}}""",
            )

            invokeHandleSocketClosed(client, 4100, null)
            delay(50)
            invokeHandleSocketClosed(client, 4100, null)
            delay(50)

            client.connect()

            val reconnectAttempts = getReconnectAttempts(client)
            assertEquals(0, reconnectAttempts)
        }

    @Test
    fun `disconnect resets reconnectAttempts`() =
        runBlocking {
            val client = testClient()

            replayRaw(
                client,
                """{"event":"sockudo:connection_established","data":{"socket_id":"1.1","activity_timeout":120}}""",
            )

            invokeHandleSocketClosed(client, 4100, null)
            delay(50)

            client.disconnect()

            val reconnectAttempts = getReconnectAttempts(client)
            assertEquals(0, reconnectAttempts)
        }

    private fun testClient(maxReconnectAttempts: Int? = 6): SockudoClient =
        SockudoClient(
            "app-key",
            SockudoOptions(
                cluster = "local",
                forceTls = false,
                enabledTransports = listOf(SockudoTransport.ws),
                wsHost = "127.0.0.1",
                wsPort = 6001,
                maxReconnectAttempts = maxReconnectAttempts,
            ),
        )

    private fun replayRaw(client: SockudoClient, raw: String) {
        val method = SockudoClient::class.java.getDeclaredMethod("handleRawMessage", Any::class.java)
        method.isAccessible = true
        try {
            method.invoke(client, raw)
        } catch (error: InvocationTargetException) {
            throw error.targetException
        }
    }

    private fun invokeHandleSocketClosed(client: SockudoClient, code: Int, reason: String?) {
        val method = SockudoClient::class.java.getDeclaredMethod("handleSocketClosed", Int::class.java, String::class.java)
        method.isAccessible = true
        try {
            method.invoke(client, code, reason)
        } catch (error: InvocationTargetException) {
            throw error.targetException
        }
    }

    private fun getReconnectAttempts(client: SockudoClient): Int {
        val field = SockudoClient::class.java.getDeclaredField("reconnectAttempts")
        field.isAccessible = true
        return field.getInt(client)
    }

    private suspend fun waitFor(timeoutMs: Long = 2_000, condition: () -> Boolean) {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < deadline) {
            if (condition()) return
            delay(20)
        }
        error("Timed out waiting for condition")
    }
}
