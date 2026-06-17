package io.sockudo.client

import kotlinx.coroutines.test.runTest
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class PushRegistrationTest {
    @Test
    fun publishUsesBackendProxyAndKeepsPushAsyncByDefault() =
        runTest {
            val server = MockWebServer()
            server.enqueue(
                MockResponse()
                    .setResponseCode(202)
                    .addHeader("Content-Type", "application/json")
                    .setBody("""{"publish_id":"pub_123"}"""),
            )
            server.use {
                val client =
                    SockudoPushRegistration(
                        PushRegistrationOptions(
                            endpoint = server.url("/push/").toString(),
                            headers = mapOf("Authorization" to "Bearer session"),
                        ),
                    )

                val response =
                    client.publish(
                        mapOf(
                            "recipients" to listOf(mapOf("type" to "channel", "channel" to "orders")),
                            "payload" to mapOf("title" to "Order", "body" to "Updated"),
                        ),
                    )

                val request = server.takeRequest()
                val body = JsonSupport.fromJsonElement(JsonSupport.decode(request.body.readUtf8())) as Map<String, Any?>

                assertEquals(mapOf("publish_id" to "pub_123"), response)
                assertEquals("/push/publish", request.path)
                assertEquals("POST", request.method)
                assertFalse(body["sync"] as Boolean)
                assertEquals("Bearer session", request.getHeader("Authorization"))
                assertTrue(request.getHeader("Content-Type")?.startsWith("application/json") == true)
            }
        }

    @Test
    fun updateDeviceRegistrationAddsDeviceIdentityTokenHeader() =
        runTest {
            val server = MockWebServer()
            server.enqueue(
                MockResponse()
                    .setResponseCode(201)
                    .addHeader("Content-Type", "application/json")
                    .setBody("""{"change":"updated"}"""),
            )
            server.use {
                val client =
                    SockudoPushRegistration(
                        PushRegistrationOptions(endpoint = server.url("/push/").toString()),
                    )

                client.updateDeviceRegistration(
                    device =
                        mapOf(
                            "id" to "device-1",
                            "formFactor" to "phone",
                            "platform" to "android",
                            "timezone" to "UTC",
                            "locale" to "en",
                            "push" to mapOf("recipient" to mapOf("transportType" to "gcm", "registrationToken" to "rotated")),
                        ),
                    deviceIdentityToken = "identity",
                )

                val request = server.takeRequest()
                assertEquals("/push/deviceRegistrations", request.path)
                assertEquals("identity", request.getHeader("X-Sockudo-Device-Identity-Token"))
            }
        }

    @Test
    fun listChannelSubscriptionsUsesCursorPaginationParams() =
        runTest {
            val server = MockWebServer()
            server.enqueue(
                MockResponse()
                    .setResponseCode(200)
                    .addHeader("Content-Type", "application/json")
                    .setBody("""{"items":[],"has_more":false}"""),
            )
            server.use {
                val client =
                    SockudoPushRegistration(
                        PushRegistrationOptions(endpoint = server.url("/push/").toString()),
                    )

                client.listChannelSubscriptions(
                    PushSubscriptionParams(
                        deviceId = "device-1",
                        limit = 10,
                        cursor = "c1",
                    ),
                )

                val request = server.takeRequest()
                assertEquals(
                    "/push/channelSubscriptions?deviceId=device-1&limit=10&cursor=c1",
                    request.path,
                )
            }
        }
}
