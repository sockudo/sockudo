package io.sockudo.client

import okhttp3.HttpUrl.Companion.toHttpUrlOrNull
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody

data class PushRegistrationOptions(
    val endpoint: String,
    val headers: Map<String, String> = emptyMap(),
    val headersProvider: (() -> Map<String, String>)? = null,
)

data class PushCursorParams(
    val limit: Int? = null,
    val cursor: String? = null,
) {
    fun toQueryParams(): Map<String, Any> =
        buildMap {
            limit?.let { put("limit", it) }
            cursor?.let { put("cursor", it) }
        }
}

data class PushSubscriptionParams(
    val channel: String? = null,
    val deviceId: String? = null,
    val limit: Int? = null,
    val cursor: String? = null,
) {
    fun toQueryParams(): Map<String, Any> =
        buildMap {
            channel?.let { put("channel", it) }
            deviceId?.let { put("deviceId", it) }
            limit?.let { put("limit", it) }
            cursor?.let { put("cursor", it) }
        }
}

class SockudoPushRegistration(
    options: PushRegistrationOptions,
    private val httpClient: OkHttpClient = OkHttpClient(),
) {
    private val endpoint = options.endpoint.trimEnd('/')
    private val headers = options.headers
    private val headersProvider = options.headersProvider

    suspend fun activateDevice(device: Map<String, Any?>): Map<String, Any?> =
        requestObject("POST", "/deviceRegistrations", body = device)

    suspend fun updateDeviceRegistration(
        device: Map<String, Any?>,
        deviceIdentityToken: String,
    ): Map<String, Any?> =
        requestObject(
            "POST",
            "/deviceRegistrations",
            body = device,
            requestHeaders = mapOf("X-Sockudo-Device-Identity-Token" to deviceIdentityToken),
        )

    suspend fun listDeviceRegistrations(
        params: PushCursorParams = PushCursorParams(),
    ): Map<String, Any?> = requestObject("GET", "/deviceRegistrations", queryParams = params.toQueryParams())

    suspend fun getDeviceRegistration(deviceId: String): Map<String, Any?> =
        requestObject("GET", "/deviceRegistrations/${encodePath(deviceId)}")

    suspend fun deleteDeviceRegistration(deviceId: String) {
        requestUnit("DELETE", "/deviceRegistrations/${encodePath(deviceId)}")
    }

    suspend fun upsertChannelSubscription(subscription: Map<String, Any?>): Map<String, Any?> =
        requestObject("POST", "/channelSubscriptions", body = subscription)

    suspend fun listChannelSubscriptions(
        params: PushSubscriptionParams = PushSubscriptionParams(),
    ): Map<String, Any?> = requestObject("GET", "/channelSubscriptions", queryParams = params.toQueryParams())

    suspend fun deleteChannelSubscriptions(params: PushSubscriptionParams) {
        requestUnit("DELETE", "/channelSubscriptions", queryParams = params.toQueryParams())
    }

    suspend fun publish(request: Map<String, Any?>): Map<String, Any?> =
        requestObject("POST", "/publish", body = request + ("sync" to false))

    suspend fun publishBatch(requests: List<Map<String, Any?>>): Any? =
        requestValue(
            "POST",
            "/batch/publish",
            body = requests.map { it + ("sync" to false) },
        )

    suspend fun schedulePublish(request: Map<String, Any?>): Map<String, Any?> = publish(request)

    suspend fun getPublishStatus(publishId: String): Map<String, Any?> =
        requestObject("GET", "/publish/${encodePath(publishId)}/status")

    suspend fun cancelScheduledPublish(publishId: String) {
        requestUnit("DELETE", "/scheduled/${encodePath(publishId)}")
    }

    suspend fun postDeliveryStatus(event: Map<String, Any?>): Map<String, Any?> =
        requestObject("POST", "/deliveryStatus", body = event)

    private suspend fun requestObject(
        method: String,
        path: String,
        body: Any? = null,
        queryParams: Map<String, Any> = emptyMap(),
        requestHeaders: Map<String, String> = emptyMap(),
    ): Map<String, Any?> = requestValue(method, path, body, queryParams, requestHeaders) as? Map<String, Any?> ?: emptyMap()

    private suspend fun requestUnit(
        method: String,
        path: String,
        body: Any? = null,
        queryParams: Map<String, Any> = emptyMap(),
        requestHeaders: Map<String, String> = emptyMap(),
    ) {
        requestValue(method, path, body, queryParams, requestHeaders)
    }

    private suspend fun requestValue(
        method: String,
        path: String,
        body: Any? = null,
        queryParams: Map<String, Any> = emptyMap(),
        requestHeaders: Map<String, String> = emptyMap(),
    ): Any? {
        val url =
            "${endpoint}${path}".toHttpUrlOrNull()?.newBuilder()?.apply {
                queryParams.forEach { (name, value) -> addQueryParameter(name, value.toString()) }
            }?.build()
                ?: throw SockudoException.InvalidUrl("Invalid push endpoint ${endpoint}${path}")

        val requestBody =
            if (body == null) {
                null
            } else {
                JsonSupport.encode(body).toRequestBody("application/json".toMediaType())
            }

        val request =
            Request.Builder()
                .url(url)
                .method(method, requestBody)
                .apply {
                    (headers + (headersProvider?.invoke() ?: emptyMap()) + requestHeaders).forEach { (name, value) ->
                        addHeader(name, value)
                    }
                    addHeader("Content-Type", "application/json")
                }
                .build()

        val response = httpClient.newCall(request).execute()
        response.use {
            if (!it.isSuccessful) {
                throw SockudoException.InvalidOptions(
                    "Sockudo push request failed with HTTP ${it.code}",
                )
            }
            if (it.code == 204) {
                return null
            }
            val raw = it.body?.string().orEmpty()
            if (raw.isBlank()) {
                return emptyMap<String, Any?>()
            }
            return JsonSupport.fromJsonElement(JsonSupport.decode(raw))
        }
    }

    private fun encodePath(value: String): String = java.net.URLEncoder.encode(value, Charsets.UTF_8)
}
