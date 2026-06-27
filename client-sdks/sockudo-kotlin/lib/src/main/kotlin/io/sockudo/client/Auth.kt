package io.sockudo.client

import java.nio.charset.StandardCharsets
import java.util.Base64
import kotlin.math.roundToLong

data class ChannelAuthorizationData(
    val auth: String,
    val channelData: String? = null,
    val sharedSecret: String? = null,
)

data class UserAuthenticationData(
    val auth: String,
    val userData: String,
)

data class ChannelAuthorizationRequest(
    val socketId: String,
    val channelName: String,
)

data class UserAuthenticationRequest(
    val socketId: String,
)

enum class ClientAuthTokenReason {
    INITIAL,
    REFRESH,
    EXPIRED,
}

data class ClientAuthTokenRequest(
    val socketId: String?,
    val reason: ClientAuthTokenReason,
)

fun interface ChannelAuthorizationHandler {
    suspend fun authorize(request: ChannelAuthorizationRequest): ChannelAuthorizationData
}

fun interface UserAuthenticationHandler {
    suspend fun authenticate(request: UserAuthenticationRequest): UserAuthenticationData
}

fun interface ClientAuthTokenProvider {
    suspend fun token(request: ClientAuthTokenRequest): String
}

internal fun clientAuthTokenRefreshDelayMillis(
    token: String,
    nowMillis: Long = System.currentTimeMillis(),
): Long? {
    val payload = decodeJwtPayload(token) ?: return null
    val exp = parseSockudoLong(payload["exp"]) ?: return null
    val iat = parseSockudoLong(payload["iat"])
    val nowSeconds = nowMillis / 1000.0
    val startSeconds = iat?.toDouble() ?: nowSeconds
    val lifetimeSeconds = exp.toDouble() - startSeconds
    if (lifetimeSeconds <= 0.0) {
        return null
    }
    val refreshAtMillis = ((startSeconds + (lifetimeSeconds * 0.8)) * 1000.0).roundToLong()
    return maxOf(0L, refreshAtMillis - nowMillis)
}

private fun decodeJwtPayload(token: String): Map<String, Any?>? {
    val parts = token.split(".")
    if (parts.size < 2) {
        return null
    }
    return runCatching {
        val payload = parts[1].padEnd(parts[1].length + ((4 - parts[1].length % 4) % 4), '=')
        val decoded = String(Base64.getUrlDecoder().decode(payload), StandardCharsets.UTF_8)
        JsonSupport.fromJsonElement(JsonSupport.decode(decoded)) as? Map<String, Any?>
    }.getOrNull()
}

data class ChannelAuthorizationOptions(
    val endpoint: String = "/sockudo/auth",
    val headers: Map<String, String> = emptyMap(),
    val params: Map<String, AuthValue> = emptyMap(),
    val headersProvider: (() -> Map<String, String>)? = null,
    val paramsProvider: (() -> Map<String, AuthValue>)? = null,
    val customHandler: ChannelAuthorizationHandler? = null,
)

data class UserAuthenticationOptions(
    val endpoint: String = "/sockudo/user-auth",
    val headers: Map<String, String> = emptyMap(),
    val params: Map<String, AuthValue> = emptyMap(),
    val headersProvider: (() -> Map<String, String>)? = null,
    val paramsProvider: (() -> Map<String, AuthValue>)? = null,
    val customHandler: UserAuthenticationHandler? = null,
)
