package io.sockudo.client

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

fun interface ChannelAuthorizationHandler {
    suspend fun authorize(request: ChannelAuthorizationRequest): ChannelAuthorizationData
}

fun interface UserAuthenticationHandler {
    suspend fun authenticate(request: UserAuthenticationRequest): UserAuthenticationData
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
