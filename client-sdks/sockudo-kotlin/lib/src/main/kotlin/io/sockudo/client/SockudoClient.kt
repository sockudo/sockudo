package io.sockudo.client

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okhttp3.MediaType.Companion.toMediaType
import okio.ByteString.Companion.toByteString
import java.net.URI
import java.util.concurrent.TimeUnit
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

class SockudoClient(
    val key: String,
    val options: SockudoOptions,
    internal val httpClient: OkHttpClient = OkHttpClient.Builder().readTimeout(0, TimeUnit.MILLISECONDS).build(),
) {
    init {
        require(key.isNotBlank()) { throw SockudoException.InvalidAppKey }
        require(options.cluster.isNotBlank()) {
            throw SockudoException.InvalidOptions("Options must provide a cluster.")
        }
    }

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    internal val p = ProtocolPrefix(options.protocolVersion)
    internal val config = ResolvedConfiguration(options, httpClient)
    private val dispatcher = EventDispatcher()
    private val channels = linkedMapOf<String, SockudoChannel>()
    private var webSocket: WebSocket? = null
    private var activityJob: Job? = null
    private var unavailableJob: Job? = null
    private var retryJob: Job? = null
    private var authRefreshJob: Job? = null
    private var currentTransport: SockudoTransport? = null
    private var attemptedFallback: Boolean = false
    private var manuallyDisconnected: Boolean = false
    private var currentAuthToken: String? = options.authToken
    private val deltaManager: DeltaCompressionManager? =
        options.deltaCompression?.let { deltaOptions ->
            DeltaCompressionManager(deltaOptions, { event, data ->
                sendEvent(event, data, null)
            }, p)
        }
    private val deduplicator: MessageDeduplicator? =
        if (options.messageDeduplication) MessageDeduplicator(options.messageDeduplicationCapacity) else null
    private val channelPositions = linkedMapOf<String, RecoveryPosition>()

    val user = UserFacade()
    val watchlist = WatchlistFacade()

    var connectionState: ConnectionState = ConnectionState.INITIALIZED
        private set

    var socketId: String? = null
        private set

    val shouldUseTls: Boolean
        get() = config.useTls

    init {
        user.attach(this)
        watchlist.attach(this)
    }

    fun on(eventName: String, callback: (Any?, EventMetadata?) -> Unit): EventBindingToken =
        dispatcher.bind(eventName, callback)

    fun bind(eventName: String, callback: (Any?, EventMetadata?) -> Unit): EventBindingToken = on(eventName, callback)

    fun onGlobal(callback: (String, Any?) -> Unit): EventBindingToken = dispatcher.bindGlobal(callback)

    fun bindGlobal(callback: (String, Any?) -> Unit): EventBindingToken = onGlobal(callback)

    fun off(eventName: String? = null, token: EventBindingToken? = null) {
        dispatcher.unbind(eventName, token)
    }

    fun unbind(eventName: String? = null, token: EventBindingToken? = null) = off(eventName, token)

    fun unbindAll() {
        dispatcher.unbind()
    }

    fun channel(name: String): SockudoChannel? = channels[name]

    fun allChannels(): List<SockudoChannel> = channels.values.sortedBy { it.name }

    fun subscribe(channelName: String, options: SubscriptionOptions? = null): SockudoChannel {
        val channel = channels.getOrPut(channelName) { createChannel(channelName) }
        options?.let {
            channel.filter = it.filter
            channel.deltaSettings = it.delta
            channel.eventsFilter = it.events
            channel.expressionFilter = it.expression
            channel.rewind = it.rewind
            channel.annotationSubscribe = it.annotationSubscribe
        }
        channel.subscribeIfPossible()
        return channel
    }

    fun subscribe(channelName: String, filter: FilterNode): SockudoChannel =
        subscribe(channelName, SubscriptionOptions(filter = filter))

    fun unsubscribe(channelName: String) {
        val channel = channels[channelName]
        when {
            channel == null -> return
            channel.subscriptionPending -> channel.subscriptionCancelled = true
            channel.isSubscribed -> {
                channels.remove(channelName)
                channel.unsubscribe()
            }

            else -> channels.remove(channelName)
        }
        channelPositions.remove(channelName)
        deltaManager?.clearChannelState(channelName)
    }

    fun connect() {
        if (webSocket != null || connectionState == ConnectionState.CONNECTING) {
            return
        }
        val transports = transportSequence()
        if (transports.isEmpty()) {
            updateState(ConnectionState.FAILED)
            return
        }
        manuallyDisconnected = false
        attemptedFallback = false
        updateState(ConnectionState.CONNECTING)
        if (options.protocolVersion >= 2 && options.authTokenProvider != null) {
            scope.launch {
                runCatching {
                    refreshAuthToken(ClientAuthTokenReason.INITIAL, sendRefreshFrame = false)
                }.onSuccess {
                    if (manuallyDisconnected) {
                        return@onSuccess
                    }
                    openWebSocket(transports.first())
                    setUnavailableTimer()
                }.onFailure { error ->
                    dispatcher.emit("error", error)
                    updateState(ConnectionState.FAILED)
                }
            }
        } else {
            openWebSocket(transports.first())
            setUnavailableTimer()
        }
    }

    fun disconnect() {
        manuallyDisconnected = true
        invalidateTimers()
        webSocket?.close(1000, null)
        webSocket = null
        currentTransport = null
        channels.values.forEach { it.disconnect() }
        updateState(ConnectionState.DISCONNECTED)
    }

    fun signIn() {
        user.signIn()
    }

    fun getDeltaStats(): DeltaStats? = deltaManager?.getStats()

    fun resetDeltaStats() {
        deltaManager?.resetStats()
    }

    fun getRecoveryPosition(channelName: String): RecoveryPosition? = channelPositions[channelName]

    fun getRecoveryPositions(): Map<String, RecoveryPosition> = channelPositions.toMap()

    fun setRecoveryPosition(
        channelName: String,
        position: RecoveryPosition?,
    ) {
        if (position == null) {
            channelPositions.remove(channelName)
        } else {
            channelPositions[channelName] = position
        }
    }

    fun setRecoveryPositions(positions: Map<String, RecoveryPosition>) {
        channelPositions.clear()
        channelPositions.putAll(positions)
    }

    fun close() {
        disconnect()
        scope.cancel()
    }

    internal suspend fun fetchPresenceHistory(
        channelName: String,
        params: PresenceHistoryParams,
    ): PresenceHistoryPage {
        val config = options.presenceHistory
            ?: throw SockudoException.UnsupportedFeature(
                "presenceHistory.endpoint must be configured to use presence.history(). This endpoint should proxy requests to the Sockudo server REST API.",
            )

        val payload = performPresenceHistoryRequest(
            endpoint = config.endpoint,
            headers = config.headers + (config.headersProvider?.invoke() ?: emptyMap()),
            channelName = channelName,
            params = params.toPayload(),
            action = "history",
        )

        return decodePresenceHistoryPage(payload) { cursor ->
            fetchPresenceHistory(channelName, params.copy(cursor = cursor))
        }
    }

    internal suspend fun fetchPresenceSnapshot(
        channelName: String,
        params: PresenceSnapshotParams,
    ): PresenceSnapshot {
        val config = options.presenceHistory
            ?: throw SockudoException.UnsupportedFeature(
                "presenceHistory.endpoint must be configured to use presence.snapshot(). This endpoint should proxy requests to the Sockudo server REST API.",
            )

        val payload = performPresenceHistoryRequest(
            endpoint = config.endpoint,
            headers = config.headers + (config.headersProvider?.invoke() ?: emptyMap()),
            channelName = channelName,
            params = params.toPayload(),
            action = "snapshot",
        )

        return decodePresenceSnapshot(payload)
    }

    internal suspend fun fetchChannelHistory(
        channelName: String,
        params: ChannelHistoryParams,
    ): ChannelHistoryPage {
        val config = options.versionedMessages
            ?: throw SockudoException.UnsupportedFeature(
                "versionedMessages.endpoint must be configured to use channelHistory(). This endpoint should proxy requests to the Sockudo server REST API.",
            )

        val payload = performPresenceHistoryRequest(
            endpoint = config.endpoint,
            headers = config.headers + (config.headersProvider?.invoke() ?: emptyMap()),
            channelName = channelName,
            params = params.toPayload(),
            action = "channel_history",
        )

        return decodeChannelHistoryPage(payload) { cursor ->
            fetchChannelHistory(channelName, params.copy(cursor = cursor))
        }
    }

    internal suspend fun fetchLatestMessage(
        channelName: String,
        messageSerial: String,
    ): Map<String, Any?> {
        val config = options.versionedMessages
            ?: throw SockudoException.UnsupportedFeature(
                "versionedMessages.endpoint must be configured to use getMessage(). This endpoint should proxy requests to the Sockudo server REST API.",
            )

        val payload = performPresenceHistoryRequest(
            endpoint = config.endpoint,
            headers = config.headers + (config.headersProvider?.invoke() ?: emptyMap()),
            channelName = channelName,
            params = emptyMap(),
            action = "get_message",
            messageSerial = messageSerial,
        )

        return payload["item"] as? Map<String, Any?> ?: emptyMap()
    }

    internal suspend fun fetchMessageVersions(
        channelName: String,
        messageSerial: String,
        params: MessageVersionsParams,
    ): MessageVersionsPage {
        val config = options.versionedMessages
            ?: throw SockudoException.UnsupportedFeature(
                "versionedMessages.endpoint must be configured to use getMessageVersions(). This endpoint should proxy requests to the Sockudo server REST API.",
            )

        val payload = performPresenceHistoryRequest(
            endpoint = config.endpoint,
            headers = config.headers + (config.headersProvider?.invoke() ?: emptyMap()),
            channelName = channelName,
            params = params.toPayload(),
            action = "get_message_versions",
            messageSerial = messageSerial,
        )

        return decodeMessageVersionsPage(payload, channelName) { cursor ->
            fetchMessageVersions(channelName, messageSerial, params.copy(cursor = cursor))
        }
    }

    internal suspend fun publishAnnotation(
        channelName: String,
        messageSerial: String,
        annotation: PublishAnnotationRequest,
    ): PublishAnnotationResponse {
        val config = options.versionedMessages
            ?: throw SockudoException.UnsupportedFeature(
                "versionedMessages.endpoint must be configured to use publishAnnotation(). This endpoint should proxy requests to the Sockudo server REST API.",
            )

        val payload = performPresenceHistoryRequest(
            endpoint = config.endpoint,
            headers = config.headers + (config.headersProvider?.invoke() ?: emptyMap()),
            channelName = channelName,
            params = emptyMap(),
            action = "publish_annotation",
            messageSerial = messageSerial,
            annotation = annotation.toPayload(),
        )

        return PublishAnnotationResponse(payload["annotationSerial"] as? String ?: "")
    }

    internal suspend fun deleteAnnotation(
        channelName: String,
        messageSerial: String,
        annotationSerial: String,
        socketId: String? = null,
    ): DeleteAnnotationResponse {
        val config = options.versionedMessages
            ?: throw SockudoException.UnsupportedFeature(
                "versionedMessages.endpoint must be configured to use deleteAnnotation(). This endpoint should proxy requests to the Sockudo server REST API.",
            )

        val payload = performPresenceHistoryRequest(
            endpoint = config.endpoint,
            headers = config.headers + (config.headersProvider?.invoke() ?: emptyMap()),
            channelName = channelName,
            params = emptyMap(),
            action = "delete_annotation",
            messageSerial = messageSerial,
            annotationSerial = annotationSerial,
            socketId = socketId,
        )

        return DeleteAnnotationResponse(
            annotationSerial = payload["annotationSerial"] as? String ?: "",
            deletedAnnotationSerial = payload["deletedAnnotationSerial"] as? String ?: "",
        )
    }

    internal suspend fun listAnnotations(
        channelName: String,
        messageSerial: String,
        params: AnnotationEventsParams,
    ): AnnotationEventsPage {
        val config = options.versionedMessages
            ?: throw SockudoException.UnsupportedFeature(
                "versionedMessages.endpoint must be configured to use listAnnotations(). This endpoint should proxy requests to the Sockudo server REST API.",
            )

        val payload = performPresenceHistoryRequest(
            endpoint = config.endpoint,
            headers = config.headers + (config.headersProvider?.invoke() ?: emptyMap()),
            channelName = channelName,
            params = params.toPayload(),
            action = "list_annotations",
            messageSerial = messageSerial,
        )

        return decodeAnnotationEventsPage(payload, channelName, messageSerial) { cursor ->
            listAnnotations(channelName, messageSerial, params.copy(fromSerial = cursor))
        }
    }

    internal suspend fun createVersionedMessage(
        channelName: String,
        request: VersionedMessageCreateRequest,
        timeout: Duration,
    ): VersionedMessageCreateAck {
        val fields = performVersionedMessageMutation(channelName, "message_create", request.toPayload(), timeout)
        return VersionedMessageCreateAck(
            messageSerial = fields.messageSerial,
            versionSerial = fields.versionSerial,
            historySerial = fields.historySerial,
            deliverySerial = fields.deliverySerial,
            eventName = fields.eventName,
            raw = fields.raw,
        )
    }

    internal suspend fun appendVersionedMessage(
        channelName: String,
        request: VersionedMessageAppendRequest,
        timeout: Duration,
    ): VersionedMessageAppendAck {
        val fields = performVersionedMessageMutation(channelName, "message_append", request.toPayload(), timeout)
        return VersionedMessageAppendAck(
            messageSerial = fields.messageSerial,
            versionSerial = fields.versionSerial,
            historySerial = fields.historySerial,
            deliverySerial = fields.deliverySerial,
            eventName = fields.eventName,
            raw = fields.raw,
        )
    }

    internal suspend fun updateVersionedMessage(
        channelName: String,
        request: VersionedMessageUpdateRequest,
        timeout: Duration,
    ): VersionedMessageUpdateAck {
        val fields = performVersionedMessageMutation(channelName, "message_update", request.toPayload(), timeout)
        return VersionedMessageUpdateAck(
            messageSerial = fields.messageSerial,
            versionSerial = fields.versionSerial,
            historySerial = fields.historySerial,
            deliverySerial = fields.deliverySerial,
            eventName = fields.eventName,
            raw = fields.raw,
        )
    }

    internal suspend fun deleteVersionedMessage(
        channelName: String,
        request: VersionedMessageDeleteRequest,
        timeout: Duration,
    ): VersionedMessageDeleteAck {
        val fields = performVersionedMessageMutation(channelName, "message_delete", request.toPayload(), timeout)
        return VersionedMessageDeleteAck(
            messageSerial = fields.messageSerial,
            versionSerial = fields.versionSerial,
            historySerial = fields.historySerial,
            deliverySerial = fields.deliverySerial,
            eventName = fields.eventName,
            raw = fields.raw,
        )
    }

    private suspend fun performVersionedMessageMutation(
        channelName: String,
        action: String,
        params: Map<String, Any?>,
        timeout: Duration,
    ): VersionedMessageAckFields {
        val config = options.versionedMessages
            ?: throw SockudoException.UnsupportedFeature(
                "versionedMessages.endpoint must be configured to use mutable message helpers. This endpoint should proxy requests to the Sockudo server REST API.",
            )

        val payload =
            performVersionedMessagesRequest(
                endpoint = config.endpoint,
                headers = config.headers + (config.headersProvider?.invoke() ?: emptyMap()),
                channelName = channelName,
                action = action,
                params = params,
                timeout = timeout,
            )
        return decodeVersionedMessageAckFields(payload)
    }

    internal fun launchSubscription(block: suspend () -> Unit) {
        scope.launch { block() }
    }

    internal fun sendEvent(name: String, data: Any?, channel: String?): Boolean {
        val socket = webSocket ?: return false
        val payload = linkedMapOf<String, Any?>(
            "event" to name,
            "data" to data,
        )
        channel?.let { payload["channel"] = it }
        return when (val encoded = ProtocolCodec.encodeEnvelope(payload, options.wireFormat)) {
            is String -> socket.send(encoded)
            is ByteArray -> socket.send(encoded.toByteString())
            else -> false
        }
    }

    private fun subscribeAll() {
        channels.values.forEach { it.subscribeIfPossible() }
    }

    private fun launchAuthRefresh(reason: ClientAuthTokenReason = ClientAuthTokenReason.REFRESH) {
        if (options.protocolVersion < 2) {
            return
        }
        scope.launch {
            runCatching {
                refreshAuthToken(reason, sendRefreshFrame = true)
            }.onFailure { dispatcher.emit("error", it) }
        }
    }

    private suspend fun refreshAuthToken(
        reason: ClientAuthTokenReason,
        sendRefreshFrame: Boolean,
    ) {
        val provider = options.authTokenProvider
        val token =
            if (provider != null) {
                provider.token(ClientAuthTokenRequest(socketId = socketId, reason = reason))
            } else {
                options.authToken
            }
        currentAuthToken = token?.takeIf { it.isNotBlank() }
        if (sendRefreshFrame && currentAuthToken != null) {
            sendEvent(p.event("auth"), mapOf("token" to currentAuthToken), null)
        }
        if (manuallyDisconnected) {
            invalidateAuthRefreshTimer()
        } else {
            scheduleAuthTokenRefresh(currentAuthToken, provider)
        }
    }

    private fun scheduleAuthTokenRefresh(
        token: String?,
        provider: ClientAuthTokenProvider?,
    ) {
        invalidateAuthRefreshTimer()
        if (token == null || provider == null) {
            return
        }
        val delayMillis = clientAuthTokenRefreshDelayMillis(token) ?: return
        authRefreshJob =
            scope.launch {
                delay(delayMillis)
                if (manuallyDisconnected) {
                    return@launch
                }
                runCatching {
                    refreshAuthToken(ClientAuthTokenReason.REFRESH, sendRefreshFrame = true)
                }.onFailure { dispatcher.emit("error", it) }
            }
    }

    private fun createChannel(name: String): SockudoChannel =
        when {
            name.startsWith("private-encrypted-") -> EncryptedChannel(name, this)
            name.startsWith("presence-") -> PresenceChannel(name, this)
            name.startsWith("private-") -> PrivateChannel(name, this)
            name.startsWith("#") -> {
                SockudoLogger.error("Cannot create a channel with name '$name'")
                SockudoChannel(name, this)
            }

            else -> SockudoChannel(name, this)
        }

    private fun openWebSocket(transport: SockudoTransport) {
        currentTransport = transport
        val url = socketUrl(transport)
        val request = Request.Builder().url(url).build()
        val webSocketClient =
            if (options.protocolVersion >= 2) {
                httpClient
                    .newBuilder()
                    .pingInterval(config.activityTimeout.inWholeMilliseconds, TimeUnit.MILLISECONDS)
                    .build()
            } else {
                httpClient
            }
        webSocket =
            webSocketClient.newWebSocket(
                request,
                object : WebSocketListener() {
                    override fun onOpen(webSocket: WebSocket, response: Response) = Unit

                    override fun onMessage(webSocket: WebSocket, text: String) {
                        handleRawMessage(text)
                    }

                    override fun onMessage(webSocket: WebSocket, bytes: okio.ByteString) {
                        handleRawMessage(bytes)
                    }

                    override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
                        dispatcher.emit("error", t)
                        handleSocketClosed(1006, t.message)
                    }

                    override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
                        handleSocketClosed(code, reason)
                    }
                },
            )
    }

    private fun handleRawMessage(rawMessage: Any) {
        try {
            val event = decodeEvent(rawMessage)
            if (event.messageId != null && deduplicator != null) {
                if (deduplicator.isDuplicate(event.messageId)) {
                    return
                }
                deduplicator.track(event.messageId)
            }
            resetActivityTimer()
            // Track serial per channel for connection recovery
            if (options.connectionRecovery && event.channel != null && event.serial != null) {
                channelPositions[event.channel] =
                    RecoveryPosition(
                        streamId = event.streamId,
                        serial = event.serial,
                        lastMessageId = event.messageId,
                    )
            }

            val eventName = event.event
            when {
                eventName == p.event("connection_established") -> {
                    val payload = event.data as? Map<*, *> ?: throw SockudoException.InvalidHandshake
                    val newSocketId = payload["socket_id"] as? String ?: throw SockudoException.InvalidHandshake
                    socketId = newSocketId
                    val negotiatedTimeout = ((payload["activity_timeout"] as? Number)?.toDouble()
                        ?: config.activityTimeout.inWholeSeconds.toDouble()) * 1000.0
                    config.activityTimeout =
                        minOf(config.activityTimeout.inWholeMilliseconds.toDouble(), negotiatedTimeout).toLong()
                            .milliseconds()
                    clearUnavailableTimer()
                    updateState(ConnectionState.CONNECTED, mapOf("socket_id" to newSocketId))
                    subscribeAll()
                    if (options.connectionRecovery && channelPositions.isNotEmpty()) {
                        sendEvent(
                            p.event("resume"),
                            JsonSupport.encode(
                                mapOf(
                                    "channel_positions" to
                                        channelPositions.mapValues { (_, position) ->
                                            linkedMapOf<String, Any?>(
                                                "serial" to position.serial,
                                                "stream_id" to position.streamId,
                                                "last_message_id" to position.lastMessageId,
                                            ).filterValues { it != null }
                                        },
                                ),
                            ),
                            null,
                        )
                    }
                    if (options.deltaCompression?.enabled == true) {
                        deltaManager?.enable()
                    }
                    user.handleConnected()
                }

                eventName == p.event("error") -> {
                    if (isTokenExpiredPayload(event.data)) {
                        launchAuthRefresh(ClientAuthTokenReason.EXPIRED)
                    }
                    dispatcher.emit("error", event.data)
                }
                eventName == p.event("auth_success") -> dispatcher.emit(eventName, event.data)
                eventName == p.event("token_expired") -> {
                    launchAuthRefresh(ClientAuthTokenReason.EXPIRED)
                    dispatcher.emit(eventName, event.data)
                }
                eventName == p.event("ping") -> sendEvent(p.event("pong"), emptyMap<String, Any>(), null)
                eventName == p.event("pong") -> Unit
                eventName == p.event("signin_success") -> user.handleSignInSuccess(event.data)
                eventName == p.internal_("watchlist_events") -> watchlist.handle(event.data)
                eventName == p.event("delta_compression_enabled") -> {
                    deltaManager?.handleEnabled(event.data)
                    dispatcher.emit(eventName, event.data)
                }

                eventName == p.event("delta_cache_sync") -> {
                    event.channel?.let { channelName ->
                        deltaManager?.handleCacheSync(channelName, event.data)
                    }
                }

                eventName == p.event("delta") -> {
                    event.channel?.let { channelName ->
                        val reconstructed = deltaManager?.handleDeltaMessage(channelName, event.data)
                        if (reconstructed != null) {
                            channels[channelName]?.handle(reconstructed)
                            dispatcher.emit(reconstructed.event, reconstructed.data)
                        }
                    }
                }

                eventName == p.event("resume_success") -> {
                    val data = decodeResumeSuccessData(event.data)
                    data.recovered.forEach { recovered ->
                        recovered.position?.let { position ->
                            channelPositions[recovered.channel] = position
                        }
                    }
                    SockudoLogger.debug("Connection recovery succeeded", data)
                    dispatcher.emit(eventName, data)
                }

                eventName == p.event("resume_failed") -> {
                    val failData = decodeResumeFailedData(event.data)
                    val failedChannelName = failData.channel
                    if (failedChannelName.isNotEmpty()) {
                        channelPositions.remove(failedChannelName)
                        SockudoLogger.warn("Connection recovery failed for channel", failedChannelName)
                        channels[failedChannelName]?.subscribeIfPossible()
                    }
                    dispatcher.emit(eventName, failData)
                }

                else -> {
                    val normalizedEvent =
                        if (eventName == p.event("rewind_complete")) {
                            event.copy(data = decodeRewindCompleteData(event.data))
                        } else {
                            event
                        }
                    normalizedEvent.channel?.let { channelName ->
                        channels[channelName]?.handle(normalizedEvent)
                        if (!p.isPlatformEvent(eventName) &&
                            !p.isInternalEvent(eventName) &&
                            normalizedEvent.sequence != null
                        ) {
                            deltaManager?.handleFullMessage(
                                channel = channelName,
                                rawMessage = stripDeltaMetadata(rawMessage),
                                sequence = normalizedEvent.sequence,
                                conflationKey = normalizedEvent.conflationKey,
                            )
                        }
                    }
                    if (!p.isInternalEvent(eventName)) {
                        dispatcher.emit(eventName, normalizedEvent.data, EventMetadata(normalizedEvent.userId))
                    }
                }
            }
        } catch (error: Throwable) {
            dispatcher.emit("error", error)
        }
    }

    private fun decodeEvent(rawMessage: Any): SockudoEvent = ProtocolCodec.decodeEvent(rawMessage, options.wireFormat)

    private fun isTokenExpiredPayload(data: Any?): Boolean {
        val map = data as? Map<*, *> ?: return false
        val code = map["code"] ?: map["status"] ?: map["error_code"]
        return parseSockudoLong(code) in setOf(40142L, 40160L)
    }

    private fun stripDeltaMetadata(rawMessage: Any): String =
        (rawMessage as? String ?: decodeEvent(rawMessage).rawMessage)
            .replace(Regex(""","__delta_seq":\d+"""), "")
            .replace(Regex(""""__delta_seq":\d+,"""), "")
            .replace(Regex(""","__conflation_key":"[^"]*"""), "")
            .replace(Regex(""""__conflation_key":"[^"]*","""), "")

    private fun handleSocketClosed(code: Int, reason: String?) {
        invalidateActivityTimer()
        clearUnavailableTimer()
        webSocket = null
        channels.values.forEach { it.disconnect() }

        when (closeAction(code)) {
            CloseAction.TlsOnly -> {
                config.useTls = true
                scheduleRetry(Duration.ZERO)
            }

            CloseAction.Backoff -> scheduleRetry(1.seconds)
            CloseAction.Retry -> scheduleRetry(Duration.ZERO)
            CloseAction.Refused -> updateState(ConnectionState.DISCONNECTED)
            null -> if (!manuallyDisconnected) {
                scheduleRetry(1.seconds)
            }
        }

        if (!reason.isNullOrBlank()) {
            dispatcher.emit("error", SockudoException.ConnectionUnavailable)
            SockudoLogger.warn("Socket closed", code, reason)
        }
    }

    private fun closeAction(code: Int): CloseAction? =
        when {
            code < 4000 -> if (code in 1002..1004) CloseAction.Backoff else null
            code == 4000 -> CloseAction.TlsOnly
            code < 4100 -> CloseAction.Refused
            code < 4200 -> CloseAction.Backoff
            code < 4300 -> CloseAction.Retry
            else -> CloseAction.Refused
        }

    private fun socketUrl(transport: SockudoTransport): String {
        val scheme = if (transport == SockudoTransport.wss) "wss" else "ws"
        val host = config.wsHost
        val port = if (transport == SockudoTransport.wss) config.wssPort else config.wsPort
        val path = "${config.wsPath}/app/$key"
        val query =
            QueryString.encode(
                linkedMapOf<String, AuthValue>().apply {
                    put("protocol", AuthValue.Integer(p.version))
                    if (options.protocolVersion >= 2) {
                        put("format", AuthValue.Text(options.wireFormat.queryValue))
                        put("append_mode", AuthValue.Text(options.appendMode.queryValue))
                        options.appendRollupWindow?.let { put("append_rollup_window", AuthValue.Integer(it)) }
                        currentAuthToken?.let { put("token", AuthValue.Text(it)) }
                    }
                    put("client", AuthValue.Text("kotlin"))
                    put("version", AuthValue.Text("2.1.0"))
                    put("flash", AuthValue.Flag(false))
                },
            )
        return URI(scheme, null, host, port, path, query, null).toString()
    }

    private fun transportSequence(): List<SockudoTransport> {
        var transports =
            if (config.useTls) listOf(SockudoTransport.wss) else listOf(SockudoTransport.ws, SockudoTransport.wss)
        config.enabledTransports?.let { enabled ->
            transports = transports.filter { it in enabled }
        }
        config.disabledTransports?.let { disabled ->
            transports = transports.filterNot { it in disabled }
        }
        return transports
    }

    private fun decodeResumeRecoveredChannel(raw: Any?): ResumeRecoveredChannel {
        val map = raw as? Map<*, *> ?: emptyMap<Any?, Any?>()
        return ResumeRecoveredChannel(
            channel = map["channel"] as? String ?: "",
            source = map["source"] as? String ?: "",
            replayed = (map["replayed"] as? Number)?.toInt() ?: 0,
            position = decodeRecoveryPosition(map["position"]),
        )
    }

    private fun decodeRecoveryPosition(raw: Any?): RecoveryPosition? {
        val map = raw as? Map<*, *> ?: return null
        val serial = parseSockudoLong(map["serial"]) ?: return null
        return RecoveryPosition(
            streamId = map["stream_id"] as? String,
            serial = serial,
            lastMessageId = map["last_message_id"] as? String,
        )
    }

    private fun decodeResumeFailedData(raw: Any?): ResumeFailedChannel {
        val map = raw as? Map<*, *> ?: emptyMap<Any?, Any?>()
        return ResumeFailedChannel(
            channel = map["channel"] as? String ?: "",
            code = map["code"] as? String ?: "",
            reason = map["reason"] as? String ?: "",
            expectedStreamId = map["expected_stream_id"] as? String,
            currentStreamId = map["current_stream_id"] as? String,
            oldestAvailableSerial = (map["oldest_available_serial"] as? Number)?.toLong(),
            newestAvailableSerial = (map["newest_available_serial"] as? Number)?.toLong(),
        )
    }

    private fun decodeResumeSuccessData(raw: Any?): ResumeSuccessData {
        val map = raw as? Map<*, *> ?: emptyMap<Any?, Any?>()
        val recovered =
            (map["recovered"] as? List<*>)?.map(::decodeResumeRecoveredChannel) ?: emptyList()
        val failed = (map["failed"] as? List<*>)?.map(::decodeResumeFailedData) ?: emptyList()
        return ResumeSuccessData(recovered = recovered, failed = failed)
    }

    private fun decodeRewindCompleteData(raw: Any?): RewindCompleteData {
        val map = raw as? Map<*, *> ?: emptyMap<Any?, Any?>()
        return RewindCompleteData(
            historicalCount = (map["historical_count"] as? Number)?.toInt() ?: 0,
            liveCount = (map["live_count"] as? Number)?.toInt() ?: 0,
            complete = map["complete"] as? Boolean ?: false,
            truncatedByRetention = map["truncated_by_retention"] as? Boolean ?: false,
            truncatedByLimit = map["truncated_by_limit"] as? Boolean ?: false,
        )
    }

    private fun sendPing() {
        if (options.protocolVersion >= 2) {
            return
        }
        sendEvent(p.event("ping"), emptyMap<String, Any>(), null)
        invalidateActivityTimer()
        activityJob =
            scope.launch {
                delay(config.pongTimeout)
                scheduleRetry(Duration.ZERO)
            }
    }

    private fun resetActivityTimer() {
        invalidateActivityTimer()
        if (options.protocolVersion >= 2) {
            return
        }
        activityJob =
            scope.launch {
                delay(config.activityTimeout)
                sendPing()
            }
    }

    private fun invalidateActivityTimer() {
        activityJob?.cancel()
        activityJob = null
    }

    private fun setUnavailableTimer() {
        clearUnavailableTimer()
        unavailableJob =
            scope.launch {
                delay(config.unavailableTimeout)
                updateState(ConnectionState.UNAVAILABLE)
            }
    }

    private fun clearUnavailableTimer() {
        unavailableJob?.cancel()
        unavailableJob = null
    }

    private fun scheduleRetry(after: Duration) {
        if (manuallyDisconnected) {
            return
        }
        retryJob?.cancel()
        retryJob =
            scope.launch {
                delay(after)
                webSocket?.cancel()
                webSocket = null
                updateState(ConnectionState.CONNECTING)
                val transports = transportSequence()
                if (currentTransport == SockudoTransport.ws && !attemptedFallback && transports.contains(
                        SockudoTransport.wss
                    )
                ) {
                    attemptedFallback = true
                    openWebSocket(SockudoTransport.wss)
                } else {
                    attemptedFallback = false
                    openWebSocket(transports.firstOrNull() ?: SockudoTransport.wss)
                }
                setUnavailableTimer()
            }
    }

    private fun invalidateTimers() {
        invalidateActivityTimer()
        invalidateAuthRefreshTimer()
        clearUnavailableTimer()
        retryJob?.cancel()
        retryJob = null
    }

    private fun invalidateAuthRefreshTimer() {
        authRefreshJob?.cancel()
        authRefreshJob = null
    }

    private fun updateState(state: ConnectionState, metadata: Map<String, Any?>? = null) {
        val previous = connectionState
        connectionState = state
        dispatcher.emit(
            "state_change",
            mapOf("previous" to previous.name.lowercase(), "current" to state.name.lowercase())
        )
        dispatcher.emit(state.name.lowercase(), metadata)
    }

    class UserFacade {
        private var client: SockudoClient? = null
        private val dispatcher = EventDispatcher { event, _ ->
            SockudoLogger.debug("No callbacks on user for $event")
        }

        var isSignInRequested: Boolean = false
            private set
        var userData: Map<String, Any?>? = null
            private set
        val userId: String?
            get() = userData?.get("id") as? String
        private var serverChannel: SockudoChannel? = null

        internal fun attach(client: SockudoClient) {
            this.client = client
        }

        fun on(eventName: String, callback: (Any?, EventMetadata?) -> Unit): EventBindingToken =
            dispatcher.bind(eventName, callback)

        fun signIn() {
            isSignInRequested = true
            attemptSignIn()
        }

        internal fun handleConnected() {
            attemptSignIn()
        }

        internal fun handleSignInSuccess(data: Any?) {
            val payload = data as? Map<*, *> ?: run {
                cleanup()
                return
            }
            val userDataString = payload["user_data"] as? String ?: run {
                cleanup()
                return
            }
            val parsed = JsonSupport.fromJsonElement(JsonSupport.decode(userDataString)) as? Map<String, Any?> ?: run {
                cleanup()
                return
            }
            val userId = parsed["id"] as? String
            if (userId.isNullOrBlank()) {
                cleanup()
                return
            }
            userData = parsed
            subscribeServerChannel(userId)
        }

        private fun attemptSignIn() {
            val client = client ?: return
            if (!isSignInRequested || client.connectionState != ConnectionState.CONNECTED) {
                return
            }
            val socketId = client.socketId ?: return
            client.scope.launch {
                runCatching {
                    client.config.userAuthenticator.authenticate(UserAuthenticationRequest(socketId))
                }.onSuccess { auth ->
                    client.sendEvent(
                        client.p.event("signin"),
                        mapOf("auth" to auth.auth, "user_data" to auth.userData),
                        null,
                    )
                }.onFailure {
                    cleanup()
                }
            }
        }

        private fun subscribeServerChannel(userId: String) {
            val client = client ?: return
            val channel = SockudoChannel("#server-to-user-$userId", client)
            channel.onGlobal { eventName, data ->
                if (!client.p.isInternalEvent(eventName) && !client.p.isPlatformEvent(eventName)) {
                    dispatcher.emit(eventName, data)
                }
            }
            serverChannel = channel
            channel.subscribeIfPossible()
        }

        private fun cleanup() {
            userData = null
            serverChannel?.unbindAll()
            serverChannel?.disconnect()
            serverChannel = null
        }
    }

    class WatchlistFacade {
        private val dispatcher = EventDispatcher { event, _ ->
            SockudoLogger.debug("No callbacks on watchlist for $event")
        }

        fun on(eventName: String, callback: (Any?, EventMetadata?) -> Unit): EventBindingToken =
            dispatcher.bind(eventName, callback)

        internal fun attach(client: SockudoClient) = Unit

        internal fun handle(data: Any?) {
            val payload = data as? Map<*, *> ?: return
            val events = payload["events"] as? List<*> ?: return
            events.filterIsInstance<Map<*, *>>().forEach { event ->
                val name = event["name"] as? String ?: return@forEach
                dispatcher.emit(name, event)
            }
        }
    }

    internal class ResolvedConfiguration(
        options: SockudoOptions,
        private val httpClient: OkHttpClient,
    ) {
        val cluster: String = options.cluster
        var activityTimeout: Duration = options.activityTimeout
        var useTls: Boolean = options.forceTls != false
        val wsHost: String = options.wsHost ?: "ws-${options.cluster}.sockudo.io"
        val wsPort: Int = options.wsPort
        val wssPort: Int = options.wssPort
        val wsPath: String = options.wsPath
        val httpHost: String = options.httpHost ?: "sockjs-${options.cluster}.sockudo.io"
        val httpPort: Int = options.httpPort
        val httpsPort: Int = options.httpsPort
        val httpPath: String = options.httpPath
        val pongTimeout: Duration = options.pongTimeout
        val unavailableTimeout: Duration = options.unavailableTimeout
        val enableStats: Boolean = options.enableStats
        val statsHost: String = options.statsHost
        val timelineParams: Map<String, AuthValue> = options.timelineParams
        val enabledTransports: List<SockudoTransport>? = options.enabledTransports
        val disabledTransports: List<SockudoTransport>? = options.disabledTransports
        val channelAuthorizer: ChannelAuthorizationHandler =
            options.channelAuthorization.customHandler ?: makeChannelAuthorizer(options.channelAuthorization)
        val userAuthenticator: UserAuthenticationHandler =
            options.userAuthentication.customHandler ?: makeUserAuthenticator(options.userAuthentication)

        private fun makeChannelAuthorizer(options: ChannelAuthorizationOptions): ChannelAuthorizationHandler =
            ChannelAuthorizationHandler { request ->
                performAuthRequest(
                    endpoint = options.endpoint,
                    headers = options.headers + (options.headersProvider?.invoke() ?: emptyMap()),
                    params =
                        options.params +
                                (options.paramsProvider?.invoke() ?: emptyMap()) +
                                mapOf(
                                    "socket_id" to AuthValue.Text(request.socketId),
                                    "channel_name" to AuthValue.Text(request.channelName),
                                ),
                    parse = { json ->
                        val auth = json["auth"] as? String
                            ?: throw SockudoException.AuthFailure(200, "JSON returned from auth endpoint was invalid")
                        ChannelAuthorizationData(
                            auth = auth,
                            channelData = json["channel_data"] as? String,
                            sharedSecret = json["shared_secret"] as? String,
                        )
                    },
                )
            }

        private fun makeUserAuthenticator(options: UserAuthenticationOptions): UserAuthenticationHandler =
            UserAuthenticationHandler { request ->
                performAuthRequest(
                    endpoint = options.endpoint,
                    headers = options.headers + (options.headersProvider?.invoke() ?: emptyMap()),
                    params =
                        options.params +
                                (options.paramsProvider?.invoke() ?: emptyMap()) +
                                mapOf("socket_id" to AuthValue.Text(request.socketId)),
                    parse = { json ->
                        val auth = json["auth"] as? String
                            ?: throw SockudoException.AuthFailure(200, "JSON returned from auth endpoint was invalid")
                        val userData = json["user_data"] as? String
                            ?: throw SockudoException.AuthFailure(200, "JSON returned from auth endpoint was invalid")
                        UserAuthenticationData(auth, userData)
                    },
                )
            }

        private suspend fun <T> performAuthRequest(
            endpoint: String,
            headers: Map<String, String>,
            params: Map<String, AuthValue>,
            parse: (Map<String, Any?>) -> T,
        ): T {
            val request =
                Request.Builder()
                    .url(endpoint)
                    .post(QueryString.encode(params).toRequestBody("application/x-www-form-urlencoded".toMediaType()))
                    .apply {
                        headers.forEach { (name, value) -> addHeader(name, value) }
                    }
                    .build()

            val response = httpClient.newCall(request).execute()
            response.use {
                if (!it.isSuccessful) {
                    throw SockudoException.AuthFailure(
                        it.code,
                        "Could not get auth info from endpoint, status: ${it.code}",
                    )
                }
                val body = it.body?.string()
                    ?: throw SockudoException.AuthFailure(it.code, "Auth endpoint returned an empty body")
                val parsed = JsonSupport.fromJsonElement(JsonSupport.decode(body)) as? Map<String, Any?>
                    ?: throw SockudoException.AuthFailure(200, "JSON returned from auth endpoint was invalid")
                return parse(parsed)
            }
        }
    }

    private enum class CloseAction {
        TlsOnly,
        Refused,
        Backoff,
        Retry,
    }
}

private suspend fun SockudoClient.performPresenceHistoryRequest(
    endpoint: String,
    headers: Map<String, String>,
    channelName: String,
    params: Map<String, Any>,
    action: String,
    messageSerial: String? = null,
    annotationSerial: String? = null,
    socketId: String? = null,
    annotation: Map<String, Any?>? = null,
): Map<String, Any?> {
    val request =
        Request.Builder()
            .url(endpoint)
            .post(
                JsonSupport.encode(
                    mapOf(
                        "channel" to channelName,
                        "params" to params,
                        "action" to action,
                        "messageSerial" to messageSerial,
                        "annotationSerial" to annotationSerial,
                        "socketId" to socketId,
                        "annotation" to annotation,
                    ),
                ).toRequestBody("application/json".toMediaType()),
            )
            .apply {
                headers.forEach { (name, value) -> addHeader(name, value) }
                addHeader("Content-Type", "application/json")
            }
            .build()

    val response = httpClient.newCall(request).execute()
    response.use {
        val body = it.body?.string().orEmpty()
        if (!it.isSuccessful) {
            throw SockudoException.InvalidOptions(
                "Presence $action request failed (${it.code}): $body",
            )
        }
        return JsonSupport.fromJsonElement(JsonSupport.decode(body)) as? Map<String, Any?>
            ?: throw SockudoException.InvalidOptions(
                "Presence $action endpoint returned invalid JSON",
            )
    }
}

private suspend fun SockudoClient.performVersionedMessagesRequest(
    endpoint: String,
    headers: Map<String, String>,
    channelName: String,
    action: String,
    params: Map<String, Any?>,
    timeout: Duration,
): Map<String, Any?> =
    withTimeout(timeout) {
        withContext(Dispatchers.IO) {
            val request =
                Request.Builder()
                    .url(endpoint)
                    .post(
                        JsonSupport.encode(
                            mapOf(
                                "channel" to channelName,
                                "params" to params,
                                "action" to action,
                            ),
                        ).toRequestBody("application/json".toMediaType()),
                    )
                    .apply {
                        headers.forEach { (name, value) -> addHeader(name, value) }
                        addHeader("Content-Type", "application/json")
                    }
                    .build()

            val response = httpClient.newCall(request).execute()
            response.use {
                val body = it.body?.string().orEmpty()
                if (!it.isSuccessful) {
                    throw SockudoException.InvalidOptions(
                        "Versioned message $action request failed (${it.code}): $body",
                    )
                }
                JsonSupport.fromJsonElement(JsonSupport.decode(body)) as? Map<String, Any?>
                    ?: throw SockudoException.InvalidOptions(
                        "Versioned message $action endpoint returned invalid JSON",
                    )
            }
        }
    }

private fun decodeChannelHistoryPage(
    payload: Map<String, Any?>,
    fetchNext: suspend (String) -> ChannelHistoryPage,
): ChannelHistoryPage {
    val items =
        (payload["items"] as? List<*>).orEmpty()
            .mapNotNull { it as? Map<String, Any?> }

    return ChannelHistoryPage(
        items = items,
        direction = payload["direction"] as? String ?: "oldest_first",
        limit = (payload["limit"] as? Number)?.toInt() ?: 0,
        hasMore = payload["has_more"] as? Boolean ?: false,
        nextCursor = payload["next_cursor"] as? String,
        bounds = payload["bounds"] as? Map<String, Any?> ?: emptyMap(),
        continuity = payload["continuity"] as? Map<String, Any?> ?: emptyMap(),
        fetchNext = fetchNext,
    )
}

private fun decodeMessageVersionsPage(
    payload: Map<String, Any?>,
    defaultChannel: String,
    fetchNext: suspend (String) -> MessageVersionsPage,
): MessageVersionsPage {
    val items =
        (payload["items"] as? List<*>).orEmpty()
            .mapNotNull { it as? Map<String, Any?> }

    return MessageVersionsPage(
        channel = payload["channel"] as? String ?: defaultChannel,
        items = items,
        direction = payload["direction"] as? String ?: "oldest_first",
        limit = (payload["limit"] as? Number)?.toInt() ?: 0,
        hasMore = payload["has_more"] as? Boolean ?: false,
        nextCursor = payload["next_cursor"] as? String,
        fetchNext = fetchNext,
    )
}

private fun decodeAnnotationEventsPage(
    payload: Map<String, Any?>,
    defaultChannel: String,
    defaultMessageSerial: String,
    fetchNext: suspend (String) -> AnnotationEventsPage,
): AnnotationEventsPage {
    val items =
        (payload["items"] as? List<*>).orEmpty()
            .mapNotNull { it as? Map<String, Any?> }

    return AnnotationEventsPage(
        channel = payload["channel"] as? String ?: defaultChannel,
        messageSerial = payload["messageSerial"] as? String ?: defaultMessageSerial,
        limit = (payload["limit"] as? Number)?.toInt() ?: 0,
        hasMore = payload["hasMore"] as? Boolean ?: false,
        nextCursor = payload["nextCursor"] as? String,
        items = items,
        fetchNext = fetchNext,
    )
}

private fun decodePresenceHistoryPage(
    payload: Map<String, Any?>,
    fetchNext: suspend (String) -> PresenceHistoryPage,
): PresenceHistoryPage {
    val items =
        (payload["items"] as? List<*>).orEmpty()
            .mapNotNull { it as? Map<String, Any?> }
            .map { item ->
                PresenceHistoryItem(
                    streamId = item["stream_id"] as? String ?: "",
                    serial = (item["serial"] as? Number)?.toLong() ?: 0L,
                    publishedAtMs = (item["published_at_ms"] as? Number)?.toLong() ?: 0L,
                    event = item["event"] as? String ?: "",
                    cause = item["cause"] as? String ?: "",
                    userId = item["user_id"] as? String ?: "",
                    connectionId = item["connection_id"] as? String,
                    deadNodeId = item["dead_node_id"] as? String,
                    payloadSizeBytes = (item["payload_size_bytes"] as? Number)?.toInt() ?: 0,
                    presenceEvent = item["presence_event"] as? Map<String, Any?> ?: emptyMap(),
                )
            }

    return PresenceHistoryPage(
        items = items,
        direction = payload["direction"] as? String ?: "oldest_first",
        limit = (payload["limit"] as? Number)?.toInt() ?: 0,
        hasMore = payload["has_more"] as? Boolean ?: false,
        nextCursor = payload["next_cursor"] as? String,
        bounds = decodePresenceHistoryBounds(payload["bounds"] as? Map<String, Any?>),
        continuity = decodePresenceHistoryContinuity(payload["continuity"] as? Map<String, Any?>),
        fetchNext = fetchNext,
    )
}

private fun decodePresenceSnapshot(payload: Map<String, Any?>): PresenceSnapshot {
    val members =
        (payload["members"] as? List<*>).orEmpty()
            .mapNotNull { it as? Map<String, Any?> }
            .map { member ->
                PresenceSnapshotMember(
                    userId = member["user_id"] as? String ?: "",
                    lastEvent = member["last_event"] as? String ?: "",
                    lastEventSerial = (member["last_event_serial"] as? Number)?.toLong() ?: 0L,
                    lastEventAtMs = (member["last_event_at_ms"] as? Number)?.toLong() ?: 0L,
                )
            }

    return PresenceSnapshot(
        channel = payload["channel"] as? String ?: "",
        members = members,
        memberCount = (payload["member_count"] as? Number)?.toInt() ?: 0,
        eventsReplayed = (payload["events_replayed"] as? Number)?.toLong() ?: 0L,
        snapshotSerial = (payload["snapshot_serial"] as? Number)?.toLong(),
        snapshotTimeMs = (payload["snapshot_time_ms"] as? Number)?.toLong(),
        continuity = decodePresenceHistoryContinuity(payload["continuity"] as? Map<String, Any?>),
    )
}

private fun decodePresenceHistoryBounds(payload: Map<String, Any?>?): PresenceHistoryBounds =
    PresenceHistoryBounds(
        startSerial = (payload?.get("start_serial") as? Number)?.toLong(),
        endSerial = (payload?.get("end_serial") as? Number)?.toLong(),
        startTimeMs = (payload?.get("start_time_ms") as? Number)?.toLong(),
        endTimeMs = (payload?.get("end_time_ms") as? Number)?.toLong(),
    )

private fun decodePresenceHistoryContinuity(payload: Map<String, Any?>?): PresenceHistoryContinuity =
    PresenceHistoryContinuity(
        streamId = payload?.get("stream_id") as? String,
        oldestAvailableSerial = (payload?.get("oldest_available_serial") as? Number)?.toLong(),
        newestAvailableSerial = (payload?.get("newest_available_serial") as? Number)?.toLong(),
        oldestAvailablePublishedAtMs = (payload?.get("oldest_available_published_at_ms") as? Number)?.toLong(),
        newestAvailablePublishedAtMs = (payload?.get("newest_available_published_at_ms") as? Number)?.toLong(),
        retainedEvents = (payload?.get("retained_events") as? Number)?.toLong() ?: 0L,
        retainedBytes = (payload?.get("retained_bytes") as? Number)?.toLong() ?: 0L,
        degraded = payload?.get("degraded") as? Boolean ?: false,
        complete = payload?.get("complete") as? Boolean ?: false,
        truncatedByRetention = payload?.get("truncated_by_retention") as? Boolean ?: false,
    )

private fun Long.milliseconds(): Duration = Duration.parse("${this}ms")
