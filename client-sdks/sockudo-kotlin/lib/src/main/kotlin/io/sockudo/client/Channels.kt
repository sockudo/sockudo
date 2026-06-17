package io.sockudo.client

import com.iwebpp.crypto.TweetNaclFast
import java.util.Base64

open class SockudoChannel internal constructor(
    val name: String,
    internal val client: SockudoClient,
) {
    internal val dispatcher = EventDispatcher { event, _ ->
        SockudoLogger.debug("No callbacks on $name for $event")
    }

    var isSubscribed: Boolean = false
        internal set
    var subscriptionPending: Boolean = false
        internal set
    var subscriptionCancelled: Boolean = false
        internal set
    var subscriptionCount: Int? = null
        internal set
    var filter: FilterNode? = null
    var deltaSettings: ChannelDeltaSettings? = null
    var eventsFilter: List<String>? = null
    var rewind: SubscriptionRewind? = null
    var annotationSubscribe: Boolean = false

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

    open fun trigger(event: String, data: Any): Boolean {
        require(event.startsWith("client-")) {
            throw SockudoException.BadEventName("Event '$event' does not start with 'client-'")
        }
        if (!isSubscribed) {
            SockudoLogger.warn("Client event triggered before channel subscription succeeded")
        }
        return client.sendEvent(event, data, name)
    }

    internal open suspend fun authorize(socketId: String): ChannelAuthorizationData =
        ChannelAuthorizationData(auth = "")

    internal fun subscribeIfPossible() {
        if (subscriptionPending && subscriptionCancelled) {
            subscriptionCancelled = false
        } else if (!subscriptionPending && client.connectionState == ConnectionState.CONNECTED) {
            client.launchSubscription { subscribe() }
        }
    }

    internal suspend fun subscribe() {
        if (isSubscribed) {
            return
        }

        subscriptionPending = true
        subscriptionCancelled = false

        try {
            val auth = authorize(client.socketId.orEmpty())
            val payload = linkedMapOf<String, Any?>(
                "auth" to auth.auth,
                "channel" to name,
            )
            auth.channelData?.let { payload["channel_data"] = it }
            filter?.let { payload["tags_filter"] = JsonSupport.fromJsonElement(JsonSupport.toJsonElement(it)) }
            deltaSettings?.let { payload["delta"] = it.subscriptionValue() }
            eventsFilter?.let { payload["events"] = it }
            rewind?.let { payload["rewind"] = it.subscriptionValue() }
            if (annotationSubscribe) {
                payload["modes"] = listOf("SUBSCRIBE", "ANNOTATION_SUBSCRIBE")
            }
            client.sendEvent(client.p.event("subscribe"), payload, null)
        } catch (error: Throwable) {
            subscriptionPending = false
            dispatcher.emit(
                client.p.event("subscription_error"),
                mapOf(
                    "type" to "AuthError",
                    "error" to (error.message ?: "Unknown auth error"),
                ),
            )
        }
    }

    suspend fun channelHistory(params: ChannelHistoryParams = ChannelHistoryParams()): ChannelHistoryPage =
        client.fetchChannelHistory(name, params)

    suspend fun getMessage(messageSerial: String): Map<String, Any?> =
        client.fetchLatestMessage(name, messageSerial)

    suspend fun getMessageVersions(
        messageSerial: String,
        params: MessageVersionsParams = MessageVersionsParams(),
    ): MessageVersionsPage = client.fetchMessageVersions(name, messageSerial, params)

    suspend fun publishAnnotation(
        messageSerial: String,
        annotation: PublishAnnotationRequest,
    ): PublishAnnotationResponse = client.publishAnnotation(name, messageSerial, annotation)

    suspend fun deleteAnnotation(
        messageSerial: String,
        annotationSerial: String,
        socketId: String? = null,
    ): DeleteAnnotationResponse = client.deleteAnnotation(name, messageSerial, annotationSerial, socketId)

    suspend fun listAnnotations(
        messageSerial: String,
        params: AnnotationEventsParams = AnnotationEventsParams(),
    ): AnnotationEventsPage = client.listAnnotations(name, messageSerial, params)

    internal open fun unsubscribe() {
        isSubscribed = false
        client.sendEvent(client.p.event("unsubscribe"), mapOf("channel" to name), null)
    }

    internal open fun disconnect() {
        isSubscribed = false
        subscriptionPending = false
    }

    internal open fun handle(event: SockudoEvent) {
        val p = client.p
        when {
            event.event == p.internal_("subscription_succeeded") -> {
                subscriptionPending = false
                isSubscribed = true
                if (subscriptionCancelled) {
                    client.unsubscribe(name)
                } else {
                    dispatcher.emit(p.event("subscription_succeeded"), event.data)
                }
            }

            event.event == p.internal_("subscription_count") -> {
                subscriptionCount = (event.data as? Map<*, *>)?.get("subscription_count") as? Int
                dispatcher.emit(p.event("subscription_count"), event.data)
            }

            event.event == p.internal_("message") -> {
                val data = event.data as? Map<*, *>
                if (data?.get("action") == "message.summary") {
                    dispatcher.emit("message.summary", event.data)
                }
            }

            event.event == p.internal_("annotation") -> {
                val data = event.data as? Map<*, *>
                val action = data?.get("action") as? String
                if (action != null) {
                    dispatcher.emit(action, event.data)
                }
            }

            else -> {
                if (!p.isInternalEvent(event.event)) {
                    dispatcher.emit(event.event, event.data, EventMetadata(userId = event.userId))
                }
            }
        }
    }
}

open class PrivateChannel internal constructor(
    name: String,
    client: SockudoClient,
) : SockudoChannel(name, client) {
    override suspend fun authorize(socketId: String): ChannelAuthorizationData =
        client.config.channelAuthorizer.authorize(ChannelAuthorizationRequest(socketId, name))
}

class PresenceMembers {
    private val members = linkedMapOf<String, Any?>()

    var count: Int = 0
        internal set
    var myId: String? = null
        internal set
    var me: PresenceMember? = null
        internal set

    fun member(id: String): PresenceMember? = members[id]?.let { PresenceMember(id, it) }

    fun forEach(body: (PresenceMember) -> Unit) {
        members.forEach { (id, info) -> body(PresenceMember(id, info)) }
    }

    internal fun rememberMyId(id: String) {
        myId = id
    }

    internal fun applySubscriptionData(data: Map<String, Any?>) {
        val presence = data["presence"] as? Map<*, *>
        val hash =
            (presence?.get("hash") as? Map<*, *>)?.entries?.associate { it.key.toString() to it.value } ?: emptyMap()
        members.clear()
        members.putAll(hash)
        count = (presence?.get("count") as? Number)?.toInt() ?: hash.size
        me = myId?.let(::member)
    }

    internal fun add(data: Map<String, Any?>): PresenceMember? {
        val userId = data["user_id"] as? String ?: return null
        val info = data["user_info"]
        if (!members.containsKey(userId)) {
            count += 1
        }
        members[userId] = info
        return PresenceMember(userId, info)
    }

    internal fun remove(data: Map<String, Any?>): PresenceMember? {
        val userId = data["user_id"] as? String ?: return null
        val info = members.remove(userId) ?: return null
        count = maxOf(0, count - 1)
        return PresenceMember(userId, info)
    }

    internal fun reset() {
        members.clear()
        count = 0
        myId = null
        me = null
    }
}

class PresenceChannel internal constructor(
    name: String,
    client: SockudoClient,
) : PrivateChannel(name, client) {
    val members = PresenceMembers()

    override suspend fun authorize(socketId: String): ChannelAuthorizationData {
        val response = super.authorize(socketId)
        val channelData = response.channelData
        if (channelData != null) {
            val parsed = JsonSupport.fromJsonElement(JsonSupport.decode(channelData)) as? Map<*, *>
            val userId = parsed?.get("user_id") as? String
            if (userId != null) {
                members.rememberMyId(userId)
                return response
            }
        }

        client.user.userId?.let {
            members.rememberMyId(it)
            return response
        }

        throw SockudoException.AuthFailure(
            null,
            "Invalid auth response for presence channel '$name'",
        )
    }

    override fun handle(event: SockudoEvent) {
        val p = client.p
        when {
            event.event == p.internal_("subscription_succeeded") -> {
                subscriptionPending = false
                isSubscribed = true
                if (subscriptionCancelled) {
                    client.unsubscribe(name)
                } else {
                    val payload = event.data as? Map<String, Any?> ?: emptyMap()
                    members.applySubscriptionData(payload)
                    dispatcher.emit(p.event("subscription_succeeded"), members)
                }
            }

            event.event == p.internal_("subscription_count") -> super.handle(event)
            event.event == p.internal_("member_added") -> {
                val payload = event.data as? Map<String, Any?> ?: return
                members.add(payload)?.let { dispatcher.emit(p.event("member_added"), it) }
            }

            event.event == p.internal_("member_removed") -> {
                val payload = event.data as? Map<String, Any?> ?: return
                members.remove(payload)?.let { dispatcher.emit(p.event("member_removed"), it) }
            }

            else -> {
                if (!p.isInternalEvent(event.event)) {
                    dispatcher.emit(event.event, event.data, EventMetadata(userId = event.userId))
                }
            }
        }
    }

    override fun disconnect() {
        members.reset()
        super.disconnect()
    }

    suspend fun history(params: PresenceHistoryParams = PresenceHistoryParams()): PresenceHistoryPage =
        client.fetchPresenceHistory(name, params)

    suspend fun snapshot(params: PresenceSnapshotParams = PresenceSnapshotParams()): PresenceSnapshot =
        client.fetchPresenceSnapshot(name, params)
}

class EncryptedChannel internal constructor(
    name: String,
    client: SockudoClient,
) : PrivateChannel(name, client) {
    private var sharedSecret: ByteArray? = null

    override suspend fun authorize(socketId: String): ChannelAuthorizationData {
        val response = super.authorize(socketId)
        val secret = response.sharedSecret
            ?: throw SockudoException.AuthFailure(
                null,
                "No shared_secret key in auth payload for encrypted channel: $name",
            )
        sharedSecret = java.util.Base64.getDecoder().decode(secret)
        return response.copy(sharedSecret = null)
    }

    override fun trigger(event: String, data: Any): Boolean {
        throw SockudoException.UnsupportedFeature(
            "Client events are not currently supported for encrypted channels",
        )
    }

    internal override fun handle(event: SockudoEvent) {
        if (client.p.isInternalEvent(event.event) || client.p.isPlatformEvent(event.event)) {
            super.handle(event)
            return
        }

        val secret = sharedSecret
        if (secret == null) {
            SockudoLogger.debug("Received encrypted event before key has been retrieved from the auth endpoint")
            return
        }

        val payload = event.data as? Map<*, *> ?: run {
            SockudoLogger.error("Unexpected format for encrypted event on $name")
            return
        }
        val cipherText = (payload["ciphertext"] as? String)?.let(Base64.getDecoder()::decode)
        val nonce = (payload["nonce"] as? String)?.let(Base64.getDecoder()::decode)
        if (cipherText == null || nonce == null) {
            SockudoLogger.error("Unexpected format for encrypted event on $name")
            return
        }
        if (cipherText.size < TweetNaclFast.SecretBox.overheadLength) {
            SockudoLogger.error("Expected encrypted event ciphertext length to be ${TweetNaclFast.SecretBox.overheadLength}, got: ${cipherText.size}")
            return
        }
        if (nonce.size < TweetNaclFast.SecretBox.nonceLength) {
            SockudoLogger.error("Expected encrypted event nonce length to be ${TweetNaclFast.SecretBox.nonceLength}, got: ${nonce.size}")
            return
        }

        val box = TweetNaclFast.SecretBox(secret)
        var bytes = box.open(cipherText, nonce)
        if (bytes == null) {
            SockudoLogger.debug("Failed to decrypt an event. Fetching a new key from the auth endpoint...")
            client.launchSubscription {
                runCatching { authorize(client.socketId.orEmpty()) }
                    .onSuccess {
                        val refreshedSecret = sharedSecret ?: return@onSuccess
                        bytes = TweetNaclFast.SecretBox(refreshedSecret).open(cipherText, nonce)
                        if (bytes == null) {
                            SockudoLogger.error("Failed to decrypt event with new key. Dropping encrypted event")
                            return@onSuccess
                        }
                        emitDecrypted(event.event, bytes!!)
                    }.onFailure {
                        SockudoLogger.error("Failed to fetch new encryption key: ${it.message}")
                    }
            }
            return
        }

        emitDecrypted(event.event, bytes)
    }

    private fun emitDecrypted(eventName: String, bytes: ByteArray) {
        val raw = bytes.decodeToString()
        val payload = runCatching { JsonSupport.fromJsonElement(JsonSupport.decode(raw)) }.getOrElse { raw }
        dispatcher.emit(eventName, payload)
    }
}
