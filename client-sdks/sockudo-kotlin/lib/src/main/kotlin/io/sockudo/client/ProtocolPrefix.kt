package io.sockudo.client

/**
 * Protocol version prefix helpers.
 *
 * Protocol v1 uses the original Pusher event prefix ("pusher:" / "pusher_internal:").
 * Protocol v2 (Sockudo-native, the default) uses "sockudo:" / "sockudo_internal:".
 */
class ProtocolPrefix(val version: Int) {
    val eventPrefix: String = if (version >= 2) "sockudo:" else "pusher:"
    val internalPrefix: String = if (version >= 2) "sockudo_internal:" else "pusher_internal:"

    fun event(name: String): String = "$eventPrefix$name"
    fun internal_(name: String): String = "$internalPrefix$name"

    fun isInternalEvent(name: String): Boolean = name.startsWith(internalPrefix)
    fun isPlatformEvent(name: String): Boolean = name.startsWith(eventPrefix)
}
