package io.sockudo.client

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class DeltaCompressionTest {
    @Test
    fun appliesFossilDeltaMessages() {
        val manager = DeltaCompressionManager(DeltaOptions(), ::ignoreSend)
        manager.handleFullMessage(
            channel = "ticker:1",
            rawMessage = """{"data":{"price":101}}""",
            sequence = 1,
            conflationKey = null,
        )

        val event =
            manager.handleDeltaMessage(
                "ticker:1",
                mapOf(
                    "event" to "price-updated",
                    "delta" to "TQpNOnsiZGF0YSI6eyJwcmljZSI6MTAyfX0yQml0bVg7",
                    "seq" to 2,
                    "algorithm" to "fossil",
                ),
            )

        assertNotNull(event)
        assertEquals("price-updated", event.event)
        assertEquals(mapOf("price" to 102L), event.data)
    }

    @Test
    fun appliesVcdiffDeltaMessages() {
        val manager = DeltaCompressionManager(DeltaOptions(), ::ignoreSend)
        manager.handleFullMessage(
            channel = "ticker:1",
            rawMessage = """{"data":{"price":101}}""",
            sequence = 1,
            conflationKey = null,
        )

        val event =
            manager.handleDeltaMessage(
                "ticker:1",
                mapOf(
                    "event" to "price-updated",
                    "delta" to "1sPEAAABFgAdFgAWAgB7ImRhdGEiOnsicHJpY2UiOjEwMn19ARY=",
                    "seq" to 2,
                    "algorithm" to "xdelta3",
                ),
            )

        assertNotNull(event)
        assertEquals("price-updated", event.event)
        assertEquals(mapOf("price" to 102L), event.data)
    }

    @Test
    fun appliesInsertOnlyFossilDelta() {
        val bytes =
            FossilDelta.apply(
                base = ByteArray(0),
                delta = "5\n5:hello3NPMmh;".encodeToByteArray(),
            )

        assertEquals("hello", bytes.decodeToString())
    }

    private fun ignoreSend(event: String, data: Any): Boolean = true
}
