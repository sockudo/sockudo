package io.sockudo.client

object FossilDelta {
    private const val nHash = 16
    private val digits = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz~".map { it.code.toByte() }
    private val values =
        IntArray(128) { -1 }.apply {
            digits.forEachIndexed { index, value ->
                this[value.toInt()] = index
            }
        }

    private class Reader(
        private val data: ByteArray,
    ) {
        var position: Int = 0
        val hasBytes: Boolean
            get() = position < data.size

        fun byte(): Byte {
            if (position >= data.size) {
                throw SockudoException.DeltaFailure("out of bounds")
            }
            return data[position++]
        }

        fun character(): Char = byte().toInt().toChar()

        fun integer(): Int {
            var value = 0
            while (hasBytes) {
                val raw = byte().toInt() and 0xff
                val mapped = if (raw < values.size) values[raw] else -1
                if (mapped < 0) {
                    position -= 1
                    break
                }
                value = (value shl 6) + mapped
            }
            return value
        }
    }

    fun apply(base: ByteArray, delta: ByteArray): ByteArray {
        val reader = Reader(delta)
        val outputSize = reader.integer()
        if (reader.character() != '\n') {
            throw SockudoException.DeltaFailure("size integer not terminated by newline")
        }

        val output = ArrayList<Byte>(outputSize)
        var total = 0

        while (reader.hasBytes) {
            val count = reader.integer()
            when (reader.character()) {
                '@' -> {
                    val offset = reader.integer()
                    if (reader.hasBytes && reader.character() != ',') {
                        throw SockudoException.DeltaFailure("copy command not terminated by comma")
                    }
                    total += count
                    if (total > outputSize) {
                        throw SockudoException.DeltaFailure("copy exceeds output file size")
                    }
                    if (offset + count > base.size) {
                        throw SockudoException.DeltaFailure("copy extends past end of input")
                    }
                    repeat(count) { index -> output.add(base[offset + index]) }
                }

                ':' -> {
                    total += count
                    if (total > outputSize) {
                        throw SockudoException.DeltaFailure("insert command gives an output larger than predicted")
                    }
                    if (reader.position + count > delta.size) {
                        throw SockudoException.DeltaFailure("insert count exceeds size of delta")
                    }
                    repeat(count) { output.add(delta[reader.position + it]) }
                    reader.position += count
                }

                ';' -> {
                    val bytes = output.toByteArray()
                    if (count != checksum(bytes)) {
                        throw SockudoException.DeltaFailure("bad checksum")
                    }
                    if (total != outputSize) {
                        throw SockudoException.DeltaFailure("generated size does not match predicted size")
                    }
                    return bytes
                }

                else -> throw SockudoException.DeltaFailure("unknown delta operator")
            }
        }

        throw SockudoException.DeltaFailure("unterminated delta")
    }

    private fun checksum(bytes: ByteArray): Int {
        var sum0 = 0
        var sum1 = 0
        var sum2 = 0
        var sum3 = 0
        var index = 0
        var remaining = bytes.size

        while (remaining >= nHash) {
            sum0 += bytes[index + 0].u() + bytes[index + 4].u() + bytes[index + 8].u() + bytes[index + 12].u()
            sum1 += bytes[index + 1].u() + bytes[index + 5].u() + bytes[index + 9].u() + bytes[index + 13].u()
            sum2 += bytes[index + 2].u() + bytes[index + 6].u() + bytes[index + 10].u() + bytes[index + 14].u()
            sum3 += bytes[index + 3].u() + bytes[index + 7].u() + bytes[index + 11].u() + bytes[index + 15].u()
            index += nHash
            remaining -= nHash
        }

        while (remaining >= 4) {
            sum0 += bytes[index + 0].u()
            sum1 += bytes[index + 1].u()
            sum2 += bytes[index + 2].u()
            sum3 += bytes[index + 3].u()
            index += 4
            remaining -= 4
        }

        sum3 += (sum2 shl 8) + (sum1 shl 16) + (sum0 shl 24)
        when (remaining) {
            3 -> {
                sum3 += bytes[index + 2].u() shl 8
                sum3 += bytes[index + 1].u() shl 16
                sum3 += bytes[index + 0].u() shl 24
            }

            2 -> {
                sum3 += bytes[index + 1].u() shl 16
                sum3 += bytes[index + 0].u() shl 24
            }

            1 -> sum3 += bytes[index + 0].u() shl 24
        }

        return sum3
    }

    private fun Byte.u(): Int = toInt() and 0xff
}
