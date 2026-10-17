package io.sockudo.client

import kotlin.test.Test
import kotlin.test.assertEquals

class QueryStringTest {
    @Test
    fun sortsKeysAndFormEncodesValues() {
        val encoded = QueryString.encode(
            mapOf(
                "socket_id" to AuthValue.Text("123.456"),
                "channel_name" to AuthValue.Text("private-a b+c&d=e/f~g*h.i-j_k"),
            ),
        )

        assertEquals("channel_name=private-a+b%2Bc%26d%3De%2Ff%7Eg*h.i-j_k&socket_id=123.456", encoded)
    }

    @Test
    fun encodesNonAsciiAsUtf8() {
        val encoded = QueryString.encode(mapOf("user name" to AuthValue.Text("Zoé 😀")))

        assertEquals("user+name=Zo%C3%A9+%F0%9F%98%80", encoded)
    }
}
