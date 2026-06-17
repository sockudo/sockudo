package io.sockudo.rest.crypto.nacl;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SecretBoxTest {

    final byte[] message = "{\"message\": \"Hello, world!\"}".getBytes(StandardCharsets.UTF_8);
    final byte[] key = Base64.getDecoder().decode("VGhlIDMyIGNoYXJzIGxvbmcgZW5jcnlwdGlvbiBrZXk=");

    @Test
    void encryptMessage() {
        final Map<String, byte[]> encryptedMessage = SecretBox.box(key, message);

        assertNotNull(encryptedMessage.get("cipher"));
        assertNotNull(encryptedMessage.get("nonce"));

        assertNotEquals(
            message,
            encryptedMessage.get("cipher")
        );

        assertArrayEquals(message, SecretBox.open(
            key,
            encryptedMessage.get("nonce"),
            encryptedMessage.get("cipher")
        ));
    }
}
