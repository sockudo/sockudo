package io.sockudo.rest.crypto;

import io.sockudo.rest.data.EncryptedMessage;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.*;

public class CryptoUtilTest {

    final String encryptedChannel = "private-encrypted-test";
    final String testMessage = "{\"message\": \"Hello, world!\"}";
    final byte[] testData = testMessage.getBytes(StandardCharsets.UTF_8);
    final String base64EncodedEncryptionMasterKey = "VGhlIDMyIGNoYXJzIGxvbmcgZW5jcnlwdGlvbiBrZXk=";
    final CryptoUtil crypto = new CryptoUtil(base64EncodedEncryptionMasterKey);

    @Test
    void parseEncryptionMasterKeyWithInvalidKey() {
        assertThrows(IllegalArgumentException.class, () -> new CryptoUtil(null));

        assertThrows(IllegalArgumentException.class, () -> new CryptoUtil(""));

        assertThrows(IllegalArgumentException.class, () -> new CryptoUtil("TG9yZW0gSXBzdW0ga"));
    }

    @Test
    void encrypt() {
        final EncryptedMessage encryptedMessage = crypto.encrypt(encryptedChannel, testData);

        assertNotNull(encryptedMessage.getNonce());
        assertNotNull(encryptedMessage.getCiphertext());

        assertNotEquals(testMessage, encryptedMessage.getCiphertext());
        assertNotEquals(testData, Base64.getDecoder().decode(encryptedMessage.getCiphertext()));
    }

    @Test
    void encryptionRandomness() {
        final EncryptedMessage m1 = crypto.encrypt(encryptedChannel, testData);
        final EncryptedMessage m2 = crypto.encrypt(encryptedChannel, testData);

        assertNotEquals(m1.getNonce(), m2.getNonce());
        assertNotEquals(m1.getCiphertext(), m2.getCiphertext());
    }

    @Test
    void encryptionMoreRandomnessCheck() {
        final int expectedDistinctItems = 10000;

        HashSet<String> uniqueValues = new HashSet<>();
        for (int i = 1; i <= expectedDistinctItems; i++) {
            final EncryptedMessage m = crypto.encrypt(encryptedChannel, testData);

            uniqueValues.add(m.getNonce().concat(m.getCiphertext())); // Set only stores unique values
        }

        assertEquals(
            expectedDistinctItems,
            uniqueValues.size(),
            "Generated fewer than expected unique values"
        );
    }

    @Test
    void decrypt() {
        final EncryptedMessage encryptedMessage = crypto.encrypt(encryptedChannel, testData);
        final String decryptedMessage = crypto.decrypt(encryptedChannel, encryptedMessage);

        assertEquals(testMessage, decryptedMessage);
    }

    @Test
    void generateBase64EncodedSharedSecret() {
        final String sharedKey = crypto.generateBase64EncodedSharedSecret(encryptedChannel);

        assertEquals("1O0FFr6NiG4d9D4A5bWBh3EG9Y/wfjzqw172LUXwVQ4", sharedKey);
    }
}
