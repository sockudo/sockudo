package io.sockudo.rest.crypto.nacl;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

/**
 * SecretBox class controls access to TweetNaclFast to prevent users from
 * reaching out to TweetNaclFast. TweetNaclFast must stay package private.
 */
public class SecretBox {

    private final static int NONCE_LENGTH = 24;

    public static Map<String, byte[]> box(final byte[] key, final byte[] message) {
        TweetNaclFast.SecretBox secretBox = new TweetNaclFast.SecretBox(key);

        final byte[] nonce = new byte[NONCE_LENGTH];
        new SecureRandom().nextBytes(nonce);
        final byte[] cipher = secretBox.box(message, nonce);

        final Map<String, byte[]> res = new HashMap<>();
        res.put("cipher", cipher);
        res.put("nonce", nonce);

        return res;
    }

    public static byte[] open(final byte[] key, final byte[] nonce, final byte[] cipher) {
        TweetNaclFast.SecretBox secretBox = new TweetNaclFast.SecretBox(key);

        final byte[] decryptedMessage = secretBox.open(cipher, nonce);

        if (decryptedMessage == null) {
            throw new RuntimeException("can't decrypt");
        }

        return decryptedMessage;
    }
}
