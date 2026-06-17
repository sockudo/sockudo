package io.sockudo.rest.crypto;

import io.sockudo.rest.crypto.nacl.SecretBox;
import io.sockudo.rest.data.EncryptedMessage;
import io.sockudo.rest.util.Prerequisites;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Map;

public class CryptoUtil {

    private static final String SHARED_SECRET_ENCRYPTION_ALGO = "SHA-256";
    private static final int MASTER_KEY_LENGTH = 32;
    private final byte[] encryptionMasterKey;

    public CryptoUtil(final String base64EncodedMasterKey) {
        Prerequisites.nonEmpty("base64EncodedMasterKey", base64EncodedMasterKey);

        this.encryptionMasterKey = parseEncryptionMasterKey(base64EncodedMasterKey);
    }

    public String generateBase64EncodedSharedSecret(final String channel) {
        return Base64.getEncoder().withoutPadding().encodeToString(
            generateSharedSecret(channel)
        );
    }

    public EncryptedMessage encrypt(final String channel, final byte[] message) {
        final byte[] sharedSecret = generateSharedSecret(channel);

        final Map<String, byte[]> res = SecretBox.box(sharedSecret, message);

        return new EncryptedMessage(
            Base64.getEncoder().encodeToString(res.get("nonce")),
            Base64.getEncoder().encodeToString(res.get("cipher"))
        );
    }

    public String decrypt(final String channel, final EncryptedMessage encryptedMessage) {
        final byte[] sharedSecret = generateSharedSecret(channel);

        final byte[] decryptMessage = SecretBox.open(
            sharedSecret,
            Base64.getDecoder().decode(encryptedMessage.getNonce()),
            Base64.getDecoder().decode(encryptedMessage.getCiphertext().getBytes())
        );

        return new String(decryptMessage, StandardCharsets.UTF_8);
    }

    private byte[] parseEncryptionMasterKey(final String base64EncodedEncryptionMasterKey) {
        final byte[] key = Base64.getDecoder().decode(base64EncodedEncryptionMasterKey);

        if (key.length != MASTER_KEY_LENGTH) {
            throw new IllegalArgumentException("encryptionMasterKeyBase64 must be a 32 byte key, base64 encoded");
        }

        return key;
    }

    private byte[] generateSharedSecret(final String channel) {
        try {
            MessageDigest digest = MessageDigest.getInstance(CryptoUtil.SHARED_SECRET_ENCRYPTION_ALGO);
            byte[] channelB = channel.getBytes(StandardCharsets.UTF_8);

            byte[] buf = new byte[channelB.length + encryptionMasterKey.length];
            System.arraycopy(channelB, 0, buf, 0, channelB.length);
            System.arraycopy(encryptionMasterKey, 0, buf, channelB.length, encryptionMasterKey.length);

            return digest.digest(buf);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
