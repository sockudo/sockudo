package io.sockudo.rest.data;

public class EncryptedMessage {

    private final String nonce;
    private final String ciphertext;

    public EncryptedMessage(String nonce, String ciphertext) {
        this.nonce = nonce;
        this.ciphertext = ciphertext;
    }

    public String getNonce() {
        return nonce;
    }

    public String getCiphertext() {
        return ciphertext;
    }
}
