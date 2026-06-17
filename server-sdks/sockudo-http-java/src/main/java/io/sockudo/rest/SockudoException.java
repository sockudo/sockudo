package io.sockudo.rest;

public class SockudoException extends RuntimeException {

    public SockudoException(String errorMessage) {
        super(errorMessage);
    }

    public static SockudoException encryptionMasterKeyRequired() {
        return new SockudoException("You cannot use encrypted channels without setting a master encryption key");
    }

    public static SockudoException cannotTriggerMultipleChannelsWithEncryption() {
        return new SockudoException("You cannot trigger to multiple channels when using encrypted channels");
    }
}
