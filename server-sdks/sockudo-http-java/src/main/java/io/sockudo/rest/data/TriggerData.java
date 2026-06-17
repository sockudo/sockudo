package io.sockudo.rest.data;

import java.util.List;

/**
 * POJO for JSON encoding of trigger request bodies.
 */
public class TriggerData {

    private final List<String> channels;
    private final String name;
    private final String data;
    private final String socketId;
    private final String idempotencyKey;
    private final MessageExtras extras;

    public TriggerData(final List<String> channels, final String eventName, final String data, final String socketId) {
        this(channels, eventName, data, socketId, null, null);
    }

    public TriggerData(final List<String> channels, final String eventName, final String data, final String socketId, final String idempotencyKey) {
        this(channels, eventName, data, socketId, idempotencyKey, null);
    }

    public TriggerData(final List<String> channels, final String eventName, final String data, final String socketId, final String idempotencyKey, final MessageExtras extras) {
        this.channels = channels;
        this.name = eventName;
        this.data = data;
        this.socketId = socketId;
        this.idempotencyKey = idempotencyKey;
        this.extras = extras;
    }

    public List<String> getChannels() {
        return channels;
    }

    public String getName() {
        return name;
    }

    public String getData() {
        return data;
    }

    public String getSocketId() {
        return socketId;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public MessageExtras getExtras() {
        return extras;
    }
}
