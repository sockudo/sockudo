package io.sockudo.rest.data;

import io.sockudo.rest.util.Prerequisites;

/**
 * POJO for JSON encoding of trigger batch events.
 */
public class Event {

    private final String channel;
    private final String name;
    private final Object data;
    private final String socketId;
    private final String idempotencyKey;
    private final MessageExtras extras;

    public Event(final String channel, final String eventName, final Object data) {
        this(channel, eventName, data, null, null, null);
    }

    public Event(final String channel, final String eventName, final Object data, final String socketId) {
        this(channel, eventName, data, socketId, null, null);
    }

    public Event(final String channel, final String eventName, final Object data, final String socketId, final String idempotencyKey) {
        this(channel, eventName, data, socketId, idempotencyKey, null);
    }

    public Event(final String channel, final String eventName, final Object data, final String socketId, final String idempotencyKey, final MessageExtras extras) {
        Prerequisites.nonNull("channel", channel);
        Prerequisites.nonNull("eventName", eventName);
        Prerequisites.nonNull("data", data);

        this.channel = channel;
        this.name = eventName;
        this.data = data;
        this.socketId = socketId;
        this.idempotencyKey = idempotencyKey;
        this.extras = extras;
    }

    public String getChannel() {
        return channel;
    }

    public String getName() {
        return name;
    }

    public Object getData() {
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
