package io.sockudo.rest.data;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * Tolerant representation of one event inside a Sockudo webhook.
 *
 * <p>Event names are intentionally strings, not an enum, so future server-side
 * events remain readable without a client release.</p>
 */
public final class WebhookEvent {
    private final JsonObject rawEvent;
    private final String name;
    private final String channel;
    private final String userId;
    private final String socketId;

    private WebhookEvent(final JsonObject rawEvent) {
        this.rawEvent = rawEvent.deepCopy();
        this.name = fieldAsString(rawEvent, "name");
        this.channel = fieldAsString(rawEvent, "channel");
        this.userId = fieldAsString(rawEvent, "user_id");
        this.socketId = fieldAsString(rawEvent, "socket_id");
    }

    static WebhookEvent fromJsonObject(final JsonObject rawEvent) {
        return new WebhookEvent(rawEvent);
    }

    public String getName() {
        return name;
    }

    public String getChannel() {
        return channel;
    }

    public String getUserId() {
        return userId;
    }

    public String getSocketId() {
        return socketId;
    }

    public JsonElement getData() {
        return get("data");
    }

    public JsonObject getRawEvent() {
        return rawEvent.deepCopy();
    }

    public JsonElement get(final String fieldName) {
        final JsonElement value = rawEvent.get(fieldName);
        return value == null ? null : value.deepCopy();
    }

    public String getFieldAsString(final String fieldName) {
        return fieldAsString(rawEvent, fieldName);
    }

    public String getMessageSerial() {
        return getFieldAsString("message_serial");
    }

    public String getHistorySerial() {
        return getFieldAsString("history_serial");
    }

    public String getDeliverySerial() {
        return getFieldAsString("delivery_serial");
    }

    public String getVersionSerial() {
        return getFieldAsString("version_serial");
    }

    private static String fieldAsString(final JsonObject object, final String fieldName) {
        final JsonElement value = object.get(fieldName);
        if (value == null || value.isJsonNull() || !value.isJsonPrimitive()) {
            return null;
        }
        return value.getAsString();
    }
}
