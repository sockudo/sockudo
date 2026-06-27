package io.sockudo.rest.data;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Tolerant representation of a Sockudo webhook body.
 */
public final class Webhook {
    private final JsonObject rawBody;
    private final Long timeMs;
    private final List<WebhookEvent> events;

    private Webhook(final JsonObject rawBody, final Long timeMs, final List<WebhookEvent> events) {
        this.rawBody = rawBody.deepCopy();
        this.timeMs = timeMs;
        this.events = Collections.unmodifiableList(new ArrayList<WebhookEvent>(events));
    }

    public static Webhook parse(final String body) {
        final JsonElement parsed = JsonParser.parseString(body);
        if (!parsed.isJsonObject()) {
            throw new JsonParseException("Webhook body must be a JSON object");
        }

        final JsonObject rawBody = parsed.getAsJsonObject();
        final List<WebhookEvent> events = new ArrayList<WebhookEvent>();
        final JsonElement eventsElement = rawBody.get("events");
        if (eventsElement != null && eventsElement.isJsonArray()) {
            final JsonArray eventArray = eventsElement.getAsJsonArray();
            for (JsonElement eventElement : eventArray) {
                if (eventElement != null && eventElement.isJsonObject()) {
                    events.add(WebhookEvent.fromJsonObject(eventElement.getAsJsonObject()));
                }
            }
        }

        return new Webhook(rawBody, fieldAsLong(rawBody, "time_ms"), events);
    }

    public Long getTimeMs() {
        return timeMs;
    }

    public List<WebhookEvent> getEvents() {
        return events;
    }

    public JsonObject getRawBody() {
        return rawBody.deepCopy();
    }

    public JsonElement get(final String fieldName) {
        final JsonElement value = rawBody.get(fieldName);
        return value == null ? null : value.deepCopy();
    }

    private static Long fieldAsLong(final JsonObject object, final String fieldName) {
        final JsonElement value = object.get(fieldName);
        if (value == null || value.isJsonNull() || !value.isJsonPrimitive()) {
            return null;
        }
        try {
            return value.getAsLong();
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
