package io.sockudo.rest.data;

import java.util.Map;

/**
 * V2 extras for publish events.
 */
public class MessageExtras {

    private final Map<String, Object> headers;
    private final Boolean ephemeral;
    private final String idempotencyKey;
    private final Boolean echo;

    public MessageExtras(final Map<String, Object> headers, final Boolean ephemeral, final String idempotencyKey, final Boolean echo) {
        this.headers = headers;
        this.ephemeral = ephemeral;
        this.idempotencyKey = idempotencyKey;
        this.echo = echo;
    }

    public Map<String, Object> getHeaders() {
        return headers;
    }

    public Boolean getEphemeral() {
        return ephemeral;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public Boolean getEcho() {
        return echo;
    }
}
