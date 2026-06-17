package io.sockudo.rest.data;

import java.util.List;

/**
 * POJO body for batch events
 */
public class EventBatch {

    private final List<Event> batch;

    public EventBatch(final List<Event> batch) {
        this.batch = batch;
    }

    public List<Event> getBatch() {
        return batch;
    }
}
