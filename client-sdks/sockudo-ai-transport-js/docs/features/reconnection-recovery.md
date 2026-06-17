# Reconnection And Recovery

Sockudo client recovery handles hot replay first and durable recovery second. The SDK folds replayed
AI events into the same tree state as live delivery.
