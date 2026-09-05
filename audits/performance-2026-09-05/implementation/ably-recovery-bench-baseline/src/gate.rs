#[derive(Clone, Default)]
struct AblyAttachGate {
    messages: Vec<AblyProtocolMessage>,
    bytes: usize,
    overflowed: bool,
}

fn push_bounded_recovery_message(gate: &mut AblyAttachGate, message: AblyProtocolMessage) {
    let message_bytes = sonic_rs::to_vec(&message)
        .map(|bytes| bytes.len())
        .unwrap_or(ABLY_ATTACH_GATE_MAX_BYTES.saturating_add(1));
    push_bounded_recovery_message_with_size(gate, message, message_bytes);
}

fn push_bounded_recovery_message_with_size(
    gate: &mut AblyAttachGate,
    message: AblyProtocolMessage,
    message_bytes: usize,
) {
    if gate.overflowed
        || gate.messages.len() >= ABLY_ATTACH_GATE_MAX_MESSAGES
        || gate.bytes.saturating_add(message_bytes) > ABLY_ATTACH_GATE_MAX_BYTES
    {
        gate.overflowed = true;
        gate.messages.clear();
        gate.bytes = 0;
        return;
    }
    gate.bytes = gate.bytes.saturating_add(message_bytes);
    gate.messages.push(message);
}

#[derive(Clone)]
struct AblyRecoveryTailMessage {
    sequence: u64,
    message: AblyProtocolMessage,
    publisher_connection_id: Option<Arc<str>>,
    echo_override: Option<bool>,
    bytes: usize,
}

struct AblyRecoveryTail {
    messages: VecDeque<AblyRecoveryTailMessage>,
    bytes: usize,
    sequence: u64,
}

impl Default for AblyRecoveryTail {
    fn default() -> Self {
        Self {
            // The common one-subscriber path records its first recovery
            // message without allocating in the delivery hot path.
            messages: VecDeque::with_capacity(1),
            bytes: 0,
            sequence: 0,
        }
    }
}

impl AblyRecoveryTail {
    fn push_with_size(
        &mut self,
        message: AblyProtocolMessage,
        publisher_connection_id: Option<&str>,
        echo_override: Option<bool>,
        message_bytes: usize,
    ) {
        self.sequence = self.sequence.saturating_add(1);
        if message_bytes > ABLY_ATTACH_GATE_MAX_BYTES {
            self.messages.clear();
            self.bytes = 0;
            return;
        }
        while self.messages.len() >= ABLY_ATTACH_GATE_MAX_MESSAGES
            || self.bytes.saturating_add(message_bytes) > ABLY_ATTACH_GATE_MAX_BYTES
        {
            let Some(expired) = self.messages.pop_front() else {
                break;
            };
            self.bytes = self.bytes.saturating_sub(expired.bytes);
        }
        self.bytes = self.bytes.saturating_add(message_bytes);
        self.messages.push_back(AblyRecoveryTailMessage {
            sequence: self.sequence,
            message,
            publisher_connection_id: publisher_connection_id.map(Arc::from),
            echo_override,
            bytes: message_bytes,
        });
    }

    fn gate_for_subscriber(
        &self,
        start: u64,
        connection_id: &str,
        echo: bool,
        mode_flags: u64,
    ) -> AblyAttachGate {
        let mut gate = AblyAttachGate::default();
        for entry in self.messages.iter().filter(|entry| entry.sequence > start) {
            if should_deliver_to_subscriber(
                entry.publisher_connection_id.as_deref(),
                connection_id,
                echo,
                entry.echo_override,
            ) && mode_flags & ABLY_MODE_SUBSCRIBE != 0
            {
                push_bounded_recovery_message(&mut gate, entry.message.clone());
            }
        }
        gate
    }
}

