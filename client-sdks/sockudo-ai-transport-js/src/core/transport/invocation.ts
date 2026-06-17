/**
 * Deterministic identity providers for client transport invocations.
 */
export interface InvocationIdProvider {
  /** Returns the next turn id. */
  turnId(): string;
  /** Returns the next invocation id. */
  invocationId(): string;
  /** Returns the next input event id. */
  inputEventId(): string;
  /** Returns the next codec message id. */
  messageId(): string;
}

/**
 * Creates the default invocation id provider.
 *
 * @defaultValue Uses `crypto.randomUUID()` and falls back to a monotonic
 * process-local id if the runtime throws.
 */
export function createDefaultInvocationIdProvider(): InvocationIdProvider {
  let counter = 0;
  const next = (prefix: string): string => {
    try {
      return `${prefix}_${globalThis.crypto.randomUUID()}`;
    } catch {
      counter += 1;
      return `${prefix}_${String(counter)}`;
    }
  };
  return {
    turnId: () => next("turn"),
    invocationId: () => next("inv"),
    inputEventId: () => next("evt"),
    messageId: () => next("msg"),
  };
}
