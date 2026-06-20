import {
  HEADER_CODEC_MESSAGE_ID,
  HEADER_STATUS,
  HEADER_STREAM,
  HEADER_STREAM_ID,
} from "../../constants.js";
import type { HeaderMap } from "../../utils.js";
import type { InboundMessage } from "../../realtime/types.js";
import type { DecodedEvent } from "./types.js";

/**
 * Stream tracker passed to decoder hooks.
 */
export interface DecoderStreamTracker {
  /** Codec message id, equal to Sockudo `message_serial`. */
  messageId: string;
  /** Latest accumulated stream content. */
  accumulated: string;
  /** Whether this tracker was synthesized from an append/update first contact. */
  firstContact: boolean;
  /** Last decoded inbound message. */
  message: InboundMessage;
}

/**
 * Hooks used by the generic decoder core.
 */
export interface DecoderCoreHooks<TEvent> {
  /** Builds stream-start events. */
  buildStartEvents(tracker: DecoderStreamTracker): DecodedEvent<TEvent>[];
  /** Builds stream-delta events. */
  buildDeltaEvents(tracker: DecoderStreamTracker, delta: string): DecodedEvent<TEvent>[];
  /** Builds stream-end events. */
  buildEndEvents(
    tracker: DecoderStreamTracker,
    closingCodecHeaders: HeaderMap,
  ): DecodedEvent<TEvent>[];
  /** Decodes a non-stream discrete payload. */
  decodeDiscrete(message: InboundMessage): DecodedEvent<TEvent>[];
  /** Builds stream-delete events. */
  buildDeleteEvents?(tracker: DecoderStreamTracker): DecodedEvent<TEvent>[];
}

/**
 * Decoder-core observability hooks.
 */
export interface DecoderCoreMetrics {
  /** Called when an old stream tracker is evicted. */
  onTrackerEvicted?(messageId: string): void;
  /** Called when an append/update is first seen without its create. */
  onFirstContact?(messageId: string): void;
  /** Called when an update replaces non-prefix accumulated content. */
  onReplacement?(messageId: string): void;
  /** Called when a tracker is closed or deleted. */
  onTrackerClosed?(messageId: string): void;
}

/**
 * Decoder-core options.
 *
 * @defaultValue `maxStreams` is `1024`.
 */
export interface DecoderCoreOptions extends DecoderCoreMetrics {
  /** Maximum open stream trackers retained by the decoder. */
  maxStreams?: number;
}

/**
 * Generic decoder core.
 */
export interface DecoderCore<TEvent> {
  /** Decodes one normalized inbound message. */
  decode(message: InboundMessage): DecodedEvent<TEvent>[];
  /** Clears all tracked streams. */
  clear(): void;
}

/**
 * Creates a decoder core that understands Sockudo mutable-message stream frames.
 */
export function createDecoderCore<TEvent>(
  hooks: DecoderCoreHooks<TEvent>,
  options: DecoderCoreOptions = {},
): DecoderCore<TEvent> {
  const maxStreams = options.maxStreams ?? 1024;
  const trackers = new Map<string, MutableTracker>();
  return {
    decode(message) {
      const messageId = codecMessageId(message);
      const transport = message.getTransportHeaders();
      const stream = transport[HEADER_STREAM] === "true";
      const trackerId = stream ? streamTrackerId(message, messageId) : messageId;
      switch (message.action) {
        case "create":
          if (!stream) {
            return hooks.decodeDiscrete(message);
          }
          return createTracker(
            trackers,
            hooks,
            options,
            maxStreams,
            trackerId,
            messageId,
            message,
            false,
          );
        case "append":
          return appendAggregateOrDeltaOrFirstContact(
            trackers,
            hooks,
            options,
            maxStreams,
            trackerId,
            messageId,
            message,
            dataAsString(message.data),
          );
        case "update":
          if (typeof message.data !== "string") {
            return metadataUpdate(trackers, hooks, options, trackerId, message);
          }
          return updateOrFirstContact(
            trackers,
            hooks,
            options,
            maxStreams,
            trackerId,
            messageId,
            message,
            dataAsString(message.data),
          );
        case "delete":
          return deleteTracker(trackers, hooks, options, messageId, message);
        case "summary":
          return [];
      }
    },
    clear() {
      trackers.clear();
    },
  };
}

function metadataUpdate<TEvent>(
  trackers: Map<string, MutableTracker>,
  hooks: DecoderCoreHooks<TEvent>,
  options: DecoderCoreOptions,
  trackerId: string,
  message: InboundMessage,
): DecodedEvent<TEvent>[] {
  const tracker = trackers.get(trackerId);
  if (!tracker) {
    return [];
  }
  touch(trackers, trackerId, tracker);
  tracker.message = message;
  return isTerminal(message) ? closeTracker(trackers, hooks, options, tracker, message) : [];
}

interface MutableTracker extends DecoderStreamTracker {
  trackerId: string;
  closed: boolean;
}

function createTracker<TEvent>(
  trackers: Map<string, MutableTracker>,
  hooks: DecoderCoreHooks<TEvent>,
  options: DecoderCoreOptions,
  maxStreams: number,
  trackerId: string,
  messageId: string,
  message: InboundMessage,
  firstContact: boolean,
  closeIfTerminal = true,
): DecodedEvent<TEvent>[] {
  const tracker: MutableTracker = {
    trackerId,
    messageId,
    accumulated: dataAsString(message.data),
    firstContact,
    message,
    closed: false,
  };
  trackers.set(trackerId, tracker);
  enforceLimit(trackers, maxStreams, options);
  const events = hooks.buildStartEvents(tracker);
  if (closeIfTerminal && isTerminal(message)) {
    events.push(...closeTracker(trackers, hooks, options, tracker, message));
  }
  return events;
}

function appendOrFirstContact<TEvent>(
  trackers: Map<string, MutableTracker>,
  hooks: DecoderCoreHooks<TEvent>,
  options: DecoderCoreOptions,
  maxStreams: number,
  trackerId: string,
  messageId: string,
  message: InboundMessage,
  delta: string,
): DecodedEvent<TEvent>[] {
  const tracker = trackers.get(trackerId);
  if (!tracker) {
    options.onFirstContact?.(messageId);
    const events = createTracker(
      trackers,
      hooks,
      options,
      maxStreams,
      trackerId,
      messageId,
      { ...message, data: "" },
      true,
      false,
    );
    const created = trackers.get(trackerId);
    if (!created) {
      return events;
    }
    created.message = message;
    created.accumulated = delta;
    events.push(...hooks.buildDeltaEvents(created, delta));
    if (isTerminal(message)) {
      events.push(...closeTracker(trackers, hooks, options, created, message));
    }
    return events;
  }
  touch(trackers, trackerId, tracker);
  tracker.message = message;
  tracker.accumulated += delta;
  const events = hooks.buildDeltaEvents(tracker, delta);
  if (isTerminal(message)) {
    events.push(...closeTracker(trackers, hooks, options, tracker, message));
  }
  return events;
}

function appendAggregateOrDeltaOrFirstContact<TEvent>(
  trackers: Map<string, MutableTracker>,
  hooks: DecoderCoreHooks<TEvent>,
  options: DecoderCoreOptions,
  maxStreams: number,
  trackerId: string,
  messageId: string,
  message: InboundMessage,
  data: string,
): DecodedEvent<TEvent>[] {
  const tracker = trackers.get(trackerId);
  if (!tracker || data.startsWith(tracker.accumulated)) {
    return updateOrFirstContact(
      trackers,
      hooks,
      options,
      maxStreams,
      trackerId,
      messageId,
      message,
      data,
    );
  }
  return appendOrFirstContact(
    trackers,
    hooks,
    options,
    maxStreams,
    trackerId,
    messageId,
    message,
    data,
  );
}

function updateOrFirstContact<TEvent>(
  trackers: Map<string, MutableTracker>,
  hooks: DecoderCoreHooks<TEvent>,
  options: DecoderCoreOptions,
  maxStreams: number,
  trackerId: string,
  messageId: string,
  message: InboundMessage,
  nextAccumulated: string,
): DecodedEvent<TEvent>[] {
  const tracker = trackers.get(trackerId);
  if (!tracker) {
    options.onFirstContact?.(messageId);
    const events = createTracker(
      trackers,
      hooks,
      options,
      maxStreams,
      trackerId,
      messageId,
      { ...message, data: "" },
      true,
      false,
    );
    const created = trackers.get(trackerId);
    if (!created) {
      return events;
    }
    created.message = message;
    created.accumulated = nextAccumulated;
    events.push(...hooks.buildDeltaEvents(created, nextAccumulated));
    if (isTerminal(message)) {
      events.push(...closeTracker(trackers, hooks, options, created, message));
    }
    return events;
  }
  touch(trackers, trackerId, tracker);
  tracker.message = message;
  const previous = tracker.accumulated;
  if (nextAccumulated.startsWith(previous)) {
    const delta = nextAccumulated.slice(previous.length);
    tracker.accumulated = nextAccumulated;
    const events = delta === "" ? [] : hooks.buildDeltaEvents(tracker, delta);
    if (isTerminal(message)) {
      events.push(...closeTracker(trackers, hooks, options, tracker, message));
    }
    return events;
  }
  options.onReplacement?.(messageId);
  tracker.accumulated = nextAccumulated;
  const events = hooks.buildStartEvents(tracker);
  events.push(...hooks.buildDeltaEvents(tracker, nextAccumulated));
  if (isTerminal(message)) {
    events.push(...closeTracker(trackers, hooks, options, tracker, message));
  }
  return events;
}

function deleteTracker<TEvent>(
  trackers: Map<string, MutableTracker>,
  hooks: DecoderCoreHooks<TEvent>,
  options: DecoderCoreOptions,
  messageId: string,
  message: InboundMessage,
): DecodedEvent<TEvent>[] {
  const trackerId = streamTrackerId(message, messageId);
  const tracker =
    trackers.get(trackerId) ??
    ({
      trackerId,
      messageId,
      accumulated: dataAsString(message.data),
      firstContact: true,
      message,
      closed: false,
    } satisfies MutableTracker);
  tracker.message = message;
  const events = hooks.buildDeleteEvents?.(tracker) ?? [];
  trackers.delete(trackerId);
  options.onTrackerClosed?.(messageId);
  return events;
}

function closeTracker<TEvent>(
  trackers: Map<string, MutableTracker>,
  hooks: DecoderCoreHooks<TEvent>,
  options: DecoderCoreOptions,
  tracker: MutableTracker,
  message: InboundMessage,
): DecodedEvent<TEvent>[] {
  if (tracker.closed) {
    return [];
  }
  tracker.closed = true;
  const events = hooks.buildEndEvents(tracker, message.getCodecHeaders());
  trackers.delete(tracker.trackerId);
  options.onTrackerClosed?.(tracker.messageId);
  return events;
}

function enforceLimit(
  trackers: Map<string, MutableTracker>,
  maxStreams: number,
  options: DecoderCoreOptions,
): void {
  while (trackers.size > maxStreams) {
    const oldest = trackers.keys().next().value;
    if (oldest === undefined) {
      return;
    }
    trackers.delete(oldest);
    options.onTrackerEvicted?.(oldest);
  }
}

function touch(
  trackers: Map<string, MutableTracker>,
  messageId: string,
  tracker: MutableTracker,
): void {
  trackers.delete(messageId);
  trackers.set(messageId, tracker);
}

function codecMessageId(message: InboundMessage): string {
  return message.getTransportHeaders()[HEADER_CODEC_MESSAGE_ID] ?? message.messageSerial;
}

function streamTrackerId(message: InboundMessage, messageId: string): string {
  return message.getTransportHeaders()[HEADER_STREAM_ID] ?? messageId;
}

function isTerminal(message: InboundMessage): boolean {
  const status = message.getTransportHeaders()[HEADER_STATUS];
  return status === "complete" || status === "cancelled";
}

function dataAsString(data: unknown): string {
  return typeof data === "string" ? data : "";
}
