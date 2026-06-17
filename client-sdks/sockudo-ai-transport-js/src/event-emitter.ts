import { LogLevel, makeLogger, type Logger } from "./logger.js";

/**
 * Generic event map used by {@link EventEmitter}.
 */
export type EventsMap = object;

/**
 * Callback returned by {@link EventEmitter.on}.
 */
export type EventUnsubscribe = () => void;

/**
 * Options for {@link EventEmitter}.
 */
export interface EventEmitterOptions {
  /** Logger used for listener exceptions.
   *
   * @defaultValue A silent SDK logger.
   */
  logger?: Logger;
}

/**
 * Tiny typed synchronous emitter.
 *
 * Listener exceptions are caught, logged, and never propagated to the emit
 * site.
 */
export class EventEmitter<Events extends EventsMap> {
  private readonly listeners = new Map<
    keyof Events,
    Set<(payload: unknown) => void>
  >();
  private readonly logger: Logger;

  /** Creates an emitter. */
  public constructor(options: EventEmitterOptions = {}) {
    this.logger = options.logger ?? makeLogger({ logLevel: LogLevel.Silent });
  }

  /** Subscribes to an event and returns an unsubscribe callback. */
  public on<K extends keyof Events>(
    event: K,
    listener: (payload: Events[K]) => void,
  ): EventUnsubscribe {
    let listeners = this.listeners.get(event);
    if (!listeners) {
      listeners = new Set();
      this.listeners.set(event, listeners);
    }
    listeners.add(listener as (payload: unknown) => void);
    return () => {
      listeners.delete(listener as (payload: unknown) => void);
    };
  }

  /** Emits an event synchronously to current listeners. */
  public emit<K extends keyof Events>(event: K, payload: Events[K]): void {
    const listeners = this.listeners.get(event);
    if (!listeners) {
      return;
    }
    for (const listener of listeners) {
      try {
        listener(payload);
      } catch (error) {
        this.logger.error("event listener failed", {
          event: String(event),
          error,
        });
      }
    }
  }
}
