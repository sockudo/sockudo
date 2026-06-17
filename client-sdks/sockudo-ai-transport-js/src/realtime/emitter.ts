import type { Unsubscribe } from "./types.js";

/** Small synchronous emitter used by realtime seam implementations. */
export class RealtimeEmitter<Events extends object> {
  private readonly listeners = new Map<
    keyof Events,
    Set<(payload: unknown) => void>
  >();

  /** Subscribes to an event. */
  public on<K extends keyof Events>(
    event: K,
    listener: (payload: Events[K]) => void,
  ): Unsubscribe {
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

  /** Emits an event to current listeners. */
  public emit<K extends keyof Events>(event: K, payload: Events[K]): void {
    const listeners = this.listeners.get(event);
    if (!listeners) {
      return;
    }
    for (const listener of listeners) {
      listener(payload);
    }
  }
}
