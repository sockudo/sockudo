import * as Collections from "../utils/collections";
import Metadata from "../channels/metadata";
import CallbackRegistry from "./callback_registry";

/** Manages callback bindings and event emitting.
 *
 * @param (...args: any[]) => any failThrough called when no listeners are bound to an event
 */
export default class Dispatcher {
  callbacks: CallbackRegistry;
  global_callbacks: Array<(...args: any[]) => any>;
  failThrough: (...args: any[]) => any;

  constructor(failThrough?: (...args: any[]) => any) {
    this.callbacks = new CallbackRegistry();
    this.global_callbacks = [];
    this.failThrough = failThrough;
  }

  bind(eventName: string, callback: (...args: any[]) => any, context?: any) {
    this.callbacks.add(eventName, callback, context);
    return this;
  }

  bind_global(callback: (...args: any[]) => any) {
    this.global_callbacks.push(callback);
    return this;
  }

  unbind(
    eventName?: string,
    callback?: (...args: any[]) => any,
    context?: any,
  ) {
    this.callbacks.remove(eventName, callback, context);
    return this;
  }

  unbind_global(callback?: (...args: any[]) => any) {
    if (!callback) {
      this.global_callbacks = [];
      return this;
    }

    this.global_callbacks = Collections.filter(
      this.global_callbacks || [],
      (c) => c !== callback,
    );

    return this;
  }

  unbind_all() {
    this.unbind();
    this.unbind_global();
    return this;
  }

  emit(eventName: string, data?: any, metadata?: Metadata): Dispatcher {
    for (let i = 0; i < this.global_callbacks.length; i++) {
      this.global_callbacks[i](eventName, data);
    }

    const callbacks = this.callbacks.get(eventName);
    const args = [];

    if (metadata) {
      // if there's a metadata argument, we need to call the callback with both
      // data and metadata regardless of whether data is undefined
      args.push(data, metadata);
    } else if (data) {
      // metadata is undefined, so we only need to call the callback with data
      // if data exists
      args.push(data);
    }

    if (callbacks && callbacks.length > 0) {
      for (let i = 0; i < callbacks.length; i++) {
        callbacks[i].fn.apply(callbacks[i].context || global, args);
      }
    } else if (this.failThrough) {
      this.failThrough(eventName, data);
    }

    return this;
  }
}
