import Callback from "./callback";
import * as Collections from "../utils/collections";
import CallbackTable from "./callback_table";

export default class CallbackRegistry {
  _callbacks: CallbackTable;

  constructor() {
    this._callbacks = {};
  }

  get(name: string): Callback[] {
    return this._callbacks[prefix(name)];
  }

  add(name: string, callback: (...args: any[]) => any, context: any) {
    const prefixedEventName = prefix(name);
    this._callbacks[prefixedEventName] =
      this._callbacks[prefixedEventName] || [];
    this._callbacks[prefixedEventName].push({
      fn: callback,
      context: context,
    });
  }

  remove(name?: string, callback?: (...args: any[]) => any, context?: any) {
    if (!name && !callback && !context) {
      this._callbacks = {};
      return;
    }

    const names = name ? [prefix(name)] : Collections.keys(this._callbacks);

    if (callback || context) {
      this.removeCallback(names, callback, context);
    } else {
      this.removeAllCallbacks(names);
    }
  }

  private removeCallback(
    names: string[],
    callback: (...args: any[]) => any,
    context: any,
  ) {
    Collections.apply(
      names,
      function (name) {
        this._callbacks[name] = Collections.filter(
          this._callbacks[name] || [],
          function (binding) {
            return (
              (callback && callback !== binding.fn) ||
              (context && context !== binding.context)
            );
          },
        );
        if (this._callbacks[name].length === 0) {
          delete this._callbacks[name];
        }
      },
      this,
    );
  }

  private removeAllCallbacks(names: string[]) {
    Collections.apply(
      names,
      function (name) {
        delete this._callbacks[name];
      },
      this,
    );
  }
}

function prefix(name: string): string {
  return "_" + name;
}
