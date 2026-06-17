import TimedCallback from "./utils/timers/timed_callback";
import { OneOffTimer } from "./utils/timers";

const Util = {
  now(): number {
    if (Date.now) {
      return Date.now();
    } else {
      return new Date().valueOf();
    }
  },

  defer(callback: TimedCallback): OneOffTimer {
    return new OneOffTimer(0, callback);
  },

  /** Builds a function that will proxy a method call to its first argument.
   *
   * Allows partial application of arguments, so additional arguments are
   * prepended to the argument list.
   *
   * @param  {String} name method name
   * @return {(...args: any[]) => any} proxy function
   */
  method(name: string, ..._args: any[]): (...args: any[]) => any {
    const boundArguments = Array.prototype.slice.call(arguments, 1);
    return function (object) {
      return object[name].apply(object, boundArguments.concat(arguments));
    };
  },
};

export default Util;
