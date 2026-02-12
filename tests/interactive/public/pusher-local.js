class ScriptReceiverFactory {
  constructor(prefix2, name) {
    this.lastId = 0;
    this.prefix = prefix2;
    this.name = name;
  }
  create(callback) {
    this.lastId++;
    var number = this.lastId;
    var id = this.prefix + number;
    var name = this.name + "[" + number + "]";
    var called = false;
    var callbackWrapper = function () {
      if (!called) {
        callback.apply(null, arguments);
        called = true;
      }
    };
    this[number] = callbackWrapper;
    return { number, id, name, callback: callbackWrapper };
  }
  remove(receiver) {
    delete this[receiver.number];
  }
}
var ScriptReceivers = new ScriptReceiverFactory(
  "_pusher_script_",
  "Pusher.ScriptReceivers",
);
var Defaults = {
  VERSION: "8.4.0",
  PROTOCOL: 7,
  wsPort: 80,
  wssPort: 443,
  wsPath: "",
  // DEPRECATED: SockJS fallback parameters
  httpHost: "sockjs.pusher.com",
  httpPort: 80,
  httpsPort: 443,
  httpPath: "/pusher",
  // DEPRECATED: Stats
  stats_host: "stats.pusher.com",
  // DEPRECATED: Other settings
  authEndpoint: "/pusher/auth",
  authTransport: "ajax",
  activityTimeout: 12e4,
  pongTimeout: 3e4,
  unavailableTimeout: 1e4,
  userAuthentication: {
    endpoint: "/pusher/user-auth",
    transport: "ajax",
  },
  channelAuthorization: {
    endpoint: "/pusher/auth",
    transport: "ajax",
  },
  // CDN configuration
  cdn_http: "//js.pusher.com/",
  cdn_https: "//js.pusher.com/",
  dependency_suffix: "",
};
class DependencyLoader {
  constructor(options) {
    this.options = options;
    this.receivers = options.receivers || ScriptReceivers;
    this.loading = {};
  }
  /** Loads the dependency from CDN.
   *
   * @param  {String} name
   * @param  {Function} callback
   */
  load(name, options, callback) {
    var self = this;
    if (self.loading[name] && self.loading[name].length > 0) {
      self.loading[name].push(callback);
    } else {
      self.loading[name] = [callback];
      var request = Runtime.createScriptRequest(self.getPath(name, options));
      var receiver = self.receivers.create(function (error) {
        self.receivers.remove(receiver);
        if (self.loading[name]) {
          var callbacks = self.loading[name];
          delete self.loading[name];
          var successCallback = function (wasSuccessful) {
            if (!wasSuccessful) {
              request.cleanup();
            }
          };
          for (var i = 0; i < callbacks.length; i++) {
            callbacks[i](error, successCallback);
          }
        }
      });
      request.send(receiver);
    }
  }
  /** Returns a root URL for pusher-js CDN.
   *
   * @returns {String}
   */
  getRoot(options) {
    var cdn;
    var protocol = Runtime.getDocument().location.protocol;
    if ((options && options.useTLS) || protocol === "https:") {
      cdn = this.options.cdn_https;
    } else {
      cdn = this.options.cdn_http;
    }
    return cdn.replace(/\/*$/, "") + "/" + this.options.version;
  }
  /** Returns a full path to a dependency file.
   *
   * @param {String} name
   * @returns {String}
   */
  getPath(name, options) {
    return this.getRoot(options) + "/" + name + this.options.suffix + ".js";
  }
}
var DependenciesReceivers = new ScriptReceiverFactory(
  "_pusher_dependencies",
  "Pusher.DependenciesReceivers",
);
var Dependencies = new DependencyLoader({
  cdn_http: Defaults.cdn_http,
  cdn_https: Defaults.cdn_https,
  version: Defaults.VERSION,
  suffix: Defaults.dependency_suffix,
  receivers: DependenciesReceivers,
});
const urlStore = {
  baseUrl: "https://pusher.com",
  urls: {
    authenticationEndpoint: {
      path: "/docs/channels/server_api/authenticating_users",
    },
    authorizationEndpoint: {
      path: "/docs/channels/server_api/authorizing-users/",
    },
    javascriptQuickStart: {
      path: "/docs/javascript_quick_start",
    },
    triggeringClientEvents: {
      path: "/docs/client_api_guide/client_events#trigger-events",
    },
    encryptedChannelSupport: {
      fullUrl:
        "https://github.com/pusher/pusher-js/tree/cc491015371a4bde5743d1c87a0fbac0feb53195#encrypted-channel-support",
    },
  },
};
const buildLogSuffix = function (key) {
  const urlPrefix = "See:";
  const urlObj = urlStore.urls[key];
  if (!urlObj) return "";
  let url;
  if (urlObj.fullUrl) {
    url = urlObj.fullUrl;
  } else if (urlObj.path) {
    url = urlStore.baseUrl + urlObj.path;
  }
  if (!url) return "";
  return `${urlPrefix} ${url}`;
};
const urlStore$1 = { buildLogSuffix };
var AuthRequestType = /* @__PURE__ */ ((AuthRequestType2) => {
  AuthRequestType2["UserAuthentication"] = "user-authentication";
  AuthRequestType2["ChannelAuthorization"] = "channel-authorization";
  return AuthRequestType2;
})(AuthRequestType || {});
class BadEventName extends Error {
  constructor(msg) {
    super(msg);
    Object.setPrototypeOf(this, new.target.prototype);
  }
}
class BadChannelName extends Error {
  constructor(msg) {
    super(msg);
    Object.setPrototypeOf(this, new.target.prototype);
  }
}
class RequestTimedOut extends Error {
  constructor(msg) {
    super(msg);
    Object.setPrototypeOf(this, new.target.prototype);
  }
}
class TransportPriorityTooLow extends Error {
  constructor(msg) {
    super(msg);
    Object.setPrototypeOf(this, new.target.prototype);
  }
}
class TransportClosed extends Error {
  constructor(msg) {
    super(msg);
    Object.setPrototypeOf(this, new.target.prototype);
  }
}
class UnsupportedFeature extends Error {
  constructor(msg) {
    super(msg);
    Object.setPrototypeOf(this, new.target.prototype);
  }
}
class UnsupportedTransport extends Error {
  constructor(msg) {
    super(msg);
    Object.setPrototypeOf(this, new.target.prototype);
  }
}
let UnsupportedStrategy$1 = class UnsupportedStrategy extends Error {
  constructor(msg) {
    super(msg);
    Object.setPrototypeOf(this, new.target.prototype);
  }
};
class HTTPAuthError extends Error {
  constructor(status, msg) {
    super(msg);
    this.status = status;
    Object.setPrototypeOf(this, new.target.prototype);
  }
}
const ajax = function (context, query, authOptions, authRequestType, callback) {
  const xhr = Runtime.createXHR();
  xhr.open("POST", authOptions.endpoint, true);
  xhr.setRequestHeader("Content-Type", "application/x-www-form-urlencoded");
  for (var headerName in authOptions.headers) {
    xhr.setRequestHeader(headerName, authOptions.headers[headerName]);
  }
  if (authOptions.headersProvider != null) {
    let dynamicHeaders = authOptions.headersProvider();
    for (var headerName in dynamicHeaders) {
      xhr.setRequestHeader(headerName, dynamicHeaders[headerName]);
    }
  }
  xhr.onreadystatechange = function () {
    if (xhr.readyState === 4) {
      if (xhr.status === 200) {
        let data;
        let parsed = false;
        try {
          data = JSON.parse(xhr.responseText);
          parsed = true;
        } catch (e) {
          callback(
            new HTTPAuthError(
              200,
              `JSON returned from ${authRequestType.toString()} endpoint was invalid, yet status code was 200. Data was: ${xhr.responseText}`,
            ),
            null,
          );
        }
        if (parsed) {
          callback(null, data);
        }
      } else {
        let suffix = "";
        switch (authRequestType) {
          case AuthRequestType.UserAuthentication:
            suffix = urlStore$1.buildLogSuffix("authenticationEndpoint");
            break;
          case AuthRequestType.ChannelAuthorization:
            suffix = `Clients must be authorized to join private or presence channels. ${urlStore$1.buildLogSuffix(
              "authorizationEndpoint",
            )}`;
            break;
        }
        callback(
          new HTTPAuthError(
            xhr.status,
            `Unable to retrieve auth string from ${authRequestType.toString()} endpoint - received status: ${xhr.status} from ${authOptions.endpoint}. ${suffix}`,
          ),
          null,
        );
      }
    }
  };
  xhr.send(query);
  return xhr;
};
function encode(s) {
  return btoa(utob(s));
}
var fromCharCode = String.fromCharCode;
var b64chars =
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
var cb_utob = function (c) {
  var cc = c.charCodeAt(0);
  return cc < 128
    ? c
    : cc < 2048
      ? fromCharCode(192 | (cc >>> 6)) + fromCharCode(128 | (cc & 63))
      : fromCharCode(224 | ((cc >>> 12) & 15)) +
        fromCharCode(128 | ((cc >>> 6) & 63)) +
        fromCharCode(128 | (cc & 63));
};
var utob = function (u) {
  return u.replace(/[^\x00-\x7F]/g, cb_utob);
};
var cb_encode = function (ccc) {
  var padlen = [0, 2, 1][ccc.length % 3];
  var ord =
    (ccc.charCodeAt(0) << 16) |
    ((ccc.length > 1 ? ccc.charCodeAt(1) : 0) << 8) |
    (ccc.length > 2 ? ccc.charCodeAt(2) : 0);
  var chars = [
    b64chars.charAt(ord >>> 18),
    b64chars.charAt((ord >>> 12) & 63),
    padlen >= 2 ? "=" : b64chars.charAt((ord >>> 6) & 63),
    padlen >= 1 ? "=" : b64chars.charAt(ord & 63),
  ];
  return chars.join("");
};
var btoa =
  window.btoa ||
  function (b) {
    return b.replace(/[\s\S]{1,3}/g, cb_encode);
  };
class Timer {
  constructor(set, clear, delay, callback) {
    this.clear = clear;
    this.timer = set(() => {
      if (this.timer) {
        this.timer = callback(this.timer);
      }
    }, delay);
  }
  /** Returns whether the timer is still running.
   *
   * @return {Boolean}
   */
  isRunning() {
    return this.timer !== null;
  }
  /** Aborts a timer when it's running. */
  ensureAborted() {
    if (this.timer) {
      this.clear(this.timer);
      this.timer = null;
    }
  }
}
function clearTimeout(timer) {
  window.clearTimeout(timer);
}
function clearInterval(timer) {
  window.clearInterval(timer);
}
class OneOffTimer extends Timer {
  constructor(delay, callback) {
    super(setTimeout, clearTimeout, delay, function (timer) {
      callback();
      return null;
    });
  }
}
class PeriodicTimer extends Timer {
  constructor(delay, callback) {
    super(setInterval, clearInterval, delay, function (timer) {
      callback();
      return timer;
    });
  }
}
var Util = {
  now() {
    if (Date.now) {
      return Date.now();
    } else {
      return /* @__PURE__ */ new Date().valueOf();
    }
  },
  defer(callback) {
    return new OneOffTimer(0, callback);
  },
  /** Builds a function that will proxy a method call to its first argument.
   *
   * Allows partial application of arguments, so additional arguments are
   * prepended to the argument list.
   *
   * @param  {String} name method name
   * @return {Function} proxy function
   */
  method(name, ...args) {
    var boundArguments = Array.prototype.slice.call(arguments, 1);
    return function (object) {
      return object[name].apply(object, boundArguments.concat(arguments));
    };
  },
};
function extend(target, ...sources) {
  for (var i = 0; i < sources.length; i++) {
    var extensions = sources[i];
    for (var property in extensions) {
      if (
        extensions[property] &&
        extensions[property].constructor &&
        extensions[property].constructor === Object
      ) {
        target[property] = extend(target[property] || {}, extensions[property]);
      } else {
        target[property] = extensions[property];
      }
    }
  }
  return target;
}
function stringify() {
  var m = ["Pusher"];
  for (var i = 0; i < arguments.length; i++) {
    if (typeof arguments[i] === "string") {
      m.push(arguments[i]);
    } else {
      m.push(safeJSONStringify(arguments[i]));
    }
  }
  return m.join(" : ");
}
function arrayIndexOf(array, item) {
  var nativeIndexOf = Array.prototype.indexOf;
  if (array === null) {
    return -1;
  }
  if (nativeIndexOf && array.indexOf === nativeIndexOf) {
    return array.indexOf(item);
  }
  for (var i = 0, l = array.length; i < l; i++) {
    if (array[i] === item) {
      return i;
    }
  }
  return -1;
}
function objectApply(object, f) {
  for (var key in object) {
    if (Object.prototype.hasOwnProperty.call(object, key)) {
      f(object[key], key, object);
    }
  }
}
function keys(object) {
  var keys2 = [];
  objectApply(object, function (_, key) {
    keys2.push(key);
  });
  return keys2;
}
function values(object) {
  var values2 = [];
  objectApply(object, function (value) {
    values2.push(value);
  });
  return values2;
}
function apply(array, f, context) {
  for (var i = 0; i < array.length; i++) {
    f.call(context || window, array[i], i, array);
  }
}
function map(array, f) {
  var result = [];
  for (var i = 0; i < array.length; i++) {
    result.push(f(array[i], i, array, result));
  }
  return result;
}
function mapObject(object, f) {
  var result = {};
  objectApply(object, function (value, key) {
    result[key] = f(value);
  });
  return result;
}
function filter(array, test) {
  test =
    test ||
    function (value) {
      return !!value;
    };
  var result = [];
  for (var i = 0; i < array.length; i++) {
    if (test(array[i], i, array, result)) {
      result.push(array[i]);
    }
  }
  return result;
}
function filterObject(object, test) {
  var result = {};
  objectApply(object, function (value, key) {
    if ((test && test(value, key, object, result)) || Boolean(value)) {
      result[key] = value;
    }
  });
  return result;
}
function flatten(object) {
  var result = [];
  objectApply(object, function (value, key) {
    result.push([key, value]);
  });
  return result;
}
function any(array, test) {
  for (var i = 0; i < array.length; i++) {
    if (test(array[i], i, array)) {
      return true;
    }
  }
  return false;
}
function all(array, test) {
  for (var i = 0; i < array.length; i++) {
    if (!test(array[i], i, array)) {
      return false;
    }
  }
  return true;
}
function encodeParamsObject(data) {
  return mapObject(data, function (value) {
    if (typeof value === "object") {
      value = safeJSONStringify(value);
    }
    return encodeURIComponent(encode(value.toString()));
  });
}
function buildQueryString(data) {
  var params = filterObject(data, function (value) {
    return value !== void 0;
  });
  var query = map(
    flatten(encodeParamsObject(params)),
    Util.method("join", "="),
  ).join("&");
  return query;
}
function decycleObject(object) {
  var objects = [],
    paths = [];
  return (function derez(value, path) {
    var i, name, nu;
    switch (typeof value) {
      case "object":
        if (!value) {
          return null;
        }
        for (i = 0; i < objects.length; i += 1) {
          if (objects[i] === value) {
            return { $ref: paths[i] };
          }
        }
        objects.push(value);
        paths.push(path);
        if (Object.prototype.toString.apply(value) === "[object Array]") {
          nu = [];
          for (i = 0; i < value.length; i += 1) {
            nu[i] = derez(value[i], path + "[" + i + "]");
          }
        } else {
          nu = {};
          for (name in value) {
            if (Object.prototype.hasOwnProperty.call(value, name)) {
              nu[name] = derez(
                value[name],
                path + "[" + JSON.stringify(name) + "]",
              );
            }
          }
        }
        return nu;
      case "number":
      case "string":
      case "boolean":
        return value;
    }
  })(object, "$");
}
function safeJSONStringify(source) {
  try {
    return JSON.stringify(source);
  } catch (e) {
    return JSON.stringify(decycleObject(source));
  }
}
let config = {
  logToConsole: false,
};
function setLoggerConfig(newConfig) {
  config = { ...config, ...newConfig };
}
class Logger {
  constructor() {
    this.globalLog = (message) => {
      if (window.console && window.console.log) {
        window.console.log(message);
      }
    };
  }
  debug(...args) {
    this.log(this.globalLog, args);
  }
  warn(...args) {
    this.log(this.globalLogWarn, args);
  }
  error(...args) {
    this.log(this.globalLogError, args);
  }
  globalLogWarn(message) {
    if (window.console && window.console.warn) {
      window.console.warn(message);
    } else {
      this.globalLog(message);
    }
  }
  globalLogError(message) {
    if (window.console && window.console.error) {
      window.console.error(message);
    } else {
      this.globalLogWarn(message);
    }
  }
  log(defaultLoggingFunction, ...args) {
    var message = stringify.apply(this, arguments);
    if (config.log) {
      config.log(message);
    } else if (config.logToConsole) {
      const log = defaultLoggingFunction.bind(this);
      log(message);
    }
  }
}
const Logger$1 = new Logger();
var jsonp$1 = function (
  context,
  query,
  authOptions,
  authRequestType,
  callback,
) {
  if (authOptions.headers !== void 0 || authOptions.headersProvider != null) {
    Logger$1.warn(
      `To send headers with the ${authRequestType.toString()} request, you must use AJAX, rather than JSONP.`,
    );
  }
  var callbackName = context.nextAuthCallbackID.toString();
  context.nextAuthCallbackID++;
  var document2 = context.getDocument();
  var script = document2.createElement("script");
  context.auth_callbacks[callbackName] = function (data) {
    callback(null, data);
  };
  var callback_name = "Pusher.auth_callbacks['" + callbackName + "']";
  script.src =
    authOptions.endpoint +
    "?callback=" +
    encodeURIComponent(callback_name) +
    "&" +
    query;
  var head =
    document2.getElementsByTagName("head")[0] || document2.documentElement;
  head.insertBefore(script, head.firstChild);
};
class ScriptRequest {
  constructor(src) {
    this.src = src;
  }
  send(receiver) {
    var self = this;
    var errorString = "Error loading " + self.src;
    self.script = document.createElement("script");
    self.script.id = receiver.id;
    self.script.src = self.src;
    self.script.type = "text/javascript";
    self.script.charset = "UTF-8";
    if (self.script.addEventListener) {
      self.script.onerror = function () {
        receiver.callback(errorString);
      };
      self.script.onload = function () {
        receiver.callback(null);
      };
    } else {
      self.script.onreadystatechange = function () {
        if (
          self.script.readyState === "loaded" ||
          self.script.readyState === "complete"
        ) {
          receiver.callback(null);
        }
      };
    }
    if (
      self.script.async === void 0 &&
      document.attachEvent &&
      /opera/i.test(navigator.userAgent)
    ) {
      self.errorScript = document.createElement("script");
      self.errorScript.id = receiver.id + "_error";
      self.errorScript.text = receiver.name + "('" + errorString + "');";
      self.script.async = self.errorScript.async = false;
    } else {
      self.script.async = true;
    }
    var head = document.getElementsByTagName("head")[0];
    head.insertBefore(self.script, head.firstChild);
    if (self.errorScript) {
      head.insertBefore(self.errorScript, self.script.nextSibling);
    }
  }
  /** Cleans up the DOM remains of the script request. */
  cleanup() {
    if (this.script) {
      this.script.onload = this.script.onerror = null;
      this.script.onreadystatechange = null;
    }
    if (this.script && this.script.parentNode) {
      this.script.parentNode.removeChild(this.script);
    }
    if (this.errorScript && this.errorScript.parentNode) {
      this.errorScript.parentNode.removeChild(this.errorScript);
    }
    this.script = null;
    this.errorScript = null;
  }
}
class JSONPRequest {
  constructor(url, data) {
    this.url = url;
    this.data = data;
  }
  /** Sends the actual JSONP request.
   *
   * @param {ScriptReceiver} receiver
   */
  send(receiver) {
    if (this.request) {
      return;
    }
    var query = buildQueryString(this.data);
    var url = this.url + "/" + receiver.number + "?" + query;
    this.request = Runtime.createScriptRequest(url);
    this.request.send(receiver);
  }
  /** Cleans up the DOM remains of the JSONP request. */
  cleanup() {
    if (this.request) {
      this.request.cleanup();
    }
  }
}
var getAgent = function (sender, useTLS) {
  return function (data, callback) {
    var scheme = "http" + (useTLS ? "s" : "") + "://";
    var url =
      scheme + (sender.host || sender.options.host) + sender.options.path;
    var request = Runtime.createJSONPRequest(url, data);
    var receiver = Runtime.ScriptReceivers.create(function (error, result) {
      ScriptReceivers.remove(receiver);
      request.cleanup();
      if (result && result.host) {
        sender.host = result.host;
      }
      if (callback) {
        callback(error, result);
      }
    });
    request.send(receiver);
  };
};
var jsonp = {
  name: "jsonp",
  getAgent,
};
function getGenericURL(baseScheme, params, path) {
  var scheme = baseScheme + (params.useTLS ? "s" : "");
  var host = params.useTLS ? params.hostTLS : params.hostNonTLS;
  return scheme + "://" + host + path;
}
function getGenericPath(key, queryString) {
  var path = "/app/" + key;
  var query =
    "?protocol=" +
    Defaults.PROTOCOL +
    "&client=js&version=" +
    Defaults.VERSION +
    (queryString ? "&" + queryString : "");
  return path + query;
}
var ws = {
  getInitial: function (key, params) {
    var path = (params.httpPath || "") + getGenericPath(key, "flash=false");
    return getGenericURL("ws", params, path);
  },
};
var http = {
  getInitial: function (key, params) {
    var path = (params.httpPath || "/pusher") + getGenericPath(key);
    return getGenericURL("http", params, path);
  },
};
var sockjs = {
  getInitial: function (key, params) {
    return getGenericURL("http", params, params.httpPath || "/pusher");
  },
  getPath: function (key, params) {
    return getGenericPath(key);
  },
};
class CallbackRegistry {
  constructor() {
    this._callbacks = {};
  }
  get(name) {
    return this._callbacks[prefix(name)];
  }
  add(name, callback, context) {
    var prefixedEventName = prefix(name);
    this._callbacks[prefixedEventName] =
      this._callbacks[prefixedEventName] || [];
    this._callbacks[prefixedEventName].push({
      fn: callback,
      context,
    });
  }
  remove(name, callback, context) {
    if (!name && !callback && !context) {
      this._callbacks = {};
      return;
    }
    var names = name ? [prefix(name)] : keys(this._callbacks);
    if (callback || context) {
      this.removeCallback(names, callback, context);
    } else {
      this.removeAllCallbacks(names);
    }
  }
  removeCallback(names, callback, context) {
    apply(
      names,
      function (name) {
        this._callbacks[name] = filter(
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
  removeAllCallbacks(names) {
    apply(
      names,
      function (name) {
        delete this._callbacks[name];
      },
      this,
    );
  }
}
function prefix(name) {
  return "_" + name;
}
class Dispatcher {
  constructor(failThrough) {
    this.callbacks = new CallbackRegistry();
    this.global_callbacks = [];
    this.failThrough = failThrough;
  }
  bind(eventName, callback, context) {
    this.callbacks.add(eventName, callback, context);
    return this;
  }
  bind_global(callback) {
    this.global_callbacks.push(callback);
    return this;
  }
  unbind(eventName, callback, context) {
    this.callbacks.remove(eventName, callback, context);
    return this;
  }
  unbind_global(callback) {
    if (!callback) {
      this.global_callbacks = [];
      return this;
    }
    this.global_callbacks = filter(
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
  emit(eventName, data, metadata) {
    for (var i = 0; i < this.global_callbacks.length; i++) {
      this.global_callbacks[i](eventName, data);
    }
    var callbacks = this.callbacks.get(eventName);
    var args = [];
    if (metadata) {
      args.push(data, metadata);
    } else if (data) {
      args.push(data);
    }
    if (callbacks && callbacks.length > 0) {
      for (var i = 0; i < callbacks.length; i++) {
        callbacks[i].fn.apply(callbacks[i].context || window, args);
      }
    } else if (this.failThrough) {
      this.failThrough(eventName, data);
    }
    return this;
  }
}
class TransportConnection extends Dispatcher {
  constructor(hooks2, name, priority, key, options) {
    super();
    this.initialize = Runtime.transportConnectionInitializer;
    this.hooks = hooks2;
    this.name = name;
    this.priority = priority;
    this.key = key;
    this.options = options;
    this.state = "new";
    this.timeline = options.timeline;
    this.activityTimeout = options.activityTimeout;
    this.id = this.timeline.generateUniqueID();
  }
  /** Checks whether the transport handles activity checks by itself.
   *
   * @return {Boolean}
   */
  handlesActivityChecks() {
    return Boolean(this.hooks.handlesActivityChecks);
  }
  /** Checks whether the transport supports the ping/pong API.
   *
   * @return {Boolean}
   */
  supportsPing() {
    return Boolean(this.hooks.supportsPing);
  }
  /** Tries to establish a connection.
   *
   * @returns {Boolean} false if transport is in invalid state
   */
  connect() {
    if (this.socket || this.state !== "initialized") {
      return false;
    }
    var url = this.hooks.urls.getInitial(this.key, this.options);
    try {
      this.socket = this.hooks.getSocket(url, this.options);
    } catch (e) {
      Util.defer(() => {
        this.onError(e);
        this.changeState("closed");
      });
      return false;
    }
    this.bindListeners();
    Logger$1.debug("Connecting", { transport: this.name, url });
    this.changeState("connecting");
    return true;
  }
  /** Closes the connection.
   *
   * @return {Boolean} true if there was a connection to close
   */
  close() {
    if (this.socket) {
      this.socket.close();
      return true;
    } else {
      return false;
    }
  }
  /** Sends data over the open connection.
   *
   * @param {String} data
   * @return {Boolean} true only when in the "open" state
   */
  send(data) {
    if (this.state === "open") {
      Util.defer(() => {
        if (this.socket) {
          this.socket.send(data);
        }
      });
      return true;
    } else {
      return false;
    }
  }
  /** Sends a ping if the connection is open and transport supports it. */
  ping() {
    if (this.state === "open" && this.supportsPing()) {
      this.socket.ping();
    }
  }
  onOpen() {
    if (this.hooks.beforeOpen) {
      this.hooks.beforeOpen(
        this.socket,
        this.hooks.urls.getPath(this.key, this.options),
      );
    }
    this.changeState("open");
    this.socket.onopen = void 0;
  }
  onError(error) {
    this.emit("error", { type: "WebSocketError", error });
    this.timeline.error(this.buildTimelineMessage({ error: error.toString() }));
  }
  onClose(closeEvent) {
    if (closeEvent) {
      this.changeState("closed", {
        code: closeEvent.code,
        reason: closeEvent.reason,
        wasClean: closeEvent.wasClean,
      });
    } else {
      this.changeState("closed");
    }
    this.unbindListeners();
    this.socket = void 0;
  }
  onMessage(message) {
    this.emit("message", message);
  }
  onActivity() {
    this.emit("activity");
  }
  bindListeners() {
    this.socket.onopen = () => {
      this.onOpen();
    };
    this.socket.onerror = (error) => {
      this.onError(error);
    };
    this.socket.onclose = (closeEvent) => {
      this.onClose(closeEvent);
    };
    this.socket.onmessage = (message) => {
      this.onMessage(message);
    };
    if (this.supportsPing()) {
      this.socket.onactivity = () => {
        this.onActivity();
      };
    }
  }
  unbindListeners() {
    if (this.socket) {
      this.socket.onopen = void 0;
      this.socket.onerror = void 0;
      this.socket.onclose = void 0;
      this.socket.onmessage = void 0;
      if (this.supportsPing()) {
        this.socket.onactivity = void 0;
      }
    }
  }
  changeState(state, params) {
    this.state = state;
    this.timeline.info(
      this.buildTimelineMessage({
        state,
        params,
      }),
    );
    this.emit(state, params);
  }
  buildTimelineMessage(message) {
    return extend({ cid: this.id }, message);
  }
}
class Transport {
  constructor(hooks2) {
    this.hooks = hooks2;
  }
  /** Returns whether the transport is supported in the environment.
   *
   * @param {Object} envronment te environment details (encryption, settings)
   * @returns {Boolean} true when the transport is supported
   */
  isSupported(environment) {
    return this.hooks.isSupported(environment);
  }
  /** Creates a transport connection.
   *
   * @param {String} name
   * @param {Number} priority
   * @param {String} key the application key
   * @param {Object} options
   * @returns {TransportConnection}
   */
  createConnection(name, priority, key, options) {
    return new TransportConnection(this.hooks, name, priority, key, options);
  }
}
var WSTransport = new Transport({
  urls: ws,
  handlesActivityChecks: false,
  supportsPing: false,
  isInitialized: function () {
    return Boolean(Runtime.getWebSocketAPI());
  },
  isSupported: function () {
    return Boolean(Runtime.getWebSocketAPI());
  },
  getSocket: function (url) {
    return Runtime.createWebSocket(url);
  },
});
var httpConfiguration = {
  urls: http,
  handlesActivityChecks: false,
  supportsPing: true,
  isInitialized: function () {
    return true;
  },
};
var streamingConfiguration = extend(
  {
    getSocket: function (url) {
      return Runtime.HTTPFactory.createStreamingSocket(url);
    },
  },
  httpConfiguration,
);
var pollingConfiguration = extend(
  {
    getSocket: function (url) {
      return Runtime.HTTPFactory.createPollingSocket(url);
    },
  },
  httpConfiguration,
);
var xhrConfiguration = {
  isSupported: function () {
    return Runtime.isXHRSupported();
  },
};
var XHRStreamingTransport = new Transport(
  extend({}, streamingConfiguration, xhrConfiguration),
);
var XHRPollingTransport = new Transport(
  extend({}, pollingConfiguration, xhrConfiguration),
);
var Transports$1 = {
  ws: WSTransport,
  xhr_streaming: XHRStreamingTransport,
  xhr_polling: XHRPollingTransport,
};
var SockJSTransport = new Transport({
  file: "sockjs",
  urls: sockjs,
  handlesActivityChecks: true,
  supportsPing: false,
  isSupported: function () {
    return true;
  },
  isInitialized: function () {
    return window.SockJS !== void 0;
  },
  getSocket: function (url, options) {
    return new window.SockJS(url, null, {
      js_path: Dependencies.getPath("sockjs", {
        useTLS: options.useTLS,
      }),
      ignore_null_origin: options.ignoreNullOrigin,
    });
  },
  beforeOpen: function (socket, path) {
    socket.send(
      JSON.stringify({
        path,
      }),
    );
  },
});
var xdrConfiguration = {
  isSupported: function (environment) {
    var yes = Runtime.isXDRSupported(environment.useTLS);
    return yes;
  },
};
var XDRStreamingTransport = new Transport(
  extend({}, streamingConfiguration, xdrConfiguration),
);
var XDRPollingTransport = new Transport(
  extend({}, pollingConfiguration, xdrConfiguration),
);
Transports$1.xdr_streaming = XDRStreamingTransport;
Transports$1.xdr_polling = XDRPollingTransport;
Transports$1.sockjs = SockJSTransport;
class NetInfo extends Dispatcher {
  constructor() {
    super();
    var self = this;
    if (window.addEventListener !== void 0) {
      window.addEventListener(
        "online",
        function () {
          self.emit("online");
        },
        false,
      );
      window.addEventListener(
        "offline",
        function () {
          self.emit("offline");
        },
        false,
      );
    }
  }
  /** Returns whether browser is online or not
   *
   * Offline means definitely offline (no connection to router).
   * Inverse does NOT mean definitely online (only currently supported in Safari
   * and even there only means the device has a connection to the router).
   *
   * @return {Boolean}
   */
  isOnline() {
    if (window.navigator.onLine === void 0) {
      return true;
    } else {
      return window.navigator.onLine;
    }
  }
}
var Network = new NetInfo();
class AssistantToTheTransportManager {
  constructor(manager, transport, options) {
    this.manager = manager;
    this.transport = transport;
    this.minPingDelay = options.minPingDelay;
    this.maxPingDelay = options.maxPingDelay;
    this.pingDelay = void 0;
  }
  /** Creates a transport connection.
   *
   * This function has the same API as Transport#createConnection.
   *
   * @param {String} name
   * @param {Number} priority
   * @param {String} key the application key
   * @param {Object} options
   * @returns {TransportConnection}
   */
  createConnection(name, priority, key, options) {
    options = extend({}, options, {
      activityTimeout: this.pingDelay,
    });
    var connection = this.transport.createConnection(
      name,
      priority,
      key,
      options,
    );
    var openTimestamp = null;
    var onOpen = function () {
      connection.unbind("open", onOpen);
      connection.bind("closed", onClosed);
      openTimestamp = Util.now();
    };
    var onClosed = (closeEvent) => {
      connection.unbind("closed", onClosed);
      if (closeEvent.code === 1002 || closeEvent.code === 1003) {
        this.manager.reportDeath();
      } else if (!closeEvent.wasClean && openTimestamp) {
        var lifespan = Util.now() - openTimestamp;
        if (lifespan < 2 * this.maxPingDelay) {
          this.manager.reportDeath();
          this.pingDelay = Math.max(lifespan / 2, this.minPingDelay);
        }
      }
    };
    connection.bind("open", onOpen);
    return connection;
  }
  /** Returns whether the transport is supported in the environment.
   *
   * This function has the same API as Transport#isSupported. Might return false
   * when the manager decides to kill the transport.
   *
   * @param {Object} environment the environment details (encryption, settings)
   * @returns {Boolean} true when the transport is supported
   */
  isSupported(environment) {
    return this.manager.isAlive() && this.transport.isSupported(environment);
  }
}
const Protocol = {
  /**
   * Decodes a message in a Pusher format.
   *
   * The MessageEvent we receive from the transport should contain a pusher event
   * (https://pusher.com/docs/pusher_protocol#events) serialized as JSON in the
   * data field
   *
   * The pusher event may contain a data field too, and it may also be
   * serialised as JSON
   *
   * Throws errors when messages are not parse'able.
   *
   * @param  {MessageEvent} messageEvent
   * @return {PusherEvent}
   */
  decodeMessage: function (messageEvent) {
    try {
      var messageData = JSON.parse(messageEvent.data);
      var pusherEventData = messageData.data;
      if (typeof pusherEventData === "string") {
        try {
          pusherEventData = JSON.parse(messageData.data);
        } catch (e) {}
      }
      var pusherEvent = {
        event: messageData.event,
        channel: messageData.channel,
        data: pusherEventData,
        rawMessage: messageEvent.data,
        // Preserve raw message for delta compression
      };
      if (messageData.user_id) {
        pusherEvent.user_id = messageData.user_id;
      }
      if (typeof messageData.sequence === "number") {
        pusherEvent.sequence = messageData.sequence;
      }
      if (messageData.conflation_key !== void 0) {
        pusherEvent.conflation_key = messageData.conflation_key;
      }
      return pusherEvent;
    } catch (e) {
      throw { type: "MessageParseError", error: e, data: messageEvent.data };
    }
  },
  /**
   * Encodes a message to be sent.
   *
   * @param  {PusherEvent} event
   * @return {String}
   */
  encodeMessage: function (event) {
    return JSON.stringify(event);
  },
  /**
   * Processes a handshake message and returns appropriate actions.
   *
   * Returns an object with an 'action' and other action-specific properties.
   *
   * There are three outcomes when calling this function. First is a successful
   * connection attempt, when pusher:connection_established is received, which
   * results in a 'connected' action with an 'id' property. When passed a
   * pusher:error event, it returns a result with action appropriate to the
   * close code and an error. Otherwise, it raises an exception.
   *
   * @param {String} message
   * @result Object
   */
  processHandshake: function (messageEvent) {
    var message = Protocol.decodeMessage(messageEvent);
    if (message.event === "pusher:connection_established") {
      if (!message.data.activity_timeout) {
        throw "No activity timeout specified in handshake";
      }
      return {
        action: "connected",
        id: message.data.socket_id,
        activityTimeout: message.data.activity_timeout * 1e3,
      };
    } else if (message.event === "pusher:error") {
      return {
        action: this.getCloseAction(message.data),
        error: this.getCloseError(message.data),
      };
    } else {
      throw "Invalid handshake";
    }
  },
  /**
   * Dispatches the close event and returns an appropriate action name.
   *
   * See:
   * 1. https://developer.mozilla.org/en-US/docs/WebSockets/WebSockets_reference/CloseEvent
   * 2. http://pusher.com/docs/pusher_protocol
   *
   * @param  {CloseEvent} closeEvent
   * @return {String} close action name
   */
  getCloseAction: function (closeEvent) {
    if (closeEvent.code < 4e3) {
      if (closeEvent.code >= 1002 && closeEvent.code <= 1004) {
        return "backoff";
      } else {
        return null;
      }
    } else if (closeEvent.code === 4e3) {
      return "tls_only";
    } else if (closeEvent.code < 4100) {
      return "refused";
    } else if (closeEvent.code < 4200) {
      return "backoff";
    } else if (closeEvent.code < 4300) {
      return "retry";
    } else {
      return "refused";
    }
  },
  /**
   * Returns an error or null basing on the close event.
   *
   * Null is returned when connection was closed cleanly. Otherwise, an object
   * with error details is returned.
   *
   * @param  {CloseEvent} closeEvent
   * @return {Object} error object
   */
  getCloseError: function (closeEvent) {
    if (closeEvent.code !== 1e3 && closeEvent.code !== 1001) {
      return {
        type: "PusherError",
        data: {
          code: closeEvent.code,
          message: closeEvent.reason || closeEvent.message,
        },
      };
    } else {
      return null;
    }
  },
};
class Connection extends Dispatcher {
  constructor(id, transport) {
    super();
    this.id = id;
    this.transport = transport;
    this.activityTimeout = transport.activityTimeout;
    this.bindListeners();
  }
  /** Returns whether used transport handles activity checks by itself
   *
   * @returns {Boolean} true if activity checks are handled by the transport
   */
  handlesActivityChecks() {
    return this.transport.handlesActivityChecks();
  }
  /** Sends raw data.
   *
   * @param {String} data
   */
  send(data) {
    return this.transport.send(data);
  }
  /** Sends an event.
   *
   * @param {String} name
   * @param {String} data
   * @param {String} [channel]
   * @returns {Boolean} whether message was sent or not
   */
  send_event(name, data, channel) {
    var event = { event: name, data };
    if (channel) {
      event.channel = channel;
    }
    Logger$1.debug("Event sent", event);
    return this.send(Protocol.encodeMessage(event));
  }
  /** Sends a ping message to the server.
   *
   * Basing on the underlying transport, it might send either transport's
   * protocol-specific ping or pusher:ping event.
   */
  ping() {
    if (this.transport.supportsPing()) {
      this.transport.ping();
    } else {
      this.send_event("pusher:ping", {});
    }
  }
  /** Closes the connection. */
  close() {
    this.transport.close();
  }
  bindListeners() {
    var listeners = {
      message: (messageEvent) => {
        var pusherEvent;
        try {
          pusherEvent = Protocol.decodeMessage(messageEvent);
        } catch (e) {
          this.emit("error", {
            type: "MessageParseError",
            error: e,
            data: messageEvent.data,
          });
        }
        if (pusherEvent !== void 0) {
          Logger$1.debug("Event recd", pusherEvent);
          switch (pusherEvent.event) {
            case "pusher:error":
              this.emit("error", {
                type: "PusherError",
                data: pusherEvent.data,
              });
              break;
            case "pusher:ping":
              this.emit("ping");
              break;
            case "pusher:pong":
              this.emit("pong");
              break;
          }
          this.emit("message", pusherEvent);
        }
      },
      activity: () => {
        this.emit("activity");
      },
      error: (error) => {
        this.emit("error", error);
      },
      closed: (closeEvent) => {
        unbindListeners();
        if (closeEvent && closeEvent.code) {
          this.handleCloseEvent(closeEvent);
        }
        this.transport = null;
        this.emit("closed");
      },
    };
    var unbindListeners = () => {
      objectApply(listeners, (listener, event) => {
        this.transport.unbind(event, listener);
      });
    };
    objectApply(listeners, (listener, event) => {
      this.transport.bind(event, listener);
    });
  }
  handleCloseEvent(closeEvent) {
    var action = Protocol.getCloseAction(closeEvent);
    var error = Protocol.getCloseError(closeEvent);
    if (error) {
      this.emit("error", error);
    }
    if (action) {
      this.emit(action, { action, error });
    }
  }
}
class Handshake {
  constructor(transport, callback) {
    this.transport = transport;
    this.callback = callback;
    this.bindListeners();
  }
  close() {
    this.unbindListeners();
    this.transport.close();
  }
  bindListeners() {
    this.onMessage = (m) => {
      this.unbindListeners();
      var result;
      try {
        result = Protocol.processHandshake(m);
      } catch (e) {
        this.finish("error", { error: e });
        this.transport.close();
        return;
      }
      if (result.action === "connected") {
        this.finish("connected", {
          connection: new Connection(result.id, this.transport),
          activityTimeout: result.activityTimeout,
        });
      } else {
        this.finish(result.action, { error: result.error });
        this.transport.close();
      }
    };
    this.onClosed = (closeEvent) => {
      this.unbindListeners();
      var action = Protocol.getCloseAction(closeEvent) || "backoff";
      var error = Protocol.getCloseError(closeEvent);
      this.finish(action, { error });
    };
    this.transport.bind("message", this.onMessage);
    this.transport.bind("closed", this.onClosed);
  }
  unbindListeners() {
    this.transport.unbind("message", this.onMessage);
    this.transport.unbind("closed", this.onClosed);
  }
  finish(action, params) {
    this.callback(extend({ transport: this.transport, action }, params));
  }
}
class TimelineSender {
  constructor(timeline, options) {
    this.timeline = timeline;
    this.options = options || {};
  }
  send(useTLS, callback) {
    if (this.timeline.isEmpty()) {
      return;
    }
    this.timeline.send(
      Runtime.TimelineTransport.getAgent(this, useTLS),
      callback,
    );
  }
}
class Channel extends Dispatcher {
  constructor(name, pusher) {
    super(function (event, data) {
      Logger$1.debug("No callbacks on " + name + " for " + event);
    });
    this.name = name;
    this.pusher = pusher;
    this.subscribed = false;
    this.subscriptionPending = false;
    this.subscriptionCancelled = false;
    this.tagsFilter = null;
  }
  /** Skips authorization, since public channels don't require it.
   *
   * @param {Function} callback
   */
  authorize(socketId, callback) {
    return callback(null, { auth: "" });
  }
  /** Triggers an event */
  trigger(event, data) {
    if (event.indexOf("client-") !== 0) {
      throw new BadEventName(
        "Event '" + event + "' does not start with 'client-'",
      );
    }
    if (!this.subscribed) {
      var suffix = urlStore$1.buildLogSuffix("triggeringClientEvents");
      Logger$1.warn(
        `Client event triggered before channel 'subscription_succeeded' event . ${suffix}`,
      );
    }
    return this.pusher.send_event(event, data, this.name);
  }
  /** Signals disconnection to the channel. For internal use only. */
  disconnect() {
    this.subscribed = false;
    this.subscriptionPending = false;
  }
  /** Handles a PusherEvent. For internal use only.
   *
   * @param {PusherEvent} event
   */
  handleEvent(event) {
    var eventName = event.event;
    var data = event.data;
    if (eventName === "pusher_internal:subscription_succeeded") {
      this.handleSubscriptionSucceededEvent(event);
    } else if (eventName === "pusher_internal:subscription_count") {
      this.handleSubscriptionCountEvent(event);
    } else if (eventName.indexOf("pusher_internal:") !== 0) {
      var metadata = {};
      this.emit(eventName, data, metadata);
    }
  }
  handleSubscriptionSucceededEvent(event) {
    this.subscriptionPending = false;
    this.subscribed = true;
    if (this.subscriptionCancelled) {
      this.pusher.unsubscribe(this.name);
    } else {
      this.emit("pusher:subscription_succeeded", event.data);
    }
  }
  handleSubscriptionCountEvent(event) {
    if (event.data.subscription_count) {
      this.subscriptionCount = event.data.subscription_count;
    }
    this.emit("pusher:subscription_count", event.data);
  }
  /** Sends a subscription request. For internal use only. */
  subscribe() {
    if (this.subscribed) {
      return;
    }
    this.subscriptionPending = true;
    this.subscriptionCancelled = false;
    this.authorize(this.pusher.connection.socket_id, (error, data) => {
      if (error) {
        this.subscriptionPending = false;
        Logger$1.error(error.toString());
        this.emit(
          "pusher:subscription_error",
          Object.assign(
            {},
            {
              type: "AuthError",
              error: error.message,
            },
            error instanceof HTTPAuthError ? { status: error.status } : {},
          ),
        );
      } else {
        const subscribeData = {
          auth: data.auth,
          channel_data: data.channel_data,
          channel: this.name,
        };
        if (this.tagsFilter) {
          subscribeData.tags_filter = this.tagsFilter;
        }
        this.pusher.send_event("pusher:subscribe", subscribeData);
      }
    });
  }
  /** Sends an unsubscription request. For internal use only. */
  unsubscribe() {
    this.subscribed = false;
    this.pusher.send_event("pusher:unsubscribe", {
      channel: this.name,
    });
  }
  /** Cancels an in progress subscription. For internal use only. */
  cancelSubscription() {
    this.subscriptionCancelled = true;
  }
  /** Reinstates an in progress subscripiton. For internal use only. */
  reinstateSubscription() {
    this.subscriptionCancelled = false;
  }
}
class PrivateChannel extends Channel {
  /** Authorizes the connection to use the channel.
   *
   * @param  {String} socketId
   * @param  {Function} callback
   */
  authorize(socketId, callback) {
    return this.pusher.config.channelAuthorizer(
      {
        channelName: this.name,
        socketId,
      },
      callback,
    );
  }
}
class Members {
  constructor() {
    this.reset();
  }
  /** Returns member's info for given id.
   *
   * Resulting object containts two fields - id and info.
   *
   * @param {Number} id
   * @return {Object} member's info or null
   */
  get(id) {
    if (Object.prototype.hasOwnProperty.call(this.members, id)) {
      return {
        id,
        info: this.members[id],
      };
    } else {
      return null;
    }
  }
  /** Calls back for each member in unspecified order.
   *
   * @param  {Function} callback
   */
  each(callback) {
    objectApply(this.members, (member, id) => {
      callback(this.get(id));
    });
  }
  /** Updates the id for connected member. For internal use only. */
  setMyID(id) {
    this.myID = id;
  }
  /** Handles subscription data. For internal use only. */
  onSubscription(subscriptionData) {
    this.members = subscriptionData.presence.hash;
    this.count = subscriptionData.presence.count;
    this.me = this.get(this.myID);
  }
  /** Adds a new member to the collection. For internal use only. */
  addMember(memberData) {
    if (this.get(memberData.user_id) === null) {
      this.count++;
    }
    this.members[memberData.user_id] = memberData.user_info;
    return this.get(memberData.user_id);
  }
  /** Adds a member from the collection. For internal use only. */
  removeMember(memberData) {
    var member = this.get(memberData.user_id);
    if (member) {
      delete this.members[memberData.user_id];
      this.count--;
    }
    return member;
  }
  /** Resets the collection to the initial state. For internal use only. */
  reset() {
    this.members = {};
    this.count = 0;
    this.myID = null;
    this.me = null;
  }
}
class PresenceChannel extends PrivateChannel {
  /** Adds presence channel functionality to private channels.
   *
   * @param {String} name
   * @param {Pusher} pusher
   */
  constructor(name, pusher) {
    super(name, pusher);
    this.members = new Members();
  }
  /** Authorizes the connection as a member of the channel.
   *
   * @param  {String} socketId
   * @param  {Function} callback
   */
  authorize(socketId, callback) {
    super.authorize(socketId, async (error, authData) => {
      if (!error) {
        authData = authData;
        if (authData.channel_data != null) {
          var channelData = JSON.parse(authData.channel_data);
          this.members.setMyID(channelData.user_id);
        } else {
          await this.pusher.user.signinDonePromise;
          if (this.pusher.user.user_data != null) {
            this.members.setMyID(this.pusher.user.user_data.id);
          } else {
            let suffix = urlStore$1.buildLogSuffix("authorizationEndpoint");
            Logger$1.error(
              `Invalid auth response for channel '${this.name}', expected 'channel_data' field. ${suffix}, or the user should be signed in.`,
            );
            callback("Invalid auth response");
            return;
          }
        }
      }
      callback(error, authData);
    });
  }
  /** Handles presence and subscription events. For internal use only.
   *
   * @param {PusherEvent} event
   */
  handleEvent(event) {
    var eventName = event.event;
    if (eventName.indexOf("pusher_internal:") === 0) {
      this.handleInternalEvent(event);
    } else {
      var data = event.data;
      var metadata = {};
      if (event.user_id) {
        metadata.user_id = event.user_id;
      }
      this.emit(eventName, data, metadata);
    }
  }
  handleInternalEvent(event) {
    var eventName = event.event;
    var data = event.data;
    switch (eventName) {
      case "pusher_internal:subscription_succeeded":
        this.handleSubscriptionSucceededEvent(event);
        break;
      case "pusher_internal:subscription_count":
        this.handleSubscriptionCountEvent(event);
        break;
      case "pusher_internal:member_added":
        var addedMember = this.members.addMember(data);
        this.emit("pusher:member_added", addedMember);
        break;
      case "pusher_internal:member_removed":
        var removedMember = this.members.removeMember(data);
        if (removedMember) {
          this.emit("pusher:member_removed", removedMember);
        }
        break;
    }
  }
  handleSubscriptionSucceededEvent(event) {
    this.subscriptionPending = false;
    this.subscribed = true;
    if (this.subscriptionCancelled) {
      this.pusher.unsubscribe(this.name);
    } else {
      this.members.onSubscription(event.data);
      this.emit("pusher:subscription_succeeded", this.members);
    }
  }
  /** Resets the channel state, including members map. For internal use only. */
  disconnect() {
    this.members.reset();
    super.disconnect();
  }
}
var utf8 = {};
var hasRequiredUtf8;
function requireUtf8() {
  if (hasRequiredUtf8) return utf8;
  hasRequiredUtf8 = 1;
  Object.defineProperty(utf8, "__esModule", { value: true });
  var INVALID_UTF16 = "utf8: invalid string";
  var INVALID_UTF8 = "utf8: invalid source encoding";
  function encode2(s) {
    var arr = new Uint8Array(encodedLength(s));
    var pos = 0;
    for (var i = 0; i < s.length; i++) {
      var c = s.charCodeAt(i);
      if (c < 128) {
        arr[pos++] = c;
      } else if (c < 2048) {
        arr[pos++] = 192 | (c >> 6);
        arr[pos++] = 128 | (c & 63);
      } else if (c < 55296) {
        arr[pos++] = 224 | (c >> 12);
        arr[pos++] = 128 | ((c >> 6) & 63);
        arr[pos++] = 128 | (c & 63);
      } else {
        i++;
        c = (c & 1023) << 10;
        c |= s.charCodeAt(i) & 1023;
        c += 65536;
        arr[pos++] = 240 | (c >> 18);
        arr[pos++] = 128 | ((c >> 12) & 63);
        arr[pos++] = 128 | ((c >> 6) & 63);
        arr[pos++] = 128 | (c & 63);
      }
    }
    return arr;
  }
  utf8.encode = encode2;
  function encodedLength(s) {
    var result = 0;
    for (var i = 0; i < s.length; i++) {
      var c = s.charCodeAt(i);
      if (c < 128) {
        result += 1;
      } else if (c < 2048) {
        result += 2;
      } else if (c < 55296) {
        result += 3;
      } else if (c <= 57343) {
        if (i >= s.length - 1) {
          throw new Error(INVALID_UTF16);
        }
        i++;
        result += 4;
      } else {
        throw new Error(INVALID_UTF16);
      }
    }
    return result;
  }
  utf8.encodedLength = encodedLength;
  function decode2(arr) {
    var chars = [];
    for (var i = 0; i < arr.length; i++) {
      var b = arr[i];
      if (b & 128) {
        var min = void 0;
        if (b < 224) {
          if (i >= arr.length) {
            throw new Error(INVALID_UTF8);
          }
          var n1 = arr[++i];
          if ((n1 & 192) !== 128) {
            throw new Error(INVALID_UTF8);
          }
          b = ((b & 31) << 6) | (n1 & 63);
          min = 128;
        } else if (b < 240) {
          if (i >= arr.length - 1) {
            throw new Error(INVALID_UTF8);
          }
          var n1 = arr[++i];
          var n2 = arr[++i];
          if ((n1 & 192) !== 128 || (n2 & 192) !== 128) {
            throw new Error(INVALID_UTF8);
          }
          b = ((b & 15) << 12) | ((n1 & 63) << 6) | (n2 & 63);
          min = 2048;
        } else if (b < 248) {
          if (i >= arr.length - 2) {
            throw new Error(INVALID_UTF8);
          }
          var n1 = arr[++i];
          var n2 = arr[++i];
          var n3 = arr[++i];
          if ((n1 & 192) !== 128 || (n2 & 192) !== 128 || (n3 & 192) !== 128) {
            throw new Error(INVALID_UTF8);
          }
          b =
            ((b & 15) << 18) | ((n1 & 63) << 12) | ((n2 & 63) << 6) | (n3 & 63);
          min = 65536;
        } else {
          throw new Error(INVALID_UTF8);
        }
        if (b < min || (b >= 55296 && b <= 57343)) {
          throw new Error(INVALID_UTF8);
        }
        if (b >= 65536) {
          if (b > 1114111) {
            throw new Error(INVALID_UTF8);
          }
          b -= 65536;
          chars.push(String.fromCharCode(55296 | (b >> 10)));
          b = 56320 | (b & 1023);
        }
      }
      chars.push(String.fromCharCode(b));
    }
    return chars.join("");
  }
  utf8.decode = decode2;
  return utf8;
}
var utf8Exports = requireUtf8();
var base64 = {};
var hasRequiredBase64;
function requireBase64() {
  if (hasRequiredBase64) return base64;
  hasRequiredBase64 = 1;
  var __extends =
    (base64 && base64.__extends) ||
    /* @__PURE__ */ (function () {
      var extendStatics = function (d, b) {
        extendStatics =
          Object.setPrototypeOf ||
          ({ __proto__: [] } instanceof Array &&
            function (d2, b2) {
              d2.__proto__ = b2;
            }) ||
          function (d2, b2) {
            for (var p in b2) if (b2.hasOwnProperty(p)) d2[p] = b2[p];
          };
        return extendStatics(d, b);
      };
      return function (d, b) {
        extendStatics(d, b);
        function __() {
          this.constructor = d;
        }
        d.prototype =
          b === null
            ? Object.create(b)
            : ((__.prototype = b.prototype), new __());
      };
    })();
  Object.defineProperty(base64, "__esModule", { value: true });
  var INVALID_BYTE = 256;
  var Coder =
    /** @class */
    (function () {
      function Coder2(_paddingCharacter) {
        if (_paddingCharacter === void 0) {
          _paddingCharacter = "=";
        }
        this._paddingCharacter = _paddingCharacter;
      }
      Coder2.prototype.encodedLength = function (length) {
        if (!this._paddingCharacter) {
          return ((length * 8 + 5) / 6) | 0;
        }
        return (((length + 2) / 3) * 4) | 0;
      };
      Coder2.prototype.encode = function (data) {
        var out = "";
        var i = 0;
        for (; i < data.length - 2; i += 3) {
          var c = (data[i] << 16) | (data[i + 1] << 8) | data[i + 2];
          out += this._encodeByte((c >>> (3 * 6)) & 63);
          out += this._encodeByte((c >>> (2 * 6)) & 63);
          out += this._encodeByte((c >>> (1 * 6)) & 63);
          out += this._encodeByte((c >>> (0 * 6)) & 63);
        }
        var left = data.length - i;
        if (left > 0) {
          var c = (data[i] << 16) | (left === 2 ? data[i + 1] << 8 : 0);
          out += this._encodeByte((c >>> (3 * 6)) & 63);
          out += this._encodeByte((c >>> (2 * 6)) & 63);
          if (left === 2) {
            out += this._encodeByte((c >>> (1 * 6)) & 63);
          } else {
            out += this._paddingCharacter || "";
          }
          out += this._paddingCharacter || "";
        }
        return out;
      };
      Coder2.prototype.maxDecodedLength = function (length) {
        if (!this._paddingCharacter) {
          return ((length * 6 + 7) / 8) | 0;
        }
        return ((length / 4) * 3) | 0;
      };
      Coder2.prototype.decodedLength = function (s) {
        return this.maxDecodedLength(s.length - this._getPaddingLength(s));
      };
      Coder2.prototype.decode = function (s) {
        if (s.length === 0) {
          return new Uint8Array(0);
        }
        var paddingLength = this._getPaddingLength(s);
        var length = s.length - paddingLength;
        var out = new Uint8Array(this.maxDecodedLength(length));
        var op = 0;
        var i = 0;
        var haveBad = 0;
        var v0 = 0,
          v1 = 0,
          v2 = 0,
          v3 = 0;
        for (; i < length - 4; i += 4) {
          v0 = this._decodeChar(s.charCodeAt(i + 0));
          v1 = this._decodeChar(s.charCodeAt(i + 1));
          v2 = this._decodeChar(s.charCodeAt(i + 2));
          v3 = this._decodeChar(s.charCodeAt(i + 3));
          out[op++] = (v0 << 2) | (v1 >>> 4);
          out[op++] = (v1 << 4) | (v2 >>> 2);
          out[op++] = (v2 << 6) | v3;
          haveBad |= v0 & INVALID_BYTE;
          haveBad |= v1 & INVALID_BYTE;
          haveBad |= v2 & INVALID_BYTE;
          haveBad |= v3 & INVALID_BYTE;
        }
        if (i < length - 1) {
          v0 = this._decodeChar(s.charCodeAt(i));
          v1 = this._decodeChar(s.charCodeAt(i + 1));
          out[op++] = (v0 << 2) | (v1 >>> 4);
          haveBad |= v0 & INVALID_BYTE;
          haveBad |= v1 & INVALID_BYTE;
        }
        if (i < length - 2) {
          v2 = this._decodeChar(s.charCodeAt(i + 2));
          out[op++] = (v1 << 4) | (v2 >>> 2);
          haveBad |= v2 & INVALID_BYTE;
        }
        if (i < length - 3) {
          v3 = this._decodeChar(s.charCodeAt(i + 3));
          out[op++] = (v2 << 6) | v3;
          haveBad |= v3 & INVALID_BYTE;
        }
        if (haveBad !== 0) {
          throw new Error("Base64Coder: incorrect characters for decoding");
        }
        return out;
      };
      Coder2.prototype._encodeByte = function (b) {
        var result = b;
        result += 65;
        result += ((25 - b) >>> 8) & (0 - 65 - 26 + 97);
        result += ((51 - b) >>> 8) & (26 - 97 - 52 + 48);
        result += ((61 - b) >>> 8) & (52 - 48 - 62 + 43);
        result += ((62 - b) >>> 8) & (62 - 43 - 63 + 47);
        return String.fromCharCode(result);
      };
      Coder2.prototype._decodeChar = function (c) {
        var result = INVALID_BYTE;
        result += (((42 - c) & (c - 44)) >>> 8) & (-INVALID_BYTE + c - 43 + 62);
        result += (((46 - c) & (c - 48)) >>> 8) & (-INVALID_BYTE + c - 47 + 63);
        result += (((47 - c) & (c - 58)) >>> 8) & (-INVALID_BYTE + c - 48 + 52);
        result += (((64 - c) & (c - 91)) >>> 8) & (-INVALID_BYTE + c - 65 + 0);
        result +=
          (((96 - c) & (c - 123)) >>> 8) & (-INVALID_BYTE + c - 97 + 26);
        return result;
      };
      Coder2.prototype._getPaddingLength = function (s) {
        var paddingLength = 0;
        if (this._paddingCharacter) {
          for (var i = s.length - 1; i >= 0; i--) {
            if (s[i] !== this._paddingCharacter) {
              break;
            }
            paddingLength++;
          }
          if (s.length < 4 || paddingLength > 2) {
            throw new Error("Base64Coder: incorrect padding");
          }
        }
        return paddingLength;
      };
      return Coder2;
    })();
  base64.Coder = Coder;
  var stdCoder = new Coder();
  function encode2(data) {
    return stdCoder.encode(data);
  }
  base64.encode = encode2;
  function decode2(s) {
    return stdCoder.decode(s);
  }
  base64.decode = decode2;
  var URLSafeCoder =
    /** @class */
    (function (_super) {
      __extends(URLSafeCoder2, _super);
      function URLSafeCoder2() {
        return (_super !== null && _super.apply(this, arguments)) || this;
      }
      URLSafeCoder2.prototype._encodeByte = function (b) {
        var result = b;
        result += 65;
        result += ((25 - b) >>> 8) & (0 - 65 - 26 + 97);
        result += ((51 - b) >>> 8) & (26 - 97 - 52 + 48);
        result += ((61 - b) >>> 8) & (52 - 48 - 62 + 45);
        result += ((62 - b) >>> 8) & (62 - 45 - 63 + 95);
        return String.fromCharCode(result);
      };
      URLSafeCoder2.prototype._decodeChar = function (c) {
        var result = INVALID_BYTE;
        result += (((44 - c) & (c - 46)) >>> 8) & (-INVALID_BYTE + c - 45 + 62);
        result += (((94 - c) & (c - 96)) >>> 8) & (-INVALID_BYTE + c - 95 + 63);
        result += (((47 - c) & (c - 58)) >>> 8) & (-INVALID_BYTE + c - 48 + 52);
        result += (((64 - c) & (c - 91)) >>> 8) & (-INVALID_BYTE + c - 65 + 0);
        result +=
          (((96 - c) & (c - 123)) >>> 8) & (-INVALID_BYTE + c - 97 + 26);
        return result;
      };
      return URLSafeCoder2;
    })(Coder);
  base64.URLSafeCoder = URLSafeCoder;
  var urlSafeCoder = new URLSafeCoder();
  function encodeURLSafe(data) {
    return urlSafeCoder.encode(data);
  }
  base64.encodeURLSafe = encodeURLSafe;
  function decodeURLSafe(s) {
    return urlSafeCoder.decode(s);
  }
  base64.decodeURLSafe = decodeURLSafe;
  base64.encodedLength = function (length) {
    return stdCoder.encodedLength(length);
  };
  base64.maxDecodedLength = function (length) {
    return stdCoder.maxDecodedLength(length);
  };
  base64.decodedLength = function (s) {
    return stdCoder.decodedLength(s);
  };
  return base64;
}
var base64Exports = requireBase64();
class EncryptedChannel extends PrivateChannel {
  constructor(name, pusher, nacl2) {
    super(name, pusher);
    this.key = null;
    this.nacl = nacl2;
  }
  /** Authorizes the connection to use the channel.
   *
   * @param  {String} socketId
   * @param  {Function} callback
   */
  authorize(socketId, callback) {
    super.authorize(socketId, (error, authData) => {
      if (error) {
        callback(error, authData);
        return;
      }
      let sharedSecret = authData["shared_secret"];
      if (!sharedSecret) {
        callback(
          new Error(
            `No shared_secret key in auth payload for encrypted channel: ${this.name}`,
          ),
          null,
        );
        return;
      }
      this.key = base64Exports.decode(sharedSecret);
      delete authData["shared_secret"];
      callback(null, authData);
    });
  }
  trigger(event, data) {
    throw new UnsupportedFeature(
      "Client events are not currently supported for encrypted channels",
    );
  }
  /** Handles an event. For internal use only.
   *
   * @param {PusherEvent} event
   */
  handleEvent(event) {
    var eventName = event.event;
    var data = event.data;
    if (
      eventName.indexOf("pusher_internal:") === 0 ||
      eventName.indexOf("pusher:") === 0
    ) {
      super.handleEvent(event);
      return;
    }
    this.handleEncryptedEvent(eventName, data);
  }
  handleEncryptedEvent(event, data) {
    if (!this.key) {
      Logger$1.debug(
        "Received encrypted event before key has been retrieved from the authEndpoint",
      );
      return;
    }
    if (!data.ciphertext || !data.nonce) {
      Logger$1.error(
        "Unexpected format for encrypted event, expected object with `ciphertext` and `nonce` fields, got: " +
          data,
      );
      return;
    }
    let cipherText = base64Exports.decode(data.ciphertext);
    if (cipherText.length < this.nacl.secretbox.overheadLength) {
      Logger$1.error(
        `Expected encrypted event ciphertext length to be ${this.nacl.secretbox.overheadLength}, got: ${cipherText.length}`,
      );
      return;
    }
    let nonce = base64Exports.decode(data.nonce);
    if (nonce.length < this.nacl.secretbox.nonceLength) {
      Logger$1.error(
        `Expected encrypted event nonce length to be ${this.nacl.secretbox.nonceLength}, got: ${nonce.length}`,
      );
      return;
    }
    let bytes = this.nacl.secretbox.open(cipherText, nonce, this.key);
    if (bytes === null) {
      Logger$1.debug(
        "Failed to decrypt an event, probably because it was encrypted with a different key. Fetching a new key from the authEndpoint...",
      );
      this.authorize(this.pusher.connection.socket_id, (error, authData) => {
        if (error) {
          Logger$1.error(
            `Failed to make a request to the authEndpoint: ${authData}. Unable to fetch new key, so dropping encrypted event`,
          );
          return;
        }
        bytes = this.nacl.secretbox.open(cipherText, nonce, this.key);
        if (bytes === null) {
          Logger$1.error(
            `Failed to decrypt event with new key. Dropping encrypted event`,
          );
          return;
        }
        this.emit(event, this.getDataToEmit(bytes));
        return;
      });
      return;
    }
    this.emit(event, this.getDataToEmit(bytes));
  }
  // Try and parse the decrypted bytes as JSON. If we can't parse it, just
  // return the utf-8 string
  getDataToEmit(bytes) {
    let raw = utf8Exports.decode(bytes);
    try {
      return JSON.parse(raw);
    } catch {
      return raw;
    }
  }
}
class ConnectionManager extends Dispatcher {
  constructor(key, options) {
    super();
    this.state = "initialized";
    this.connection = null;
    this.key = key;
    this.options = options;
    this.timeline = this.options.timeline;
    this.usingTLS = this.options.useTLS;
    this.errorCallbacks = this.buildErrorCallbacks();
    this.connectionCallbacks = this.buildConnectionCallbacks(
      this.errorCallbacks,
    );
    this.handshakeCallbacks = this.buildHandshakeCallbacks(this.errorCallbacks);
    var Network2 = Runtime.getNetwork();
    Network2.bind("online", () => {
      this.timeline.info({ netinfo: "online" });
      if (this.state === "connecting" || this.state === "unavailable") {
        this.retryIn(0);
      }
    });
    Network2.bind("offline", () => {
      this.timeline.info({ netinfo: "offline" });
      if (this.connection) {
        this.sendActivityCheck();
      }
    });
    this.updateStrategy();
  }
  /** Establishes a connection to Pusher.
   *
   * Does nothing when connection is already established. See top-level doc
   * to find events emitted on connection attempts.
   */
  connect() {
    if (this.connection || this.runner) {
      return;
    }
    if (!this.strategy.isSupported()) {
      this.updateState("failed");
      return;
    }
    this.updateState("connecting");
    this.startConnecting();
    this.setUnavailableTimer();
  }
  /** Sends raw data.
   *
   * @param {String} data
   */
  send(data) {
    if (this.connection) {
      return this.connection.send(data);
    } else {
      return false;
    }
  }
  /** Sends an event.
   *
   * @param {String} name
   * @param {String} data
   * @param {String} [channel]
   * @returns {Boolean} whether message was sent or not
   */
  send_event(name, data, channel) {
    if (this.connection) {
      return this.connection.send_event(name, data, channel);
    } else {
      return false;
    }
  }
  /** Closes the connection. */
  disconnect() {
    this.disconnectInternally();
    this.updateState("disconnected");
  }
  isUsingTLS() {
    return this.usingTLS;
  }
  startConnecting() {
    var callback = (error, handshake) => {
      if (error) {
        this.runner = this.strategy.connect(0, callback);
      } else {
        if (handshake.action === "error") {
          this.emit("error", {
            type: "HandshakeError",
            error: handshake.error,
          });
          this.timeline.error({ handshakeError: handshake.error });
        } else {
          this.abortConnecting();
          this.handshakeCallbacks[handshake.action](handshake);
        }
      }
    };
    this.runner = this.strategy.connect(0, callback);
  }
  abortConnecting() {
    if (this.runner) {
      this.runner.abort();
      this.runner = null;
    }
  }
  disconnectInternally() {
    this.abortConnecting();
    this.clearRetryTimer();
    this.clearUnavailableTimer();
    if (this.connection) {
      var connection = this.abandonConnection();
      connection.close();
    }
  }
  updateStrategy() {
    this.strategy = this.options.getStrategy({
      key: this.key,
      timeline: this.timeline,
      useTLS: this.usingTLS,
    });
  }
  retryIn(delay) {
    this.timeline.info({ action: "retry", delay });
    if (delay > 0) {
      this.emit("connecting_in", Math.round(delay / 1e3));
    }
    this.retryTimer = new OneOffTimer(delay || 0, () => {
      this.disconnectInternally();
      this.connect();
    });
  }
  clearRetryTimer() {
    if (this.retryTimer) {
      this.retryTimer.ensureAborted();
      this.retryTimer = null;
    }
  }
  setUnavailableTimer() {
    this.unavailableTimer = new OneOffTimer(
      this.options.unavailableTimeout,
      () => {
        this.updateState("unavailable");
      },
    );
  }
  clearUnavailableTimer() {
    if (this.unavailableTimer) {
      this.unavailableTimer.ensureAborted();
    }
  }
  sendActivityCheck() {
    this.stopActivityCheck();
    this.connection.ping();
    this.activityTimer = new OneOffTimer(this.options.pongTimeout, () => {
      this.timeline.error({ pong_timed_out: this.options.pongTimeout });
      this.retryIn(0);
    });
  }
  resetActivityCheck() {
    this.stopActivityCheck();
    if (this.connection && !this.connection.handlesActivityChecks()) {
      this.activityTimer = new OneOffTimer(this.activityTimeout, () => {
        this.sendActivityCheck();
      });
    }
  }
  stopActivityCheck() {
    if (this.activityTimer) {
      this.activityTimer.ensureAborted();
    }
  }
  buildConnectionCallbacks(errorCallbacks) {
    return extend({}, errorCallbacks, {
      message: (message) => {
        this.resetActivityCheck();
        this.emit("message", message);
      },
      ping: () => {
        this.send_event("pusher:pong", {});
      },
      activity: () => {
        this.resetActivityCheck();
      },
      error: (error) => {
        this.emit("error", error);
      },
      closed: () => {
        this.abandonConnection();
        if (this.shouldRetry()) {
          this.retryIn(1e3);
        }
      },
    });
  }
  buildHandshakeCallbacks(errorCallbacks) {
    return extend({}, errorCallbacks, {
      connected: (handshake) => {
        this.activityTimeout = Math.min(
          this.options.activityTimeout,
          handshake.activityTimeout,
          handshake.connection.activityTimeout || Infinity,
        );
        this.clearUnavailableTimer();
        this.setConnection(handshake.connection);
        this.socket_id = this.connection.id;
        this.updateState("connected", { socket_id: this.socket_id });
      },
    });
  }
  buildErrorCallbacks() {
    let withErrorEmitted = (callback) => {
      return (result) => {
        if (result.error) {
          this.emit("error", { type: "WebSocketError", error: result.error });
        }
        callback(result);
      };
    };
    return {
      tls_only: withErrorEmitted(() => {
        this.usingTLS = true;
        this.updateStrategy();
        this.retryIn(0);
      }),
      refused: withErrorEmitted(() => {
        this.disconnect();
      }),
      backoff: withErrorEmitted(() => {
        this.retryIn(1e3);
      }),
      retry: withErrorEmitted(() => {
        this.retryIn(0);
      }),
    };
  }
  setConnection(connection) {
    this.connection = connection;
    for (var event in this.connectionCallbacks) {
      this.connection.bind(event, this.connectionCallbacks[event]);
    }
    this.resetActivityCheck();
  }
  abandonConnection() {
    if (!this.connection) {
      return;
    }
    this.stopActivityCheck();
    for (var event in this.connectionCallbacks) {
      this.connection.unbind(event, this.connectionCallbacks[event]);
    }
    var connection = this.connection;
    this.connection = null;
    return connection;
  }
  updateState(newState, data) {
    var previousState = this.state;
    this.state = newState;
    if (previousState !== newState) {
      var newStateDescription = newState;
      if (newStateDescription === "connected") {
        newStateDescription += " with new socket ID " + data.socket_id;
      }
      Logger$1.debug(
        "State changed",
        previousState + " -> " + newStateDescription,
      );
      this.timeline.info({ state: newState, params: data });
      this.emit("state_change", { previous: previousState, current: newState });
      this.emit(newState, data);
    }
  }
  shouldRetry() {
    return this.state === "connecting" || this.state === "connected";
  }
}
class Channels {
  constructor() {
    this.channels = {};
  }
  /** Creates or retrieves an existing channel by its name.
   *
   * @param {String} name
   * @param {Pusher} pusher
   * @return {Channel}
   */
  add(name, pusher) {
    if (!this.channels[name]) {
      this.channels[name] = createChannel(name, pusher);
    }
    return this.channels[name];
  }
  /** Returns a list of all channels
   *
   * @return {Array}
   */
  all() {
    return values(this.channels);
  }
  /** Finds a channel by its name.
   *
   * @param {String} name
   * @return {Channel} channel or null if it doesn't exist
   */
  find(name) {
    return this.channels[name];
  }
  /** Removes a channel from the map.
   *
   * @param {String} name
   */
  remove(name) {
    var channel = this.channels[name];
    delete this.channels[name];
    return channel;
  }
  /** Proxies disconnection signal to all channels. */
  disconnect() {
    objectApply(this.channels, function (channel) {
      channel.disconnect();
    });
  }
}
function createChannel(name, pusher) {
  if (name.indexOf("private-encrypted-") === 0) {
    if (pusher.config.nacl) {
      return Factory.createEncryptedChannel(name, pusher, pusher.config.nacl);
    }
    let errMsg =
      "Tried to subscribe to a private-encrypted- channel but no nacl implementation available";
    let suffix = urlStore$1.buildLogSuffix("encryptedChannelSupport");
    throw new UnsupportedFeature(`${errMsg}. ${suffix}`);
  } else if (name.indexOf("private-") === 0) {
    return Factory.createPrivateChannel(name, pusher);
  } else if (name.indexOf("presence-") === 0) {
    return Factory.createPresenceChannel(name, pusher);
  } else if (name.indexOf("#") === 0) {
    throw new BadChannelName(
      'Cannot create a channel with name "' + name + '".',
    );
  } else {
    return Factory.createChannel(name, pusher);
  }
}
var Factory = {
  createChannels() {
    return new Channels();
  },
  createConnectionManager(key, options) {
    return new ConnectionManager(key, options);
  },
  createChannel(name, pusher) {
    return new Channel(name, pusher);
  },
  createPrivateChannel(name, pusher) {
    return new PrivateChannel(name, pusher);
  },
  createPresenceChannel(name, pusher) {
    return new PresenceChannel(name, pusher);
  },
  createEncryptedChannel(name, pusher, nacl2) {
    return new EncryptedChannel(name, pusher, nacl2);
  },
  createTimelineSender(timeline, options) {
    return new TimelineSender(timeline, options);
  },
  createHandshake(transport, callback) {
    return new Handshake(transport, callback);
  },
  createAssistantToTheTransportManager(manager, transport, options) {
    return new AssistantToTheTransportManager(manager, transport, options);
  },
};
class TransportManager {
  constructor(options) {
    this.options = options || {};
    this.livesLeft = this.options.lives || Infinity;
  }
  /** Creates a assistant for the transport.
   *
   * @param {Transport} transport
   * @returns {AssistantToTheTransportManager}
   */
  getAssistant(transport) {
    return Factory.createAssistantToTheTransportManager(this, transport, {
      minPingDelay: this.options.minPingDelay,
      maxPingDelay: this.options.maxPingDelay,
    });
  }
  /** Returns whether the transport has any lives left.
   *
   * @returns {Boolean}
   */
  isAlive() {
    return this.livesLeft > 0;
  }
  /** Takes one life from the transport. */
  reportDeath() {
    this.livesLeft -= 1;
  }
}
class SequentialStrategy {
  constructor(strategies, options) {
    this.strategies = strategies;
    this.loop = Boolean(options.loop);
    this.failFast = Boolean(options.failFast);
    this.timeout = options.timeout;
    this.timeoutLimit = options.timeoutLimit;
  }
  isSupported() {
    return any(this.strategies, Util.method("isSupported"));
  }
  connect(minPriority, callback) {
    var strategies = this.strategies;
    var current = 0;
    var timeout = this.timeout;
    var runner = null;
    var tryNextStrategy = (error, handshake) => {
      if (handshake) {
        callback(null, handshake);
      } else {
        current = current + 1;
        if (this.loop) {
          current = current % strategies.length;
        }
        if (current < strategies.length) {
          if (timeout) {
            timeout = timeout * 2;
            if (this.timeoutLimit) {
              timeout = Math.min(timeout, this.timeoutLimit);
            }
          }
          runner = this.tryStrategy(
            strategies[current],
            minPriority,
            { timeout, failFast: this.failFast },
            tryNextStrategy,
          );
        } else {
          callback(true);
        }
      }
    };
    runner = this.tryStrategy(
      strategies[current],
      minPriority,
      { timeout, failFast: this.failFast },
      tryNextStrategy,
    );
    return {
      abort: function () {
        runner.abort();
      },
      forceMinPriority: function (p) {
        minPriority = p;
        if (runner) {
          runner.forceMinPriority(p);
        }
      },
    };
  }
  tryStrategy(strategy, minPriority, options, callback) {
    var timer = null;
    var runner = null;
    if (options.timeout > 0) {
      timer = new OneOffTimer(options.timeout, function () {
        runner.abort();
        callback(true);
      });
    }
    runner = strategy.connect(minPriority, function (error, handshake) {
      if (error && timer && timer.isRunning() && !options.failFast) {
        return;
      }
      if (timer) {
        timer.ensureAborted();
      }
      callback(error, handshake);
    });
    return {
      abort: function () {
        if (timer) {
          timer.ensureAborted();
        }
        runner.abort();
      },
      forceMinPriority: function (p) {
        runner.forceMinPriority(p);
      },
    };
  }
}
class BestConnectedEverStrategy {
  constructor(strategies) {
    this.strategies = strategies;
  }
  isSupported() {
    return any(this.strategies, Util.method("isSupported"));
  }
  connect(minPriority, callback) {
    return connect(this.strategies, minPriority, function (i, runners) {
      return function (error, handshake) {
        runners[i].error = error;
        if (error) {
          if (allRunnersFailed(runners)) {
            callback(true);
          }
          return;
        }
        apply(runners, function (runner) {
          runner.forceMinPriority(handshake.transport.priority);
        });
        callback(null, handshake);
      };
    });
  }
}
function connect(strategies, minPriority, callbackBuilder) {
  var runners = map(strategies, function (strategy, i, _, rs) {
    return strategy.connect(minPriority, callbackBuilder(i, rs));
  });
  return {
    abort: function () {
      apply(runners, abortRunner);
    },
    forceMinPriority: function (p) {
      apply(runners, function (runner) {
        runner.forceMinPriority(p);
      });
    },
  };
}
function allRunnersFailed(runners) {
  return all(runners, function (runner) {
    return Boolean(runner.error);
  });
}
function abortRunner(runner) {
  if (!runner.error && !runner.aborted) {
    runner.abort();
    runner.aborted = true;
  }
}
class WebSocketPrioritizedCachedStrategy {
  constructor(strategy, transports, options) {
    this.strategy = strategy;
    this.transports = transports;
    this.ttl = options.ttl || 1800 * 1e3;
    this.usingTLS = options.useTLS;
    this.timeline = options.timeline;
  }
  isSupported() {
    return this.strategy.isSupported();
  }
  connect(minPriority, callback) {
    var usingTLS = this.usingTLS;
    var info = fetchTransportCache(usingTLS);
    var cacheSkipCount = info && info.cacheSkipCount ? info.cacheSkipCount : 0;
    var strategies = [this.strategy];
    if (info && info.timestamp + this.ttl >= Util.now()) {
      var transport = this.transports[info.transport];
      if (transport) {
        if (["ws", "wss"].includes(info.transport) || cacheSkipCount > 3) {
          this.timeline.info({
            cached: true,
            transport: info.transport,
            latency: info.latency,
          });
          strategies.push(
            new SequentialStrategy([transport], {
              timeout: info.latency * 2 + 1e3,
              failFast: true,
            }),
          );
        } else {
          cacheSkipCount++;
        }
      }
    }
    var startTimestamp = Util.now();
    var runner = strategies
      .pop()
      .connect(minPriority, function cb(error, handshake) {
        if (error) {
          flushTransportCache(usingTLS);
          if (strategies.length > 0) {
            startTimestamp = Util.now();
            runner = strategies.pop().connect(minPriority, cb);
          } else {
            callback(error);
          }
        } else {
          storeTransportCache(
            usingTLS,
            handshake.transport.name,
            Util.now() - startTimestamp,
            cacheSkipCount,
          );
          callback(null, handshake);
        }
      });
    return {
      abort: function () {
        runner.abort();
      },
      forceMinPriority: function (p) {
        minPriority = p;
        if (runner) {
          runner.forceMinPriority(p);
        }
      },
    };
  }
}
function getTransportCacheKey(usingTLS) {
  return "pusherTransport" + (usingTLS ? "TLS" : "NonTLS");
}
function fetchTransportCache(usingTLS) {
  var storage = Runtime.getLocalStorage();
  if (storage) {
    try {
      var serializedCache = storage[getTransportCacheKey(usingTLS)];
      if (serializedCache) {
        return JSON.parse(serializedCache);
      }
    } catch (e) {
      flushTransportCache(usingTLS);
    }
  }
  return null;
}
function storeTransportCache(usingTLS, transport, latency, cacheSkipCount) {
  var storage = Runtime.getLocalStorage();
  if (storage) {
    try {
      storage[getTransportCacheKey(usingTLS)] = safeJSONStringify({
        timestamp: Util.now(),
        transport,
        latency,
        cacheSkipCount,
      });
    } catch (e) {}
  }
}
function flushTransportCache(usingTLS) {
  var storage = Runtime.getLocalStorage();
  if (storage) {
    try {
      delete storage[getTransportCacheKey(usingTLS)];
    } catch (e) {}
  }
}
class DelayedStrategy {
  constructor(strategy, { delay: number }) {
    this.strategy = strategy;
    this.options = { delay: number };
  }
  isSupported() {
    return this.strategy.isSupported();
  }
  connect(minPriority, callback) {
    var strategy = this.strategy;
    var runner;
    var timer = new OneOffTimer(this.options.delay, function () {
      runner = strategy.connect(minPriority, callback);
    });
    return {
      abort: function () {
        timer.ensureAborted();
        if (runner) {
          runner.abort();
        }
      },
      forceMinPriority: function (p) {
        minPriority = p;
        if (runner) {
          runner.forceMinPriority(p);
        }
      },
    };
  }
}
class IfStrategy {
  constructor(test, trueBranch, falseBranch) {
    this.test = test;
    this.trueBranch = trueBranch;
    this.falseBranch = falseBranch;
  }
  isSupported() {
    var branch = this.test() ? this.trueBranch : this.falseBranch;
    return branch.isSupported();
  }
  connect(minPriority, callback) {
    var branch = this.test() ? this.trueBranch : this.falseBranch;
    return branch.connect(minPriority, callback);
  }
}
class FirstConnectedStrategy {
  constructor(strategy) {
    this.strategy = strategy;
  }
  isSupported() {
    return this.strategy.isSupported();
  }
  connect(minPriority, callback) {
    var runner = this.strategy.connect(
      minPriority,
      function (error, handshake) {
        if (handshake) {
          runner.abort();
        }
        callback(error, handshake);
      },
    );
    return runner;
  }
}
function testSupportsStrategy(strategy) {
  return function () {
    return strategy.isSupported();
  };
}
var getDefaultStrategy = function (config2, baseOptions, defineTransport2) {
  var definedTransports = {};
  function defineTransportStrategy(name, type, priority, options, manager) {
    var transport = defineTransport2(
      config2,
      name,
      type,
      priority,
      options,
      manager,
    );
    definedTransports[name] = transport;
    return transport;
  }
  var ws_options = Object.assign({}, baseOptions, {
    hostNonTLS: config2.wsHost + ":" + config2.wsPort,
    hostTLS: config2.wsHost + ":" + config2.wssPort,
    httpPath: config2.wsPath,
  });
  var wss_options = Object.assign({}, ws_options, {
    useTLS: true,
  });
  var sockjs_options = Object.assign({}, baseOptions, {
    hostNonTLS: config2.httpHost + ":" + config2.httpPort,
    hostTLS: config2.httpHost + ":" + config2.httpsPort,
    httpPath: config2.httpPath,
  });
  var timeouts = {
    loop: true,
    timeout: 15e3,
    timeoutLimit: 6e4,
  };
  var ws_manager = new TransportManager({
    minPingDelay: 1e4,
    maxPingDelay: config2.activityTimeout,
  });
  var streaming_manager = new TransportManager({
    lives: 2,
    minPingDelay: 1e4,
    maxPingDelay: config2.activityTimeout,
  });
  var ws_transport = defineTransportStrategy(
    "ws",
    "ws",
    3,
    ws_options,
    ws_manager,
  );
  var wss_transport = defineTransportStrategy(
    "wss",
    "ws",
    3,
    wss_options,
    ws_manager,
  );
  var sockjs_transport = defineTransportStrategy(
    "sockjs",
    "sockjs",
    1,
    sockjs_options,
  );
  var xhr_streaming_transport = defineTransportStrategy(
    "xhr_streaming",
    "xhr_streaming",
    1,
    sockjs_options,
    streaming_manager,
  );
  var xdr_streaming_transport = defineTransportStrategy(
    "xdr_streaming",
    "xdr_streaming",
    1,
    sockjs_options,
    streaming_manager,
  );
  var xhr_polling_transport = defineTransportStrategy(
    "xhr_polling",
    "xhr_polling",
    1,
    sockjs_options,
  );
  var xdr_polling_transport = defineTransportStrategy(
    "xdr_polling",
    "xdr_polling",
    1,
    sockjs_options,
  );
  var ws_loop = new SequentialStrategy([ws_transport], timeouts);
  var wss_loop = new SequentialStrategy([wss_transport], timeouts);
  var sockjs_loop = new SequentialStrategy([sockjs_transport], timeouts);
  var streaming_loop = new SequentialStrategy(
    [
      new IfStrategy(
        testSupportsStrategy(xhr_streaming_transport),
        xhr_streaming_transport,
        xdr_streaming_transport,
      ),
    ],
    timeouts,
  );
  var polling_loop = new SequentialStrategy(
    [
      new IfStrategy(
        testSupportsStrategy(xhr_polling_transport),
        xhr_polling_transport,
        xdr_polling_transport,
      ),
    ],
    timeouts,
  );
  var http_loop = new SequentialStrategy(
    [
      new IfStrategy(
        testSupportsStrategy(streaming_loop),
        new BestConnectedEverStrategy([
          streaming_loop,
          new DelayedStrategy(polling_loop, { delay: 4e3 }),
        ]),
        polling_loop,
      ),
    ],
    timeouts,
  );
  var http_fallback_loop = new IfStrategy(
    testSupportsStrategy(http_loop),
    http_loop,
    sockjs_loop,
  );
  var wsStrategy;
  if (baseOptions.useTLS) {
    wsStrategy = new BestConnectedEverStrategy([
      ws_loop,
      new DelayedStrategy(http_fallback_loop, { delay: 2e3 }),
    ]);
  } else {
    wsStrategy = new BestConnectedEverStrategy([
      ws_loop,
      new DelayedStrategy(wss_loop, { delay: 2e3 }),
      new DelayedStrategy(http_fallback_loop, { delay: 5e3 }),
    ]);
  }
  return new WebSocketPrioritizedCachedStrategy(
    new FirstConnectedStrategy(
      new IfStrategy(
        testSupportsStrategy(ws_transport),
        wsStrategy,
        http_fallback_loop,
      ),
    ),
    definedTransports,
    {
      ttl: 18e5,
      timeline: baseOptions.timeline,
      useTLS: baseOptions.useTLS,
    },
  );
};
function transportConnectionInitializer() {
  var self = this;
  self.timeline.info(
    self.buildTimelineMessage({
      transport: self.name + (self.options.useTLS ? "s" : ""),
    }),
  );
  if (self.hooks.isInitialized()) {
    self.changeState("initialized");
  } else if (self.hooks.file) {
    self.changeState("initializing");
    Dependencies.load(
      self.hooks.file,
      { useTLS: self.options.useTLS },
      function (error, callback) {
        if (self.hooks.isInitialized()) {
          self.changeState("initialized");
          callback(true);
        } else {
          if (error) {
            self.onError(error);
          }
          self.onClose();
          callback(false);
        }
      },
    );
  } else {
    self.onClose();
  }
}
var hooks$3 = {
  getRequest: function (socket) {
    var xdr = new window.XDomainRequest();
    xdr.ontimeout = function () {
      socket.emit("error", new RequestTimedOut());
      socket.close();
    };
    xdr.onerror = function (e) {
      socket.emit("error", e);
      socket.close();
    };
    xdr.onprogress = function () {
      if (xdr.responseText && xdr.responseText.length > 0) {
        socket.onChunk(200, xdr.responseText);
      }
    };
    xdr.onload = function () {
      if (xdr.responseText && xdr.responseText.length > 0) {
        socket.onChunk(200, xdr.responseText);
      }
      socket.emit("finished", 200);
      socket.close();
    };
    return xdr;
  },
  abortRequest: function (xdr) {
    xdr.ontimeout = xdr.onerror = xdr.onprogress = xdr.onload = null;
    xdr.abort();
  },
};
const MAX_BUFFER_LENGTH = 256 * 1024;
class HTTPRequest extends Dispatcher {
  constructor(hooks2, method, url) {
    super();
    this.hooks = hooks2;
    this.method = method;
    this.url = url;
  }
  start(payload) {
    this.position = 0;
    this.xhr = this.hooks.getRequest(this);
    this.unloader = () => {
      this.close();
    };
    Runtime.addUnloadListener(this.unloader);
    this.xhr.open(this.method, this.url, true);
    if (this.xhr.setRequestHeader) {
      this.xhr.setRequestHeader("Content-Type", "application/json");
    }
    this.xhr.send(payload);
  }
  close() {
    if (this.unloader) {
      Runtime.removeUnloadListener(this.unloader);
      this.unloader = null;
    }
    if (this.xhr) {
      this.hooks.abortRequest(this.xhr);
      this.xhr = null;
    }
  }
  onChunk(status, data) {
    while (true) {
      var chunk = this.advanceBuffer(data);
      if (chunk) {
        this.emit("chunk", { status, data: chunk });
      } else {
        break;
      }
    }
    if (this.isBufferTooLong(data)) {
      this.emit("buffer_too_long");
    }
  }
  advanceBuffer(buffer) {
    var unreadData = buffer.slice(this.position);
    var endOfLinePosition = unreadData.indexOf("\n");
    if (endOfLinePosition !== -1) {
      this.position += endOfLinePosition + 1;
      return unreadData.slice(0, endOfLinePosition);
    } else {
      return null;
    }
  }
  isBufferTooLong(buffer) {
    return this.position === buffer.length && buffer.length > MAX_BUFFER_LENGTH;
  }
}
var State = /* @__PURE__ */ ((State2) => {
  State2[(State2["CONNECTING"] = 0)] = "CONNECTING";
  State2[(State2["OPEN"] = 1)] = "OPEN";
  State2[(State2["CLOSED"] = 3)] = "CLOSED";
  return State2;
})(State || {});
var autoIncrement = 1;
class HTTPSocket {
  constructor(hooks2, url) {
    this.hooks = hooks2;
    this.session = randomNumber(1e3) + "/" + randomString(8);
    this.location = getLocation(url);
    this.readyState = State.CONNECTING;
    this.openStream();
  }
  send(payload) {
    return this.sendRaw(JSON.stringify([payload]));
  }
  ping() {
    this.hooks.sendHeartbeat(this);
  }
  close(code, reason) {
    this.onClose(code, reason, true);
  }
  /** For internal use only */
  sendRaw(payload) {
    if (this.readyState === State.OPEN) {
      try {
        Runtime.createSocketRequest(
          "POST",
          getUniqueURL(getSendURL(this.location, this.session)),
        ).start(payload);
        return true;
      } catch (e) {
        return false;
      }
    } else {
      return false;
    }
  }
  /** For internal use only */
  reconnect() {
    this.closeStream();
    this.openStream();
  }
  /** For internal use only */
  onClose(code, reason, wasClean) {
    this.closeStream();
    this.readyState = State.CLOSED;
    if (this.onclose) {
      this.onclose({
        code,
        reason,
        wasClean,
      });
    }
  }
  onChunk(chunk) {
    if (chunk.status !== 200) {
      return;
    }
    if (this.readyState === State.OPEN) {
      this.onActivity();
    }
    var payload;
    var type = chunk.data.slice(0, 1);
    switch (type) {
      case "o":
        payload = JSON.parse(chunk.data.slice(1) || "{}");
        this.onOpen(payload);
        break;
      case "a":
        payload = JSON.parse(chunk.data.slice(1) || "[]");
        for (var i = 0; i < payload.length; i++) {
          this.onEvent(payload[i]);
        }
        break;
      case "m":
        payload = JSON.parse(chunk.data.slice(1) || "null");
        this.onEvent(payload);
        break;
      case "h":
        this.hooks.onHeartbeat(this);
        break;
      case "c":
        payload = JSON.parse(chunk.data.slice(1) || "[]");
        this.onClose(payload[0], payload[1], true);
        break;
    }
  }
  onOpen(options) {
    if (this.readyState === State.CONNECTING) {
      if (options && options.hostname) {
        this.location.base = replaceHost(this.location.base, options.hostname);
      }
      this.readyState = State.OPEN;
      if (this.onopen) {
        this.onopen();
      }
    } else {
      this.onClose(1006, "Server lost session", true);
    }
  }
  onEvent(event) {
    if (this.readyState === State.OPEN && this.onmessage) {
      this.onmessage({ data: event });
    }
  }
  onActivity() {
    if (this.onactivity) {
      this.onactivity();
    }
  }
  onError(error) {
    if (this.onerror) {
      this.onerror(error);
    }
  }
  openStream() {
    this.stream = Runtime.createSocketRequest(
      "POST",
      getUniqueURL(this.hooks.getReceiveURL(this.location, this.session)),
    );
    this.stream.bind("chunk", (chunk) => {
      this.onChunk(chunk);
    });
    this.stream.bind("finished", (status) => {
      this.hooks.onFinished(this, status);
    });
    this.stream.bind("buffer_too_long", () => {
      this.reconnect();
    });
    try {
      this.stream.start();
    } catch (error) {
      Util.defer(() => {
        this.onError(error);
        this.onClose(1006, "Could not start streaming", false);
      });
    }
  }
  closeStream() {
    if (this.stream) {
      this.stream.unbind_all();
      this.stream.close();
      this.stream = null;
    }
  }
}
function getLocation(url) {
  var parts = /([^\?]*)\/*(\??.*)/.exec(url);
  return {
    base: parts[1],
    queryString: parts[2],
  };
}
function getSendURL(url, session) {
  return url.base + "/" + session + "/xhr_send";
}
function getUniqueURL(url) {
  var separator = url.indexOf("?") === -1 ? "?" : "&";
  return (
    url +
    separator +
    "t=" +
    +(/* @__PURE__ */ new Date()) +
    "&n=" +
    autoIncrement++
  );
}
function replaceHost(url, hostname) {
  var urlParts = /(https?:\/\/)([^\/:]+)((\/|:)?.*)/.exec(url);
  return urlParts[1] + hostname + urlParts[3];
}
function randomNumber(max) {
  return Runtime.randomInt(max);
}
function randomString(length) {
  var result = [];
  for (var i = 0; i < length; i++) {
    result.push(randomNumber(32).toString(32));
  }
  return result.join("");
}
var hooks$2 = {
  getReceiveURL: function (url, session) {
    return url.base + "/" + session + "/xhr_streaming" + url.queryString;
  },
  onHeartbeat: function (socket) {
    socket.sendRaw("[]");
  },
  sendHeartbeat: function (socket) {
    socket.sendRaw("[]");
  },
  onFinished: function (socket, status) {
    socket.onClose(1006, "Connection interrupted (" + status + ")", false);
  },
};
var hooks$1 = {
  getReceiveURL: function (url, session) {
    return url.base + "/" + session + "/xhr" + url.queryString;
  },
  onHeartbeat: function () {},
  sendHeartbeat: function (socket) {
    socket.sendRaw("[]");
  },
  onFinished: function (socket, status) {
    if (status === 200) {
      socket.reconnect();
    } else {
      socket.onClose(1006, "Connection interrupted (" + status + ")", false);
    }
  },
};
var hooks = {
  getRequest: function (socket) {
    var Constructor = Runtime.getXHRAPI();
    var xhr = new Constructor();
    xhr.onreadystatechange = xhr.onprogress = function () {
      switch (xhr.readyState) {
        case 3:
          if (xhr.responseText && xhr.responseText.length > 0) {
            socket.onChunk(xhr.status, xhr.responseText);
          }
          break;
        case 4:
          if (xhr.responseText && xhr.responseText.length > 0) {
            socket.onChunk(xhr.status, xhr.responseText);
          }
          socket.emit("finished", xhr.status);
          socket.close();
          break;
      }
    };
    return xhr;
  },
  abortRequest: function (xhr) {
    xhr.onreadystatechange = null;
    xhr.abort();
  },
};
var HTTP = {
  createStreamingSocket(url) {
    return this.createSocket(hooks$2, url);
  },
  createPollingSocket(url) {
    return this.createSocket(hooks$1, url);
  },
  createSocket(hooks2, url) {
    return new HTTPSocket(hooks2, url);
  },
  createXHR(method, url) {
    return this.createRequest(hooks, method, url);
  },
  createRequest(hooks2, method, url) {
    return new HTTPRequest(hooks2, method, url);
  },
};
HTTP.createXDR = function (method, url) {
  return this.createRequest(hooks$3, method, url);
};
var Runtime = {
  // for jsonp auth
  nextAuthCallbackID: 1,
  auth_callbacks: {},
  ScriptReceivers,
  DependenciesReceivers,
  getDefaultStrategy,
  Transports: Transports$1,
  transportConnectionInitializer,
  HTTPFactory: HTTP,
  TimelineTransport: jsonp,
  getXHRAPI() {
    return window.XMLHttpRequest;
  },
  getWebSocketAPI() {
    return window.WebSocket || window.MozWebSocket;
  },
  setup(PusherClass) {
    window.Pusher = PusherClass;
    var initializeOnDocumentBody = () => {
      this.onDocumentBody(PusherClass.ready);
    };
    if (!window.JSON) {
      Dependencies.load("json2", {}, initializeOnDocumentBody);
    } else {
      initializeOnDocumentBody();
    }
  },
  getDocument() {
    return document;
  },
  getProtocol() {
    return this.getDocument().location.protocol;
  },
  getAuthorizers() {
    return { ajax, jsonp: jsonp$1 };
  },
  onDocumentBody(callback) {
    if (document.body) {
      callback();
    } else {
      setTimeout(() => {
        this.onDocumentBody(callback);
      }, 0);
    }
  },
  createJSONPRequest(url, data) {
    return new JSONPRequest(url, data);
  },
  createScriptRequest(src) {
    return new ScriptRequest(src);
  },
  getLocalStorage() {
    try {
      return window.localStorage;
    } catch (e) {
      return void 0;
    }
  },
  createXHR() {
    if (this.getXHRAPI()) {
      return this.createXMLHttpRequest();
    } else {
      return this.createMicrosoftXHR();
    }
  },
  createXMLHttpRequest() {
    var Constructor = this.getXHRAPI();
    return new Constructor();
  },
  createMicrosoftXHR() {
    return new ActiveXObject("Microsoft.XMLHTTP");
  },
  getNetwork() {
    return Network;
  },
  createWebSocket(url) {
    var Constructor = this.getWebSocketAPI();
    return new Constructor(url);
  },
  createSocketRequest(method, url) {
    if (this.isXHRSupported()) {
      return this.HTTPFactory.createXHR(method, url);
    } else if (this.isXDRSupported(url.indexOf("https:") === 0)) {
      return this.HTTPFactory.createXDR(method, url);
    } else {
      throw "Cross-origin HTTP requests are not supported";
    }
  },
  isXHRSupported() {
    var Constructor = this.getXHRAPI();
    return Boolean(Constructor) && new Constructor().withCredentials !== void 0;
  },
  isXDRSupported(useTLS) {
    var protocol = useTLS ? "https:" : "http:";
    var documentProtocol = this.getProtocol();
    return Boolean(window["XDomainRequest"]) && documentProtocol === protocol;
  },
  addUnloadListener(listener) {
    if (window.addEventListener !== void 0) {
      window.addEventListener("unload", listener, false);
    } else if (window.attachEvent !== void 0) {
      window.attachEvent("onunload", listener);
    }
  },
  removeUnloadListener(listener) {
    if (window.addEventListener !== void 0) {
      window.removeEventListener("unload", listener, false);
    } else if (window.detachEvent !== void 0) {
      window.detachEvent("onunload", listener);
    }
  },
  randomInt(max) {
    const random = function () {
      const crypto = window.crypto || window["msCrypto"];
      const random2 = crypto.getRandomValues(new Uint32Array(1))[0];
      return random2 / 2 ** 32;
    };
    return Math.floor(random() * max);
  },
};
var TimelineLevel = /* @__PURE__ */ ((TimelineLevel2) => {
  TimelineLevel2[(TimelineLevel2["ERROR"] = 3)] = "ERROR";
  TimelineLevel2[(TimelineLevel2["INFO"] = 6)] = "INFO";
  TimelineLevel2[(TimelineLevel2["DEBUG"] = 7)] = "DEBUG";
  return TimelineLevel2;
})(TimelineLevel || {});
class Timeline {
  constructor(key, session, options) {
    this.key = key;
    this.session = session;
    this.events = [];
    this.options = options || {};
    this.sent = 0;
    this.uniqueID = 0;
  }
  log(level, event) {
    if (level <= this.options.level) {
      this.events.push(extend({}, event, { timestamp: Util.now() }));
      if (this.options.limit && this.events.length > this.options.limit) {
        this.events.shift();
      }
    }
  }
  error(event) {
    this.log(TimelineLevel.ERROR, event);
  }
  info(event) {
    this.log(TimelineLevel.INFO, event);
  }
  debug(event) {
    this.log(TimelineLevel.DEBUG, event);
  }
  isEmpty() {
    return this.events.length === 0;
  }
  send(sendfn, callback) {
    var data = extend(
      {
        session: this.session,
        bundle: this.sent + 1,
        key: this.key,
        lib: "js",
        version: this.options.version,
        cluster: this.options.cluster,
        features: this.options.features,
        timeline: this.events,
      },
      this.options.params,
    );
    this.events = [];
    sendfn(data, (error, result) => {
      if (!error) {
        this.sent++;
      }
      if (callback) {
        callback(error, result);
      }
    });
    return true;
  }
  generateUniqueID() {
    this.uniqueID++;
    return this.uniqueID;
  }
}
class TransportStrategy {
  constructor(name, priority, transport, options) {
    this.name = name;
    this.priority = priority;
    this.transport = transport;
    this.options = options || {};
  }
  /** Returns whether the transport is supported in the browser.
   *
   * @returns {Boolean}
   */
  isSupported() {
    return this.transport.isSupported({
      useTLS: this.options.useTLS,
    });
  }
  /** Launches a connection attempt and returns a strategy runner.
   *
   * @param  {Function} callback
   * @return {Object} strategy runner
   */
  connect(minPriority, callback) {
    if (!this.isSupported()) {
      return failAttempt(new UnsupportedStrategy$1(), callback);
    } else if (this.priority < minPriority) {
      return failAttempt(new TransportPriorityTooLow(), callback);
    }
    var connected = false;
    var transport = this.transport.createConnection(
      this.name,
      this.priority,
      this.options.key,
      this.options,
    );
    var handshake = null;
    var onInitialized = function () {
      transport.unbind("initialized", onInitialized);
      transport.connect();
    };
    var onOpen = function () {
      handshake = Factory.createHandshake(transport, function (result) {
        connected = true;
        unbindListeners();
        callback(null, result);
      });
    };
    var onError = function (error) {
      unbindListeners();
      callback(error);
    };
    var onClosed = function () {
      unbindListeners();
      var serializedTransport;
      serializedTransport = safeJSONStringify(transport);
      callback(new TransportClosed(serializedTransport));
    };
    var unbindListeners = function () {
      transport.unbind("initialized", onInitialized);
      transport.unbind("open", onOpen);
      transport.unbind("error", onError);
      transport.unbind("closed", onClosed);
    };
    transport.bind("initialized", onInitialized);
    transport.bind("open", onOpen);
    transport.bind("error", onError);
    transport.bind("closed", onClosed);
    transport.initialize();
    return {
      abort: () => {
        if (connected) {
          return;
        }
        unbindListeners();
        if (handshake) {
          handshake.close();
        } else {
          transport.close();
        }
      },
      forceMinPriority: (p) => {
        if (connected) {
          return;
        }
        if (this.priority < p) {
          if (handshake) {
            handshake.close();
          } else {
            transport.close();
          }
        }
      },
    };
  }
}
function failAttempt(error, callback) {
  Util.defer(function () {
    callback(error);
  });
  return {
    abort: function () {},
    forceMinPriority: function () {},
  };
}
const { Transports } = Runtime;
var defineTransport = function (
  config2,
  name,
  type,
  priority,
  options,
  manager,
) {
  var transportClass = Transports[type];
  if (!transportClass) {
    throw new UnsupportedTransport(type);
  }
  var enabled =
    (!config2.enabledTransports ||
      arrayIndexOf(config2.enabledTransports, name) !== -1) &&
    (!config2.disabledTransports ||
      arrayIndexOf(config2.disabledTransports, name) === -1);
  var transport;
  if (enabled) {
    options = Object.assign(
      { ignoreNullOrigin: config2.ignoreNullOrigin },
      options,
    );
    transport = new TransportStrategy(
      name,
      priority,
      manager ? manager.getAssistant(transportClass) : transportClass,
      options,
    );
  } else {
    transport = UnsupportedStrategy2;
  }
  return transport;
};
var UnsupportedStrategy2 = {
  isSupported: function () {
    return false;
  },
  connect: function (_, callback) {
    var deferred = Util.defer(function () {
      callback(new UnsupportedStrategy$1());
    });
    return {
      abort: function () {
        deferred.ensureAborted();
      },
      forceMinPriority: function () {},
    };
  },
};
function validateOptions(options) {
  if (options == null) {
    throw "You must pass an options object";
  }
  if (options.cluster == null) {
    throw "Options object must provide a cluster";
  }
  if ("disableStats" in options) {
    Logger$1.warn(
      "The disableStats option is deprecated in favor of enableStats",
    );
  }
}
const composeChannelQuery$1 = (params, authOptions) => {
  var query = "socket_id=" + encodeURIComponent(params.socketId);
  for (var key in authOptions.params) {
    query +=
      "&" +
      encodeURIComponent(key) +
      "=" +
      encodeURIComponent(authOptions.params[key]);
  }
  if (authOptions.paramsProvider != null) {
    let dynamicParams = authOptions.paramsProvider();
    for (var key in dynamicParams) {
      query +=
        "&" +
        encodeURIComponent(key) +
        "=" +
        encodeURIComponent(dynamicParams[key]);
    }
  }
  return query;
};
const UserAuthenticator = (authOptions) => {
  if (typeof Runtime.getAuthorizers()[authOptions.transport] === "undefined") {
    throw `'${authOptions.transport}' is not a recognized auth transport`;
  }
  return (params, callback) => {
    const query = composeChannelQuery$1(params, authOptions);
    Runtime.getAuthorizers()[authOptions.transport](
      Runtime,
      query,
      authOptions,
      AuthRequestType.UserAuthentication,
      callback,
    );
  };
};
const composeChannelQuery = (params, authOptions) => {
  var query = "socket_id=" + encodeURIComponent(params.socketId);
  query += "&channel_name=" + encodeURIComponent(params.channelName);
  for (var key in authOptions.params) {
    query +=
      "&" +
      encodeURIComponent(key) +
      "=" +
      encodeURIComponent(authOptions.params[key]);
  }
  if (authOptions.paramsProvider != null) {
    let dynamicParams = authOptions.paramsProvider();
    for (var key in dynamicParams) {
      query +=
        "&" +
        encodeURIComponent(key) +
        "=" +
        encodeURIComponent(dynamicParams[key]);
    }
  }
  return query;
};
const ChannelAuthorizer = (authOptions) => {
  if (typeof Runtime.getAuthorizers()[authOptions.transport] === "undefined") {
    throw `'${authOptions.transport}' is not a recognized auth transport`;
  }
  return (params, callback) => {
    const query = composeChannelQuery(params, authOptions);
    Runtime.getAuthorizers()[authOptions.transport](
      Runtime,
      query,
      authOptions,
      AuthRequestType.ChannelAuthorization,
      callback,
    );
  };
};
const ChannelAuthorizerProxy = (
  pusher,
  authOptions,
  channelAuthorizerGenerator,
) => {
  const deprecatedAuthorizerOptions = {
    authTransport: authOptions.transport,
    authEndpoint: authOptions.endpoint,
    auth: {
      params: authOptions.params,
      headers: authOptions.headers,
    },
  };
  return (params, callback) => {
    const channel = pusher.channel(params.channelName);
    const channelAuthorizer = channelAuthorizerGenerator(
      channel,
      deprecatedAuthorizerOptions,
    );
    channelAuthorizer.authorize(params.socketId, callback);
  };
};
function getConfig(opts, pusher) {
  let config2 = {
    activityTimeout: opts.activityTimeout || Defaults.activityTimeout,
    cluster: opts.cluster,
    httpPath: opts.httpPath || Defaults.httpPath,
    httpPort: opts.httpPort || Defaults.httpPort,
    httpsPort: opts.httpsPort || Defaults.httpsPort,
    pongTimeout: opts.pongTimeout || Defaults.pongTimeout,
    statsHost: opts.statsHost || Defaults.stats_host,
    unavailableTimeout: opts.unavailableTimeout || Defaults.unavailableTimeout,
    wsPath: opts.wsPath || Defaults.wsPath,
    wsPort: opts.wsPort || Defaults.wsPort,
    wssPort: opts.wssPort || Defaults.wssPort,
    enableStats: getEnableStatsConfig(opts),
    httpHost: getHttpHost(opts),
    useTLS: shouldUseTLS(opts),
    wsHost: getWebsocketHost(opts),
    userAuthenticator: buildUserAuthenticator(opts),
    channelAuthorizer: buildChannelAuthorizer(opts, pusher),
  };
  if ("disabledTransports" in opts)
    config2.disabledTransports = opts.disabledTransports;
  if ("enabledTransports" in opts)
    config2.enabledTransports = opts.enabledTransports;
  if ("ignoreNullOrigin" in opts)
    config2.ignoreNullOrigin = opts.ignoreNullOrigin;
  if ("timelineParams" in opts) config2.timelineParams = opts.timelineParams;
  if ("nacl" in opts) {
    config2.nacl = opts.nacl;
  }
  return config2;
}
function getHttpHost(opts) {
  if (opts.httpHost) {
    return opts.httpHost;
  }
  if (opts.cluster) {
    return `sockjs-${opts.cluster}.pusher.com`;
  }
  return Defaults.httpHost;
}
function getWebsocketHost(opts) {
  if (opts.wsHost) {
    return opts.wsHost;
  }
  return getWebsocketHostFromCluster(opts.cluster);
}
function getWebsocketHostFromCluster(cluster) {
  return `ws-${cluster}.pusher.com`;
}
function shouldUseTLS(opts) {
  if (Runtime.getProtocol() === "https:") {
    return true;
  } else if (opts.forceTLS === false) {
    return false;
  }
  return true;
}
function getEnableStatsConfig(opts) {
  if ("enableStats" in opts) {
    return opts.enableStats;
  }
  if ("disableStats" in opts) {
    return !opts.disableStats;
  }
  return false;
}
function buildUserAuthenticator(opts) {
  const userAuthentication = {
    ...Defaults.userAuthentication,
    ...opts.userAuthentication,
  };
  if (
    "customHandler" in userAuthentication &&
    userAuthentication["customHandler"] != null
  ) {
    return userAuthentication["customHandler"];
  }
  return UserAuthenticator(userAuthentication);
}
function buildChannelAuth(opts, pusher) {
  let channelAuthorization;
  if ("channelAuthorization" in opts) {
    channelAuthorization = {
      ...Defaults.channelAuthorization,
      ...opts.channelAuthorization,
    };
  } else {
    channelAuthorization = {
      transport: opts.authTransport || Defaults.authTransport,
      endpoint: opts.authEndpoint || Defaults.authEndpoint,
    };
    if ("auth" in opts) {
      if ("params" in opts.auth) channelAuthorization.params = opts.auth.params;
      if ("headers" in opts.auth)
        channelAuthorization.headers = opts.auth.headers;
    }
    if ("authorizer" in opts)
      channelAuthorization.customHandler = ChannelAuthorizerProxy(
        pusher,
        channelAuthorization,
        opts.authorizer,
      );
  }
  return channelAuthorization;
}
function buildChannelAuthorizer(opts, pusher) {
  const channelAuthorization = buildChannelAuth(opts, pusher);
  if (
    "customHandler" in channelAuthorization &&
    channelAuthorization["customHandler"] != null
  ) {
    return channelAuthorization["customHandler"];
  }
  return ChannelAuthorizer(channelAuthorization);
}
class WatchlistFacade extends Dispatcher {
  constructor(pusher) {
    super(function (eventName, data) {
      Logger$1.debug(`No callbacks on watchlist events for ${eventName}`);
    });
    this.pusher = pusher;
    this.bindWatchlistInternalEvent();
  }
  handleEvent(pusherEvent) {
    pusherEvent.data.events.forEach((watchlistEvent) => {
      this.emit(watchlistEvent.name, watchlistEvent);
    });
  }
  bindWatchlistInternalEvent() {
    this.pusher.connection.bind("message", (pusherEvent) => {
      var eventName = pusherEvent.event;
      if (eventName === "pusher_internal:watchlist_events") {
        this.handleEvent(pusherEvent);
      }
    });
  }
}
function flatPromise() {
  let resolve, reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}
class UserFacade extends Dispatcher {
  constructor(pusher) {
    super(function (eventName, data) {
      Logger$1.debug("No callbacks on user for " + eventName);
    });
    this.signin_requested = false;
    this.user_data = null;
    this.serverToUserChannel = null;
    this.signinDonePromise = null;
    this._signinDoneResolve = null;
    this._onAuthorize = (err, authData) => {
      if (err) {
        Logger$1.warn(`Error during signin: ${err}`);
        this._cleanup();
        return;
      }
      this.pusher.send_event("pusher:signin", {
        auth: authData.auth,
        user_data: authData.user_data,
      });
    };
    this.pusher = pusher;
    this.pusher.connection.bind("state_change", ({ previous, current }) => {
      if (previous !== "connected" && current === "connected") {
        this._signin();
      }
      if (previous === "connected" && current !== "connected") {
        this._cleanup();
        this._newSigninPromiseIfNeeded();
      }
    });
    this.watchlist = new WatchlistFacade(pusher);
    this.pusher.connection.bind("message", (event) => {
      var eventName = event.event;
      if (eventName === "pusher:signin_success") {
        this._onSigninSuccess(event.data);
      }
      if (
        this.serverToUserChannel &&
        this.serverToUserChannel.name === event.channel
      ) {
        this.serverToUserChannel.handleEvent(event);
      }
    });
  }
  signin() {
    if (this.signin_requested) {
      return;
    }
    this.signin_requested = true;
    this._signin();
  }
  _signin() {
    if (!this.signin_requested) {
      return;
    }
    this._newSigninPromiseIfNeeded();
    if (this.pusher.connection.state !== "connected") {
      return;
    }
    this.pusher.config.userAuthenticator(
      {
        socketId: this.pusher.connection.socket_id,
      },
      this._onAuthorize,
    );
  }
  _onSigninSuccess(data) {
    try {
      this.user_data = JSON.parse(data.user_data);
    } catch (e) {
      Logger$1.error(
        `Failed parsing user data after signin: ${data.user_data}`,
      );
      this._cleanup();
      return;
    }
    if (typeof this.user_data.id !== "string" || this.user_data.id === "") {
      Logger$1.error(
        `user_data doesn't contain an id. user_data: ${this.user_data}`,
      );
      this._cleanup();
      return;
    }
    this._signinDoneResolve();
    this._subscribeChannels();
  }
  _subscribeChannels() {
    const ensure_subscribed = (channel) => {
      if (channel.subscriptionPending && channel.subscriptionCancelled) {
        channel.reinstateSubscription();
      } else if (
        !channel.subscriptionPending &&
        this.pusher.connection.state === "connected"
      ) {
        channel.subscribe();
      }
    };
    this.serverToUserChannel = new Channel(
      `#server-to-user-${this.user_data.id}`,
      this.pusher,
    );
    this.serverToUserChannel.bind_global((eventName, data) => {
      if (
        eventName.indexOf("pusher_internal:") === 0 ||
        eventName.indexOf("pusher:") === 0
      ) {
        return;
      }
      this.emit(eventName, data);
    });
    ensure_subscribed(this.serverToUserChannel);
  }
  _cleanup() {
    this.user_data = null;
    if (this.serverToUserChannel) {
      this.serverToUserChannel.unbind_all();
      this.serverToUserChannel.disconnect();
      this.serverToUserChannel = null;
    }
    if (this.signin_requested) {
      this._signinDoneResolve();
    }
  }
  _newSigninPromiseIfNeeded() {
    if (!this.signin_requested) {
      return;
    }
    if (this.signinDonePromise && !this.signinDonePromise.done) {
      return;
    }
    const { promise, resolve } = flatPromise();
    promise.done = false;
    const setDone = () => {
      promise.done = true;
    };
    promise.then(setDone).catch(setDone);
    this.signinDonePromise = promise;
    this._signinDoneResolve = resolve;
  }
}
class ChannelState {
  constructor(channelName) {
    this.channelName = channelName;
    this.conflationKey = null;
    this.maxMessagesPerKey = 10;
    this.conflationCaches = /* @__PURE__ */ new Map();
    this.baseMessage = null;
    this.baseSequence = null;
    this.lastSequence = null;
    this.deltaCount = 0;
    this.fullMessageCount = 0;
  }
  /**
   * Initialize cache from server sync
   */
  initializeFromCacheSync(data) {
    this.conflationKey = data.conflation_key || null;
    this.maxMessagesPerKey = data.max_messages_per_key || 10;
    this.conflationCaches.clear();
    if (data.states) {
      for (const [key, messages] of Object.entries(data.states)) {
        const cache = messages.map((msg) => ({
          content: msg.content,
          sequence: msg.seq,
        }));
        this.conflationCaches.set(key, cache);
      }
    }
  }
  /**
   * Set new base message (legacy - for non-conflation channels)
   */
  setBase(message, sequence) {
    this.baseMessage = message;
    this.baseSequence = sequence;
    this.lastSequence = sequence;
  }
  /**
   * Get base message for a conflation key at specific index
   * Note: baseIndex is the sequence number, not array index
   */
  getBaseMessage(conflationKeyValue, baseIndex) {
    if (!this.conflationKey) {
      return this.baseMessage;
    }
    const key = conflationKeyValue || "";
    const cache = this.conflationCaches.get(key);
    if (!cache || baseIndex === void 0) {
      console.error("[ChannelState] No cache or baseIndex undefined", {
        hasCache: !!cache,
        baseIndex,
        key,
        allCacheKeys: Array.from(this.conflationCaches.keys()),
      });
      return null;
    }
    const message = cache.find((msg) => msg.sequence === baseIndex);
    if (!message) {
      console.error("[ChannelState] Could not find message with sequence", {
        searchingFor: baseIndex,
        cacheSize: cache.length,
        cacheSequences: cache.map((m) => m.sequence),
        key: conflationKeyValue,
      });
      return null;
    }
    console.log("[ChannelState] Found base message", {
      sequence: baseIndex,
      key: conflationKeyValue,
      messageLength: message.content.length,
    });
    return message.content;
  }
  /**
   * Add or update message in conflation cache
   */
  updateConflationCache(conflationKeyValue, message, sequence) {
    const key = conflationKeyValue || "";
    let cache = this.conflationCaches.get(key);
    if (!cache) {
      cache = [];
      this.conflationCaches.set(key, cache);
    }
    cache.push({ content: message, sequence });
    while (cache.length > this.maxMessagesPerKey) {
      cache.shift();
    }
  }
  /**
   * Check if we have a valid base
   */
  hasBase() {
    if (this.conflationKey) {
      return this.conflationCaches.size > 0;
    }
    return this.baseMessage !== null && this.baseSequence !== null;
  }
  /**
   * Validate sequence number
   */
  isValidSequence(sequence) {
    if (this.lastSequence === null) {
      return true;
    }
    return sequence > this.lastSequence;
  }
  /**
   * Update sequence after processing a message
   */
  updateSequence(sequence) {
    this.lastSequence = sequence;
  }
  /**
   * Record delta received
   */
  recordDelta() {
    this.deltaCount++;
  }
  /**
   * Record full message received
   */
  recordFullMessage() {
    this.fullMessageCount++;
  }
  /**
   * Get statistics
   */
  getStats() {
    return {
      channelName: this.channelName,
      conflationKey: this.conflationKey,
      conflationGroupCount: this.conflationCaches.size,
      deltaCount: this.deltaCount,
      fullMessageCount: this.fullMessageCount,
      totalMessages: this.deltaCount + this.fullMessageCount,
    };
  }
}
const zDigits =
  "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz~"
    .split("")
    .map(function (x) {
      return x.charCodeAt(0);
    });
const zValue = [
  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1,
  -1, -1, -1, -1, -1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
  24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, -1, -1, -1, -1, 36, -1, 37,
  38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56,
  57, 58, 59, 60, 61, 62, -1, -1, -1, 63, -1,
];
class Reader {
  a;
  pos;
  constructor(array) {
    this.a = array;
    this.pos = 0;
  }
  haveBytes() {
    return this.pos < this.a.length;
  }
  getByte() {
    const b = this.a[this.pos];
    this.pos++;
    if (this.pos > this.a.length) throw new RangeError("out of bounds");
    return b;
  }
  getChar() {
    return String.fromCharCode(this.getByte());
  }
  // Read base64-encoded unsigned integer.
  getInt() {
    let v = 0;
    let c;
    while (this.haveBytes() && (c = zValue[127 & this.getByte()]) >= 0) {
      v = (v << 6) + c;
    }
    this.pos--;
    return v >>> 0;
  }
}
class Writer {
  a = [];
  toByteArray(sourceType) {
    if (Array.isArray(sourceType)) {
      return this.a;
    }
    return new Uint8Array(this.a);
  }
  putByte(b) {
    this.a.push(b & 255);
  }
  // Write an ASCII character (s is a one-char string).
  putChar(s) {
    this.putByte(s.charCodeAt(0));
  }
  // Write a base64 unsigned integer.
  putInt(v) {
    const zBuf = [];
    if (v === 0) {
      this.putChar("0");
      return;
    }
    let i = 0;
    for (; v > 0; i++, v >>>= 6) {
      zBuf.push(zDigits[v & 63]);
    }
    for (let j = i - 1; j >= 0; j--) {
      this.putByte(zBuf[j]);
    }
  }
  // Copy from array at start to end.
  putArray(a, start, end) {
    for (let i = start; i < end; i++) this.a.push(a[i]);
  }
}
function checksum(arr) {
  let sum0 = 0,
    sum1 = 0,
    sum2 = 0,
    sum3 = 0,
    z = 0,
    N = arr.length;
  while (N >= 16) {
    sum0 = (sum0 + arr[z + 0]) | 0;
    sum1 = (sum1 + arr[z + 1]) | 0;
    sum2 = (sum2 + arr[z + 2]) | 0;
    sum3 = (sum3 + arr[z + 3]) | 0;
    sum0 = (sum0 + arr[z + 4]) | 0;
    sum1 = (sum1 + arr[z + 5]) | 0;
    sum2 = (sum2 + arr[z + 6]) | 0;
    sum3 = (sum3 + arr[z + 7]) | 0;
    sum0 = (sum0 + arr[z + 8]) | 0;
    sum1 = (sum1 + arr[z + 9]) | 0;
    sum2 = (sum2 + arr[z + 10]) | 0;
    sum3 = (sum3 + arr[z + 11]) | 0;
    sum0 = (sum0 + arr[z + 12]) | 0;
    sum1 = (sum1 + arr[z + 13]) | 0;
    sum2 = (sum2 + arr[z + 14]) | 0;
    sum3 = (sum3 + arr[z + 15]) | 0;
    z += 16;
    N -= 16;
  }
  while (N >= 4) {
    sum0 = (sum0 + arr[z + 0]) | 0;
    sum1 = (sum1 + arr[z + 1]) | 0;
    sum2 = (sum2 + arr[z + 2]) | 0;
    sum3 = (sum3 + arr[z + 3]) | 0;
    z += 4;
    N -= 4;
  }
  sum3 = (((((sum3 + (sum2 << 8)) | 0) + (sum1 << 16)) | 0) + (sum0 << 24)) | 0;
  switch (N) {
    case 3:
      sum3 = (sum3 + (arr[z + 2] << 8)) | 0;
    /* falls through */
    case 2:
      sum3 = (sum3 + (arr[z + 1] << 16)) | 0;
    /* falls through */
    case 1:
      sum3 = (sum3 + (arr[z + 0] << 24)) | 0;
  }
  return sum3 >>> 0;
}
function applyDelta(source, delta2, opts) {
  let limit,
    total = 0;
  const zDelta = new Reader(delta2);
  const lenSrc = source.length;
  const lenDelta = delta2.length;
  limit = zDelta.getInt();
  if (zDelta.getChar() !== "\n")
    throw new Error("size integer not terminated by '\\n'");
  const zOut = new Writer();
  while (zDelta.haveBytes()) {
    let cnt = zDelta.getInt();
    let ofst;
    switch (zDelta.getChar()) {
      case "@":
        ofst = zDelta.getInt();
        if (zDelta.haveBytes() && zDelta.getChar() !== ",")
          throw new Error("copy command not terminated by ','");
        total += cnt;
        if (total > limit) throw new Error("copy exceeds output file size");
        if (ofst + cnt > lenSrc)
          throw new Error("copy extends past end of input");
        zOut.putArray(source, ofst, ofst + cnt);
        break;
      case ":":
        total += cnt;
        if (total > limit)
          throw new Error(
            "insert command gives an output larger than predicted",
          );
        if (cnt > lenDelta)
          throw new Error("insert count exceeds size of delta");
        zOut.putArray(zDelta.a, zDelta.pos, zDelta.pos + cnt);
        zDelta.pos += cnt;
        break;
      case ";":
        const out = zOut.toByteArray(source);
        if ((!opts || opts.verifyChecksum !== false) && cnt !== checksum(out))
          throw new Error("bad checksum");
        if (total !== limit)
          throw new Error("generated size does not match predicted size");
        return out;
      default:
        throw new Error("unknown delta operator");
    }
  }
  throw new Error("unterminated delta");
}
function CustomErrors(names) {
  let errors2 = {};
  names.forEach((name) => {
    let CustomError = function CustomError2(message) {
      var temp = Error.apply(this, arguments);
      temp.name = this.name = name;
      this.stack = temp.stack;
      this.message = temp.message;
      this.name = name;
      this.message = message;
    };
    CustomError.prototype = Object.create(Error.prototype, {
      constructor: {
        value: CustomError,
        writable: true,
        configurable: true,
      },
    });
    errors2[name] = CustomError;
  });
  return errors2;
}
const errors = /* @__PURE__ */ CustomErrors(["NotImplemented", "InvalidDelta"]);
class TypedArrayList {
  constructor() {
    this.typedArrays = [];
    this.startIndexes = [];
    this.length = 0;
  }
  add(typedArray) {
    let typedArrayTypes = [
      Int8Array,
      Uint8Array,
      Uint8ClampedArray,
      Int16Array,
      Uint16Array,
      Int32Array,
      Uint32Array,
      Float32Array,
      Float64Array,
    ];
    let matchingTypedArrayTypes = typedArrayTypes.filter(
      (typedArrayType) => typedArray instanceof typedArrayType,
    );
    if (matchingTypedArrayTypes.length < 1) {
      throw Error("Given " + typeof typedArray + " when expected a TypedArray");
    }
    let startIndex;
    if (this.typedArrays.length === 0) {
      startIndex = 0;
    } else {
      let lastIndex = this.startIndexes.length - 1;
      let lastStartIndex = this.startIndexes[lastIndex];
      let lastLength = this.typedArrays[lastIndex].length;
      startIndex = lastStartIndex + lastLength;
    }
    this.startIndexes.push(startIndex);
    this.typedArrays.push(typedArray);
    this.length += startIndex + typedArray.length;
  }
  get(index) {
    let listIndex = getIndex(this.startIndexes, index);
    let typedArray = index - this.startIndexes[listIndex];
    return this.typedArrays[listIndex][typedArray];
  }
  set(index, value) {
    if (typeof index !== "number" || isNaN(index)) {
      throw new Error("Given non-number index: " + index);
    }
    let listIndex = getIndex(this.startIndexes, index);
    let typedArrayIndex = index - this.startIndexes[listIndex];
    this.typedArrays[listIndex][typedArrayIndex] = value;
  }
}
function getIndex(arr, element) {
  if (arr.length === 2) {
    return element < arr[1] ? 0 : 1;
  }
  let low = 0;
  let high = arr.length - 1;
  while (low < high) {
    let mid = Math.floor((low + high) / 2);
    if (arr[mid] === element) {
      return mid;
    } else if (arr[mid] < element) {
      low = mid + 1;
    } else {
      high = mid - 1;
    }
  }
  if (arr[high] > element) {
    return high - 1;
  } else {
    return high;
  }
}
function integer(buffer, position) {
  const result = { position, value: 0 };
  do {
    result.value = (result.value << 7) | (buffer[result.position] & 127);
    if (result.value < 0) {
      throw new Error("RFC 3284 Integer conversion: Buffer overflow");
    }
  } while (buffer[result.position++] & 128);
  return result;
}
class ADD {
  constructor(size) {
    this.size = size;
  }
  name = "ADD";
  execute(delta2) {
    for (let i = 0; i < this.size; i++) {
      delta2.U.set(
        delta2.UTargetPosition + i,
        delta2.data[delta2.dataPosition + i],
      );
    }
    delta2.dataPosition += this.size;
    delta2.UTargetPosition += this.size;
  }
}
class COPY {
  constructor(size, mode) {
    this.size = size;
    this.mode = mode;
  }
  name = "COPY";
  execute(delta2) {
    let address, m, next;
    if (this.mode === 0) {
      address = delta2.getNextAddressInteger();
    } else if (this.mode === 1) {
      next = delta2.getNextAddressInteger();
      address = delta2.UTargetPosition - next;
    } else if ((m = this.mode - 2) >= 0 && m < delta2.nearCache.size) {
      next = delta2.getNextAddressInteger();
      address = delta2.nearCache.get(m, next);
    } else {
      m = this.mode - (2 + delta2.nearCache.size);
      next = delta2.getNextAddressByte();
      address = delta2.sameCache.get(m, next);
    }
    delta2.nearCache.update(address);
    delta2.sameCache.update(address);
    for (let i = 0; i < this.size; i++) {
      delta2.U.set(delta2.UTargetPosition + i, delta2.U.get(address + i));
    }
    delta2.UTargetPosition += this.size;
  }
}
class RUN {
  constructor(size) {
    this.size = size;
  }
  name = "RUN";
  execute(delta2) {
    for (let i = 0; i < this.size; i++) {
      delta2.U.set(
        delta2.UTargetPosition + i,
        delta2.data[delta2.dataPosition],
      );
    }
    delta2.dataPosition++;
    delta2.UTargetPosition += this.size;
  }
}
let instructions = {
  ADD,
  COPY,
  RUN,
};
function tokenizeInstructions(instructionsBuffer) {
  let deserializedInstructions = [];
  let instructionsPosition = 0;
  while (instructionsPosition < instructionsBuffer.length) {
    let index = instructionsBuffer[instructionsPosition++];
    let addSize, copySize, size;
    if (index === 0) {
      ({ value: size, position: instructionsPosition } = integer(
        instructionsBuffer,
        instructionsPosition,
      ));
      deserializedInstructions.push(new instructions.RUN(size));
    } else if (index === 1) {
      ({ value: size, position: instructionsPosition } = integer(
        instructionsBuffer,
        instructionsPosition,
      ));
      deserializedInstructions.push(new instructions.ADD(size));
    } else if (index < 19) {
      deserializedInstructions.push(new instructions.ADD(index - 1));
    } else if (index === 19) {
      ({ value: size, position: instructionsPosition } = integer(
        instructionsBuffer,
        instructionsPosition,
      ));
      deserializedInstructions.push(new instructions.COPY(size, 0));
    } else if (index < 35) {
      deserializedInstructions.push(new instructions.COPY(index - 16, 0));
    } else if (index === 35) {
      ({ value: size, position: instructionsPosition } = integer(
        instructionsBuffer,
        instructionsPosition,
      ));
      deserializedInstructions.push(new instructions.COPY(size, 1));
    } else if (index < 51) {
      deserializedInstructions.push(new instructions.COPY(index - 32, 1));
    } else if (index === 51) {
      ({ value: size, position: instructionsPosition } = integer(
        instructionsBuffer,
        instructionsPosition,
      ));
      deserializedInstructions.push(new instructions.COPY(size, 2));
    } else if (index < 67) {
      deserializedInstructions.push(new instructions.COPY(index - 48, 2));
    } else if (index === 67) {
      ({ value: size, position: instructionsPosition } = integer(
        instructionsBuffer,
        instructionsPosition,
      ));
      deserializedInstructions.push(new instructions.COPY(size, 3));
    } else if (index < 83) {
      deserializedInstructions.push(new instructions.COPY(index - 64, 3));
    } else if (index === 83) {
      ({ value: size, position: instructionsPosition } = integer(
        instructionsBuffer,
        instructionsPosition,
      ));
      deserializedInstructions.push(new instructions.COPY(size, 4));
    } else if (index < 99) {
      deserializedInstructions.push(new instructions.COPY(index - 80, 4));
    } else if (index === 99) {
      ({ value: size, position: instructionsPosition } = integer(
        instructionsBuffer,
        instructionsPosition,
      ));
      deserializedInstructions.push(new instructions.COPY(size, 5));
    } else if (index < 115) {
      deserializedInstructions.push(new instructions.COPY(index - 96, 5));
    } else if (index === 115) {
      ({ value: size, position: instructionsPosition } = integer(
        instructionsBuffer,
        instructionsPosition,
      ));
      deserializedInstructions.push(new instructions.COPY(size, 6));
    } else if (index < 131) {
      deserializedInstructions.push(new instructions.COPY(index - 112, 6));
    } else if (index === 131) {
      ({ value: size, position: instructionsPosition } = integer(
        instructionsBuffer,
        instructionsPosition,
      ));
      deserializedInstructions.push(new instructions.COPY(size, 7));
    } else if (index < 147) {
      deserializedInstructions.push(new instructions.COPY(index - 128, 7));
    } else if (index === 147) {
      ({ value: size, position: instructionsPosition } = integer(
        instructionsBuffer,
        instructionsPosition,
      ));
      deserializedInstructions.push(new instructions.COPY(size, 8));
    } else if (index < 163) {
      deserializedInstructions.push(new instructions.COPY(index - 144, 8));
    } else if (index < 175) {
      ({ addSize, copySize } = ADD_COPY(index, 163));
      deserializedInstructions.push(new instructions.ADD(addSize));
      deserializedInstructions.push(new instructions.COPY(copySize, 0));
    } else if (index < 187) {
      ({ addSize, copySize } = ADD_COPY(index, 175));
      deserializedInstructions.push(new instructions.ADD(addSize));
      deserializedInstructions.push(new instructions.COPY(copySize, 1));
    } else if (index < 199) {
      ({ addSize, copySize } = ADD_COPY(index, 187));
      deserializedInstructions.push(new instructions.ADD(addSize));
      deserializedInstructions.push(new instructions.COPY(copySize, 2));
    } else if (index < 211) {
      ({ addSize, copySize } = ADD_COPY(index, 199));
      deserializedInstructions.push(new instructions.ADD(addSize));
      deserializedInstructions.push(new instructions.COPY(copySize, 3));
    } else if (index < 223) {
      ({ addSize, copySize } = ADD_COPY(index, 211));
      deserializedInstructions.push(new instructions.ADD(addSize));
      deserializedInstructions.push(new instructions.COPY(copySize, 4));
    } else if (index < 235) {
      ({ addSize, copySize } = ADD_COPY(index, 223));
      deserializedInstructions.push(new instructions.ADD(addSize));
      deserializedInstructions.push(new instructions.COPY(copySize, 5));
    } else if (index < 239) {
      deserializedInstructions.push(new instructions.ADD(index - 235 + 1));
      deserializedInstructions.push(new instructions.COPY(4, 6));
    } else if (index < 243) {
      deserializedInstructions.push(new instructions.ADD(index - 239 + 1));
      deserializedInstructions.push(new instructions.COPY(4, 7));
    } else if (index < 247) {
      deserializedInstructions.push(new instructions.ADD(index - 243 + 1));
      deserializedInstructions.push(new instructions.COPY(4, 8));
    } else if (index < 256) {
      deserializedInstructions.push(new instructions.COPY(4, index - 247));
      deserializedInstructions.push(new instructions.ADD(1));
    } else {
      throw new Error("Should not get here");
    }
  }
  return deserializedInstructions;
}
function ADD_COPY(index, baseIndex) {
  let zeroBased = index - baseIndex;
  let addSizeIndex = Math.floor(zeroBased / 3);
  let addSize = addSizeIndex + 1;
  let copySizeIndex = zeroBased % 3;
  let copySize = copySizeIndex + 4;
  return { addSize, copySize };
}
function delta(delta2, position) {
  let targetWindowLength, dataLength, instructionsLength, addressesLength;
  ({ value: targetWindowLength, position } = integer(delta2, position));
  if (delta2[position] !== 0) {
    throw new errors.NotImplemented(
      "VCD_DECOMPRESS is not supported, Delta_Indicator must be zero at byte " +
        position +
        " and not " +
        delta2[position],
    );
  }
  position++;
  ({ value: dataLength, position } = integer(delta2, position));
  ({ value: instructionsLength, position } = integer(delta2, position));
  ({ value: addressesLength, position } = integer(delta2, position));
  let dataNextPosition = position + dataLength;
  let data = delta2.slice(position, dataNextPosition);
  let instructionsNextPosition = dataNextPosition + instructionsLength;
  let instructions2 = delta2.slice(dataNextPosition, instructionsNextPosition);
  let deserializedInstructions = tokenizeInstructions(instructions2);
  let addressesNextPosition = instructionsNextPosition + addressesLength;
  let addresses = delta2.slice(instructionsNextPosition, addressesNextPosition);
  position = addressesNextPosition;
  let window2 = {
    targetWindowLength,
    position,
    data,
    instructions: deserializedInstructions,
    addresses,
  };
  return window2;
}
class NearCache {
  constructor(size) {
    this.size = size;
    this.near = new Array(this.size).fill(0);
    this.nextSlot = 0;
  }
  update(address) {
    if (this.near.length > 0) {
      this.near[this.nextSlot] = address;
      this.nextSlot = (this.nextSlot + 1) % this.near.length;
    }
  }
  get(m, offset) {
    let address = this.near[m] + offset;
    return address;
  }
}
class SameCache {
  constructor(size) {
    this.size = size;
    this.same = new Array(this.size * 256).fill(0);
  }
  update(address) {
    if (this.same.length > 0) {
      this.same[address % (this.size * 256)] = address;
    }
  }
  get(m, offset) {
    let address = this.same[m * 256 + offset];
    return address;
  }
}
class VCDiff {
  constructor(delta2, source) {
    this.delta = delta2;
    this.position = 0;
    this.source = source;
    this.targetWindows = new TypedArrayList();
  }
  decode() {
    this._consumeHeader();
    while (this._consumeWindow()) {}
    let targetLength = this.targetWindows.typedArrays.reduce(
      (sum, uint8Array) => uint8Array.length + sum,
      0,
    );
    let target = new Uint8Array(targetLength);
    let position = 0;
    for (
      let arrayNum = 0;
      arrayNum < this.targetWindows.typedArrays.length;
      arrayNum++
    ) {
      let array = this.targetWindows.typedArrays[arrayNum];
      let length = array.length;
      target.set(array, position);
      position += length;
    }
    return target;
  }
  _consumeHeader() {
    let hasVCDiffHeader =
      this.delta[0] === 214 && // V
      this.delta[1] === 195 && // C
      this.delta[2] === 196 && // D
      this.delta[3] === 0;
    if (!hasVCDiffHeader) {
      throw new errors.InvalidDelta("first 3 bytes not VCD");
    }
    let hdrIndicator = this.delta[4];
    let vcdDecompress = 1 & hdrIndicator;
    let vcdCodetable = 1 & (hdrIndicator >> 1);
    if (vcdDecompress || vcdCodetable) {
      throw new errors.NotImplemented(
        "non-zero Hdr_Indicator (VCD_DECOMPRESS or VCD_CODETABLE bit is set)",
      );
    }
    this.position += 5;
  }
  _consumeWindow() {
    let winIndicator = this.delta[this.position++];
    let vcdSource = 1 & winIndicator;
    let vcdTarget = 1 & (winIndicator >> 1);
    if (vcdSource && vcdTarget) {
      throw new errors.InvalidDelta(
        "VCD_SOURCE and VCD_TARGET cannot both be set in Win_Indicator",
      );
    } else if (vcdSource) {
      let sourceSegmentLength, sourceSegmentPosition, deltaLength;
      ({ value: sourceSegmentLength, position: this.position } = integer(
        this.delta,
        this.position,
      ));
      ({ value: sourceSegmentPosition, position: this.position } = integer(
        this.delta,
        this.position,
      ));
      ({ value: deltaLength, position: this.position } = integer(
        this.delta,
        this.position,
      ));
      let sourceSegment = this.source.slice(
        sourceSegmentPosition,
        sourceSegmentPosition + sourceSegmentLength,
      );
      this._buildTargetWindow(this.position, sourceSegment);
      this.position += deltaLength;
    } else if (vcdTarget) {
      throw new errors.NotImplemented("non-zero VCD_TARGET in Win_Indicator");
    } else {
      let deltaLength;
      ({ value: deltaLength, position: this.position } = integer(
        this.delta,
        this.position,
      ));
      this._buildTargetWindow(this.position);
      this.position += deltaLength;
    }
    return this.position < this.delta.length;
  }
  // first integer is target window length
  _buildTargetWindow(position, sourceSegment) {
    let window2 = delta(this.delta, position);
    let T = new Uint8Array(window2.targetWindowLength);
    let U = new TypedArrayList();
    let uTargetPosition = 0;
    if (sourceSegment) {
      U.add(sourceSegment);
      uTargetPosition = sourceSegment.length;
    }
    U.add(T);
    this.source.length;
    let delta$1 = new Delta(
      U,
      uTargetPosition,
      window2.data,
      window2.addresses,
    );
    window2.instructions.forEach((instruction) => {
      instruction.execute(delta$1);
    });
    this.targetWindows.add(T);
  }
}
class Delta {
  constructor(U, UTargetPosition, data, addresses) {
    this.U = U;
    this.UTargetPosition = UTargetPosition;
    this.data = data;
    this.dataPosition = 0;
    this.addresses = addresses;
    this.addressesPosition = 0;
    this.nearCache = new NearCache(4);
    this.sameCache = new SameCache(3);
  }
  getNextAddressInteger() {
    let value;
    ({ value, position: this.addressesPosition } = integer(
      this.addresses,
      this.addressesPosition,
    ));
    return value;
  }
  getNextAddressByte() {
    let value = this.addresses[this.addressesPosition++];
    return value;
  }
}
function decode(delta2, source) {
  let vcdiff = new VCDiff(delta2, source);
  return vcdiff.decode();
}
const fossilDeltaGlobal =
  typeof window !== "undefined" ? window.fossilDelta : void 0;
const vcdiffGlobal = typeof window !== "undefined" ? window.vcdiff : void 0;
function base64ToBytes(base642) {
  const binaryString = atob(base642);
  const bytes = new Uint8Array(binaryString.length);
  for (let i = 0; i < binaryString.length; i++) {
    bytes[i] = binaryString.charCodeAt(i);
  }
  return bytes;
}
function bytesToString(bytes) {
  if (Array.isArray(bytes) || !(bytes instanceof Uint8Array)) {
    bytes = new Uint8Array(bytes);
  }
  return new TextDecoder().decode(bytes);
}
function stringToBytes(str) {
  return new TextEncoder().encode(str);
}
class FossilDeltaDecoder {
  static isAvailable() {
    return (
      typeof applyDelta !== "undefined" ||
      (typeof fossilDeltaGlobal !== "undefined" && fossilDeltaGlobal.apply)
    );
  }
  static apply(base, delta2) {
    if (!this.isAvailable()) {
      throw new Error("Fossil Delta library not loaded");
    }
    const baseBytes = typeof base === "string" ? stringToBytes(base) : base;
    const deltaBytes =
      typeof delta2 === "string" ? stringToBytes(delta2) : delta2;
    try {
      let result;
      if (typeof applyDelta !== "undefined") {
        result = applyDelta(baseBytes, deltaBytes);
      } else if (fossilDeltaGlobal && fossilDeltaGlobal.apply) {
        result = fossilDeltaGlobal.apply(baseBytes, deltaBytes);
      } else {
        throw new Error("No fossil-delta implementation found");
      }
      return bytesToString(result);
    } catch (error) {
      throw new Error(
        `Fossil delta decode failed: ${error.message} (base=${baseBytes.length}B delta=${deltaBytes.length}B)`,
      );
    }
  }
}
class Xdelta3Decoder {
  static isAvailable() {
    return (
      typeof decode !== "undefined" ||
      (typeof vcdiffGlobal !== "undefined" && vcdiffGlobal.decode)
    );
  }
  static apply(base, delta2) {
    if (!this.isAvailable()) {
      throw new Error("Xdelta3/VCDIFF library not loaded");
    }
    const baseBytes = typeof base === "string" ? stringToBytes(base) : base;
    const deltaBytes =
      typeof delta2 === "string" ? stringToBytes(delta2) : delta2;
    try {
      let result;
      if (typeof decode !== "undefined") {
        result = decode(deltaBytes, baseBytes);
      } else if (vcdiffGlobal && vcdiffGlobal.decode) {
        result = vcdiffGlobal.decode(deltaBytes, baseBytes);
      } else {
        throw new Error("No VCDIFF decoder implementation found");
      }
      return bytesToString(result);
    } catch (error) {
      throw new Error(
        `Xdelta3 decode failed: ${error.message} (base=${baseBytes.length}B delta=${deltaBytes.length}B)`,
      );
    }
  }
}
class DeltaCompressionManager {
  constructor(options = {}, sendEventCallback) {
    this.options = {
      enabled: options.enabled !== false,
      algorithms: options.algorithms || ["fossil", "xdelta3"],
      debug: options.debug || false,
      onStats: options.onStats || null,
      onError: options.onError || null,
    };
    this.enabled = false;
    this.channelStates = /* @__PURE__ */ new Map();
    this.stats = {
      totalMessages: 0,
      deltaMessages: 0,
      fullMessages: 0,
      totalBytesWithoutCompression: 0,
      totalBytesWithCompression: 0,
      errors: 0,
    };
    this.sendEventCallback = sendEventCallback;
    this.availableAlgorithms = this.detectAvailableAlgorithms();
    if (this.availableAlgorithms.length === 0) {
      Logger$1.warn(
        "[DeltaCompression] No delta algorithms available. Please include fossil-delta or vcdiff-decoder libraries.",
      );
    }
  }
  /**
   * Detect which algorithm libraries are loaded
   */
  detectAvailableAlgorithms() {
    const available = [];
    if (FossilDeltaDecoder.isAvailable()) {
      available.push("fossil");
      this.log("Fossil Delta decoder available");
    }
    if (Xdelta3Decoder.isAvailable()) {
      available.push("xdelta3");
      this.log("Xdelta3 decoder available");
    }
    return available;
  }
  /**
   * Enable delta compression
   */
  enable() {
    console.log("[DeltaManager] enable() called", {
      enabled: this.enabled,
      optionsEnabled: this.options.enabled,
      stack: new Error().stack,
    });
    if (this.enabled || !this.options.enabled) {
      return;
    }
    if (this.availableAlgorithms.length === 0) {
      this.log("No delta algorithms available, cannot enable");
      return;
    }
    const supportedAlgorithms = this.availableAlgorithms.filter((algo) =>
      this.options.algorithms.includes(algo),
    );
    if (supportedAlgorithms.length === 0) {
      this.log("No mutually supported algorithms");
      return;
    }
    this.log("Sending enable request", supportedAlgorithms);
    this.sendEventCallback("pusher:enable_delta_compression", {
      algorithms: supportedAlgorithms,
    });
  }
  /**
   * Disable delta compression
   */
  disable() {
    this.enabled = false;
    this.channelStates.clear();
  }
  /**
   * Handle delta compression enabled confirmation
   */
  handleEnabled(data) {
    this.enabled = data.enabled || true;
    this.log("Delta compression enabled", data);
  }
  /**
   * Handle cache sync message (conflation keys)
   */
  handleCacheSync(channel, data) {
    this.log("Received cache sync", {
      channel,
      conflationKey: data.conflation_key,
      groupCount: Object.keys(data.states || {}).length,
    });
    let channelState = this.channelStates.get(channel);
    if (!channelState) {
      channelState = new ChannelState(channel);
      this.channelStates.set(channel, channelState);
    }
    channelState.initializeFromCacheSync(data);
    this.log("Cache initialized", channelState.getStats());
  }
  /**
   * Handle delta-compressed message
   */
  handleDeltaMessage(channel, deltaData) {
    console.log("[DeltaManager] handleDeltaMessage called", {
      channel,
      deltaData,
    });
    let deltaBytes = null;
    try {
      const event = deltaData.event;
      const delta2 = deltaData.delta;
      const sequence = deltaData.seq;
      const algorithm = deltaData.algorithm || "fossil";
      const conflationKey = deltaData.conflation_key;
      const baseIndex = deltaData.base_index;
      console.log("[DeltaManager] Extracted delta params", {
        event,
        sequence,
        algorithm,
        conflationKey,
        baseIndex,
        deltaLength: delta2?.length,
      });
      this.log("Received delta message", {
        channel,
        event,
        sequence,
        algorithm,
        conflationKey,
        baseIndex,
        deltaSize: delta2.length,
        deltaPreview: this.options.debug ? delta2.slice(0, 80) : void 0,
      });
      let channelState = this.channelStates.get(channel);
      console.log("[DeltaManager] Channel state lookup", {
        channel,
        hasState: !!channelState,
        totalStates: this.channelStates.size,
      });
      if (!channelState) {
        console.error("[DeltaManager] No channel state for", channel);
        this.error(`No channel state for ${channel}`);
        this.requestResync(channel);
        return null;
      }
      let baseMessage;
      console.log("[DeltaManager] Getting base message", {
        hasConflationKey: !!channelState.conflationKey,
        conflationKey,
        baseIndex,
      });
      if (channelState.conflationKey) {
        baseMessage = channelState.getBaseMessage(conflationKey, baseIndex);
        console.log("[DeltaManager] Got base from conflation cache", {
          hasBase: !!baseMessage,
          baseLength: baseMessage?.length,
        });
        if (!baseMessage) {
          this.error(
            `No base message for channel ${channel}, key ${conflationKey}, index ${baseIndex}`,
          );
          if (this.options.debug) {
            this.log("Current conflation cache snapshot", {
              channel,
              conflationKey: channelState.conflationKey,
              cacheSizes: Array.from(
                channelState.conflationCaches.entries(),
              ).map(([key, cache]) => ({ key, size: cache.length })),
            });
          }
          this.requestResync(channel);
          return null;
        }
      } else {
        baseMessage = channelState.baseMessage;
        if (!baseMessage) {
          this.error(`No base message for channel ${channel}`);
          if (this.options.debug) {
            this.log("Channel state missing base", {
              channel,
              lastSequence: channelState.lastSequence,
            });
          }
          this.requestResync(channel);
          return null;
        }
      }
      deltaBytes = base64ToBytes(delta2);
      if (this.options.debug) {
        this.log("Applying delta", {
          channel,
          event,
          sequence,
          algorithm,
          conflationKey,
          baseIndex,
          baseLength: baseMessage.length,
          deltaLength: deltaBytes.length,
          basePreview: baseMessage.slice(0, 120),
        });
      }
      console.log("[DeltaManager] About to apply delta", {
        algorithm,
        baseLength: baseMessage.length,
        deltaLength: deltaBytes.length,
        basePreview: baseMessage.substring(0, 100),
      });
      let reconstructedMessage;
      try {
        if (algorithm === "fossil") {
          reconstructedMessage = FossilDeltaDecoder.apply(
            baseMessage,
            deltaBytes,
          );
          console.log("[DeltaManager] Fossil delta applied successfully", {
            resultLength: reconstructedMessage.length,
            resultPreview: reconstructedMessage.substring(0, 100),
          });
        } else if (algorithm === "xdelta3") {
          reconstructedMessage = Xdelta3Decoder.apply(baseMessage, deltaBytes);
        } else {
          throw Error(`Unknown algorithm: ${algorithm}`);
        }
      } catch (decodeError) {
        console.error("[DeltaManager] Delta decode error:", decodeError);
        throw decodeError;
      }
      console.log("[DeltaManager] Updating conflation cache", {
        hasConflationKey: !!channelState.conflationKey,
        conflationKey,
        sequence,
        messageLength: reconstructedMessage.length,
      });
      if (channelState.conflationKey) {
        channelState.updateConflationCache(
          conflationKey,
          reconstructedMessage,
          sequence,
        );
        console.log("[DeltaManager] Updated conflation cache", {
          conflationKey,
          sequence,
          cacheSize: channelState.conflationCaches.get(conflationKey || "")
            ?.length,
        });
      } else {
        channelState.setBase(reconstructedMessage, sequence);
        console.log("[DeltaManager] Updated base message (non-conflation)", {
          sequence,
        });
      }
      channelState.updateSequence(sequence);
      channelState.recordDelta();
      this.stats.totalMessages++;
      this.stats.deltaMessages++;
      this.stats.totalBytesWithCompression += deltaBytes.length;
      this.stats.totalBytesWithoutCompression += reconstructedMessage.length;
      this.updateStats();
      this.log("Delta applied successfully", {
        channel,
        event,
        conflationKey,
        originalSize: reconstructedMessage.length,
        deltaSize: deltaBytes.length,
        compressionRatio:
          ((deltaBytes.length / reconstructedMessage.length) * 100).toFixed(1) +
          "%",
      });
      try {
        const parsedMessage = JSON.parse(reconstructedMessage);
        return {
          event,
          channel,
          data: parsedMessage.data || parsedMessage,
        };
      } catch (e) {
        return {
          event,
          channel,
          data: reconstructedMessage,
        };
      }
    } catch (error) {
      this.error("Delta decode failed", {
        channel,
        event: deltaData.event,
        sequence: deltaData.seq,
        algorithm: deltaData.algorithm,
        conflationKey: deltaData.conflation_key,
        baseIndex: deltaData.base_index,
        deltaSize: deltaData.delta?.length,
        decodedDeltaBytes: deltaBytes ? deltaBytes.length : "n/a",
        message: error.message,
      });
      this.stats.errors++;
      return null;
    }
  }
  /**
   * Handle regular (full) message with delta sequence markers
   */
  handleFullMessage(channel, rawMessage, sequence, conflationKey) {
    console.log("[DeltaManager] handleFullMessage called", {
      channel,
      sequence,
      hasSequence: sequence !== void 0,
      messageLength: rawMessage?.length,
      messagePreview: rawMessage?.substring(0, 100),
    });
    if (!sequence && sequence !== 0) {
      try {
        const parsed = JSON.parse(rawMessage);
        const candidate =
          typeof parsed.data === "string"
            ? (JSON.parse(parsed.data).__delta_seq ?? parsed.__delta_seq)
            : (parsed.data?.__delta_seq ?? parsed.__delta_seq);
        if (candidate === 0 || candidate) {
          sequence = candidate;
        } else {
          this.log("handleFullMessage missing sequence, skipping", {
            channel,
            hasSequence: false,
          });
          return;
        }
      } catch (e) {
        this.log("handleFullMessage missing sequence and parse failed", {
          channel,
          hasSequence: false,
        });
        return;
      }
    }
    const messageSize = rawMessage.length;
    let channelState = this.channelStates.get(channel);
    if (!channelState) {
      channelState = new ChannelState(channel);
      this.channelStates.set(channel, channelState);
    }
    try {
      const parsed = JSON.parse(rawMessage);
      const data =
        typeof parsed.data === "string" ? JSON.parse(parsed.data) : parsed.data;
      const extractedConflationKey =
        parsed.conflation_key || data?.__conflation_key;
      const finalConflationKey = conflationKey || extractedConflationKey;
      console.log("[DeltaManager] Extracting conflation key", {
        provided: conflationKey,
        topLevel: parsed.conflation_key,
        dataField: data?.__conflation_key,
        final: finalConflationKey,
      });
      const sanitizedBase = JSON.stringify({
        event: parsed.event,
        channel: parsed.channel,
        data: parsed.data,
        // Explicitly exclude: sequence, conflation_key
      });
      if (finalConflationKey !== void 0 && !channelState.conflationKey) {
        console.log("[DeltaManager] Initializing conflation for channel", {
          channel,
          conflationKey: finalConflationKey,
        });
        channelState.conflationKey = "enabled";
      }
      if (channelState.conflationKey && finalConflationKey !== void 0) {
        channelState.updateConflationCache(
          finalConflationKey,
          sanitizedBase,
          sequence,
        );
        console.log("[DeltaManager] Stored full message (conflation)", {
          channel,
          conflationKey: finalConflationKey,
          sequence,
          size: messageSize,
          cacheSize:
            channelState.conflationCaches.get(finalConflationKey)?.length,
        });
        this.log("Stored full message (conflation)", {
          channel,
          conflationKey: finalConflationKey,
          sequence,
          size: messageSize,
        });
      } else {
        channelState.setBase(sanitizedBase, sequence);
        console.log("[DeltaManager] Stored full message (non-conflation)", {
          channel,
          sequence,
          size: messageSize,
        });
        this.log("Stored full message", {
          channel,
          sequence,
          size: messageSize,
          basePreview: this.options.debug ? rawMessage.slice(0, 120) : void 0,
        });
      }
    } catch (e) {
      console.error("[DeltaManager] Failed to parse message", e);
      channelState.setBase(rawMessage, sequence);
    }
    channelState.recordFullMessage();
    this.stats.totalMessages++;
    this.stats.fullMessages++;
    this.stats.totalBytesWithoutCompression += messageSize;
    this.stats.totalBytesWithCompression += messageSize;
    this.updateStats();
  }
  /**
   * Request resync for a channel
   */
  requestResync(channel) {
    console.error(
      "[DeltaManager] ⚠️ RESYNC REQUESTED - Deleting channel state",
      {
        channel,
        reason: new Error().stack,
      },
    );
    this.log("Requesting resync for channel", channel);
    this.sendEventCallback("pusher:delta_sync_error", { channel });
    this.channelStates.delete(channel);
    console.error(
      "[DeltaManager] Channel state deleted, totalStates now:",
      this.channelStates.size,
    );
  }
  /**
   * Update and emit stats
   */
  updateStats() {
    if (this.options.onStats) {
      this.options.onStats(this.getStats());
    }
  }
  /**
   * Get current statistics
   */
  getStats() {
    const bandwidthSaved =
      this.stats.totalBytesWithoutCompression -
      this.stats.totalBytesWithCompression;
    const bandwidthSavedPercent =
      this.stats.totalBytesWithoutCompression > 0
        ? (bandwidthSaved / this.stats.totalBytesWithoutCompression) * 100
        : 0;
    const channelStats = Array.from(this.channelStates.values()).map((state) =>
      state.getStats(),
    );
    return {
      ...this.stats,
      bandwidthSaved,
      bandwidthSavedPercent,
      channelCount: this.channelStates.size,
      channels: channelStats,
    };
  }
  /**
   * Reset statistics
   */
  resetStats() {
    this.stats = {
      totalMessages: 0,
      deltaMessages: 0,
      fullMessages: 0,
      totalBytesWithoutCompression: 0,
      totalBytesWithCompression: 0,
      errors: 0,
    };
    this.updateStats();
  }
  /**
   * Clear channel state
   */
  clearChannelState(channel) {
    if (channel) {
      this.channelStates.delete(channel);
    } else {
      this.channelStates.clear();
    }
  }
  /**
   * Check if delta compression is enabled
   */
  isEnabled() {
    return this.enabled;
  }
  /**
   * Get available algorithms
   */
  getAvailableAlgorithms() {
    return this.availableAlgorithms;
  }
  /**
   * Log message (if debug enabled)
   */
  log(...args) {
    if (this.options.debug) {
      Logger$1.debug("[DeltaCompression]", ...args);
    }
  }
  /**
   * Log error
   */
  error(...args) {
    Logger$1.error("[DeltaCompression]", ...args);
    if (this.options.onError) {
      this.options.onError(args);
    }
  }
}
const _Pusher = class _Pusher {
  static get logToConsole() {
    return this._logToConsole;
  }
  static set logToConsole(value) {
    this._logToConsole = value;
    setLoggerConfig({ logToConsole: value, log: this._log });
  }
  static get log() {
    return this._log;
  }
  static set log(fn) {
    this._log = fn;
    setLoggerConfig({ logToConsole: this._logToConsole, log: fn });
  }
  static ready() {
    _Pusher.isReady = true;
    for (var i = 0, l = _Pusher.instances.length; i < l; i++) {
      _Pusher.instances[i].connect();
    }
  }
  static getClientFeatures() {
    return keys(
      filterObject({ ws: Runtime.Transports.ws }, function (t) {
        return t.isSupported({});
      }),
    );
  }
  constructor(app_key, options) {
    checkAppKey(app_key);
    validateOptions(options);
    this.key = app_key;
    this.config = getConfig(options, this);
    this.channels = Factory.createChannels();
    this.global_emitter = new Dispatcher();
    this.sessionID = Runtime.randomInt(1e9);
    this.timeline = new Timeline(this.key, this.sessionID, {
      cluster: this.config.cluster,
      features: _Pusher.getClientFeatures(),
      params: this.config.timelineParams || {},
      limit: 50,
      level: TimelineLevel.INFO,
      version: Defaults.VERSION,
    });
    if (this.config.enableStats) {
      this.timelineSender = Factory.createTimelineSender(this.timeline, {
        host: this.config.statsHost,
        path: "/timeline/v2/" + Runtime.TimelineTransport.name,
      });
    }
    var getStrategy = (options2) => {
      return Runtime.getDefaultStrategy(this.config, options2, defineTransport);
    };
    this.connection = Factory.createConnectionManager(this.key, {
      getStrategy,
      timeline: this.timeline,
      activityTimeout: this.config.activityTimeout,
      pongTimeout: this.config.pongTimeout,
      unavailableTimeout: this.config.unavailableTimeout,
      useTLS: Boolean(this.config.useTLS),
    });
    if (options.deltaCompression !== void 0) {
      this.deltaCompression = new DeltaCompressionManager(
        options.deltaCompression || {},
        (event, data) => this.send_event(event, data),
      );
    }
    this.connection.bind("connected", () => {
      this.subscribeAll();
      if (this.timelineSender) {
        this.timelineSender.send(this.connection.isUsingTLS());
      }
      if (this.deltaCompression && options.deltaCompression?.enabled === true) {
        this.deltaCompression.enable();
      }
    });
    this.connection.bind("message", (event) => {
      console.log("[Pusher] Message handler called", {
        event: event.event,
        channel: event.channel,
      });
      var eventName = event.event;
      var internal = eventName.indexOf("pusher_internal:") === 0;
      if (this.deltaCompression) {
        if (eventName === "pusher:delta_compression_enabled") {
          this.deltaCompression.handleEnabled(event.data);
        } else if (eventName === "pusher:delta_cache_sync" && event.channel) {
          this.deltaCompression.handleCacheSync(event.channel, event.data);
          return;
        } else if (eventName === "pusher:delta" && event.channel) {
          const reconstructedEvent = this.deltaCompression.handleDeltaMessage(
            event.channel,
            event.data,
          );
          if (reconstructedEvent) {
            var channel = this.channel(reconstructedEvent.channel);
            if (channel) {
              channel.handleEvent(reconstructedEvent);
            }
            this.global_emitter.emit(
              reconstructedEvent.event,
              reconstructedEvent.data,
            );
          }
          return;
        }
      }
      if (event.channel) {
        var channel = this.channel(event.channel);
        if (channel) {
          channel.handleEvent(event);
        }
        if (
          this.deltaCompression &&
          event.event &&
          !event.event.startsWith("pusher:") &&
          !event.event.startsWith("pusher_internal:")
        ) {
          let sequence;
          if (typeof event.sequence === "number") {
            sequence = event.sequence;
          }
          let conflationKey;
          if (typeof event.conflation_key === "string") {
            conflationKey = event.conflation_key;
          }
          let fullMessage = event.rawMessage || "";
          if (fullMessage && sequence !== void 0) {
            try {
              const parsed = JSON.parse(fullMessage);
              delete parsed.sequence;
              delete parsed.conflation_key;
              delete parsed.rawMessage;
              fullMessage = JSON.stringify(parsed);
            } catch (e) {
              console.error(
                "[Pusher] Failed to strip sequence from raw message:",
                e,
              );
            }
          }
          this.deltaCompression.handleFullMessage(
            event.channel,
            fullMessage,
            sequence,
            conflationKey,
          );
        }
      }
      if (!internal) {
        this.global_emitter.emit(event.event, event.data);
      }
    });
    this.connection.bind("connecting", () => {
      this.channels.disconnect();
    });
    this.connection.bind("disconnected", () => {
      this.channels.disconnect();
    });
    this.connection.bind("error", (err) => {
      Logger$1.warn(err);
    });
    _Pusher.instances.push(this);
    this.timeline.info({ instances: _Pusher.instances.length });
    this.user = new UserFacade(this);
    if (_Pusher.isReady) {
      this.connect();
    }
  }
  channel(name) {
    return this.channels.find(name);
  }
  allChannels() {
    return this.channels.all();
  }
  connect() {
    this.connection.connect();
    if (this.timelineSender) {
      if (!this.timelineSenderTimer) {
        var usingTLS = this.connection.isUsingTLS();
        var timelineSender = this.timelineSender;
        this.timelineSenderTimer = new PeriodicTimer(6e4, function () {
          timelineSender.send(usingTLS);
        });
      }
    }
  }
  disconnect() {
    this.connection.disconnect();
    if (this.timelineSenderTimer) {
      this.timelineSenderTimer.ensureAborted();
      this.timelineSenderTimer = null;
    }
  }
  bind(event_name, callback, context) {
    this.global_emitter.bind(event_name, callback, context);
    return this;
  }
  unbind(event_name, callback, context) {
    this.global_emitter.unbind(event_name, callback, context);
    return this;
  }
  bind_global(callback) {
    this.global_emitter.bind_global(callback);
    return this;
  }
  unbind_global(callback) {
    this.global_emitter.unbind_global(callback);
    return this;
  }
  unbind_all(callback) {
    this.global_emitter.unbind_all();
    return this;
  }
  subscribeAll() {
    var channelName;
    for (channelName in this.channels.channels) {
      if (this.channels.channels.hasOwnProperty(channelName)) {
        this.subscribe(channelName);
      }
    }
  }
  subscribe(channel_name, tagsFilter) {
    var channel = this.channels.add(channel_name, this);
    if (tagsFilter) {
      channel.tagsFilter = tagsFilter;
    }
    if (channel.subscriptionPending && channel.subscriptionCancelled) {
      channel.reinstateSubscription();
    } else if (
      !channel.subscriptionPending &&
      this.connection.state === "connected"
    ) {
      channel.subscribe();
    }
    return channel;
  }
  unsubscribe(channel_name) {
    var channel = this.channels.find(channel_name);
    if (channel && channel.subscriptionPending) {
      channel.cancelSubscription();
    } else {
      channel = this.channels.remove(channel_name);
      if (channel && channel.subscribed) {
        channel.unsubscribe();
      }
      // Clear delta compression state for this channel when unsubscribing
      // This ensures that if the client resubscribes, they start fresh
      if (this.deltaManager) {
        this.deltaManager.clearChannelState(channel_name);
      }
    }
  }
  send_event(event_name, data, channel) {
    return this.connection.send_event(event_name, data, channel);
  }
  shouldUseTLS() {
    return this.config.useTLS;
  }
  signin() {
    this.user.signin();
  }
  /**
   * Get delta compression statistics
   * @returns {DeltaStats} Statistics about delta compression bandwidth savings
   */
  getDeltaStats() {
    if (!this.deltaCompression) {
      return null;
    }
    return this.deltaCompression.getStats();
  }
  /**
   * Reset delta compression statistics
   */
  resetDeltaStats() {
    if (this.deltaCompression) {
      this.deltaCompression.resetStats();
    }
  }
};
_Pusher.instances = [];
_Pusher.isReady = false;
_Pusher._logToConsole = false;
_Pusher.Runtime = Runtime;
_Pusher.ScriptReceivers = Runtime.ScriptReceivers;
_Pusher.DependenciesReceivers = Runtime.DependenciesReceivers;
_Pusher.auth_callbacks = Runtime.auth_callbacks;
let Pusher = _Pusher;
function checkAppKey(key) {
  if (key === null || key === void 0) {
    throw "You must pass your app key when you instantiate Pusher.";
  }
}
Runtime.setup(Pusher);
export { Pusher as default };
//# sourceMappingURL=pusher.mjs.map
