import HTTPRequest from "core/http/http_request";
import RequestHooks from "core/http/request_hooks";
import Ajax from "core/http/ajax";

type XHRCtor = new () => Ajax;

const getXHRConstructor = (): XHRCtor | undefined => {
  return globalThis.XMLHttpRequest as unknown as XHRCtor | undefined;
};

const hooks: RequestHooks = {
  getRequest: function (socket: HTTPRequest): Ajax {
    const Constructor = getXHRConstructor();
    if (!Constructor) {
      throw new Error("XMLHttpRequest is not available in this environment.");
    }
    const xhr = new Constructor();
    xhr.onreadystatechange = xhr.onprogress = function () {
      switch (xhr.readyState) {
        case 3:
          if (xhr.responseText && xhr.responseText.length > 0) {
            socket.onChunk(xhr.status, xhr.responseText);
          }
          break;
        case 4:
          // this happens only on errors, never after calling close
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
  abortRequest: function (xhr: Ajax) {
    xhr.onreadystatechange = null;
    xhr.abort();
  },
};

export default hooks;
