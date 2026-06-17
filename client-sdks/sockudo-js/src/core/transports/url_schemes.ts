import Defaults from "../defaults";
import { default as URLScheme, URLSchemeParams } from "./url_scheme";
import { protocolVersion } from "../protocol_prefix";

function getGenericURL(
  baseScheme: string,
  params: URLSchemeParams,
  path: string,
): string {
  const scheme = baseScheme + (params.useTLS ? "s" : "");
  const host = params.useTLS ? params.hostTLS : params.hostNonTLS;
  return scheme + "://" + host + path;
}

function getGenericPath(key: string, queryString?: string): string {
  const path = "/app/" + key;
  const query =
    "?protocol=" +
    protocolVersion() +
    "&client=js" +
    "&version=" +
    Defaults.VERSION +
    (queryString ? "&" + queryString : "");
  return path + query;
}

export const ws: URLScheme = {
  getInitial: function (key: string, params: URLSchemeParams): string {
    let queryString = "flash=false";
    if (params.echoMessages === false) {
      queryString += "&echo_messages=false";
    }
    if (protocolVersion() === 2 && params.wireFormat) {
      queryString += "&format=" + encodeURIComponent(params.wireFormat);
    }
    const path = (params.httpPath || "") + getGenericPath(key, queryString);
    return getGenericURL("ws", params, path);
  },
};

export const http: URLScheme = {
  getInitial: function (key: string, params: URLSchemeParams): string {
    const path = (params.httpPath || "/sockudo") + getGenericPath(key);
    return getGenericURL("http", params, path);
  },
};

export const sockjs: URLScheme = {
  getInitial: function (key: string, params: URLSchemeParams): string {
    return getGenericURL("http", params, params.httpPath || "/sockudo");
  },
  getPath: function (key: string, _params: URLSchemeParams): string {
    return getGenericPath(key);
  },
};
