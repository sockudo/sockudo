import fetch, { type Response } from "node-fetch";
import AbortController from "abort-controller";
import { RequestError } from "./errors";
import { getMD5, toOrderedArray, toOrderedArrayLowercaseKeys } from "./util";
import sockudoLibraryVersion = require("./version");
import type {
  RequestOptions,
  SignedQueryStringOptions,
  SignedRequest,
} from "./types";
import type SockudoConfig = require("./sockudo_config");
import type Token = require("./token");

const RESERVED_QUERY_KEYS: Record<string, boolean> = {
  auth_key: true,
  auth_timestamp: true,
  auth_version: true,
  auth_signature: true,
  body_md5: true,
};

export function send(
  config: SockudoConfig,
  options: RequestOptions & { method: string; body?: unknown },
): Promise<Response> {
  const method = options.method;
  const path = config.prefixPath(options.path);
  const body = options.body ? JSON.stringify(options.body) : undefined;

  const url = `${config.getBaseURL()}${path}?${createSignedQueryString(
    config.token,
    {
      method,
      path,
      params: options.params,
      body,
    },
  )}`;

  const headers: Record<string, string> = {
    "x-pusher-library": `sockudo-http-node ${sockudoLibraryVersion}`,
  };

  if (body) {
    headers["content-type"] = "application/json";
  }

  if (options.headers) {
    Object.assign(headers, options.headers);
  }

  let signal: unknown;
  let timeout: ReturnType<typeof setTimeout> | undefined;
  if (config.timeout) {
    const controller = new AbortController();
    timeout = setTimeout(() => controller.abort(), config.timeout);
    signal = controller.signal as unknown as AbortSignal;
  }

  return fetch(url, {
    method,
    body,
    headers,
    signal: signal as any,
    agent: config.agent,
  }).then(
    (res) => {
      if (timeout) {
        clearTimeout(timeout);
      }
      if (res.status >= 400) {
        return res.text().then((responseBody) => {
          throw new RequestError(
            `Unexpected status code ${res.status}`,
            url,
            undefined,
            res.status,
            responseBody,
          );
        });
      }
      return res;
    },
    (err: Error) => {
      if (timeout) {
        clearTimeout(timeout);
      }
      throw new RequestError("Request failed with an error", url, err);
    },
  );
}

export function createSignedQueryString(
  token: Token,
  request: SignedRequest | SignedQueryStringOptions,
): string {
  const timestamp = (Date.now() / 1000) | 0;
  const params: Record<string, unknown> = {
    auth_key: token.key,
    auth_timestamp: timestamp,
    auth_version: "1.0",
  };

  if (request.body) {
    params.body_md5 = getMD5(request.body);
  }

  if (request.params) {
    for (const key of Object.keys(request.params)) {
      if (RESERVED_QUERY_KEYS[key] !== undefined) {
        throw new Error(
          `${key} is a required parameter and cannot be overidden`,
        );
      }
      params[key] = request.params[key];
    }
  }

  const method = request.method.toUpperCase();
  const sortedKeyVal = toOrderedArray(params);
  const queryString = sortedKeyVal.join("&");
  const queryStringForSigning = toOrderedArrayLowercaseKeys(params).join("&");

  const signData = [method, request.path, queryStringForSigning].join("\n");

  return `${queryString}&auth_signature=${token.sign(signData)}`;
}
