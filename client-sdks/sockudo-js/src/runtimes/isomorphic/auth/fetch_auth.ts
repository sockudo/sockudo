import AbstractRuntime from "runtimes/interface";
import { AuthTransport } from "core/auth/auth_transports";
import {
  AuthRequestType,
  AuthTransportCallback,
  AuthHeaders,
  InternalAuthOptions,
} from "core/auth/options";
import { HTTPAuthError } from "core/errors";

const FORM_URLENCODED_CONTENT_TYPE = "application/x-www-form-urlencoded";

const applyHeaders = (headers: Headers, values?: AuthHeaders): void => {
  if (!values) {
    return;
  }

  for (const headerName in values) {
    headers.set(headerName, values[headerName]);
  }
};

const fetchAuth: AuthTransport = (
  _context: AbstractRuntime,
  query: string,
  authOptions: InternalAuthOptions,
  authRequestType: AuthRequestType,
  callback: AuthTransportCallback,
) => {
  const headers = new Headers();
  headers.set("Content-Type", FORM_URLENCODED_CONTENT_TYPE);

  applyHeaders(headers, authOptions.headers);
  applyHeaders(headers, authOptions.headersProvider?.());

  const request = new Request(authOptions.endpoint, {
    method: "POST",
    headers,
    body: query,
    credentials: "same-origin",
  });

  void fetch(request)
    .then(async (response) => {
      if (response.status !== 200) {
        throw new HTTPAuthError(
          response.status,
          `Could not get ${authRequestType.toString()} info from your auth endpoint, status: ${response.status}`,
        );
      }

      const body = await response.text();
      try {
        callback(null, JSON.parse(body));
      } catch {
        throw new HTTPAuthError(
          200,
          `JSON returned from ${authRequestType.toString()} endpoint was invalid, yet status code was 200. Data was: ${body}`,
        );
      }
    })
    .catch((error: unknown) => {
      callback(error as Error, null);
    });
};

export default fetchAuth;
