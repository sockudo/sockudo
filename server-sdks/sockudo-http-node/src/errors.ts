export class RequestError extends Error {
  url: string;
  error?: Error;
  status?: number;
  statusCode?: number;
  body?: string;

  constructor(
    message: string,
    url: string,
    error?: Error,
    status?: number,
    body?: string,
  ) {
    super(message);
    this.name = "SockudoRequestError";
    this.url = url;
    this.error = error;
    this.status = status;
    this.statusCode = status;
    this.body = body;
  }
}

export class WebHookError extends Error {
  contentType: string | undefined;
  body: string;
  signature: string | undefined;

  constructor(
    message: string,
    contentType: string | undefined,
    body: string,
    signature: string | undefined,
  ) {
    super(message);
    this.name = "SockudoWebHookError";
    this.contentType = contentType;
    this.body = body;
    this.signature = signature;
  }
}
