import { describe, expect, it } from "vitest";

import {
  ErrorCode,
  ErrorInfo,
  errorInfoIs,
  formatErrorMessage,
  statusCodeForErrorCode,
  toErrorInfo,
} from "./errors.js";

describe("ErrorInfo", () => {
  it("derives status codes for every SDK error code", () => {
    expect(statusCodeForErrorCode(ErrorCode.BadRequest)).toBe(400);
    expect(statusCodeForErrorCode(ErrorCode.InvalidArgument)).toBe(400);
    expect(statusCodeForErrorCode(ErrorCode.TokenExpired)).toBe(401);
    expect(statusCodeForErrorCode(ErrorCode.InsufficientCapability)).toBe(401);
    expect(statusCodeForErrorCode(ErrorCode.EncoderRecoveryFailed)).toBe(500);
    expect(statusCodeForErrorCode(ErrorCode.TransportSubscriptionError)).toBe(500);
    expect(statusCodeForErrorCode(ErrorCode.CancelListenerError)).toBe(500);
    expect(statusCodeForErrorCode(ErrorCode.TurnLifecycleError)).toBe(500);
    expect(statusCodeForErrorCode(ErrorCode.TransportClosed)).toBe(500);
    expect(statusCodeForErrorCode(ErrorCode.TransportSendFailed)).toBe(500);
    expect(statusCodeForErrorCode(ErrorCode.ChannelContinuityLost)).toBe(500);
    expect(statusCodeForErrorCode(ErrorCode.ChannelNotReady)).toBe(500);
    expect(statusCodeForErrorCode(ErrorCode.StreamError)).toBe(500);
    expect(statusCodeForErrorCode(ErrorCode.TurnStartDeadlineExceeded)).toBe(504);
    expect(statusCodeForErrorCode(ErrorCode.InputEventNotFound)).toBe(504);
    expect(statusCodeForErrorCode(93002)).toBe(500);
  });

  it("stores code, status, cause, and detail", () => {
    const cause = new Error("root");
    const detail = { reason: "test" };
    const error = new ErrorInfo({
      code: ErrorCode.InvalidArgument,
      message: formatErrorMessage("send", "bad input"),
      cause,
      detail,
      statusCode: 418,
    });

    expect(error.name).toBe("ErrorInfo");
    expect(error.message).toBe("unable to send; bad input");
    expect(error.code).toBe(ErrorCode.InvalidArgument);
    expect(error.statusCode).toBe(418);
    expect(error.cause).toBe(cause);
    expect(error.detail).toBe(detail);
    expect(errorInfoIs(error, ErrorCode.InvalidArgument)).toBe(true);
    expect(errorInfoIs(error, ErrorCode.BadRequest)).toBe(false);
    expect(errorInfoIs(new Error("x"), ErrorCode.BadRequest)).toBe(false);
  });

  it("normalizes thrown values and capability HTTP failures", () => {
    const existing = new ErrorInfo({
      code: ErrorCode.TransportClosed,
      message: formatErrorMessage("publish", "closed"),
    });
    expect(toErrorInfo(existing, existing)).toBe(existing);

    expect(
      toErrorInfo(
        { data: { code: 40009, message: "too large", status: 413 } },
        {
          code: ErrorCode.TransportSendFailed,
          message: formatErrorMessage("publish", "failed"),
        },
      ),
    ).toMatchObject({ code: 40009, message: "too large", statusCode: 413 });

    expect(
      toErrorInfo(
        { data: { code: 40009, error: "too large" } },
        {
          code: ErrorCode.TransportSendFailed,
          message: formatErrorMessage("publish", "failed"),
        },
      ),
    ).toMatchObject({ code: 40009, message: "too large", statusCode: 400 });

    expect(
      toErrorInfo(
        { data: { code: 40003, message: "forbidden", status: 403 } },
        {
          code: ErrorCode.TransportSendFailed,
          message: formatErrorMessage("publish", "failed"),
        },
      ),
    ).toMatchObject({
      code: ErrorCode.InsufficientCapability,
      message: "forbidden",
      statusCode: 403,
    });

    expect(
      toErrorInfo(
        { code: "40142", error: "expired", statusCode: 401 },
        {
          code: ErrorCode.TransportSendFailed,
          message: formatErrorMessage("publish", "failed"),
        },
      ),
    ).toMatchObject({
      code: ErrorCode.InsufficientCapability,
      message: "expired",
      statusCode: 401,
    });

    expect(
      toErrorInfo(
        { code: "40142", message: "expired" },
        {
          code: ErrorCode.TransportSendFailed,
          message: formatErrorMessage("publish", "failed"),
        },
      ),
    ).toMatchObject({
      code: ErrorCode.TokenExpired,
      message: "expired",
      statusCode: 401,
    });

    expect(
      toErrorInfo(
        { code: "not-numeric", error: "bad" },
        {
          code: ErrorCode.TransportSendFailed,
          message: formatErrorMessage("publish", "failed"),
        },
      ),
    ).toMatchObject({
      code: ErrorCode.TransportSendFailed,
      message: "bad",
      statusCode: 500,
    });

    expect(
      toErrorInfo(
        { data: { code: "not-numeric", error: "denied", status: 403 } },
        {
          code: ErrorCode.TransportSendFailed,
          message: formatErrorMessage("publish", "failed"),
        },
      ),
    ).toMatchObject({
      code: ErrorCode.InsufficientCapability,
      message: "denied",
      statusCode: 403,
    });

    expect(
      toErrorInfo(
        { status: 403 },
        {
          code: ErrorCode.TransportSendFailed,
          message: formatErrorMessage("publish", "failed"),
        },
      ),
    ).toMatchObject({
      code: ErrorCode.InsufficientCapability,
      message: "unable to publish; failed",
      statusCode: 403,
    });

    expect(
      toErrorInfo(undefined, {
        code: ErrorCode.TransportSendFailed,
        message: formatErrorMessage("publish", "failed"),
      }),
    ).toMatchObject({
      code: ErrorCode.TransportSendFailed,
      message: "unable to publish; failed",
    });
  });
});
