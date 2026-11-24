import { afterEach, describe, expect, it, vi } from "vitest";
import UserFacade from "../src/core/user";
import { HTTPAuthError } from "../src/core/errors";
import { setProtocolVersion } from "../src/core/protocol_prefix";

function makeFacade(authError: Error) {
  const userAuthenticator = vi.fn((_params, callback) => callback(authError));
  const stub = {
    connection: {
      bind: vi.fn(),
      state: "connected",
      socket_id: "1.1",
    },
    config: { userAuthenticator },
    send_event: vi.fn(),
  };
  return { facade: new UserFacade(stub as any), userAuthenticator };
}

describe("user signin errors", () => {
  afterEach(() => {
    setProtocolVersion(7);
  });

  it("emits pusher:signin_error under Protocol V1", () => {
    const { facade } = makeFacade(new Error("signin failed"));
    const handler = vi.fn();
    facade.bind("pusher:signin_error", handler);

    facade.signin();

    expect(handler).toHaveBeenCalledWith({ type: "AuthError", error: "signin failed" });
  });

  it("emits sockudo:signin_error under Protocol V2", () => {
    setProtocolVersion(2);
    const { facade } = makeFacade(new Error("signin failed"));
    const handler = vi.fn();
    facade.bind("sockudo:signin_error", handler);

    facade.signin();

    expect(handler).toHaveBeenCalledWith({ type: "AuthError", error: "signin failed" });
  });

  it("includes the HTTP status for HTTPAuthError", () => {
    const { facade } = makeFacade(new HTTPAuthError(403, "forbidden"));
    const handler = vi.fn();
    facade.bind("pusher:signin_error", handler);

    facade.signin();

    expect(handler).toHaveBeenCalledWith({ type: "AuthError", error: "forbidden", status: 403 });
  });

  it("still cleans up when a signin_error listener throws", async () => {
    const { facade } = makeFacade(new Error("signin failed"));
    facade.bind("pusher:signin_error", () => {
      throw new Error("listener exploded");
    });

    expect(() => facade.signin()).toThrow("listener exploded");

    const settled = await Promise.race([
      facade.signinDonePromise.then(() => "resolved"),
      new Promise<string>((resolve) => setTimeout(() => resolve("pending"), 0)),
    ]);
    expect(settled).toBe("resolved");
  });
});
