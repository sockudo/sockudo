import { expect, test } from "bun:test";
import { SignJWT } from "jose";
import { verifySession } from "./session.ts";

test("rejects a token signed with the removed public fallback secret", async () => {
  const token = await new SignJWT({
    userId: "attacker",
    email: "attacker@example.com",
    name: "Attacker",
    role: "admin",
    credentialVersion: "attacker-controlled",
  })
    .setProtectedHeader({ alg: "HS256" })
    .setSubject("attacker")
    .setIssuer("sockudo-dashboard")
    .setAudience("sockudo-dashboard-api")
    .setIssuedAt()
    .setExpirationTime("8h")
    .sign(new TextEncoder().encode("dev-only-change-me-in-production"));

  expect(await verifySession(token)).toBeNull();
});

test("rejects unsigned JWTs", async () => {
  const header = Buffer.from(JSON.stringify({ alg: "none", typ: "JWT" })).toString(
    "base64url",
  );
  const payload = Buffer.from(
    JSON.stringify({ sub: "attacker", role: "admin" }),
  ).toString("base64url");

  expect(await verifySession(`${header}.${payload}.`)).toBeNull();
});
