import { SignJWT, jwtVerify } from "jose";
import { createHmac, timingSafeEqual } from "node:crypto";
import { config } from "../config.ts";
import type { UserRole } from "../types/user.ts";

const COOKIE_NAME = "sockudo_dashboard_session";
const encoder = new TextEncoder();
const secret = encoder.encode(config.sessionSecret);

export interface SessionPayload {
  sub: string;
  userId: string;
  email: string;
  name: string;
  role: UserRole;
  credentialVersion: string;
}

export async function createSession(user: {
  id: string;
  email: string;
  name: string;
  role: UserRole;
  passwordHash: string;
}): Promise<string> {
  return new SignJWT({
    email: user.email,
    name: user.name,
    role: user.role,
    userId: user.id,
    credentialVersion: credentialVersion(user.passwordHash),
  })
    .setProtectedHeader({ alg: "HS256" })
    .setSubject(user.id)
    .setIssuer("sockudo-dashboard")
    .setAudience("sockudo-dashboard-api")
    .setIssuedAt()
    .setExpirationTime("8h")
    .sign(secret);
}

export async function verifySession(
  token: string,
): Promise<SessionPayload | null> {
  try {
    const { payload } = await jwtVerify(token, secret, {
      algorithms: ["HS256"],
      issuer: "sockudo-dashboard",
      audience: "sockudo-dashboard-api",
    });
    const claims = payload as {
      sub?: string;
      email?: string;
      name?: string;
      role?: UserRole;
      userId?: string;
      credentialVersion?: string;
    };
    const userId = String(claims.userId ?? claims.sub ?? "");
    if (
      !userId ||
      claims.sub !== userId ||
      typeof claims.credentialVersion !== "string" ||
      (claims.role !== "admin" && claims.role !== "operator")
    ) {
      return null;
    }
    return {
      sub: userId,
      userId,
      email: String(claims.email ?? ""),
      name: String(claims.name ?? ""),
      role: claims.role,
      credentialVersion: claims.credentialVersion,
    };
  } catch {
    return null;
  }
}

export function sessionMatchesPasswordHash(
  session: SessionPayload,
  passwordHash: string,
): boolean {
  const actual = Buffer.from(session.credentialVersion, "utf8");
  const expected = Buffer.from(credentialVersion(passwordHash), "utf8");
  return actual.length === expected.length && timingSafeEqual(actual, expected);
}

function credentialVersion(passwordHash: string): string {
  return createHmac("sha256", secret)
    .update(passwordHash, "utf8")
    .digest("base64url");
}

export function sessionCookie(token: string): string {
  const secure = process.env.NODE_ENV === "production" ? "; Secure" : "";
  return `${COOKIE_NAME}=${token}; Path=/; HttpOnly; SameSite=Lax; Max-Age=28800${secure}`;
}

export function clearSessionCookie(): string {
  return `${COOKIE_NAME}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0`;
}

export function readSessionCookie(
  cookieHeader: string | undefined,
): string | null {
  if (!cookieHeader) return null;
  for (const part of cookieHeader.split(";")) {
    const [name, ...rest] = part.trim().split("=");
    if (name === COOKIE_NAME) return rest.join("=");
  }
  return null;
}
