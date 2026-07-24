const MINIMUM_SECRET_BYTES = 32;
const TEST_ONLY_SECRET = "test-only-dashboard-session-secret-32-bytes";
const DOCUMENTED_PLACEHOLDERS = new Set([
  "dev-only-change-me-in-production",
  "change-me-in-production",
  "change-me-use-openssl-rand-base64-32",
]);

export function resolveSessionSecret(
  env: Record<string, string | undefined>,
): string {
  const value = env.DASHBOARD_SESSION_SECRET;
  if (!value) {
    throw new Error(
      "DASHBOARD_SESSION_SECRET is required; generate one with `openssl rand -base64 32`",
    );
  }
  if (new TextEncoder().encode(value).byteLength < MINIMUM_SECRET_BYTES) {
    throw new Error("DASHBOARD_SESSION_SECRET must be at least 32 bytes");
  }
  if (DOCUMENTED_PLACEHOLDERS.has(value)) {
    throw new Error("DASHBOARD_SESSION_SECRET must not use a public placeholder");
  }
  if (value === TEST_ONLY_SECRET && env.NODE_ENV !== "test") {
    throw new Error("The dashboard test session secret is only valid in NODE_ENV=test");
  }
  return value;
}
