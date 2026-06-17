import { describe, expect, it } from "vitest";

const hasSockudoServer =
  typeof process.env.SOCKUDO_URL === "string" &&
  typeof process.env.SOCKUDO_APP_KEY === "string" &&
  typeof process.env.SOCKUDO_APP_ID === "string" &&
  typeof process.env.SOCKUDO_APP_SECRET === "string";

const describeIntegration = hasSockudoServer ? describe : describe.skip;

describeIntegration("Sockudo AI Transport integration environment", () => {
  it("is wired for a real Sockudo server", () => {
    expect(process.env.SOCKUDO_URL).toMatch(/^https?:\/\//u);
    expect(process.env.SOCKUDO_APP_KEY).not.toHaveLength(0);
  });
});
