import { describe, expect, it } from "vitest";

import { version } from "./version.js";

describe("version", () => {
  it("exposes a deterministic placeholder outside the build", () => {
    expect(version).toBe("0.0.0-dev");
  });
});
