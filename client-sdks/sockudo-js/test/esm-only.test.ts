import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const currentDir = dirname(fileURLToPath(import.meta.url));

describe("esm-only packaging", () => {
  it("does not expose CommonJS export paths", () => {
    const packageJson = JSON.parse(
      readFileSync(join(currentDir, "..", "package.json"), "utf8"),
    ) as Record<string, any>;

    expect(packageJson.type).toBe("module");
    expect(packageJson.main).toBe("dist/node/sockudo.js");
    expect(packageJson.browser).toBe("dist/web/sockudo.mjs");
    expect(packageJson["react-native"]).toBe("dist/react-native/sockudo.js");
    expect(packageJson.nativescript).toBe("dist/nativescript/sockudo.js");
    expect(packageJson.exports["./react"].default).toBe("./react/index.js");
    expect(packageJson.exports["./vue"].default).toBe("./vue/index.js");
    expect(packageJson.exports["./nativescript"].default).toBe(
      "./nativescript/index.js",
    );
    expect(JSON.stringify(packageJson.exports).includes(".cjs")).toBe(false);
    expect(JSON.stringify(packageJson.exports).includes('"require"')).toBe(
      false,
    );
  });
});
