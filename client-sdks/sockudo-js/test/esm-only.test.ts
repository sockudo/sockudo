import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  checkEntrypoints,
  collectWrapperFiles,
  importSpecifiers,
} from "../scripts/entrypoints.mjs";

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
    expect(packageJson.exports["./nativescript"].default).toBe("./nativescript/index.js");
    expect(JSON.stringify(packageJson.exports).includes(".cjs")).toBe(false);
    expect(JSON.stringify(packageJson.exports).includes('"require"')).toBe(false);
  });

  it("points every entrypoint wrapper at its shipped dist bundle", () => {
    const root = join(currentDir, "..");

    // Pinned per wrapper: an existing-but-wrong bundle (e.g. the web build for
    // react-native) must fail here, not only a stale pusher-* name.
    const expectedBundles: Record<string, string> = {
      "node.js": "./dist/node/sockudo.js",
      "with-encryption/index.js": "../dist/web/sockudo-with-encryption.mjs",
      "worker/index.js": "../dist/worker/sockudo.worker.js",
      "worker/with-encryption/index.js": "../../dist/worker/sockudo-with-encryption.worker.js",
      "react/index.js": "../dist/web/react.mjs",
      "vue/index.js": "../dist/web/vue.mjs",
      "react-native/index.js": "../dist/react-native/sockudo.js",
      "nativescript/index.js": "../dist/nativescript/sockudo.js",
    };

    expect(collectWrapperFiles(root)).toEqual(Object.keys(expectedBundles).sort());

    for (const [file, bundle] of Object.entries(expectedBundles)) {
      const specifiers = importSpecifiers(readFileSync(join(root, file), "utf8"));
      expect(specifiers.length, file).toBeGreaterThan(0);
      for (const specifier of specifiers) {
        expect(specifier, `${file} imports ${specifier}`).toBe(bundle);
      }
    }

    expect(checkEntrypoints(root).problems).toEqual([]);
  });
});
