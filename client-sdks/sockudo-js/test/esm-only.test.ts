import { describe, expect, it } from "vitest";
import { existsSync, readFileSync } from "node:fs";
import { dirname, join, relative, resolve, sep } from "node:path";
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
    expect(packageJson.exports["./nativescript"].default).toBe("./nativescript/index.js");
    expect(JSON.stringify(packageJson.exports).includes(".cjs")).toBe(false);
    expect(JSON.stringify(packageJson.exports).includes('"require"')).toBe(false);
  });

  it("pins entrypoint wrappers to the bundles the build emits", () => {
    const root = join(currentDir, "..");

    // One entry per published wrapper. The target is the exact file each vite
    // config writes, so a renamed bundle or a wrong-but-existing target fails.
    const expectedBundles: Record<string, string> = {
      "node.js": "dist/node/sockudo.js",
      "with-encryption/index.js": "dist/web/sockudo-with-encryption.mjs",
      "worker/index.js": "dist/worker/sockudo.worker.js",
      "worker/with-encryption/index.js": "dist/worker/sockudo-with-encryption.worker.js",
      "react/index.js": "dist/web/react.mjs",
      "vue/index.js": "dist/web/vue.mjs",
      "react-native/index.js": "dist/react-native/sockudo.js",
      "nativescript/index.js": "dist/nativescript/sockudo.js",
    };

    for (const [file, bundle] of Object.entries(expectedBundles)) {
      const source = readFileSync(join(root, file), "utf8");
      const targets = [...source.matchAll(/from\s+"([^"]+)"/g)].map((match) =>
        relative(root, resolve(root, dirname(file), match[1]))
          .split(sep)
          .join("/"),
      );

      expect(targets.length, file).toBeGreaterThan(0);
      expect(new Set(targets), file).toEqual(new Set([bundle]));
      // Only assert what this build produced: `build` is web-only, `build:all`
      // covers every variant, and both leave their own dist/<dir> behind.
      const target = join(root, bundle);
      if (existsSync(dirname(target))) expect(existsSync(target), bundle).toBe(true);
    }
  });
});
