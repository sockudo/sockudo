import { access } from "node:fs/promises";
import { dirname, join } from "node:path";
import { spawn } from "node:child_process";
import { describe, expect, it } from "vitest";

import { serverRoot } from "./golden-fixtures.js";
import { SERVER_CONFORMANCE_RUNNER } from "./server-pin.js";

const hasLiveServer =
  process.env.SOCKUDO_URL !== undefined ||
  process.env.SOCKUDO_BASE_URL !== undefined;

describe("golden transcript produce", () => {
  it.skipIf(!hasLiveServer)(
    "matches server golden transcripts through the live raw-wire harness",
    async () => {
      const runner = join(serverRoot(), SERVER_CONFORMANCE_RUNNER);
      await access(runner);
      const result = await runNode(runner, {
        ...process.env,
        SOCKUDO_BASE_URL:
          process.env.SOCKUDO_BASE_URL ??
          process.env.SOCKUDO_URL ??
          "http://127.0.0.1:6001",
        SOCKUDO_APP_ID: process.env.SOCKUDO_APP_ID ?? "app-id",
        SOCKUDO_APP_KEY: process.env.SOCKUDO_APP_KEY ?? "app-key",
        SOCKUDO_APP_SECRET: process.env.SOCKUDO_APP_SECRET ?? "app-secret",
      });
      expect(result).toContain("AI conformance passed");
    },
  );

  it("validates server golden fixtures offline when no live server is configured", async () => {
    const runner = join(serverRoot(), SERVER_CONFORMANCE_RUNNER);
    await access(runner);
    const result = await runNode(runner, {
      ...process.env,
      AIT_CONFORMANCE_OFFLINE: "1",
    });
    expect(result).toContain("offline fixture validation passed");
  });
});

function runNode(file: string, env: NodeJS.ProcessEnv): Promise<string> {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [file], {
      cwd: join(dirname(file), "../../.."),
      env,
      stdio: ["ignore", "pipe", "pipe"],
    });
    let output = "";
    child.stdout.on("data", (chunk: Buffer) => {
      output += chunk.toString("utf8");
    });
    child.stderr.on("data", (chunk: Buffer) => {
      output += chunk.toString("utf8");
    });
    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) {
        resolve(output);
      } else {
        reject(new Error(output));
      }
    });
  });
}
