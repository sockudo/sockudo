import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { spawn } from "node:child_process";

const root = process.cwd();
const temp = await mkdtemp(join(tmpdir(), "sockudo-ai-transport-release-"));
const registry = "http://127.0.0.1:4873";
const storage = join(temp, "storage");
const config = join(temp, "verdaccio.yml");
const npmrc = join(temp, ".npmrc");
const packDir = join(temp, "pack");
const appDir = join(temp, "app");
let server;

try {
  await writeFile(
    config,
    [
      `storage: ${storage}`,
      "auth:",
      "  htpasswd:",
      `    file: ${join(temp, "htpasswd")}`,
      "    max_users: 1000",
      "uplinks:",
      "  npmjs:",
      "    url: https://registry.npmjs.org/",
      "packages:",
      "  '@sockudo/*':",
      "    access: $all",
      "    publish: $all",
      "    unpublish: $all",
      "    proxy: npmjs",
      "  '**':",
      "    access: $all",
      "    publish: $all",
      "    unpublish: $all",
      "    proxy: npmjs",
      "logs: { type: stdout, format: pretty, level: error }",
      "",
    ].join("\n"),
  );

  await run("pnpm", ["build"]);
  server = spawn("pnpm", ["exec", "verdaccio", "--config", config, "--listen", registry], {
    cwd: root,
    stdio: ["ignore", "pipe", "pipe"],
  });
  await waitForRegistry(registry);
  const token = await createVerdaccioUser(registry);
  await writeFile(npmrc, `//127.0.0.1:4873/:_authToken=${token}\nregistry=${registry}\n`);

  await run("mkdir", ["-p", packDir, appDir]);
  await run("npm", ["pack", "--pack-destination", packDir]);
  const tarball = await findPackedTarball(packDir);
  await run("npm", [
    "publish",
    tarball,
    "--registry",
    registry,
    "--userconfig",
    npmrc,
    "--access",
    "public",
    "--provenance=false",
  ]);

  await writeFile(
    join(appDir, "package.json"),
    JSON.stringify(
      {
        private: true,
        type: "module",
        dependencies: {
          "@sockudo/ai-transport": "2.1.0",
          "@sockudo/client": "^2.1.0",
          ai: "^6",
          react: "^19",
          "react-dom": "^19",
        },
      },
      null,
      2,
    ),
  );
  await run("npm", ["install", "--registry", registry, "--ignore-scripts", "--legacy-peer-deps"], {
    cwd: appDir,
  });
  await writeFile(
    join(appDir, "smoke.mjs"),
    [
      "import { createClientTransport, createMockClient } from '@sockudo/ai-transport';",
      "import { TransportProvider } from '@sockudo/ai-transport/react';",
      "import { createChatTransport } from '@sockudo/ai-transport/vercel';",
      "import { ChatTransportProvider } from '@sockudo/ai-transport/vercel/react';",
      "if (typeof createClientTransport !== 'function') throw new Error('missing core export');",
      "if (typeof createMockClient !== 'function') throw new Error('missing mock export');",
      "if (typeof TransportProvider !== 'function') throw new Error('missing react export');",
      "if (typeof createChatTransport !== 'function') throw new Error('missing vercel export');",
      "if (typeof ChatTransportProvider !== 'function') throw new Error('missing vercel/react export');",
      "",
    ].join("\n"),
  );
  await run("node", ["smoke.mjs"], { cwd: appDir });
  console.log(`Dry-run release verified against ${registry}`);
} finally {
  if (server) {
    server.kill("SIGTERM");
  }
  await rm(temp, { recursive: true, force: true });
}

function run(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: options.cwd ?? root,
      stdio: options.input === undefined ? "inherit" : ["pipe", "inherit", "inherit"],
    });
    if (options.input !== undefined) {
      child.stdin.end(options.input);
    }
    child.on("error", reject);
    child.on("exit", (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`${command} ${args.join(" ")} exited with ${String(code)}`));
      }
    });
  });
}

async function waitForRegistry(url) {
  const deadline = Date.now() + 20_000;
  while (Date.now() < deadline) {
    try {
      const response = await globalThis.fetch(`${url}/-/ping`);
      if (response.ok) {
        return;
      }
    } catch {
      // Retry until Verdaccio binds.
    }
    await new Promise((resolve) => globalThis.setTimeout(resolve, 250));
  }
  throw new Error("Verdaccio did not become ready");
}

async function createVerdaccioUser(url) {
  const response = await globalThis.fetch(`${url}/-/user/org.couchdb.user:sockudo`, {
    method: "PUT",
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify({
      name: "sockudo",
      password: "sockudo",
      email: "release@example.com",
      type: "user",
      roles: [],
      date: new Date().toISOString(),
    }),
  });
  if (!response.ok) {
    throw new Error(`Verdaccio user creation failed with ${String(response.status)}`);
  }
  const payload = await response.json();
  if (typeof payload.token !== "string" || payload.token.length === 0) {
    throw new Error("Verdaccio did not return an auth token");
  }
  return payload.token;
}

async function findPackedTarball(directory) {
  const { readdir } = await import("node:fs/promises");
  const files = await readdir(directory);
  const tarball = files.find((file) => file.endsWith(".tgz"));
  if (!tarball) {
    throw new Error("npm pack did not produce a tarball");
  }
  return join(directory, tarball);
}
