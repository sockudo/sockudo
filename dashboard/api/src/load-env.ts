import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";

const envFiles = [
  join(import.meta.dir, "..", ".env"),
  join(import.meta.dir, "..", "..", ".env"),
];

function parseEnvValue(raw: string): string {
  const value = raw.trim();
  if (
    (value.startsWith('"') && value.endsWith('"')) ||
    (value.startsWith("'") && value.endsWith("'"))
  ) {
    return value.slice(1, -1);
  }
  return value;
}

function loadDashboardEnv(): void {
  for (const file of envFiles) {
    if (!existsSync(file)) continue;
    for (const line of readFileSync(file, "utf8").split("\n")) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#")) continue;
      const idx = trimmed.indexOf("=");
      if (idx <= 0) continue;
      const key = trimmed.slice(0, idx).trim();
      if (!key || process.env[key] !== undefined) continue;
      process.env[key] = parseEnvValue(trimmed.slice(idx + 1));
    }
  }
}

loadDashboardEnv();
