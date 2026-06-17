import { readFile } from "node:fs/promises";

const requiredFiles = ["LICENSE", "NOTICE"];
const packageJson = JSON.parse(await readFile("package.json", "utf8"));
const packageFiles = new Set(packageJson.files ?? []);
const missing = [];

for (const file of requiredFiles) {
  const content = await readFile(file, "utf8").catch(() => "");
  if (content.trim().length === 0) {
    missing.push(`${file} is missing or empty`);
  }
  if (!packageFiles.has(file)) {
    missing.push(`${file} is not included in package.json files`);
  }
}

if (missing.length > 0) {
  console.error("License/notice release gate failed:");
  for (const item of missing) {
    console.error(`- ${item}`);
  }
  process.exitCode = 1;
}
