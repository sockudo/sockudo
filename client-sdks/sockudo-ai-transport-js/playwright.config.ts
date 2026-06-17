import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "test/browser",
  timeout: 30_000,
  expect: { timeout: 10_000 },
  webServer: {
    command: "pnpm --filter @sockudo/ai-transport-demo-nuxt dev",
    url: "http://localhost:5174",
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
  },
  use: {
    baseURL: "http://localhost:5174",
    trace: "retain-on-failure",
  },
  projects: [
    { name: "chromium", use: { ...devices["Desktop Chrome"] } },
    { name: "firefox", use: { ...devices["Desktop Firefox"] } },
    { name: "webkit", use: { ...devices["Desktop Safari"] } },
  ],
});
