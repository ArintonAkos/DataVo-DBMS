import { defineConfig, devices } from "@playwright/test";

/**
 * Playwright configuration for browser-based docs runtime tests.
 */
export default defineConfig({
  testDir: "./tests/browser",
  fullyParallel: true,
  workers: process.env.CI ? 2 : undefined,
  timeout: 120_000,
  expect: {
    timeout: 10_000,
  },
  use: {
    // VitePress serves the site under base "/DataVo-DBMS/"; the trailing slash keeps relative paths inside it.
    baseURL: "http://127.0.0.1:4173/DataVo-DBMS/",
    headless: true,
    trace: "on-first-retry",
  },
  webServer: {
    command: "npm run docs:dev -- --host 127.0.0.1 --port 4173",
    url: "http://127.0.0.1:4173/DataVo-DBMS/",
    reuseExistingServer: !process.env.CI,
    timeout: 180_000,
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
});
