import { defineConfig } from "@playwright/test";
export default defineConfig({
  testDir: "./browser-tests",
  fullyParallel: false,
  use: {
    baseURL: "http://127.0.0.1:4173/NornicDB/",
    browserName: "chromium",
    channel: process.env.CI ? undefined : "chrome",
    trace: "retain-on-failure",
  },
  webServer: [
    {
      command: "npx vitepress preview . --port 4173",
      url: "http://127.0.0.1:4173/NornicDB/",
      reuseExistingServer: true,
      timeout: 120000,
    },
    {
      command: "npx vitepress preview . --host 127.0.0.1 --port 4176",
      url: "http://127.0.0.1:4176/NornicDB/",
      reuseExistingServer: true,
      timeout: 120000,
    },
  ],
});
