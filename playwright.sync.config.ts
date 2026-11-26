import { defineConfig } from '@playwright/test'

export default defineConfig({
  testDir: './tests/sync',
  workers: 1,
  timeout: 30_000,
  use: {
    baseURL: 'http://127.0.0.1:18173',
    channel: process.env.PLAYWRIGHT_CHANNEL,
  },
  webServer: {
    command: 'node node_modules/vite/bin/vite.js --host 127.0.0.1 --port 18173 --strictPort',
    url: 'http://127.0.0.1:18173',
    reuseExistingServer: false,
  },
})
