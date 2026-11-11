import {defineConfig} from '@playwright/test';
export default defineConfig({testDir:'./browser-tests',workers:2,use:{baseURL:'http://127.0.0.1:5181',channel:process.env.CI ? undefined : 'chrome',trace:'retain-on-failure'},webServer:{command:'npm run dev -- --host 127.0.0.1 --port 5181',url:'http://127.0.0.1:5181/index.html',reuseExistingServer:!process.env.CI}});
