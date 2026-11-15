import {defineConfig} from '@playwright/test';
export default defineConfig({
 testDir:'./browser-tests',
 timeout:60000,
 workers:2,
 use:{baseURL:'http://127.0.0.1:5181',channel:process.env.CI ? undefined : 'chrome',viewport:{width:1440,height:960},trace:'retain-on-failure'},
 // Test built assets: trace HTML and screenshots must not trigger Vite HMR.
 webServer:{command:'npm run build && npm run preview -- --host 127.0.0.1 --port 5181 --strictPort',url:'http://127.0.0.1:5181/index.html',reuseExistingServer:!process.env.CI},
});
