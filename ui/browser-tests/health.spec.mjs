import {test,expect} from '@playwright/test';
import {healthFixture} from './health-fixture.mjs';

for(const admin of [true,false])test(`health dashboard presents ${admin?'admin':'restricted user'} telemetry and browser history`,async({page})=>{
 await page.clock.install();await healthFixture(page,admin);const errors=[];page.on('pageerror',e=>errors.push(e.message));
 await page.goto('/operations/health');await expect(page.getByRole('heading',{name:'Server responding'})).toBeVisible();await expect(page.locator('.health-history-note')).toContainText('1 samples');
 for(let i=0;i<14;i++){await page.clock.fastForward(5000);await expect.poll(()=>page.locator('.health-history-note').textContent()).toContain(`${i+2} samples`);}
 await expect(page.getByText(admin?'Admin · full catalog':'Database-scoped catalog',{exact:true})).toBeVisible();
 if(admin)await expect(page.locator('.health-kpi').first()).not.toContainText('Unavailable');else{await expect(page.locator('.health-kpi').first()).toContainText('Readable nodes');await expect(page.getByRole('region',{name:'Process memory',exact:true})).toHaveCount(0);await expect(page.locator('.health-permission-note')).toBeVisible();await expect(page.locator('body')).not.toContainText('finance-private');}
 await page.screenshot({path:`review/prototype/${admin?'34-health-admin':'35-health-restricted'}.png`,fullPage:true});
 await page.getByRole('button',{name:/Metric explorer/}).click();
 await expect(page.getByRole('heading',{name:'Metric explorer',exact:true})).toBeVisible();
 if(admin)await page.screenshot({path:'review/prototype/40-health-metric-catalog.png',fullPage:false});
 await page.getByRole('button',{name:'nornicdb_cypher_queries_total',exact:true}).click();
 await expect(page).toHaveURL(/metrics\/nornicdb_cypher_queries_total/);
 await expect(page.getByRole('region',{name:'Metric details'})).toContainText('catalog');
 if(!admin)await expect(page.getByRole('region',{name:'Metric details'})).not.toContainText('finance-private');
 await page.screenshot({path:`review/prototype/${admin?'36-health-metric-admin':'37-health-metric-restricted'}.png`,fullPage:false});
 if(admin){await page.getByRole('link',{name:'Back to metric catalog'}).click();await page.getByRole('button',{name:'nornicdb_http_request_duration_seconds',exact:true}).click();await expect(page.locator('.health-distribution')).toBeVisible();await expect(page.locator('.health-series-top')).not.toContainText('Unavailable');await page.screenshot({path:'review/prototype/39-health-histogram.png',fullPage:false});await page.getByRole('button',{name:'Overview',exact:true}).click();await expect(page.getByRole('region',{name:'Metric details'})).toHaveCount(0);}
 expect(errors).toEqual([]);
});

test('history pauses, survives route changes, shows failed refreshes and resets on reload',async({page})=>{
 await page.clock.install();const fixture=await healthFixture(page);await page.goto('/operations/health');await expect(page.getByRole('heading',{name:'Server responding'})).toBeVisible();await expect(page.locator('.health-history-note')).toContainText('1 samples');await page.clock.fastForward(5000);await expect(page.locator('.health-history-note')).toContainText('2 samples');
 await page.getByRole('button',{name:'Pause history',exact:true}).click();await page.clock.fastForward(10000);await expect(page.locator('.health-history-note')).toContainText('History paused');
 await page.getByRole('button',{name:'Resume history',exact:true}).click();await page.clock.fastForward(5000);await expect(page.locator('.health-history-note')).toContainText('3 samples');
 await page.getByRole('navigation',{name:'Application'}).getByRole('link',{name:'Databases',exact:true}).click();await expect(page).toHaveURL(/databases/);await expect(page.locator('.header-server-connection')).toHaveText('Connected');
 await page.getByRole('navigation',{name:'Application'}).getByRole('link',{name:'Health & monitoring',exact:true}).click();await expect(page.locator('.health-history-note')).not.toContainText('1 samples');
 fixture.fail();await page.clock.fastForward(5000);await expect(page.locator('.health-notice')).toContainText('interrupted');await expect(page.locator('.header-server-connection')).toHaveText('Disconnected');
 fixture.fail(false);await page.reload();await expect(page.locator('.health-history-note')).toContainText('1 samples');
});

test('health overview works on a narrow viewport',async({page})=>{
 await page.setViewportSize({width:390,height:844});await page.clock.install();await healthFixture(page);await page.goto('/operations/health');await expect(page.getByRole('heading',{name:'Server responding'})).toBeVisible();await expect(page.locator('.health-history-note')).toContainText('1 samples');for(let i=0;i<6;i++){await page.clock.fastForward(5000);await expect(page.locator('.health-history-note')).toContainText(`${i+2} samples`);}
 await expect(page.locator('.health-history-note')).toContainText('7 samples');
 expect(await page.evaluate(()=>document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);
 await page.screenshot({path:'review/prototype/38-health-mobile.png',fullPage:true});
});

test('hidden tabs stop polling and resume with an explicit history gap',async({page})=>{
 await page.clock.install();const fixture=await healthFixture(page);await page.goto('/operations/health');await expect(page.getByRole('heading',{name:'Server responding'})).toBeVisible();
 const before=fixture.index;
 await page.evaluate(()=>{Object.defineProperty(document,'hidden',{configurable:true,get:()=>true});document.dispatchEvent(new Event('visibilitychange'));});
 await page.clock.fastForward(20000);expect(fixture.index).toBe(before);await expect(page.getByRole('heading',{name:'Updates paused'})).toBeVisible();
 await page.evaluate(()=>{Object.defineProperty(document,'hidden',{configurable:true,get:()=>false});document.dispatchEvent(new Event('visibilitychange'));});
 await expect.poll(()=>fixture.index).toBe(before+1);await expect(page.getByRole('heading',{name:'Server responding'})).toBeVisible();
});
