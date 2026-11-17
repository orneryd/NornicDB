import {test,expect} from '@playwright/test';
import {fixture} from './fixture.mjs';

test('workspace and management headers share an overlay and URL state',async({page})=>{
 test.setTimeout(120000);
 await fixture(page);
 const errors=[];page.on('pageerror',error=>errors.push(error.message));
 for(const path of ['/workspace/catalog','/databases','/schema','/backups','/security','/security/admin','/security/database-access','/security/lifecycle','/security/retention','/security/knowledge-policies','/embeddings']){
  await page.goto(path);
  const status=page.locator('.server-status-popover');
  await expect(status).toHaveCount(1);
  await expect(page.locator('.header-server-connection')).toHaveText('Connected');
  const header=status.locator('xpath=ancestor::header');
  const before=await header.boundingBox();
  await status.locator('summary').click();
  await expect(status).toHaveAttribute('open','');
  await expect(page).toHaveURL(/server=true/);
  await expect(status.locator('dl')).toContainText('13,160');
  expect(await status.locator('dl').evaluate(element=>getComputedStyle(element).position)).toBe('absolute');
  expect((await header.boundingBox()).height).toBe(before.height);
  if(path==='/databases')await page.screenshot({path:'review/prototype/32-shared-server-status.png'});
  await page.reload();
  await expect(page.locator('.server-status-popover')).toHaveAttribute('open','');
 }
 // The public demo catalog shares the same header after signed-in navigation.
 await page.getByRole('navigation',{name:'Application'}).getByRole('link',{name:'Overview',exact:true}).click();
 await expect(page.locator('.server-status-popover')).toHaveCount(1);
 expect(errors).toEqual([]);
});

test('the status owner polls once and cleans up when changing pages',async({page})=>{
 await page.clock.install();await fixture(page);let requests=0,failed=false;
 await page.route('**/status',async route=>{
  if(new URL(route.request().url()).pathname!=='/status')return route.fallback();
  requests++;
  await route.fulfill({status:failed?503:200,json:failed?{message:'Unavailable'}:{database:{nodes:1,edges:2},server:{uptime_seconds:3600,version:'1.5.0'}}});
 });
 await page.goto('/workspace/catalog');
 await expect(page.locator('.header-server-connection')).toHaveText('Connected');
 expect(requests).toBe(1);
 await page.clock.fastForward(5000);await expect.poll(()=>requests).toBe(2);
 await page.getByRole('navigation',{name:'Application'}).getByRole('link',{name:'Databases',exact:true}).click();
 await expect(page).toHaveURL(/\/databases\//);await expect.poll(()=>requests).toBe(3);
 await page.clock.fastForward(5000);await expect.poll(()=>requests).toBe(4);
 failed=true;await page.clock.fastForward(5000);
 await expect(page.locator('.header-server-connection')).toHaveText('Disconnected');
 expect(requests).toBe(5);
});

test('the shared header popover fits mobile without moving the header',async({page})=>{
 await page.setViewportSize({width:390,height:844});await fixture(page);
 for(const path of ['/workspace/catalog','/databases','/security/admin']){
  await page.goto(path);const status=page.locator('.server-status-popover');
  await expect(page.locator('.header-server-connection')).toHaveText('Connected');
  const header=status.locator('xpath=ancestor::header'),before=await header.boundingBox();
  await status.locator('summary').focus();await page.keyboard.press('Enter');
  await expect(status.locator('dl')).toBeVisible();
  const panel=await status.locator('dl').boundingBox();
  expect(panel.x).toBeGreaterThanOrEqual(0);expect(panel.x+panel.width).toBeLessThanOrEqual(390);
  expect((await header.boundingBox()).height).toBe(before.height);
  if(path==='/databases')await page.screenshot({path:'review/prototype/33-shared-server-status-mobile.png'});
 }
});
