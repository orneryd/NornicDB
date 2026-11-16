import {test,expect} from '@playwright/test';
import {fixture} from './fixture.mjs';
import {checkAccessibility} from './accessibility.mjs';
const toggle=(page,name)=>page.getByRole('switch',{name:`Managed generation for ${name}`,exact:true});
test('generation switches apply independently, persist and remain discoverable on mobile',async({page},testInfo)=>{
 const state=await fixture(page);state.configDelay=250;
 state.databases.push({name:'federated',type:'composite',status:'online',nodeCount:0,edgeCount:0});
 await page.goto('/embeddings');
 await expect(toggle(page,'catalog')).toHaveAttribute('aria-checked','true');await expect(toggle(page,'research')).toHaveAttribute('aria-checked','false');
 await expect(page.getByText('Manage generation on its constituent databases.')).toBeVisible();
 await toggle(page,'catalog').click();await expect(toggle(page,'catalog')).toBeDisabled();await expect(toggle(page,'research')).toBeEnabled();
 await expect(toggle(page,'catalog')).toHaveAttribute('aria-checked','false');await expect(toggle(page,'catalog')).toBeEnabled();
 await expect(toggle(page,'research')).toHaveAttribute('aria-checked','false');
 expect(state.configChanges).toEqual([{database:'catalog',overrides:{'db.nornic.embedding.enabled':'false'}}]);
 await page.reload();await expect(toggle(page,'catalog')).toHaveAttribute('aria-checked','false');
 await toggle(page,'research').click();await expect(toggle(page,'research')).toHaveAttribute('aria-checked','true');
 await checkAccessibility(page,testInfo);
 await page.screenshot({path:'review/prototype/28-database-embedding-switches.png',fullPage:false});
 const search=page.getByRole('searchbox',{name:'Filter database names'});await search.fill('RESEARCH');await expect(page).toHaveURL(/embeddingDatabase=RESEARCH/);await expect(toggle(page,'catalog')).not.toBeVisible();
 await page.reload();await expect(search).toHaveValue('RESEARCH');await expect(toggle(page,'research')).toHaveAttribute('aria-checked','true');
 await search.fill('');await page.getByRole('combobox',{name:'Generation status',exact:true}).selectOption('off');await expect(toggle(page,'catalog')).toBeVisible();await expect(toggle(page,'research')).not.toBeVisible();
 await page.getByRole('combobox',{name:'Generation status',exact:true}).selectOption('all');
 await page.setViewportSize({width:390,height:844});expect(await page.evaluate(()=>document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);
 await checkAccessibility(page,testInfo);await page.screenshot({path:'review/prototype/29-mobile-embedding-switches.png',fullPage:true});
});
test('failed saves retain the confirmed state and retry; read-only users cannot toggle',async({page})=>{
 const state=await fixture(page);state.failConfig=true;await page.goto('/embeddings');
 await expect(toggle(page,'research')).toBeEnabled();await toggle(page,'research').click();
 await expect(page.getByRole('alert')).toContainText('Configuration storage unavailable');await expect(toggle(page,'research')).toHaveAttribute('aria-checked','false');
 state.failConfig=false;await page.getByRole('button',{name:'Retry research',exact:true}).click();await expect(toggle(page,'research')).toHaveAttribute('aria-checked','true');
 state.roles=['viewer'];await page.reload();await expect(toggle(page,'research')).toBeDisabled();await expect(toggle(page,'research')).toContainText('Unknown');
 expect(state.configChanges).toHaveLength(2);
});
test('an older refresh cannot undo a confirmed switch change',async({page})=>{
 await fixture(page);await page.goto('/embeddings');await expect(toggle(page,'catalog')).toBeEnabled();
 let release,started;const held=new Promise(resolve=>release=resolve),received=new Promise(resolve=>started=resolve);
 await page.route('**/admin/databases/catalog/config',async route=>{
  if(route.request().method()!=='GET')return route.fallback();
  started();await held;await route.fulfill({json:{effective:{'db.nornic.embedding.enabled':'true'},overrides:{}}});
 });
 await page.getByRole('button',{name:'Refresh status',exact:true}).click();await received;
 await toggle(page,'catalog').click();await expect(toggle(page,'catalog')).toHaveAttribute('aria-checked','false');
 release();await expect(toggle(page,'catalog')).toHaveAttribute('aria-checked','false');
});
