import {test,expect} from '@playwright/test';
import {fixture} from './fixture.mjs';
import AxeBuilder from '@axe-core/playwright';
import {mkdir} from 'node:fs/promises';
async function shot(page,name){await mkdir('review/prototype',{recursive:true});await page.screenshot({path:`review/prototype/${name}.png`,fullPage:true});}
async function accessible(page,info){const report=await new AxeBuilder({page}).withTags(['wcag2a','wcag2aa','wcag21aa']).analyze();await info.attach('full-accessibility-report',{body:JSON.stringify(report.violations,null,2),contentType:'application/json'});const shell=await new AxeBuilder({page}).exclude('.nornic-grid').withTags(['wcag2a','wcag2aa','wcag21aa']).analyze();expect(shell.violations.map(v=>({id:v.id,nodes:v.nodes.map(n=>n.html)}))).toEqual([]);}

test('database list retains task CTAs; details and creation are routed pages, delete is a confirmation',async({page},info)=>{
 const state=await fixture(page);Object.assign(state.databases[0],{searchReady:true,searchStrategy:'hybrid',nodeStorageBytes:15728640,managedEmbeddingBytes:3145728});
 await page.goto('/databases');const row=page.locator('.database-record').first();await expect(row.getByRole('button',{name:'Open workspace',exact:true})).toBeVisible();for(const label of ['Settings','Lifecycle','Schema','Details','Delete'])await expect(row.getByRole('button',{name:label,exact:true})).toBeVisible();await expect(row.getByRole('button',{name:'Delete',exact:true})).toBeDisabled();
 await accessible(page,info);await shot(page,'14-databases');
 await row.getByRole('button',{name:'Details',exact:true}).click();await expect(page).toHaveURL(/databases\/catalog/);await expect(page.getByRole('region',{name:'Database details',exact:true})).toBeVisible();await expect(page.getByRole('dialog')).not.toBeVisible();await expect(page.getByRole('navigation',{name:'Database tasks'})).toContainText('Open workspace');await shot(page,'15-database-details');
 await page.getByRole('button',{name:'Back to databases',exact:true}).click();await page.locator('.database-record').last().getByRole('button',{name:'Delete',exact:true}).click();await expect(page.getByRole('dialog',{name:'Delete Database',exact:true})).toContainText('research');await expect(page.getByRole('button',{name:'Cancel',exact:true})).toBeFocused();await page.keyboard.press('Escape');
 await page.getByRole('button',{name:'Create Database',exact:true}).click();await expect(page.getByRole('region',{name:'Create database',exact:true})).toBeVisible();await expect(page.getByRole('dialog')).not.toBeVisible();await page.getByLabel('Database Name').fill('sandbox');await accessible(page,info);await shot(page,'16-create-database');await page.getByRole('button',{name:'Back to databases',exact:true}).click();await expect(page.getByRole('dialog',{name:'Leave unsaved changes?',exact:true})).toBeVisible();await page.getByRole('button',{name:'Keep editing',exact:true}).click();await expect(page.getByLabel('Database Name')).toHaveValue('sandbox');await page.getByRole('button',{name:'Cancel',exact:true}).click();await expect(page).toHaveURL(/databases\/$/);
 await page.setViewportSize({width:390,height:844});await accessible(page,info);expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);await shot(page,'17-mobile-databases');
});

test('schema creation is an accessible contextual page with a definition preview and unsaved guard',async({page},info)=>{
 await fixture(page);await page.goto('/schema/catalog/constraints/new');const form=page.getByRole('region',{name:'Create constraint',exact:true});await expect(form).toBeVisible();await expect(page.getByRole('dialog')).not.toBeVisible();await form.getByLabel('Schema name').fill('person_email');await form.getByLabel('Node label').fill('Person');await form.getByRole('textbox',{name:'Properties',exact:true}).fill('email');await expect(form).toContainText('REQUIRE (n.`email`) IS UNIQUE');await accessible(page,info);await shot(page,'18-create-constraint');
 await form.getByRole('button',{name:'Back to schema',exact:true}).click();await expect(page.getByRole('dialog',{name:'Leave unsaved changes?',exact:true})).toBeVisible();await page.getByRole('button',{name:'Keep editing',exact:true}).click();await expect(form.getByLabel('Schema name')).toHaveValue('person_email');
 await page.setViewportSize({width:390,height:844});await accessible(page,info);expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);await shot(page,'19-mobile-schema-create');await form.getByRole('button',{name:'Cancel',exact:true}).click();await expect(page).not.toHaveURL(/\/new/);
});

test('row controls remain pinned; row expansion owns full values without a duplicate node expander',async({page})=>{
 await fixture(page);const legacy={pinnedColumns:{},columnOrder:['nornicSelect','n']};await page.goto('/workspace/catalog?grid.query-results-grid='+encodeURIComponent(JSON.stringify(legacy)));await page.getByRole('button',{name:'Run Query',exact:true}).click();
 const inspect=page.locator('.body-cell[data-column="nornicSelect"]').first();await expect(inspect).toBeVisible();await expect(inspect).toHaveCSS('position','sticky');await expect(page.locator('.body-cell[data-column="selectionRowHeaderCol"]').first()).toHaveCSS('position','sticky');
 await expect(page.locator('.body-cell[data-column="n"] button')).toHaveCount(0);await page.getByRole('button',{name:'Expand details',exact:true}).first().click();await expect(page.getByLabel('Result details')).toContainText('Getting started');await expect(page).toHaveURL(/expandedRows/);
});

test('similar records stay in the inspector, show loading/errors, retry, and open a match',async({page},info)=>{
 const state=await fixture(page);let fail=true;let delay=100;
 await page.route('**/nornicdb/similar',async route=>{await new Promise(resolve=>setTimeout(resolve,delay));if(fail)return route.fulfill({status:503,json:{message:'Similar search unavailable'}});return route.fulfill({json:[{node:{...state.node,id:'record-2',labels:['Guide'],properties:{title:'Vector indexing'}},score:0.88}]});});
 await page.goto('/workspace/catalog/nodes/record-1');await page.getByRole('button',{name:'Find similar',exact:true}).click();await expect(page).toHaveURL(/inspectorView=similar/);await expect(page.getByRole('region',{name:'Similar records',exact:true})).toBeVisible();await expect(page.getByRole('alert')).toContainText('Similar search unavailable');fail=false;delay=0;await page.getByRole('button',{name:'Try again',exact:true}).click();await expect(page.getByRole('button',{name:'Inspect Vector indexing',exact:true})).toBeVisible();await accessible(page,info);await shot(page,'20-similar-records');
 await page.getByRole('button',{name:'Back to details',exact:true}).click();await expect(page.getByRole('heading',{name:'Node Details',exact:true})).toBeVisible();await page.getByRole('button',{name:'Find similar',exact:true}).click();await page.getByRole('button',{name:'Inspect Vector indexing',exact:true}).click();await expect(page).toHaveURL(/nodes\/record-2/);await expect(page).not.toHaveURL(/inspectorView=similar/);await expect(page.getByRole('heading',{name:'Node Details',exact:true})).toBeVisible();
});

test('long JSON is clipped in cells; pinned controls remain opaque and above scrolled data',async({page})=>{
 const state=await fixture(page);const json=JSON.stringify({content:'long-value-'.repeat(1000)});
 await page.route('**/db/*/tx/commit',route=>{
  const statement=route.request().postDataJSON().statements[0].statement;if(statement.includes('SHOW DATABASES'))return route.fallback();
  return route.fulfill({json:{results:[{columns:['n','payload','a','b','c','d','e','f'],data:[{row:[{elementId:'record-1',labels:['Document'],properties:{payload:json}},json,'a','b','c','d','e','f'],meta:[]}]}],errors:[]}});
 });
 await page.goto('/workspace/catalog');await page.getByRole('button',{name:'Run Query',exact:true}).click();
 const inspect=page.locator('.body-cell[data-column="nornicSelect"]').first(),cell=page.locator('.body-cell[data-column="payload"]').first(),value=page.locator('.query-cell-value').filter({hasText:'long-value-'});await expect(inspect).toBeVisible();await expect(value).toHaveCSS('text-overflow','ellipsis');await expect(value).toHaveCSS('overflow','hidden');await expect(value).toBeVisible();await expect(page.locator('.query-node-summary')).toContainText('1 property');await expect(page.locator('.query-node-summary')).not.toContainText('long-value-');
 const cellBounds=await cell.boundingBox(),valueBounds=await value.boundingBox();expect(valueBounds.width).toBeLessThanOrEqual(cellBounds.width);
 const before=await inspect.boundingBox();await page.locator('.grid-body-viewport').evaluate(element=>element.scrollLeft=500);const after=await inspect.boundingBox();expect(Math.abs(before.x-after.x)).toBeLessThan(1);
 for(const pinned of [inspect,page.locator('.body-cell[data-column="selectionRowHeaderCol"]').first()]){const style=await pinned.evaluate(element=>{const css=getComputedStyle(element);return {color:css.backgroundColor,z:Number(css.zIndex),position:css.position}});expect(style.position).toBe('sticky');expect(style.color).toMatch(/^rgb\(/);expect(style.z).toBeGreaterThan(1);}
 await shot(page,'21-pinned-horizontal-scroll');await page.getByRole('button',{name:'Expand details',exact:true}).first().click();await expect(page.getByLabel('Result details')).toContainText('long-value-'.repeat(1000));expect(state.node.id).toBe('record-1');
});


test('default workspace canonicalizes its database and inspection keeps the result set',async({page})=>{
 await fixture(page);await page.goto('/');await expect(page).toHaveURL(/workspace\/catalog/);await page.getByRole('button',{name:'Run Query',exact:true}).click();await page.getByRole('button',{name:'Inspect',exact:true}).first().click();await expect(page).toHaveURL(/workspace\/catalog\/nodes\/record-1/);await expect(page.getByRole('heading',{name:'Results',exact:true})).toBeVisible();await page.getByRole('button',{name:'Close details',exact:true}).click();await expect(page).toHaveURL(/workspace\/catalog\/$/);await expect(page.getByRole('heading',{name:'Results',exact:true})).toBeVisible();
});
