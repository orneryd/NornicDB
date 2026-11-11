import {test,expect} from '@playwright/test';
import {checkAccessibility} from './accessibility.mjs';
import {mkdir} from 'node:fs/promises';
import {fixture} from './fixture.mjs';

test('pivot seeds only after consent and virtualizes 1,000 account rows',async({page},testInfo)=>{
 test.setTimeout(60000);
 const state=await fixture(page);state.seedDelay=20;await page.setViewportSize({width:1920,height:1400});await page.goto('/demos/pivot');
 await expect(page.getByText('Create a disposable demo database')).toBeVisible();
 expect(state.queries.some(body=>/CREATE DATABASE|MERGE/.test(body.statements[0].statement))).toBe(false);
 await page.getByRole('button',{name:'Seed new demo database'}).click();
 await expect(page).toHaveURL(/\/demos\/pivot\/nornic_demo_pivot_/);
 await expect(page.getByRole('progressbar',{name:'Seeding progress'})).toBeVisible();
 await expect(page.getByRole('status')).toContainText('25,000 orders · 1,000 pivot rows');
 expect(state.demoDatabases.size).toBe(1);
 expect([...state.demoDatabases.values()][0].sales.size).toBe(25000);
 expect([...state.demoDatabases.values()][0].ready).toBe(true);
 const grid=page.getByRole('grid');await expect(grid).toBeVisible();
 await expect(grid.getByText('Account 0001',{exact:true})).toBeVisible();
 const renderedAccounts=grid.locator('ui-grid-body-cell[data-column=dimension]');
 expect(await renderedAccounts.count()).toBeGreaterThan(0);expect(await renderedAccounts.count()).toBeLessThan(100);
 await page.getByRole('combobox',{name:'Measure',exact:true}).selectOption('revenuePerUnit');
 await expect(page.getByText('sum(revenue) ÷ sum(units). Totals recalculate the ratio from all contributing orders.',{exact:false})).toBeVisible();
 await expect(page.getByRole('combobox',{name:'Aggregation',exact:true})).toBeDisabled();
 await checkAccessibility(page,testInfo);
 await grid.locator('ui-grid-body-cell[data-column=dimension][data-row="Account 0001"]').getByRole('button',{name:'Expand details',exact:true}).click();
 const firstDetails=page.getByRole('region',{name:'Order details for Account 0001',exact:true});
 await expect(firstDetails.getByRole('grid',{name:'Orders for Account 0001'})).toBeVisible();
 await expect(firstDetails).toContainText('Showing 25 of 25 orders');
 await grid.locator('ui-grid-body-cell[data-column=dimension][data-row="Account 0002"]').getByRole('button',{name:'Expand details',exact:true}).click();
 await expect(page.getByRole('region',{name:'Order details for Account 0002',exact:true})).toBeVisible();
 await expect(page.getByRole('region',{name:'Order details for Account 0001',exact:true})).toBeAttached();
 await checkAccessibility(page,testInfo,'section[aria-label="Order details for Account 0001"]');
 await grid.locator('ui-grid-body-cell[data-column=dimension][data-row="Account 0001"]').scrollIntoViewIfNeeded();
 await mkdir('review/screenshots',{recursive:true});await page.screenshot({path:'review/screenshots/pivot-expanded.png',fullPage:false});
 await page.getByRole('combobox',{name:'Measure',exact:true}).selectOption('revenue');
 await page.getByRole('combobox',{name:'Columns',exact:true}).selectOption('product');
 await page.getByRole('combobox',{name:'Aggregation',exact:true}).selectOption('average');
 await expect(page).toHaveURL(/columns=product/);
 await expect(page.getByRole('status')).toContainText('25,000 orders');
 await page.reload();await expect(page.getByRole('combobox',{name:'Columns',exact:true})).toHaveValue('product');
 await expect(page.getByRole('combobox',{name:'Aggregation',exact:true})).toHaveValue('average');
 await expect(page.getByRole('status')).toContainText('25,000 orders');
 expect(state.demoDatabases.size).toBe(1);
});

test('failed setup can resume without creating another database or duplicate orders',async({page},testInfo)=>{
 const state=await fixture(page);state.failSeedAfter=3;await page.goto('/demos/pivot');
 await page.getByRole('button',{name:'Seed new demo database'}).click();
 await expect(page.getByRole('alert')).toContainText('Seed request failed');
 expect(state.demoDatabases.size).toBe(1);expect([...state.demoDatabases.values()][0].sales.size).toBe(1200);state.failSeedAfter=Infinity;
 await page.getByRole('button',{name:'Resume setup'}).click();
 await expect(page.getByRole('status')).toContainText('25,000 orders · 1,000 pivot rows');
 expect(state.demoDatabases.size).toBe(1);
 expect([...state.demoDatabases.values()][0].sales.size).toBe(25000);
});

test('opening an unknown demo URL never seeds or mutates a database',async({page},testInfo)=>{
 const state=await fixture(page);await page.goto('/demos/pivot/existing');
 await expect(page.getByText('This demo database is unavailable or its setup is incomplete.')).toBeVisible();
 expect(state.queries.some(body=>/CREATE DATABASE|MERGE|SET /.test(body.statements[0].statement))).toBe(false);
});

test('mobile pivot keeps the page within the viewport and restores virtual browsing after closing details',async({page})=>{
 const state=await fixture(page);await page.setViewportSize({width:390,height:844});await page.goto('/demos/pivot');
 await page.getByRole('button',{name:'Seed new demo database'}).click();await expect(page.getByRole('status')).toContainText('1,000 pivot rows');
 expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
 const grid=page.getByRole('grid',{name:'Sales pivot',exact:true});
 await grid.locator('ui-grid-body-cell[data-column=dimension][data-row="Account 0001"]').getByRole('button',{name:'Expand details',exact:true}).click();
 await expect(page.getByRole('region',{name:'Order details for Account 0001',exact:true})).toContainText('Showing 25 of 25');
 await page.screenshot({path:'review/screenshots/pivot-mobile-expanded.png',fullPage:false});
 await grid.locator('ui-grid-body-cell[data-column=dimension][data-row="Account 0001"]').getByRole('button',{name:'Collapse details',exact:true}).click();
 await expect.poll(()=>grid.locator('ui-grid-body-cell[data-column=dimension]').count()).toBeLessThan(100);
 expect(state.demoDatabases.size).toBe(1);
});
