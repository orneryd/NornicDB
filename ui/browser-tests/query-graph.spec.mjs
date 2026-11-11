import {test,expect} from '@playwright/test';
import {fixture} from './fixture.mjs';
import {checkAccessibility} from './accessibility.mjs';
import {mkdir} from 'node:fs/promises';

test('query graph renders in 3D, supports keyboard inspection and preserves neighborhood controls',async({page},testInfo)=>{
 const state=await fixture(page);state.queryGraph=true;const errors=[];page.on('pageerror',e=>errors.push(e.message));
 await page.setViewportSize({width:1440,height:1000});await page.goto('/workspace/catalog');
 await page.getByRole('button',{name:'Run Query',exact:true}).click();await page.getByRole('tab',{name:'Graph Explorer',exact:true}).click();
 await expect(page.getByRole('region',{name:'3D graph workspace'}).locator('canvas')).toBeVisible();
 await expect(page.getByText('2 nodes, 1 edges',{exact:false})).toBeVisible();
 await page.getByText('Nodes and relationships (keyboard access)',{exact:true}).click();
 const nodes=page.getByRole('region',{name:'Graph nodes',exact:true});
 await nodes.getByRole('button',{name:'Vector indexing (Guide)',exact:true}).click();
 await expect(page).toHaveURL(/nodes\/record-2/);await expect(nodes.getByRole('button',{name:'Vector indexing (Guide)',exact:true})).toHaveAttribute('aria-pressed','true');
 await page.getByRole('button',{name:'Focus selected',exact:true}).click();await expect(page).toHaveURL(/graph.camera=/);
 await page.getByRole('region',{name:'Graph relationships',exact:true}).getByRole('button',{name:'Getting started → LINKS_TO → Vector indexing',exact:true}).click();
 await expect(page).toHaveURL(/graph.edge=edge-1/);
 await page.getByRole('checkbox',{name:'Node labels',exact:true}).click();await expect(page.getByRole('checkbox',{name:'Node labels',exact:true})).toBeChecked();await page.getByRole('button',{name:'Pause motion',exact:true}).click();
 await expect(page).toHaveURL(/graph.paused=true/);
 await checkAccessibility(page,testInfo);expect(errors).toEqual([]);
 await page.evaluate(()=>window.scrollTo(0,0));
 await mkdir('review/screenshots',{recursive:true});await page.screenshot({path:'review/screenshots/query-graph.png',fullPage:false});
 await page.getByRole('button',{name:'Explore selected node’s neighbors',exact:true}).click();
 await expect(page).toHaveURL(/graph.root=.*record-2/);await expect(page.getByText('1 nodes, 0 edges',{exact:false})).toBeVisible();
 await page.getByRole('button',{name:'Back to query graph',exact:true}).click();await expect(page.getByText('2 nodes, 1 edges',{exact:false})).toBeVisible();
});

test('mobile graph offers keyboard camera controls without overflowing the page',async({page},testInfo)=>{
 await fixture(page);await page.setViewportSize({width:390,height:844});await page.goto('/workspace/catalog/nodes/record-1?tab=graph');
 await expect(page.getByRole('region',{name:'3D graph workspace'}).locator('canvas')).toBeVisible();
 await page.getByText('View controls (keyboard)',{exact:true}).click();
 await page.getByRole('button',{name:'Zoom in',exact:true}).click();await expect(page).toHaveURL(/graph.camera=/);
 expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
 await checkAccessibility(page,testInfo);await page.evaluate(()=>window.scrollTo(0,0));
 await page.screenshot({path:'review/screenshots/query-graph-mobile.png',fullPage:true});
});
