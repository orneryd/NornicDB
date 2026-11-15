import {test,expect} from '@playwright/test';
import {fixture} from './fixture.mjs';
import AxeBuilder from '@axe-core/playwright';
async function checkAccessibility(page,info){
 const full=await new AxeBuilder({page}).withTags(['wcag2a','wcag2aa','wcag21aa']).analyze();
 await info.attach('full-accessibility-report',{body:JSON.stringify(full.violations,null,2),contentType:'application/json'});
 const report=await new AxeBuilder({page}).exclude('.nornic-grid').withTags(['wcag2a','wcag2aa','wcag21aa']).analyze();
 expect(report.violations.map(v=>({id:v.id,nodes:v.nodes.map(n=>n.html)}))).toEqual([]);
}
import {mkdir} from 'node:fs/promises';
const shot=async(page,name)=>{await mkdir('review/prototype',{recursive:true});await page.screenshot({path:`review/prototype/${name}.png`,fullPage:true});};
test('prototype workspace tasks and responsive layout',async({page},info)=>{
 const errors=[];page.on('pageerror',error=>errors.push(error.message));const state=await fixture(page);
 await page.route('**/nornicdb/search',route=>route.fulfill({json:[{node:state.node,score:0.94},{node:{...state.node,id:'record-2',labels:['Guide'],properties:{title:'Vector indexing',description:'Build and query a vector index for semantic search.'}},score:0.87},{node:{...state.node,id:'record-3',properties:{title:'Hybrid search',description:'Combine full-text and vector retrieval.'}},score:0.81}]}));

 await page.route('**/db/*/tx/commit',async route=>{
  const statement=route.request().postDataJSON()?.statements?.[0]?.statement;
  if(statement!=='MATCH (n) RETURN n LIMIT 25'||state.queryGraph)return route.fallback();
  const titles=['Getting started','Vector indexing','Hybrid search','Database configuration','Access tokens','Cypher functions','Backup and restore','Memory management','Embedding models','Query optimization','Schema constraints','Importing data'];
  return route.fulfill({json:{results:[{columns:['n','section','revision'],data:titles.map((title,i)=>({row:[{elementId:i?`record-${i+1}`:'record-1',labels:['Document'],properties:{title,published:true,revision:i+1}},i%2?'Reference':'Guide',i+1],meta:[]}))}],errors:[]}});
 });
 await page.goto('/workspace/catalog');await expect(page.getByRole('status')).toContainText('12,840 nodes');
 await expect(page.getByRole('region',{name:'Query guidance'})).toBeVisible();await expect(page.locator('.workspace-inspector')).not.toBeVisible();
 await checkAccessibility(page,info);await shot(page,'01-query-ready');
 await page.getByRole('button',{name:'Use a sample query'}).click();await expect(page.getByText('Sample query loaded. Review it in the editor, then choose Run Query.',{exact:true})).toBeVisible();await expect(page.getByLabel('Cypher query',{exact:true})).toBeFocused();await page.getByLabel('Cypher query',{exact:true}).press('Control+Enter');
 await expect(page.getByRole('heading',{name:'Results',exact:true})).toBeVisible();await expect(page.getByText('success',{exact:true})).toBeVisible();await expect(page.getByRole('button',{name:'Inspect',exact:true}).first()).toBeVisible();await checkAccessibility(page,info);await shot(page,'02-query-results');
 await page.getByRole('button',{name:'History',exact:true}).click();await expect(page).toHaveURL(/history=true/);await page.getByRole('button',{name:'History',exact:true}).click();
 await page.getByText('Execution options',{exact:true}).click();await page.getByRole('checkbox',{name:'Continue on error'}).click();await expect(page.getByRole('checkbox',{name:'Continue on error'})).toBeChecked();await expect(page).toHaveURL(/continueOnError=true/);await page.getByText('Execution options',{exact:true}).click();
 await page.getByRole('button',{name:'Inspect',exact:true}).first().click();await expect(page.locator('.workspace-inspector')).toBeVisible();await checkAccessibility(page,info);await shot(page,'03-inspector');
 await page.getByRole('button',{name:'Close details'}).click();await expect(page.locator('.workspace-inspector')).not.toBeVisible();
 await page.getByRole('tab',{name:'Semantic Search',exact:true}).click();await page.getByRole('textbox',{name:'Search terms'}).fill('getting started');await page.getByRole('button',{name:'Search',exact:true}).click();await expect(page.getByText('Score: 0.94',{exact:true})).toBeVisible();await checkAccessibility(page,info);await shot(page,'04-semantic-search');
 await page.getByLabel('Database',{exact:true}).selectOption('research');await expect(page).toHaveURL(/workspace\/research/);await expect(page.getByRole('status')).toContainText('320 nodes');
 await page.reload();await expect(page.getByRole('tab',{name:'Semantic Search',exact:true})).toHaveAttribute('aria-selected','true');
 await page.setViewportSize({width:390,height:844});await page.goto('/workspace/catalog');await expect(page.getByRole('region',{name:'Query guidance'})).toBeVisible();expect(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth)).toBe(true);await checkAccessibility(page,info);await shot(page,'05-mobile-query');await page.goto('/workspace/catalog/nodes/record-1');await expect(page.getByRole('complementary',{name:'Selected result'})).toBeVisible();await page.getByText('Selected result details',{exact:true}).click();await expect(page).toHaveURL(/inspector=true/);await shot(page,'07-mobile-inspector');await page.getByRole('button',{name:'Close details'}).click();
 await page.setViewportSize({width:1440,height:960});state.queryGraph=true;await page.getByRole('button',{name:'Run Query',exact:true}).click();await page.getByRole('tab',{name:'Graph Explorer',exact:true}).click();await expect(page.getByRole('region',{name:'3D graph workspace'}).locator('canvas')).toBeVisible();await page.getByRole('button',{name:'Pause motion',exact:true}).click();await checkAccessibility(page,info);await shot(page,'06-graph-workspace');expect(errors).toEqual([]);
});
