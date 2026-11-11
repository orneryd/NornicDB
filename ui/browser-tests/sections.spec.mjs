import {test,expect} from '@playwright/test';
import {checkAccessibility} from './accessibility.mjs';
import {mkdir} from 'node:fs/promises';
import {fixture} from './fixture.mjs';

const sections=[
 ['workspace','/workspace/catalog','NornicDB'],
 ['databases','/databases','Databases'],
 ['embeddings','/embeddings?database=catalog&maintenance=open','Embeddings'],
 ['users','/security/admin','User Management'],
 ['account','/security','Security & API Tokens'],
 ['database-access','/security/database-access','Database Access'],
 ['lifecycle','/security/lifecycle?db=catalog','MVCC Lifecycle'],
 ['retention','/security/retention','Retention Policies'],
 ['knowledge-policies','/security/knowledge-policies?db=catalog','Knowledge Policies'],
 ['pivot','/demos/pivot','Pivot analysis'],
];
for(const [name,url,heading] of sections)test(`${name}: section renders and controls have accessible names`,async({page},testInfo)=>{
 await fixture(page);const errors=[];page.on('pageerror',error=>errors.push(error.message));
 await page.setViewportSize({width:1440,height:1000});await page.goto(url);
 await expect(page.getByRole('heading',{name:heading,exact:true,level:1})).toBeVisible();
 await expect(page.getByText('Connecting to NornicDB...',{exact:true})).not.toBeVisible();
 await checkAccessibility(page,testInfo);
 expect(errors).toEqual([]);
 await mkdir('review/screenshots',{recursive:true});await page.screenshot({path:`review/screenshots/${name}.png`,fullPage:false});
});

test('database edit URL resolves configuration without first opening a card',async({page},testInfo)=>{
 await fixture(page);await page.goto('/databases/catalog/edit');
 await expect(page.getByRole('heading',{name:'Database Settings',exact:true})).toBeVisible();
 await expect(page.getByText('Editing configuration for catalog.')).toBeVisible();
});
