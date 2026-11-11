import {test,expect} from '@playwright/test';
import {checkAccessibility} from './accessibility.mjs';
import {fixture} from './fixture.mjs';
import {mkdir} from 'node:fs/promises';
for(const [name,url] of [['code-graph','/graphify?database=catalog'],['galaxy','/demo'],['fleet','/cyber']])test(`${name}: demo remains discoverable and keyboard controls are named`,async({page},testInfo)=>{
 const state=await fixture(page);const errors=[];page.on('pageerror',error=>errors.push(error.message));
 await page.setViewportSize({width:1440,height:1000});await page.goto(url);
 await expect(page.getByRole('navigation',{name:'Application'})).toBeVisible();
 if(name!=='code-graph'){
  await expect(page.getByRole('button',{name:'Seed new demo database'})).toBeVisible();
  expect(state.queries.some(body=>/CREATE DATABASE|MERGE/.test(body.statements[0].statement))).toBe(false);
  await page.getByRole('button',{name:'Seed new demo database'}).click();
 }
 await expect(page.locator('canvas')).toBeVisible();
 await checkAccessibility(page,testInfo);
 expect(errors).toEqual([]);
 await mkdir('review/screenshots',{recursive:true});await page.screenshot({path:`review/screenshots/${name}.png`,fullPage:false});
});
