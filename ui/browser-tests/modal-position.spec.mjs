import {test,expect} from '@playwright/test';
import {fixture} from './fixture.mjs';
import {mkdir} from 'node:fs/promises';
const cases=[
 {name:'record',url:'/workspace/catalog/nodes/record-1/edit',field:'Value for title',leave:'Close details'},
 {name:'schema',url:'/schema/catalog/indexes/new',field:'Schema name',leave:'Back to schema'},
 {name:'database',url:'/databases/new',field:'Database Name',leave:'Back to databases'},
];
for(const viewport of [{width:1440,height:960},{width:390,height:844}])for(const task of cases)test(`${task.name} unsaved dialog is centered at ${viewport.width}px`,async({page})=>{
 await fixture(page);await page.setViewportSize(viewport);await page.goto(task.url);
 if(task.name==='record'&&viewport.width<1024)await page.getByText('Selected result details',{exact:true}).click();
 await page.getByRole('textbox',{name:task.field,exact:true}).fill('Unsaved change');await page.getByRole('button',{name:task.leave,exact:true}).click();
 const dialog=page.getByRole('dialog',{name:'Leave unsaved changes?',exact:true});await expect(dialog).toBeVisible();
 const panel=dialog.getByRole('document');const bounds=await panel.boundingBox();expect(bounds).not.toBeNull();
 expect(Math.abs(bounds.x+bounds.width/2-viewport.width/2)).toBeLessThanOrEqual(2);
 expect(Math.abs(bounds.y+bounds.height/2-viewport.height/2)).toBeLessThanOrEqual(2);
 expect(bounds.x).toBeGreaterThanOrEqual(0);expect(bounds.x+bounds.width).toBeLessThanOrEqual(viewport.width);
 await expect(dialog.getByRole('button',{name:'Keep editing',exact:true})).toBeFocused();
 await mkdir('review/prototype',{recursive:true});if(task.name==='record')await page.screenshot({path:`review/prototype/${viewport.width===1440?'13-unsaved-warning':'22-mobile-unsaved-warning'}.png`,fullPage:false});
 await page.keyboard.press('Escape');await expect(dialog).not.toBeVisible();await expect(page.getByRole('textbox',{name:task.field,exact:true})).toHaveValue('Unsaved change');
});
