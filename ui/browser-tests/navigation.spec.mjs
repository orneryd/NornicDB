import {test,expect} from '@playwright/test';
import {fixture} from './fixture.mjs';
test.beforeEach(async({page})=>{await fixture(page)});
test('navigation is discoverable, database counts follow route, tabs survive reload and history',async({page})=>{
 await page.goto('/workspace/catalog');
 await expect(page.getByRole('navigation',{name:'Application'})).toBeVisible();
 await expect(page.getByRole('heading',{name:'Demos',exact:true})).toBeVisible();
 await expect(page.getByRole('status')).toContainText('12,840 nodes');
 await page.getByRole('tab',{name:'Semantic Search',exact:true}).click();
 await expect(page).toHaveURL(/tab=search/);
 await page.reload();await expect(page.getByRole('button',{name:'Search',exact:true})).toBeVisible();
 await page.getByLabel('Database',{exact:true}).selectOption('research');
 await expect(page).toHaveURL(/workspace\/research/);
 await expect(page.getByRole('status')).toContainText('320 nodes');
 await page.goBack();await expect(page.getByRole('status')).toContainText('12,840 nodes');
});
test('maintenance disclosure restores from URL and confirmation sends only selected database',async({page})=>{
 const state=await fixture(page);
 await page.goto('/embeddings?database=research&maintenance=open');
 await page.getByRole('button',{name:'Regenerate database embeddings',exact:true}).click();
 const dialog=page.getByRole('dialog');await expect(dialog).toContainText('research');
 await expect(dialog.getByRole('button',{name:'Cancel'})).toBeFocused();
 await page.keyboard.press('Escape');await expect(dialog).not.toBeVisible();
 await page.getByRole('button',{name:'Regenerate database embeddings',exact:true}).click();
 await dialog.getByRole('button',{name:'Regenerate database embeddings',exact:true}).click();
 await expect.poll(()=>state.requests.length).toBe(1);
 expect(state.requests[0].searchParams.get('database')).toBe('research');
});
test('record edit route resolves ID after reload and provides typed property controls',async({page})=>{
 await page.goto('/workspace/catalog/nodes/record-1/edit?tab=search');
 await expect(page.getByLabel('Value for revision')).toHaveAttribute('type','number');
 await expect(page.getByLabel('Value for published')).toHaveRole('combobox');
 await page.reload();await expect(page.getByLabel('Value for revision')).toHaveValue('3');
});
test('mobile navigation uses an accessible disclosure with URL state',async({page})=>{
 await page.setViewportSize({width:390,height:844});await page.goto('/');
 const navigation=page.getByText('Navigation',{exact:true});await navigation.click();
 await expect(page).toHaveURL(/navigation=open/);
 await expect(page.getByRole('link',{name:'Users',exact:true})).toBeVisible();
 await page.reload();await expect(page.getByRole('link',{name:'Users',exact:true})).toBeVisible();
});
