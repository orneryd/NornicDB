import {test,expect} from '@playwright/test';
import {fixture} from './fixture.mjs';

test('users keep identity pinned and failed inline edits remain visible and retryable',async({page})=>{
 const state=await fixture(page);state.failSave=true;
 await page.goto('/security/admin');
 const identity=page.locator('.body-cell[data-row="alex"][data-column="username"]');
 await expect(identity).toBeVisible();
 await expect(identity).toHaveCSS('position','sticky');
 await page.getByRole('button',{name:'Edit',exact:true}).first().click();
 await expect(page).toHaveURL(/admin\/alex\/edit/);
 const email=page.locator('.body-cell[data-row="alex"][data-column="email"] input');
 await expect(email).toBeVisible();await email.fill('updated@example.test');await email.press('Enter');
 await expect(page.getByRole('alert')).toContainText('Save failed');
 await expect(page.getByLabel('Unsaved changes for alex')).toBeVisible();
 await expect(page.locator('.body-cell[data-row="alex"][data-column="email"]')).toContainText('updated@example.test');
 await page.screenshot({path:'review/screenshots/users-save-error.png',fullPage:false});
 state.failSave=false;await page.getByRole('button',{name:'Retry saving alex'}).click();
 await expect(page.getByText('Saved',{exact:true})).toBeVisible();
 await expect(page.getByLabel('Unsaved changes for alex')).not.toBeVisible();
 expect(state.updates.at(-1).email).toBe('updated@example.test');
});

test('grid filtering is URL owned and survives reload and Back',async({page})=>{
 await fixture(page);await page.goto('/security/admin');
 const filter=page.locator('.ui-grid-filter-input').first();
 await filter.fill('sam');
 await expect(page).toHaveURL(/grid.admin-users-grid=/);
 await expect(page.getByText('alex',{exact:true})).not.toBeVisible();
 await page.reload();await expect(filter).toHaveValue('sam');
 await expect(page.getByText('sam',{exact:true})).toBeVisible();
});

test('query result row details expand and restore through the shared URL bridge',async({page})=>{
 await fixture(page);await page.goto('/workspace/catalog');
 await page.getByRole('button',{name:'Run Query',exact:true}).click();
 const toggle=page.getByRole('button',{name:'Expand details',exact:true}).first();
 await expect(toggle).toBeVisible();await toggle.click();
 await expect(page.getByLabel('Result details')).toBeVisible();
 await expect(page).toHaveURL(/grid.query-results-grid=/);
 await page.goBack();await expect(page.getByLabel('Result details')).not.toBeVisible();
 await page.goForward();await expect(page.getByLabel('Result details')).toBeVisible();
});
