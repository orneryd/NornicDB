import {test,expect} from '@playwright/test';
import {fixture} from './fixture.mjs';
import {checkAccessibility} from './accessibility.mjs';
import {mkdir} from 'node:fs/promises';

test('schema creates and drops a named constraint only after explicit review',async({page},testInfo)=>{
 const state=await fixture(page);await page.goto('/schema/catalog?tab=constraints');
 await expect(page.getByRole('grid',{name:'Constraints',exact:true})).toContainText('document_id');
 await page.getByRole('button',{name:'Create constraint',exact:true}).click();await expect(page).toHaveURL(/schema\/catalog\/constraints\/new/);
 const dialog=page.getByRole('dialog');await dialog.getByLabel('Schema name').fill('email_unique');await dialog.getByLabel('Node label').fill('Person');await dialog.getByRole('textbox',{name:'Properties',exact:true}).fill('email');
 await expect(dialog).toContainText('REQUIRE (n.`email`) IS UNIQUE');expect(state.schema.constraints).toHaveLength(1);
 await dialog.getByRole('button',{name:'Create constraint',exact:true}).click();await expect(dialog).not.toBeVisible();
 await expect(page.getByRole('grid',{name:'Constraints',exact:true})).toContainText('email_unique');
 await page.getByRole('button',{name:'Drop',exact:true}).last().click();await expect(page.getByRole('dialog')).toContainText('email_unique');await expect(page.getByRole('button',{name:'Cancel',exact:true})).toBeFocused();
 await page.keyboard.press('Escape');expect(state.schema.constraints).toHaveLength(2);
 await page.getByRole('button',{name:'Drop',exact:true}).last().click();await page.getByRole('dialog').getByRole('button',{name:'Drop constraint',exact:true}).click();
 await expect(page.getByRole('grid',{name:'Constraints',exact:true})).not.toContainText('email_unique');
 await checkAccessibility(page,testInfo);await mkdir('review/screenshots',{recursive:true});await page.screenshot({path:'review/screenshots/schema.png',fullPage:false});
});

test('schema permission failure preserves the creation form',async({page})=>{
 const state=await fixture(page);state.denySchema=true;await page.goto('/schema/research/indexes/new');
 const dialog=page.getByRole('dialog');await dialog.getByLabel('Schema name').fill('by_title');await dialog.getByLabel('Node label').fill('Document');await dialog.getByRole('textbox',{name:'Properties',exact:true}).fill('title');
 await dialog.getByRole('button',{name:'Create index',exact:true}).click();await expect(dialog.getByRole('alert')).toContainText('Schema permission denied');await expect(dialog.getByLabel('Schema name')).toHaveValue('by_title');
});

test('backup and restore preserve server scope, require confirmation and expose failures',async({page},testInfo)=>{
 const state=await fixture(page);await page.goto('/backups');
 await page.getByLabel('Destination backup file on server').fill('/backups/demo.bin');await page.getByRole('button',{name:'Review backup',exact:true}).click();
 expect(state.operations).toHaveLength(0);await expect(page.getByRole('dialog')).toContainText('all server databases');
 state.failOperation=true;await page.getByRole('button',{name:'Create backup',exact:true}).click();await expect(page.getByRole('alert')).toContainText('Backup storage unavailable');expect(state.operations).toHaveLength(1);
 await page.getByRole('button',{name:'Cancel',exact:true}).click();state.failOperation=false;
 await page.getByRole('combobox',{name:'Operation',exact:true}).selectOption('restore');await page.getByRole('button',{name:'Review restore',exact:true}).click();
 await expect(page.getByRole('button',{name:'Restore server',exact:true})).toBeDisabled();await page.getByLabel('Type RESTORE to confirm').fill('RESTORE');
 await page.getByRole('button',{name:'Restore server',exact:true}).click();await expect(page.getByRole('status',{name:'Backup operation status'})).toContainText('restore complete');
 expect(state.operations.at(-1)).toEqual({path:'/admin/restore',body:{path:'/backups/demo.bin'}});
 await checkAccessibility(page,testInfo);await page.screenshot({path:'review/screenshots/backups.png',fullPage:false});
});

for(const role of ['admin','editor','viewer','token-only'])test(`${role} can generate a personal token and receives complete MCP guidance`,async({page})=>{
 const state=await fixture(page);state.roles=[role];await page.goto('/security');
 await page.getByText('Connect an MCP client',{exact:true}).click();await expect(page.getByText('Read-only accounts can use read tools',{exact:false})).toBeVisible();
 await page.getByLabel('Token Label (Subject)').fill('my-client');await page.getByRole('button',{name:'Generate Token',exact:true}).click();
 await expect(page.getByRole('heading',{name:'Token generated',exact:true})).toBeVisible();expect(state.tokens).toHaveLength(1);
 const configuration=page.locator('pre').filter({hasText:'mcpServers'});await expect(configuration).toContainText('Bearer fixture-personal-token');await expect(configuration).not.toContainText('...');await expect(configuration).toContainText('/mcp');
});

test('guided policy setup previews targets, shows inverse curves and resumes a failed binding',async({page},testInfo)=>{
 const state=await fixture(page);await page.goto('/security/knowledge-policies?db=catalog');
 await page.getByRole('button',{name:'Guided policy setup',exact:true}).click();
 await page.getByLabel('Node labels (colon-separated), or * for all nodes').fill('MemoryEpisode');await page.getByRole('button',{name:'Preview matching entities',exact:true}).click();await expect(page.getByRole('status')).toContainText('Showing 1 matching entities');
 expect(state.policyWrites).toHaveLength(0);await page.getByRole('button',{name:'Next: choose a profile',exact:true}).click();
 await page.getByRole('combobox',{name:'Profile source',exact:true}).selectOption('new');await page.getByRole('textbox',{name:'Profile name',exact:true}).fill('idle_strength');await page.getByRole('combobox',{name:'Direction',exact:true}).selectOption('inverse');
 await expect(page.getByLabel('Minimum score (floor)')).toHaveValue('0.1');await expect(page.getByRole('combobox',{name:'Measure age from',exact:true})).toHaveValue('LAST_ACCESSED');await expect(page.getByRole('region',{name:'Decay curve preview'})).toContainText('0.938');
 await checkAccessibility(page,testInfo);await page.screenshot({path:'review/screenshots/knowledge-policy-guide.png',fullPage:true});
 await page.getByRole('button',{name:'Next: review binding',exact:true}).click();await page.getByRole('textbox',{name:'Binding name',exact:true}).fill('episode_idle');state.failBinding=true;
 await page.getByRole('button',{name:'Apply binding',exact:true}).click();await expect(page.getByRole('alert').filter({hasText:'The binding was not saved'})).toBeVisible();expect(state.profiles.filter(p=>p.Name==='idle_strength')).toHaveLength(1);
 state.failBinding=false;await page.getByRole('button',{name:'Apply binding',exact:true}).click();await expect(page.getByText('Binding episode_idle is saved in catalog.',{exact:false})).toBeVisible();expect(state.profiles.filter(p=>p.Name==='idle_strength')).toHaveLength(1);expect(state.bindings).toHaveLength(1);
});

test('mobile schema creation stays within the viewport and navigation identifies the current area',async({page},testInfo)=>{
 await fixture(page);await page.setViewportSize({width:390,height:844});await page.goto('/schema/catalog/indexes/new');
 await expect(page.getByRole('dialog')).toBeVisible();expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
 const bounds=await page.getByRole('dialog').boundingBox();expect(bounds.x).toBeGreaterThanOrEqual(0);expect(bounds.x+bounds.width).toBeLessThanOrEqual(390);
 await checkAccessibility(page,testInfo);await page.screenshot({path:'review/screenshots/schema-mobile-create.png',fullPage:false});
});

test('policy setup can reopen from a URL without applying any statements',async({page},testInfo)=>{
 const state=await fixture(page);await page.setViewportSize({width:390,height:844});await page.goto('/security/knowledge-policies?db=catalog&kp.setup=target&kp.target=MemoryEpisode');
 const guide=page.getByRole('region',{name:'Guided policy setup',exact:true});await expect(guide).toBeVisible();await expect(page.getByLabel('Node labels (colon-separated), or * for all nodes')).toHaveValue('MemoryEpisode');expect(state.policyWrites).toHaveLength(0);
 expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);await checkAccessibility(page,testInfo);await page.screenshot({path:'review/screenshots/knowledge-policy-guide-mobile.png',fullPage:true});
});
