import {test,expect} from '@playwright/test';
import {fixture} from './fixture.mjs';
import AxeBuilder from '@axe-core/playwright';
import {mkdir} from 'node:fs/promises';
async function screenshot(page,name){await mkdir('review/prototype',{recursive:true});await page.screenshot({path:`review/prototype/${name}.png`,fullPage:true});}
async function accessibleEditor(page){const report=await new AxeBuilder({page}).include('.property-edit-session').withTags(['wcag2a','wcag2aa','wcag21aa']).analyze();expect(report.violations.map(v=>({id:v.id,nodes:v.nodes.map(n=>n.html)}))).toEqual([]);}

test('staged typed edits validate without writing, preserve focus and retry a failed save',async({page})=>{
 const state=await fixture(page);Object.assign(state.node.properties,{tags:['graph','docs'],obsolete:'legacy',has_embedding:true});
 const writes=[];let fail=true;
 await page.route('**/db/*/tx/commit',async route=>{
  const request=route.request().postDataJSON(),statement=request.statements[0];
  if(!statement.statement.startsWith('MATCH (n) WHERE id(n)'))return route.fallback();
  writes.push(statement);
  if(fail)return route.fulfill({status:503,json:{message:'Database temporarily unavailable'}});
  const expected={'display title':'Revised guide',published:false,revision:4,tags:['graph','docs'],has_embedding:true};
  state.node.properties=expected;
  return route.fulfill({json:{results:[{columns:['n'],data:[{row:[{elementId:'record-1',labels:['Document'],properties:expected}]}]}],errors:[]}});
 });
 await page.goto('/workspace/catalog/nodes/record-1/edit');
 await expect(page.getByLabel('Value for revision')).toHaveAttribute('type','number');
 await expect(page.getByRole('button',{name:'Save changes',exact:true})).toBeDisabled();
 await expect(page.getByLabel('Value for published')).toHaveRole('combobox');
 await page.getByLabel('Value for revision').fill('');await page.getByLabel('Value for revision').blur();
 await expect(page.getByText('Enter a finite number.',{exact:true})).toBeVisible();
 await page.getByRole('button',{name:'Save changes',exact:true}).click();expect(writes).toHaveLength(0);
 await page.getByLabel('Value for revision').fill('4');await page.getByLabel('Value for published').selectOption('false');
 await page.getByText('Name and type for title',{exact:true}).click();
 const name=page.locator('#property-0-key');await name.fill('display');await name.pressSequentially(' title');await expect(name).toBeFocused();await expect(name).toHaveValue('display title');
 await page.getByLabel('Value for display title').fill('Revised guide');
 await page.getByRole('button',{name:'Remove property obsolete',exact:true}).click();await expect(page.getByText('Will be removed on save',{exact:true})).toBeVisible();
 await page.getByRole('button',{name:'Undo',exact:true}).click();await expect(page.getByLabel('Value for obsolete')).toHaveValue('legacy');
 await page.getByRole('button',{name:'Remove property obsolete',exact:true}).click();
 await page.getByText('Review 4 changes',{exact:true}).click();await accessibleEditor(page);await page.locator('.inspector-content > .overflow-auto').evaluate(el=>el.scrollTop=0);await screenshot(page,'08-edit-review');await page.locator('.inspector-content > .overflow-auto').evaluate(el=>el.scrollTop=el.scrollHeight);await screenshot(page,'12-edit-review-details');
 expect(writes).toHaveLength(0);expect(new URL(page.url()).search).not.toContain('Revised');
 await page.getByRole('button',{name:'Save changes',exact:true}).click();
 await expect(page.getByRole('alert')).toContainText('Your changes are still here');await expect(page.getByLabel('Value for display title')).toHaveValue('Revised guide');
 await accessibleEditor(page);await screenshot(page,'09-edit-save-error');
 expect(writes).toHaveLength(1);
 const sent=writes[0];expect(sent.statement).toContain('n.`display title` = $');expect(sent.statement).toContain('REMOVE n.`title`, n.`obsolete`');expect(sent.statement).not.toContain('has_embedding');
 expect(Object.values(sent.parameters)).toContain(false);expect(Object.values(sent.parameters)).toContain(4);expect(Object.values(sent.parameters)).toContain('Revised guide');
 fail=false;await page.getByRole('button',{name:'Save changes',exact:true}).click();await expect(page).not.toHaveURL(/\/edit/);await expect(page.getByText('Changes saved.',{exact:true})).toBeVisible();
 await expect(page.getByRole('complementary',{name:'Selected result'})).toContainText('Revised guide');await expect(page.getByRole('complementary',{name:'Selected result'})).not.toContainText('obsolete');
 expect(writes).toHaveLength(2);await screenshot(page,'10-edit-saved');
});

test('duplicate and reserved names are explained; Cancel and staged additions do not write',async({page})=>{
 const state=await fixture(page);await page.goto('/workspace/catalog/nodes/record-1/edit');
 await page.getByRole('button',{name:'Add property',exact:true}).click();
 const name=page.locator('#property-3-key');await name.fill('published');await page.getByRole('button',{name:'Save changes',exact:true}).click();await expect(page.getByText('This name is already used. Choose a unique name.',{exact:true}).first()).toBeVisible();
 await name.fill('embedding');await page.getByRole('button',{name:'Save changes',exact:true}).click();await expect(page.getByText('This name is managed by the server. Choose another name.',{exact:true})).toBeVisible();
 await name.fill('extra');await page.getByLabel('Type for extra').selectOption('json');await page.getByLabel('Value for extra').fill('{invalid');await page.getByLabel('Value for extra').blur();await expect(page.getByText('Enter valid JSON, such as [1, 2] or {"key": "value"}.',{exact:true})).toBeVisible();
 await page.getByLabel('Value for extra').fill('[1,2]');
 await page.setViewportSize({width:390,height:844});await page.getByText('Selected result details',{exact:true}).click();await expect(page.getByRole('form',{name:'Edit node properties'})).toBeVisible();await accessibleEditor(page);await screenshot(page,'11-mobile-edit');
 await page.getByRole('button',{name:'Cancel',exact:true}).click();await expect(page).not.toHaveURL(/\/edit/);
 expect(state.queries.filter(q=>q.statements[0].statement.startsWith('MATCH (n) WHERE id(n)'))).toHaveLength(0);expect(state.node.properties).not.toHaveProperty('extra');
});

test('removing the last property sends REMOVE without SET and quotes arbitrary keys',async({page})=>{
 const state=await fixture(page);state.node.properties={'odd` property':'value'};const writes=[];
 await page.route('**/db/*/tx/commit',async route=>{
  const statement=route.request().postDataJSON().statements[0];if(!statement.statement.startsWith('MATCH (n) WHERE id(n)'))return route.fallback();writes.push(statement);state.node.properties={};
  return route.fulfill({json:{results:[{columns:['n'],data:[{row:[{elementId:'record-1',labels:['Document'],properties:{}}]}]}],errors:[]}});
 });
 await page.goto('/workspace/catalog/nodes/record-1/edit');await page.getByRole('button',{name:'Remove property odd` property',exact:true}).click();await page.getByRole('button',{name:'Save changes',exact:true}).click();await expect(page).not.toHaveURL(/\/edit/);
 expect(writes).toHaveLength(1);expect(writes[0].statement).toContain(' REMOVE n.`odd`` property` RETURN n');expect(writes[0].statement).not.toContain(' SET ');
});

test('dirty edits warn on navigation and close; keep editing preserves the draft',async({page})=>{
 await fixture(page);await page.goto('/workspace/catalog/nodes/record-1/edit');await page.getByLabel('Value for title').fill('Unsaved guide');
 await page.getByRole('link',{name:'Databases',exact:true}).click();
 const dialog=page.getByRole('dialog',{name:'Leave unsaved changes?',exact:true});await expect(dialog).toBeVisible();await expect(dialog.getByRole('button',{name:'Keep editing',exact:true})).toBeFocused();
 await page.keyboard.press('Escape');await expect(dialog).not.toBeVisible();await expect(page.getByLabel('Value for title')).toHaveValue('Unsaved guide');
 await page.getByRole('button',{name:'Find similar',exact:true}).click();await expect(dialog).toBeVisible();await dialog.getByRole('button',{name:'Keep editing',exact:true}).click();await expect(page.getByLabel('Value for title')).toHaveValue('Unsaved guide');
 await page.getByLabel('Database',{exact:true}).selectOption('research');await expect(dialog).toBeVisible();await dialog.getByRole('button',{name:'Keep editing',exact:true}).click();await expect(page.getByLabel('Database',{exact:true})).toHaveValue('catalog');await expect(page.getByLabel('Value for title')).toHaveValue('Unsaved guide');
 await page.getByRole('button',{name:'Close details',exact:true}).click();await expect(dialog).toBeVisible();await dialog.getByRole('button',{name:'Keep editing',exact:true}).click();await expect(page).toHaveURL(/\/edit/);
 await page.getByRole('link',{name:'Databases',exact:true}).click();await expect(dialog).toBeVisible();await screenshot(page,'13-unsaved-warning');await dialog.getByRole('button',{name:'Discard changes and leave',exact:true}).click();await expect(page).toHaveURL(/\/databases\/$/);
});

test('browser Back warns, disclosure state does not, and reload uses the native warning',async({page})=>{
 await fixture(page);await page.goto('/workspace/catalog/nodes/record-1');await page.getByRole('button',{name:'Edit',exact:true}).click();await page.getByLabel('Value for title').fill('Uncommitted title');
 await page.getByText('Name and type for title',{exact:true}).click();await expect(page.getByRole('dialog',{name:'Leave unsaved changes?',exact:true})).not.toBeVisible();
 // The first Back restores the disclosure query parameter; the second leaves /edit.
 await page.goBack();await expect(page).not.toHaveURL(/section.edit-property-0/);await page.goBack();
 const dialog=page.getByRole('dialog',{name:'Leave unsaved changes?',exact:true});await expect(dialog).toBeVisible();await dialog.getByRole('button',{name:'Keep editing',exact:true}).click();await expect(page.getByLabel('Value for title')).toHaveValue('Uncommitted title');
 let warned=false;page.once('dialog',async dialog=>{warned=dialog.type()==='beforeunload';await dialog.dismiss();});await page.reload({timeout:3000}).catch(()=>{});expect(warned).toBe(true);await expect(page.getByLabel('Value for title')).toHaveValue('Uncommitted title');
 await page.getByRole('button',{name:'Cancel',exact:true}).click();await expect(page).not.toHaveURL(/\/edit/);await expect(dialog).not.toBeVisible();
});
