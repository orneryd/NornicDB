import { expect, test } from '@playwright/test';
import { checkAccessibility } from './accessibility.mjs';
import { fixture } from './fixture.mjs';

const databaseId = '58d54b29-4ec9-4057-a070-3581bdbda201';
const roleActions = ['dbms.security.policy.manage', 'dbms.role.create', 'dbms.role.rename',
  'dbms.role.drop', 'dbms.user.show', 'dbms.role.assign', 'dbms.role.remove'];

async function fgacFixture(page) {
  const state = await fixture(page);
  state.roles = ['policy_operator'];
  state.fgacEnabled = true;
  const roles = ['admin', 'public', 'policy_operator'];
  let catalog = {
    definitions: [], assignments: {},
    privileges: [{ key: 'native-public', clause: 'ACCESS ON HOME DATABASE', role: 'public' }],
  };
  let revision = 1;
  const writes = [];
  const changes = [];
  let conflict = false;
  await page.route('**/auth/fgac/effective', (route) => route.fulfill({
    json: { enforced: true, rules: roleActions.map(id => ({ id, action: id, scope: { kind: 'dbms' } })) },
  }));
  await page.route('**/auth/fgac/catalog', async (route) => {
    if (route.request().method() === 'PUT') {
      const body = route.request().postDataJSON();
      writes.push(body);
      if (conflict || body.revision !== String(revision)) return route.fulfill({ status: 409 });
      catalog = { ...catalog, ...body.catalog };
      revision++;
      return route.fulfill({ status: 204 });
    }
    return route.fulfill({ json: { catalog, revision: String(revision), enforced: true, roles, actions: [
      { id: 'graph.node.property.read', resourceKind: 'node', property: true },
      { id: 'dbms.role.create', resourceKind: 'dbms' },
    ] } });
  });
  await page.route('**/db/system/tx/commit', (route) => {
    const { statement, parameters } = route.request().postDataJSON().statements[0];
    if (statement.startsWith('SHOW DATABASES')) {
      const includesIdentity = statement === 'SHOW DATABASES YIELD name, databaseID';
      const result = includesIdentity
        ? { columns: ['name', 'databaseID'], data: [{ row: ['catalog', databaseId] }] }
        : { columns: ['name'], data: [{ row: ['catalog'] }] };
      return route.fulfill({ json: { results: [result], errors: [] } });
    }
    changes.push({ statement, parameters });
    if (statement === 'CREATE ROLE $role') roles.push(parameters.role);
    if (statement === 'GRANT ROLE $role TO $user') {
      state.users.find((account) => account.username === parameters.user).roles.push(parameters.role);
    }
    return route.fulfill({ json: { results: [], errors: [] } });
  });
  return { writes, changes, conflictNextSave: () => { conflict = true; } };
}

test('create a role, stage its scoped entitlement, and assign a member', async ({ page }, info) => {
  const state = await fgacFixture(page);
  await page.goto('/security');
  await page.getByRole('button', { name: 'Database Access', exact: true }).click();
  await expect(page.getByRole('heading', { name: /Fine-grained access/ })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'User-defined roles' })).toHaveCount(0);
  await page.getByRole('button', { name: 'New role', exact: true }).click();
  const roleDialog = page.getByRole('dialog', { name: 'Create role', exact: true });
  await roleDialog.getByLabel('Role name').fill('report_viewer');
  await roleDialog.getByRole('button', { name: 'Create role', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'report_viewer', exact: true })).toBeVisible();

  await page.getByRole('button', { name: 'Create and assign', exact: true }).click();
  const entitlement = page.getByRole('dialog', { name: 'Create entitlement for report_viewer', exact: true });
  await entitlement.getByLabel('Entitlement name').fill('report_title');
  await entitlement.getByRole('searchbox', { name: 'Find an action' }).fill('read properties');
  await expect(entitlement.getByRole('option', { name: /dbms role create/ })).toHaveCount(0);
  await entitlement.getByLabel('Action', { exact: true }).selectOption('graph.node.property.read');
  await entitlement.getByLabel('Labels', { exact: true }).fill('Report');
  await entitlement.getByLabel('Properties', { exact: true }).fill('title');
  await entitlement.getByRole('button', { name: 'Stage assignment' }).click();
  await expect(entitlement.getByRole('alert')).toContainText('Choose the database scope');
  await entitlement.getByLabel('Database', { exact: true }).selectOption({ label: 'catalog' });
  await entitlement.getByRole('button', { name: 'Stage assignment' }).click();

  await expect(page.getByText('Unsaved changes', { exact: true })).toBeVisible();
  await page.getByRole('button', { name: 'Back to Security' }).click();
  const leave = page.getByRole('dialog', { name: 'Leave unsaved changes?' });
  await expect(leave).toBeVisible();
  await leave.getByRole('button', { name: 'Keep editing', exact: true }).click();
  await expect(page.getByRole('button', { name: 'Unassign report_title from report_viewer' })).toBeVisible();
  await page.getByRole('button', { name: 'Save changes', exact: true }).click();
  await expect(page.getByText('All changes saved', { exact: true })).toBeVisible();
  expect(state.writes).toEqual([{ revision: '1', catalog: {
    definitions: [{ id: 'report_title', action: 'graph.node.property.read', scope: {
      kind: 'node', databaseId, types: ['Report'], properties: ['title'],
    } }],
    assignments: { report_viewer: ['report_title'] },
  } }]);

  await page.getByRole('button', { name: 'Members', exact: true }).click();
  await page.getByRole('combobox', { name: 'Add a user', exact: true }).selectOption('sam');
  await page.getByRole('button', { name: 'Assign role', exact: true }).click();
  await expect(page.getByText('Assigned report_viewer to sam.')).toBeVisible();
  expect(state.changes).toContainEqual({ statement: 'GRANT ROLE $role TO $user',
    parameters: { role: 'report_viewer', user: 'sam' } });
  await page.getByRole('button', { name: 'Entitlements', exact: true }).click();
  await checkAccessibility(page, info, 'main');
  await page.setViewportSize({ width: 390, height: 844 });
  await expect(page.getByRole('button', { name: 'Create and assign' })).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);
  await checkAccessibility(page, info, 'main');
});

test('revision conflicts preserve the draft and discard restores the saved catalog', async ({ page }) => {
  const state = await fgacFixture(page);
  await page.goto('/security/database-access');
  await page.getByRole('button', { name: 'Entitlement library', exact: true }).click();
  await page.getByRole('button', { name: 'New entitlement', exact: true }).click();
  const dialog = page.getByRole('dialog', { name: 'Create entitlement', exact: true });
  await dialog.getByLabel('Entitlement name').fill('manage_roles');
  await dialog.getByLabel('Action', { exact: true }).selectOption('dbms.role.create');
  await dialog.getByRole('button', { name: 'Add to catalog', exact: true }).click();
  state.conflictNextSave();
  await page.getByRole('button', { name: 'Save changes', exact: true }).click();
  await expect(page.getByRole('alert')).toContainText('Your draft is kept');
  await expect(page.getByText('manage_roles', { exact: true })).toBeVisible();
  await page.getByRole('button', { name: 'Discard', exact: true }).click();
  await page.getByRole('dialog', { name: 'Discard unsaved changes', exact: true })
    .getByRole('button', { name: 'Discard changes', exact: true }).click();
  await expect(page.getByText('manage_roles', { exact: true })).toHaveCount(0);
  await expect(page.getByRole('button', { name: 'Save changes', exact: true })).toBeDisabled();
});

test('disabled mode uses the existing Database Access layout', async ({ page }) => {
  await fixture(page);
  const fgacRequests = [];
  page.on('request', request => {
    if (/\/auth\/fgac\/|FineGrainedEntitlements|fgacCatalog/.test(request.url())) fgacRequests.push(request.url());
  });
  await page.goto('/security');
  await page.getByRole('button', { name: 'Database Access', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'User-defined roles', exact: true })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Entitlement library', exact: true })).toHaveCount(0);
  expect(fgacRequests).toEqual([]);
});
