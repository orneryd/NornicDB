import { expect, test } from '@playwright/test';
import { fixture } from './fixture.mjs';

for (const [name, suggestion, expected] of [
  ['ordinary alias', 'RETURN 1 AS ordinary', 'RETURN 1 AS ordinary'],
  ['quoted ordinary alias', 'RETURN 1 AS `ordinary`', 'RETURN 1 AS `ordinary`'],
  ['double space in a bare alias', 'RETURN 1 AS `a  b`', 'RETURN 1 AS `a  b`'],
  ['escaped backtick in a bare alias', 'RETURN 1 AS `a```', 'RETURN 1 AS `a```'],
  ['escaped internal backtick', 'RETURN 1 AS `a``b`', 'RETURN 1 AS `a``b`'],
  ['quoted instruction text', 'RETURN 1 AS `a IMPORTANT b`', 'RETURN 1 AS `a IMPORTANT b`'],
  ['quoted newline', 'RETURN 1 AS `a\n  RETURN b`', 'RETURN 1 AS `a\n  RETURN b`'],
]) {
  test('autocomplete preserves ' + name, async ({ page }) => {
    await fixture(page);
    await page.route('**/api/bifrost/autocomplete', route => {
      const query = route.request().postDataJSON().query;
      return route.fulfill({ json: { suggestion: query === 'RETURN 1 AS' ? suggestion : '' } });
    });

    await page.goto('/workspace/catalog');
    const editor = page.getByLabel('Cypher query', { exact: true });
    await editor.fill('RETURN 1 AS');
    await page.getByText(expected, { exact: true }).click();
    await expect(editor).toHaveValue(expected);
  });
}
