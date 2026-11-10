import { test, expect } from '@playwright/test';

for (const width of [390, 1440]) {
  test(`inquiry page retains fields and retries without duplicates at ${width}px`, async ({ page }) => {
    await page.setViewportSize({width, height:900});
    const payloads = [];
    await page.route('https://nornicdb.duckdns.org:4269/inquiries', async route => {
      payloads.push(route.request().postDataJSON());
      await route.fulfill({status: payloads.length === 1 ? 429 : 201, contentType:'application/json', body: JSON.stringify(payloads.length === 1 ? {error:'busy'} : {accepted:true})});
    });
    await page.goto('start');
    if (width < 960) await page.locator('button[aria-controls="VPSidebarNav"]').click();
    const link = page.getByRole('link', {name: 'Managed services', exact:true});
    await expect(link).toBeVisible();
    await link.click();
    const form = page.getByRole('region', {name:'Managed services inquiry'});
    await expect(form).toBeVisible();
    await expect(page).toHaveURL(/\/managed-services$/);
    const box = await form.boundingBox();expect(box.x).toBeGreaterThanOrEqual(0);expect(box.x+box.width).toBeLessThanOrEqual(width);
    await form.getByLabel('Name',{exact:true}).fill('Private name');
    await form.getByLabel('Work email').fill('private@example.invalid');
    await form.getByLabel('What would you like to build?').fill('Private workload');
    await form.getByRole('checkbox').check();
    await form.getByRole('button',{name:'Send inquiry'}).click();
    await expect(form.getByRole('alert')).toContainText('busy');
    await expect(form.getByLabel('Name',{exact:true})).toHaveValue('Private name');
    await form.getByRole('button',{name:'Send inquiry'}).click();
    await expect(form.getByRole('status')).toContainText('received');
    expect(payloads).toHaveLength(2);expect(payloads[0].requestId).toBe(payloads[1].requestId);
    await expect(form.getByLabel('Name',{exact:true})).toHaveCount(0);
  });
}
