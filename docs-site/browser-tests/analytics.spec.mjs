import { test, expect } from '@playwright/test';
// Exercise real visitor collection without disabling the SDK's production bot filter.
test.use({ userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36' });

async function routePublicDocs(context) {
  await context.route('https://orneryd.github.io/**', async route => {
    const url = new URL(route.request().url());
    const response = await route.fetch({url: 'http://127.0.0.1:4173' + url.pathname + url.search});
    await route.fulfill({response});
  });
}

test('analytics requires acceptance, persists, and stops recording after withdrawal', async ({page, context}) => {
  await context.addInitScript(() => {
    Object.defineProperty(navigator, 'webdriver', {get: () => false});
    // Headless Shell also advertises HeadlessChrome through client hints.
    // Model an ordinary visitor across both signals; keep production bot filtering enabled.
    Object.defineProperty(navigator, 'userAgentData', {get: () => ({
      brands: [{brand: 'Chromium', version: '142'}, {brand: 'Google Chrome', version: '142'}],
      mobile: false,
      platform: 'Windows',
    })});
  });
  const events = [], requests = [];
  await context.route('https://*.posthog.com/**', async route => {
    requests.push(route.request().url());
    const body = route.request().postData();
    if (body) {
      const data = JSON.parse(body);
      events.push(...(data.batch || (Array.isArray(data) ? data : [data])));
    }
    await route.fulfill({status: 200, contentType: 'application/json', body: '{"status":1}'});
  });
  await routePublicDocs(context);
  await page.goto('https://orneryd.github.io/NornicDB/start?secret=never-collect#private');
  const prompt = page.getByRole('complementary', {name: 'Allow optional analytics?'});
  await expect(prompt).toBeVisible();
  await page.waitForTimeout(4000);
  expect(requests).toEqual([]);
  await prompt.getByRole('button', {name: 'Accept analytics'}).click();
  await expect(prompt).toBeHidden();
  await expect.poll(() => events.filter(e => e.event === '$pageview').length, {timeout: 20000}).toBe(1);
  const view = events.find(e => e.event === '$pageview');
  expect(view.properties.$current_url).toBe('https://orneryd.github.io/NornicDB/start');
  expect(JSON.stringify(events)).not.toContain('never-collect');
  expect(view.properties.$process_person_profile).toBe(false);
  expect(view.properties.$geoip_disable).toBe(true);
  await page.getByRole('combobox', {name: 'Search documentation and code'}).fill('private-search-phrase');
  await expect.poll(() => events.some(e => e.event === 'docs_search_results'), {timeout: 15000}).toBeTruthy();
  expect(JSON.stringify(events)).not.toContain('private-search-phrase');
  await page.keyboard.press('Escape');
  await context.route('https://nornicdb.duckdns.org:4269/inquiries', route => route.fulfill({status:201, contentType:'application/json', body:'{"accepted":true}'}));
  await page.goto('https://orneryd.github.io/NornicDB/managed-services');
  await expect(prompt).toBeHidden();
  await page.getByLabel('Name',{exact:true}).fill('private-inquiry-name');
  await page.getByLabel('Work email').fill('private-inquiry@example.invalid');
  await page.getByLabel('What would you like to build?').fill('private-inquiry-workload');
  await page.getByRole('checkbox',{name:'You may contact me about this inquiry.'}).check();
  await page.getByRole('button',{name:'Send inquiry'}).click();
  await expect.poll(() => events.some(e => e.event === 'docs_inquiry_submitted'), {timeout:15000}).toBeTruthy();
  expect(events.some(e => e.event === 'docs_inquiry_opened')).toBeTruthy();
  expect(JSON.stringify(events)).not.toContain('private-inquiry');
  const settings = page.getByRole('button', {name: 'Analytics settings'});
  await settings.click();
  await expect(prompt.getByRole('heading')).toBeFocused();
  await prompt.getByRole('button', {name: 'Decline analytics'}).click();
  await expect(settings).toBeFocused();
  requests.length = 0;
  await page.getByRole('link', {name: 'Analytics privacy', exact:true}).click();
  await expect(page).toHaveURL(/\/privacy$/);
  await page.reload();
  await expect(settings).toBeVisible();
  await expect(prompt).toBeHidden();
  await page.waitForTimeout(4000);
  expect(requests).toEqual([]);
});

test('declining on mobile keeps analytics off across visits', async ({page, context}) => {
  await page.setViewportSize({width:320, height:568});
  await routePublicDocs(context);
  const requests = [];
  await context.route('https://*.posthog.com/**', async route => { requests.push(route.request().url()); await route.abort(); });
  await page.goto('https://orneryd.github.io/NornicDB/start');
  await page.getByRole('button', {name: 'Decline analytics'}).click();
  await page.reload();
  const settings = page.getByRole('button', {name: 'Analytics settings'});
  const prompt = page.getByRole('complementary', {name: 'Allow optional analytics?'});
  await expect(settings).toBeVisible();
  await expect(prompt).toBeHidden();
  await settings.click();
  await expect(prompt.getByRole('button', {name: 'Accept analytics'})).toBeInViewport();
  await page.keyboard.press('Escape');
  await expect(prompt).toBeHidden();
  await page.waitForTimeout(4000);
  expect(requests).toEqual([]);
});

for (const [name, property, value] of [['Do Not Track', 'doNotTrack', '1'], ['Global Privacy Control', 'globalPrivacyControl', true]]) {
  test(`${name} keeps analytics off even after saved acceptance`, async ({page, context}) => {
    await context.addInitScript(({property, value}) => {
      Object.defineProperty(navigator, property, {get: () => value});
      localStorage.setItem('nornic-docs-analytics-consent', 'accepted');
    }, {property, value});
    const requests = [];
    await context.route('https://*.posthog.com/**', async route => { requests.push(route.request().url()); await route.abort(); });
    await routePublicDocs(context);
    await page.goto('https://orneryd.github.io/NornicDB/start');
    await expect(page.getByRole('link', {name: 'Analytics privacy', exact:true})).toBeVisible();
    await expect(page.getByRole('button', {name: 'Accept analytics'})).toBeHidden();
    await page.waitForTimeout(4000);
    expect(requests).toEqual([]);
  });
}

test('local preview never sends analytics', async ({page}) => {
  const requests = [];
  page.on('request', request => { if (request.url().includes('posthog.com')) requests.push(request.url()); });
  await page.goto('start');
  await expect(page.getByRole('link', {name: 'Analytics privacy', exact:true})).toBeVisible();
  await expect(page.getByRole('button', {name: 'Accept analytics'})).toBeHidden();
  expect(requests).toEqual([]);
});
