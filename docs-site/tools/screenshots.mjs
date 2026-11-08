import { chromium } from "@playwright/test";
import fs from "node:fs";
fs.mkdirSync(".screenshots", { recursive: true });
const browser = await chromium.launch({ channel: "chrome", headless: true });
const page = await browser.newPage({
  viewport: { width: 1440, height: 1050 },
  colorScheme: "dark",
  reducedMotion: "reduce",
});
for (const [name, url, selector] of [
  ["home", "http://127.0.0.1:4173/NornicDB/", ".VPHero"],
  ["quick-start", "http://127.0.0.1:4173/NornicDB/start", ".vp-doc h1"],
  [
    "code-explorer",
    "http://127.0.0.1:4173/NornicDB/explore?symbol=pkg%2Fsearch%2Fsearch.go%3AService.Search",
    ".graph3d canvas",
  ],
]) {
  await page.goto(
    url.replace(
      "http://127.0.0.1:4173",
      process.env.DOCS_SCREENSHOT_ORIGIN || "http://127.0.0.1:4173",
    ),
  );
  await page.locator(selector).waitFor({ timeout: 30000 });
  if (name === "code-explorer")
    await page
      .locator('.graph3d[data-state="settled"]')
      .waitFor({ timeout: 30000 });
  await page.waitForTimeout(600);
  await page.screenshot({ path: `.screenshots/${name}.png`, fullPage: false });
  console.log(name, await page.title());
}
const origin = process.env.DOCS_SCREENSHOT_ORIGIN || "http://127.0.0.1:4173";
await page.route("**/api/search?**", (route) =>
  route.fulfill({ status: 429, body: "{}" }),
);
await page.goto(origin + "/NornicDB/search");
await page.getByLabel("Search terms").fill("hybrid search");
await page.locator(".search-result").first().waitFor();
await page.locator(".search-fallback").waitFor();
await page.screenshot({ path: ".screenshots/search-fallback.png" });
const map = await page.request
  .get(origin + "/NornicDB/doc-code.json")
  .then((r) => r.json());
const document = Object.keys(map).find((key) =>
  key.startsWith("/user-guides/"),
);
await page.goto(origin + "/NornicDB" + document);
await page.locator(".related-row button").first().click();
await page.locator(".source-dialog pre").first().waitFor();
await page.screenshot({ path: ".screenshots/source-preview.png" });
await page.unroute('**/api/search?**');
for(const [name,width,height] of [['desktop',1440,1050],['mobile',390,844]]){
  await page.setViewportSize({width,height});
  await page.goto(origin+'/NornicDB/start');
  await page.evaluate(()=>scrollTo(0,450));
  const input=page.getByRole('combobox',{name:'Search documentation and code'});
  await input.click();
  await page.screenshot({path:`.screenshots/search-guidance-${name}.png`});
  await input.fill('hybrid search');
  await page.getByText('Semantic search', {exact: true}).first().waitFor({timeout:10000});
  await page.screenshot({path:`.screenshots/search-results-${name}.png`});
}
await browser.close();
