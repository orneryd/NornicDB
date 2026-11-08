import { test, expect } from "@playwright/test";
test("fresh visitors get dark mode and keyboard search", async ({ page }) => {
  await page.goto("");
  await expect(page.locator("html")).toHaveClass(/dark/);
  await page.keyboard.press("Control+k");
  await expect(
    page.getByRole("dialog", { name: "Documentation search", exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("combobox", { name: "Search documentation and code" }),
  ).toBeFocused();
  await expect(page.locator("dialog.search-dialog")).toHaveCount(0);
  await page.keyboard.press("Escape");
  await expect(
    page.getByRole("dialog", { name: "Documentation search", exact: true }),
  ).not.toBeVisible();
});
test("code full-text search survives a busy semantic server", async ({
  page,
}) => {
  await page.route("**/api/search?**", (route) =>
    route.fulfill({ status: 429, body: "{}" }),
  );
  await page.goto("search");
  await page.getByLabel("Search in").selectOption("code");
  await page.getByLabel("Search terms").fill("ensureEmpty");
  await expect(page.locator(".search-result").first()).toBeVisible({
    timeout: 30000,
  });
  await expect(page.locator(".search-result").first()).toContainText(
    "ensureEmpty",
  );
  await expect(page.locator(".search-fallback")).toBeVisible();
  await page
    .getByRole("button", { name: "Dismiss search notification" })
    .click();
  await expect(page.locator(".search-fallback")).not.toBeVisible();
});
test("source previews preserve document context and restore keyboard focus", async ({
  page,
  request,
}) => {
  const map = await request.get("doc-code.json").then((r) => r.json()),
    route = Object.keys(map).find((key) => key.startsWith("/user-guides/"));
  await page.goto(route.slice(1));
  const original = page.url(),
    trigger = page.locator(".related-row button").first();
  await trigger.click();
  const dialog = page.getByRole("dialog", { name: "Related source code" });
  await expect(dialog.locator("pre").first()).toBeVisible({ timeout: 30000 });
  expect(page.url()).toBe(original);
  await page.keyboard.press("Escape");
  await expect(dialog).not.toBeVisible();
  await expect(trigger).toBeFocused();
});
test("historical bundles are pinned and make no dynamic API calls", async ({
  page,
}) => {
  let requests = 0;
  await page.route("**/api/**", (route) => {
    requests++;
    return route.abort();
  });
  await page.goto("http://127.0.0.1:4176/NornicDB/");
  await page
    .getByLabel("Documentation version")
    .selectOption("/NornicDB/versions/v1.4.3/");
  await expect(
    page.getByText("v1.4.3 · archived documentation · browser search only"),
  ).toBeVisible();
  await page.goto("http://127.0.0.1:4176/NornicDB/versions/v1.4.3/search");
  await page.getByLabel("Search terms").fill("hybrid search");
  await expect(page.locator(".search-result").first()).toBeVisible({
    timeout: 30000,
  });
  expect(requests).toBe(0);
});
