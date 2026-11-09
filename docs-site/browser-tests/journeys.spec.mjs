import { test, expect } from "@playwright/test";
test("quick start is reachable and query examples are readable", async ({
  page,
}) => {
  await page.goto("");
  await page
    .getByRole("link", { name: "Quick start", exact: true })
    .first()
    .click();
  await expect(
    page.getByRole("heading", { name: /^Quick start/, level: 1 }),
  ).toBeVisible();
  await expect(page.getByText("Expected names:")).toBeVisible();
});
test("search remains usable while semantic service is unavailable", async ({
  page,
}) => {
  await page.route("**/api/search?**", (route) =>
    route.fulfill({ status: 503, body: "{}" }),
  );
  await page.goto("search");
  await page.getByLabel("Search terms").fill("hybrid search");
  await expect(page.locator(".search-result").first()).toBeVisible();
  await expect(
    page.getByRole("status").filter({ hasText: "Browser full-text search" }),
  ).toBeVisible();
  await expect(page.locator(".search-fallback")).toHaveText(
    /Nornic server is busy/,
  );
});
test("source graph navigation updates source and supports keyboard selection", async ({
  page,
}) => {
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.goto("explore");
  await expect(page.locator(".graph-panel pre")).toBeVisible({
    timeout: 30000,
  });
  await expect(page.getByRole("heading", { level: 1 })).not.toContainText(
    "&#8203;",
  );
  const first = await page.locator(".graph-panel h2").textContent();
  await page.getByLabel("Find a symbol or file").fill("Normalize");
  const candidate = page.locator(".symbol-list button").first();
  await candidate.focus();
  await page.keyboard.press("Enter");
  await expect(page.locator(".graph-panel h2")).not.toHaveText(first);
  await expect(page.locator(".graph3d canvas")).toBeVisible();
  await page.getByLabel("Call depth").selectOption("3");
  await page.getByLabel("Relationships").selectOption("out");
  await page.getByRole("button", { name: "Fit graph", exact: true }).click();
  await expect(page.locator(".graph3d")).toHaveAttribute(
    "data-state",
    "settled",
    { timeout: 15000 },
  );
  expect(errors).toEqual([]);
  await expect(
    page.getByRole("link", { name: /View source, lines/ }),
  ).toHaveAttribute(
    "href",
    /github.com\/orneryd\/NornicDB\/blob\/[a-f0-9]{40}/,
  );
});
test("unknown routes become an editable search page", async ({ page }) => {
  await page.goto("unknown-hybrid-search");
  await expect(page.getByLabel("Search terms")).toBeVisible();
  await expect(page.getByLabel("Search terms")).toHaveValue(
    "unknown hybrid search",
  );
});
for (const width of [320, 390])
  test(`mobile code explorer has no horizontal page overflow at ${width}px`, async ({
    page,
  }) => {
    await page.setViewportSize({ width, height: 844 });
    await page.goto("explore");
    await expect(page.locator(".graph-panel pre")).toBeVisible({
      timeout: 30000,
    });
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth + 1,
      ),
    ).toBeTruthy();
  });
