import { test, expect } from "@playwright/test";

test("development leads to embedding and the canonical contribution guide", async ({
  page,
}) => {
  await page.goto("development/");
  await page
    .locator("main")
    .getByRole("link", { name: "Embed in Go", exact: true })
    .click();
  await expect(page.getByRole("heading", { level: 1 })).toHaveText(
    "Embed NornicDB in a Go application",
  );
  await expect(page.locator("main")).toContainText("ExecuteCypherTyped");
  await page.goto("development/");
  await page
    .locator("main")
    .getByRole("link", { name: "contribution guide", exact: true })
    .click();
  await expect(page.getByRole("heading", { level: 1 })).toHaveText(
    "Contributing to NornicDB",
  );
  await expect(
    page.getByRole("heading", { name: /^Local setup/ }),
  ).toBeVisible();
  await expect(page.locator('a[href*="development/archive"]')).toHaveCount(0);
});

for (const width of [320, 390])
  test(`operating and embedding guides fit ${width}px with usable tables and source links`, async ({
    page,
  }) => {
    await page.setViewportSize({ width, height: 844 });
    for (const route of [
      "operations/memory-management",
      "operations/durability",
      "development/embedding",
      "development/contributing",
    ]) {
      await page.goto(route);
      await expect(page.getByRole("heading", { level: 1 })).toBeVisible();
      if (!route.endsWith("/contributing"))
        await expect(page.locator(".related")).toBeVisible();
      expect(
        await page.evaluate(
          () => document.documentElement.scrollWidth <= innerWidth + 1,
        ),
        route,
      ).toBeTruthy();
      await expect(page.locator('a[href*="development/archive"]')).toHaveCount(
        0,
      );
    }
  });
