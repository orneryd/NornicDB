import { test, expect } from "@playwright/test";

test("temporal patterns has a dedicated navigation section and a learning path", async ({ page }) => {
  await page.goto("temporal-patterns/index");
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("Temporal patterns");
  const sidebar = page.locator(".VPSidebar .VPSidebarItem.level-0").filter({ has: page.getByText("Temporal patterns", { exact: true }) });
  await expect(sidebar.getByText("Temporal patterns", { exact: true })).toBeVisible();
  for (const route of [
    "temporal-patterns/index",
    "user-guides/canonical-graph-ledger",
    "user-guides/managing-constraints",
    "user-guides/historical-reads-mvcc-retention",
    "performance/temporal-read-and-constraint-costs",
  ]) {
    await expect(sidebar.locator(`a[href="/NornicDB/${route}"]`)).toHaveCount(1);
  }
  await page.locator("main").getByRole("link", { name: "Canonical graph ledger patterns" }).click();
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("Canonical Graph Ledger Patterns");
  await expect(page.locator("main")).toContainText("starts_at:null");
  await expect(page.locator("main")).toContainText("not merely an unknown start");
});

for (const width of [390, 1280]) {
  test(`temporal examples and independent read contracts are readable at ${width}px`, async ({ page }) => {
    await page.setViewportSize({ width, height: 844 });
    for (const route of ["temporal-patterns/index", "user-guides/canonical-graph-ledger", "user-guides/managing-constraints", "user-guides/historical-reads-mvcc-retention"]) {
      await page.goto(route);
      await expect(page.getByRole("heading", { level: 1 })).toBeVisible();
      expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1), route).toBeTruthy();
    }
    await page.goto("user-guides/managing-constraints#epoch-entries-with-their-own-grouping");
    await expect(page.locator("main")).toContainText("temporal.epoch(r.startedAt, r.endedAt) GROUP BY r.tenant, r.employeeId");
    await expect(page.locator("main")).toContainText("nine entries");
    await page.goto("user-guides/historical-reads-mvcc-retention");
    await expect(page.locator("main")).toContainText("balance: 100");
    await expect(page.locator("main")).toContainText("explicit error");
  });
}
