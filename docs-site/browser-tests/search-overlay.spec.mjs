import { test, expect } from "@playwright/test";
for (const width of [1440, 390, 320])
  test(`anchored search preserves input and document positions at ${width}px`, async ({
    page,
  }) => {
    await page.setViewportSize({ width, height: 900 });
    await page.route("**/api/search?**", async (route) => {
      await new Promise((resolve) => setTimeout(resolve, 90));
      await route.fulfill({
        status: 200,
        headers: {
          "content-type": "application/json",
          "server-timing": "app;dur=7.25",
          "access-control-allow-origin": "*",
          "access-control-expose-headers": "Server-Timing",
        },
        body: JSON.stringify({
          mode: "semantic",
          results: [
            {
              id: "guide",
              title: "Hybrid search guide",
              url: "/user-guides/hybrid-search",
              text: "Semantic result",
              category: "Documentation",
            },
          ],
        }),
      });
    });
    await page.goto("start");
    await page.evaluate(() => scrollTo(0, 450));
    const input = page.getByRole("combobox", {
        name: "Search documentation and code",
      }),
      before = await input.boundingBox(),
      scroll = await page.evaluate(() => scrollY);
    await input.click();
    const panel = page.getByRole("dialog", { name: "Documentation search" });
    await expect(panel).toBeVisible();
    await expect(panel).toHaveAttribute("aria-modal", "false");
    await expect(panel.locator(".search-guidance")).toBeVisible();
    const focused = await input.boundingBox();
    expect(Math.abs(focused.x - before.x)).toBeLessThan(1);
    expect(Math.abs(focused.y - before.y)).toBeLessThan(1);
    expect(await page.evaluate(() => scrollY)).toBe(scroll);
    await input.fill("hybrid search");
    await expect(
      panel.getByText("Hybrid search guide", { exact: true }),
    ).toBeVisible();
    await expect(panel.locator(".search-guidance")).toHaveCount(0);
    await expect(panel.getByLabel("Semantic search timing")).toContainText(
      "Server 7.25 ms",
    );
    await expect(panel.getByLabel("Semantic search timing")).toContainText(
      /Latency \d+ ms/,
    );
    const bounds = await panel.boundingBox();
    expect(bounds.x).toBeGreaterThanOrEqual(0);
    expect(bounds.x + bounds.width).toBeLessThanOrEqual(width);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth + 1,
      ),
    ).toBe(true);
    expect(await page.evaluate(() => scrollY)).toBe(scroll);
    if (width < 768) {
      await page.setViewportSize({ width, height: 500 });
      await expect
        .poll(async () => {
          const b = await panel.boundingBox();
          return b.y + b.height <= 500;
        })
        .toBe(true);
      expect(await page.evaluate(() => scrollY)).toBe(scroll);
    }
    await input.press("ArrowDown");
    await expect(
      panel.getByRole("link", { name: "Hybrid search guide" }),
    ).toBeFocused();
    expect(await page.evaluate(() => scrollY)).toBe(scroll);
    await page.keyboard.press("Escape");
    await expect(panel).toHaveCount(0);
    await expect(input).toBeFocused();
    await expect(input).toHaveValue("hybrid search");
    expect(await page.evaluate(() => scrollY)).toBe(scroll);
    await input.press("x");
    await expect(panel).toBeVisible();
    expect(await page.evaluate(() => scrollY)).toBe(scroll);
  });
test("version choices are newest first regardless of manifest order", async ({
  page,
}) => {
  await page.route("**/versions.json", (route) =>
    route.fulfill({
      json: [
        { tag: "v1.4.9", url: "/old/" },
        { tag: "v2.0.0", url: "/new/" },
        { tag: "v1.4.10", url: "/middle/" },
      ],
    }),
  );
  await page.goto("");
  await expect(
    page.getByLabel("Documentation version").locator("option"),
  ).toHaveText(["Current", "v2.0.0", "v1.4.10", "v1.4.9"]);
});
