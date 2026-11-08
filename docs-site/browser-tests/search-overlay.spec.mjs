import { test, expect } from "@playwright/test";
test("semantic requests wait until typing stops", async ({ page }) => {
  const queries = [];
  await page.route("**/api/search?**", async (route) => {
    queries.push(new URL(route.request().url()).searchParams.get("q"));
    await route.fulfill({
      json: {
        mode: "semantic",
        results: [
          {
            id: "result",
            title: "Semantic result",
            url: "/start",
            text: "result",
          },
        ],
      },
    });
  });
  await page.goto("start");
  const input = page.getByRole("combobox", {
    name: "Search documentation and code",
  });
  await input.fill("hyb");
  await page.waitForTimeout(350);
  await input.fill("hybrid");
  await page.waitForTimeout(350);
  await input.fill("hybrid search");
  expect(queries).toEqual([]);
  await expect(
    page.getByText("Semantic result", { exact: true }),
  ).toBeVisible();
  expect(queries).toEqual(["hybrid search"]);
  await expect(page.getByLabel("Semantic search timing")).toHaveCount(0);
});
test("a new query cancels an older semantic request", async ({ page }) => {
  const aborted = [];
  let releaseOld;
  const holdOld = new Promise((resolve) => {
    releaseOld = resolve;
  });
  page.on("requestfailed", (request) => {
    if (request.url().includes("/api/search?"))
      aborted.push(new URL(request.url()).searchParams.get("q"));
  });
  await page.route("**/api/search?**", async (route) => {
    const query = new URL(route.request().url()).searchParams.get("q");
    if (query === "old search") await holdOld;
    await route
      .fulfill({
        json: {
          mode: "semantic",
          results: [{ id: query, title: query, url: "/start", text: query }],
        },
      })
      .catch(() => {});
  });
  await page.goto("start");
  const input = page.getByRole("combobox", {
    name: "Search documentation and code",
  });
  const first = page.waitForRequest((request) =>
    request.url().includes("q=old+search"),
  );
  await input.fill("old search");
  await first;
  await input.fill("new search");
  await expect(page.getByRole("link", { name: "new search" })).toBeVisible();
  await expect.poll(() => aborted).toContain("old search");
  await expect(
    page.getByText(
      "Nornic search did not respond. Using full-text search instead.",
    ),
  ).toHaveCount(0);
  releaseOld();
  await page.unrouteAll({ behavior: "wait" });
});
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
    await expect(input).toHaveAttribute("placeholder", "What can we find?");
    await input.click();
    const panel = page.getByRole("dialog", { name: "Documentation search" });
    await expect(panel).toBeVisible();
    await expect(panel).toHaveAttribute("aria-modal", "false");
    await expect(panel.locator(".search-guidance")).toBeVisible();
    await expect(panel.locator(".search-guidance p")).toHaveText(
      "Ask a question or search for a guide, setting, or code.",
    );
    const focused = await input.boundingBox();
    expect(Math.abs(focused.x - before.x)).toBeLessThan(1);
    expect(Math.abs(focused.y - before.y)).toBeLessThan(1);
    expect(await page.evaluate(() => scrollY)).toBe(scroll);
    await input.fill("hybrid search");
    await expect(
      panel.getByText("Hybrid search guide", { exact: true }),
    ).toBeVisible();
    await expect(panel.locator(".search-guidance")).toHaveCount(0);
    await expect(panel.getByLabel("Semantic search timing")).toHaveCount(0);
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
