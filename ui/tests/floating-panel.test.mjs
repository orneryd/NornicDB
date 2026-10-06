import assert from "node:assert/strict";
import { test } from "node:test";
import { clampPanelBounds } from "../src/utils/floatingPanelBounds.ts";

test("drag bounds account for the full panel dimensions", () => {
  assert.deepEqual(clampPanelBounds({ left: 9999, top: 9999, width: 400, height: 300 }, 1000, 800), {
    left: 584, top: 484, width: 400, height: 300,
  });
  const result = clampPanelBounds({ left: -100, top: -100, width: 400, height: 300 }, 1000, 800);
  assert.equal(result.left, 16);
  assert.equal(result.top, 16);
});

test("resizing and viewport shrinking keep the entire panel visible", () => {
  for (const [width, height] of [[1440, 900], [390, 844], [240, 160], [0, 0]]) {
    const result = clampPanelBounds({ left: 800, top: 600, width: 900, height: 1000 }, width, height);
    assert.ok(result.left >= 0 && result.top >= 0);
    assert.ok(result.left + result.width <= width);
    assert.ok(result.top + result.height <= height);
  }
});