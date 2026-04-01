import { describe, expect, it } from "vitest";

import {
  isGraphPathRequestDisabled,
  isGraphTemporalRequestDisabled,
} from "./graphExplorerA11y";

describe("graph explorer a11y helpers", () => {
  it("disables path requests until both node ids are present and distinct", () => {
    expect(isGraphPathRequestDisabled("", "node-2")).toBe(true);
    expect(isGraphPathRequestDisabled("node-1", "")).toBe(true);
    expect(isGraphPathRequestDisabled(" node-1 ", "node-1")).toBe(true);
    expect(isGraphPathRequestDisabled(" node-1 ", " node-2 ")).toBe(false);
  });

  it("requires an as-of timestamp for temporal and diff requests", () => {
    expect(isGraphTemporalRequestDisabled("standard", "")).toBe(false);
    expect(isGraphTemporalRequestDisabled("temporal", "")).toBe(true);
    expect(isGraphTemporalRequestDisabled("diff", "   ")).toBe(true);
    expect(isGraphTemporalRequestDisabled("diff", "2026-03-15T00:00:00Z")).toBe(false);
  });
});
