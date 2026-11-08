import { test } from "node:test";
import assert from "node:assert/strict";
import { newestVersions } from "../.vitepress/theme/versions.mjs";
test("versions sort numerically newest first, with stable releases ahead of prereleases", () => {
  const tags = ["v1.4.9", "v1.4.10", "v2.0.0-rc.2", "v2.0.0", "v1.3.99"];
  assert.deepEqual(
    newestVersions(tags.map((tag) => ({ tag }))).map((v) => v.tag),
    ["v2.0.0", "v2.0.0-rc.2", "v1.4.10", "v1.4.9", "v1.3.99"],
  );
  assert.equal(tags[0], "v1.4.9");
});
