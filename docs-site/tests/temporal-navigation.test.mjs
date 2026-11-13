import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

const site = path.resolve(import.meta.dirname, "..");
const generated = path.join(site, ".generated");
const config = fs.readFileSync(
  path.join(site, ".vitepress/config.mts"),
  "utf8",
);
const temporalLinks = [
  ["Overview", "/temporal-patterns/index"],
  ["Epoch records", "/user-guides/canonical-graph-ledger"],
  ["Independent constraints", "/user-guides/managing-constraints"],
  ["Retained as-of history", "/user-guides/historical-reads-mvcc-retention"],
  [
    "Read and constraint costs",
    "/performance/temporal-read-and-constraint-costs",
  ],
];

function sidebarSection(name) {
  const start = config.indexOf(`section("${name}"`);
  assert.notEqual(start, -1, `sidebar declares a ${name} section`);
  const end = config.indexOf("]),", start);
  assert.notEqual(end, -1, `sidebar ${name} section is closed`);
  return [
    ...config
      .slice(start, end)
      .matchAll(/\[\s*"([^"]+)",\s*"([^"]+)"\s*,?\s*\]/g),
  ].map((match) => [match[1], match[2]]);
}

test("the temporal sidebar links its landing page, guides and costs in reading order", () => {
  assert.deepEqual(sidebarSection("Temporal patterns"), temporalLinks);
});

test("each temporal sidebar target generates a page and a search record", () => {
  const search = JSON.parse(
    fs.readFileSync(path.join(generated, "public/search-index.json"), "utf8"),
  );
  for (const [text, link] of temporalLinks) {
    const page = path.join(generated, `${link}.md`);
    assert.ok(fs.existsSync(page), `${text}: ${link}`);
    const record = search.find((doc) => doc.url === link);
    assert.ok(record, `${text}: ${link}`);
    assert.equal(record.url, link);
    assert.ok(record.title.trim(), `${text}: title`);
    assert.ok(record.text.trim(), `${text}: body`);
  }
});

test("the temporal landing page separates epochs, constraints and retained history", () => {
  const page = fs.readFileSync(
    path.join(generated, "temporal-patterns/index.md"),
    "utf8",
  );
  assert.match(page, /^# Temporal patterns$/m);
  for (const [, link] of temporalLinks.slice(1))
    assert.ok(page.includes(`](${link}.md)`), link);
  assert.match(page, /\bepoch/i);
  assert.match(page, /\bconstraint/i);
  assert.match(page, /as of|as-of|asOf/i);
  assert.match(page, /1\.4\.x/);
  assert.match(page, /deprecated/i);
  assert.match(page, /no storage migration/i);
  assert.match(page, /old\s+positional read remains an interval lookup/i);
  assert.match(page, /null start means all earlier time/i);
  assert.doesNotMatch(page, /^```/m);
});

test("the user guide index routes readers to the temporal patterns page", () => {
  const page = fs.readFileSync(
    path.join(generated, "user-guides/index.md"),
    "utf8",
  );
  assert.ok(page.includes("](/temporal-patterns/index.md)"));
});
