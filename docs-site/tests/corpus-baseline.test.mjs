import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFileSync, spawnSync } from "node:child_process";

function fixture(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "docs-corpus-base-"));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const git = (...args) =>
    execFileSync("git", args, {
      cwd: root,
      encoding: "utf8",
      stdio: "pipe",
    }).trim();
  git("init", "--quiet", "-b", "main");
  git("config", "user.name", "Corpus baseline fixture");
  git("config", "user.email", "corpus@example.invalid");
  fs.mkdirSync(path.join(root, "docs"));
  fs.writeFileSync(path.join(root, "docs/guide.md"), "Existing guide\n");
  git("add", ".");
  git("commit", "--quiet", "-m", "recorded base");
  const base = git("rev-parse", "HEAD");
  fs.writeFileSync(path.join(root, "docs/later.md"), "Later upstream guide\n");
  git("add", ".");
  git("commit", "--quiet", "-m", "advance upstream after qualification starts");
  git("update-ref", "refs/remotes/origin/main", "HEAD");
  git("checkout", "--quiet", "--detach", base);
  const site = path.join(root, "docs-site");
  for (const file of [
    "tests/corpus.test.mjs",
    "tools/content-policy.mjs",
    "tools/maintenance-scope.mjs",
    "content-policy.json",
  ]) {
    const dest = path.join(site, file);
    fs.mkdirSync(path.dirname(dest), { recursive: true });
    fs.copyFileSync(path.resolve(file), dest);
  }
  fs.writeFileSync(
    path.join(site, "corpus-manifest.json"),
    JSON.stringify({ files: [{ path: "guide.md" }] }),
  );
  return {
    root,
    base,
    run(ref) {
      const env = { ...process.env };
      delete env.VERIFY_BASE;
      delete env.NODE_TEST_CONTEXT;
      if (ref !== undefined) env.VERIFY_BASE = ref;
      return spawnSync(
        process.execPath,
        [
          "--test",
          "--test-name-pattern=main documentation is retained|generated inventory covers",
          "tests/corpus.test.mjs",
        ],
        { cwd: site, env, encoding: "utf8" },
      );
    },
  };
}

test("corpus checks retain the recorded base when origin/main advances", (t) => {
  const f = fixture(t);
  const result = f.run(f.base);
  assert.equal(result.status, 0, result.stdout + result.stderr);
  assert.match(result.stdout, /main documentation is retained/);
  assert.match(result.stdout, /generated inventory covers/);
});

test("corpus checks still reject missing documentation from the recorded base", (t) => {
  const f = fixture(t);
  fs.unlinkSync(path.join(f.root, "docs/guide.md"));
  const result = f.run(f.base);
  assert.notEqual(result.status, 0);
  assert.match(result.stdout + result.stderr, /docs\/guide\.md/);
});

test("standalone corpus checks compare origin/main and reject an invalid explicit base", (t) => {
  const f = fixture(t);
  const current = f.run();
  assert.notEqual(current.status, 0);
  assert.match(current.stdout + current.stderr, /docs\/later\.md/);
  const invalid = f.run("refs/heads/absent-base");
  assert.notEqual(invalid.status, 0);
  assert.match(invalid.stdout + invalid.stderr, /absent-base/);
});
