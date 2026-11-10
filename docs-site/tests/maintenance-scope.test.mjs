import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFileSync } from "node:child_process";
import { checkMaintenanceScope } from "../tools/maintenance-scope.mjs";

function fixture(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "docs-scope-"));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const git = (...args) =>
    execFileSync("git", args, {
      cwd: root,
      encoding: "utf8",
      stdio: "pipe",
    }).trim();
  git("init", "--quiet", "-b", "docs/maintenance");
  git("config", "user.name", "Docs scope fixture");
  git("config", "user.email", "docs-scope@example.invalid");
  const files = [
    "docs/guide.md",
    "CONTRIBUTING.md",
    "docs-site/tests/example.mjs",
    ".github/workflows/docs-pages.yml",
    "scripts/verify.sh",
    "scripts/check-actions-policy.py",
    "scripts/ci/verify_selftest.sh",
    "pkg/example.go",
    "scripts/graphify_local.py",
    "CHANGELOG.md",
    "docs/release-notes/example.md",
  ];
  for (const file of files) {
    fs.mkdirSync(path.dirname(path.join(root, file)), { recursive: true });
    fs.writeFileSync(path.join(root, file), "baseline\n");
  }
  git("add", ".");
  git("commit", "--quiet", "-m", "scope baseline");
  git("update-ref", "refs/remotes/origin/main", "HEAD");
  return {
    root,
    git,
    change: (file) => fs.appendFileSync(path.join(root, file), "changed\n"),
  };
}

test("docs and explicitly scoped operations pass the real Git diff check", (t) => {
  const f = fixture(t);
  for (const file of [
    "docs/guide.md",
    "CONTRIBUTING.md",
    "docs-site/tests/example.mjs",
    ".github/workflows/docs-pages.yml",
    "scripts/verify.sh",
    "scripts/check-actions-policy.py",
    "scripts/ci/verify_selftest.sh",
  ])
    f.change(file);
  assert.equal(checkMaintenanceScope(f.root, {}), "maintenance");
});

for (const file of [
  "pkg/example.go",
  "scripts/graphify_local.py",
  "CHANGELOG.md",
  "docs/release-notes/example.md",
]) {
  test(`maintenance rejects ${file} in committed and uncommitted diffs`, (t) => {
    const f = fixture(t);
    f.change(file);
    assert.throws(() => checkMaintenanceScope(f.root, {}), {
      message: `Outside docs maintenance scope: ${file}`,
    });
    f.git("add", file);
    f.git("commit", "--quiet", "-m", "outside scope");
    assert.throws(() => checkMaintenanceScope(f.root, {}), {
      message: `Outside docs maintenance scope: ${file}`,
    });
  });
}

test("release metadata admits release product changes while PR head takes precedence", (t) => {
  const f = fixture(t);
  f.change("pkg/example.go");
  assert.equal(
    checkMaintenanceScope(f.root, {
      GITHUB_HEAD_REF: "release/v1.4.4",
      GITHUB_REF_NAME: "main",
    }),
    "release",
  );
  assert.equal(
    checkMaintenanceScope(f.root, { GITHUB_REF_NAME: "release/v1.4.4" }),
    "release",
  );
  assert.throws(
    () =>
      checkMaintenanceScope(f.root, {
        GITHUB_HEAD_REF: "docs/maintenance",
        GITHUB_REF_NAME: "release/v1.4.4",
      }),
    /pkg\/example\.go/,
  );
  assert.throws(
    () =>
      checkMaintenanceScope(f.root, {
        GITHUB_HEAD_REF: "release-not-a-branch",
      }),
    /pkg\/example\.go/,
  );
});

test("local release heads are recognized and detached heads remain strict", (t) => {
  const f = fixture(t);
  f.change("pkg/example.go");
  f.git("checkout", "--quiet", "-b", "release/v1.4.4");
  assert.equal(checkMaintenanceScope(f.root, {}), "release");
  f.git("checkout", "--quiet", "--detach");
  assert.throws(() => checkMaintenanceScope(f.root, {}), /pkg\/example\.go/);
});
