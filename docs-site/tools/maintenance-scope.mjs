import { execFileSync } from "node:child_process";

const operationalFiles = new Set([
  "CONTRIBUTING.md",
  "README.md",
  ".github/workflows/docs-pages.yml",
  "scripts/verify.sh",
  "scripts/check-actions-policy.py",
  "scripts/ci/verify_selftest.sh",
]);

// Release-to-main checks include product changes. Scoped docs maintenance keeps
// the existing restriction, using the same head metadata as release verification.
export function checkMaintenanceScope(root, env = process.env) {
  const git = (...args) =>
    execFileSync("git", args, { cwd: root, encoding: "utf8" }).trim();
  const head =
    env.GITHUB_HEAD_REF ||
    env.GITHUB_REF_NAME ||
    git("rev-parse", "--abbrev-ref", "HEAD");
  const base = (env.GITHUB_BASE_REF || env.VERIFY_BASE || "").replace(
    /^origin\//,
    "",
  );
  if (head.startsWith("release/") || base.startsWith("release/"))
    return "release";

  const changed = git(
    "diff",
    "--name-only",
    env.VERIFY_BASE || "origin/main",
    "--",
  )
    .split("\n")
    .filter(Boolean);
  const outside = changed.filter(
    (file) =>
      (!file.startsWith("docs/") &&
        !file.startsWith("docs-site/") &&
        !operationalFiles.has(file)) ||
      /changelog|release-notes|changes-before/i.test(file),
  );
  if (outside.length)
    throw new Error(`Outside docs maintenance scope: ${outside.join(", ")}`);
  return "maintenance";
}
