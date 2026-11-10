import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFileSync } from "node:child_process";
const site = path.resolve(import.meta.dirname, ".."),
  root = path.resolve(site, ".."),
  tag = process.argv[2];
const tags = execFileSync("git", ["tag", "--list"], {
  cwd: root,
  encoding: "utf8",
})
  .trim()
  .split("\n");
if (
  !tag ||
  !/^v\d+\.\d+\.\d+(?:[-.][\w.-]+)?$/.test(tag) ||
  !tags.includes(tag)
)
  throw Error("Choose an existing release tag, for example v1.4.3.");
const temp = fs.mkdtempSync(path.join(os.tmpdir(), "nornic-docs-version-")),
  archive = path.join(temp, "source.tar");
execFileSync(
  "git",
  [
    "archive",
    "--format=tar",
    "--output=" + archive,
    tag,
    "docs",
    "pkg",
    "CONTRIBUTING.md",
  ],
  { cwd: root },
);
execFileSync("tar", ["-xf", archive, "-C", temp]);
const originalGraph = fs.readFileSync(path.join(site, ".source-graph.json"));
const env = {
  ...process.env,
  DOCS_SOURCE_ROOT: temp,
  DOCS_SOURCE_REF: tag,
  DOCS_BASE: `/NornicDB/versions/${tag}/`,
  DOCS_VERSION: tag,
  VITE_DOCS_VERSION: tag,
};
try {
  execFileSync(process.execPath, ["tools/graph.mjs"], {
    cwd: site,
    env,
    stdio: "inherit",
  });
  execFileSync(process.execPath, ["tools/prepare.mjs"], {
    cwd: site,
    env,
    stdio: "inherit",
  });
  execFileSync(process.execPath, ["tools/pages.mjs"], {
    cwd: site,
    env,
    stdio: "inherit",
  });
  execFileSync(
    process.execPath,
    [
      "node_modules/vitepress/bin/vitepress.js",
      "build",
      ".",
      "--outDir",
      `.vitepress/dist/versions/${tag}`,
    ],
    { cwd: site, env, stdio: "inherit" },
  );
  const manifestPath = path.join(site, ".vitepress/dist/versions.json"),
    versions = fs.existsSync(manifestPath)
      ? JSON.parse(fs.readFileSync(manifestPath, "utf8"))
      : [];
  const revision = execFileSync("git", ["rev-parse", tag], {
    cwd: root,
    encoding: "utf8",
  }).trim();
  fs.writeFileSync(
    manifestPath,
    JSON.stringify([
      ...versions.filter((v) => v.tag !== tag),
      { tag, revision, url: `/NornicDB/versions/${tag}/` },
    ]),
  );
} finally {
  fs.writeFileSync(path.join(site, ".source-graph.json"), originalGraph);
  execFileSync(process.execPath, ["tools/prepare.mjs"], {
    cwd: site,
    stdio: "inherit",
  });
  execFileSync(process.execPath, ["tools/pages.mjs"], {
    cwd: site,
    stdio: "inherit",
  });
  const temporaryPrefix =
    path.resolve(os.tmpdir()) + path.sep + "nornic-docs-version-";
  if (!path.resolve(temp).startsWith(temporaryPrefix))
    throw Error("Unexpected version temporary directory");
  fs.rmSync(temp, { recursive: true, force: true });
}
console.log(
  `Built ${tag}. Historical search and source links are pinned to its commit; no semantic index is ingested.`,
);
