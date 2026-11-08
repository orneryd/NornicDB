import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
const site = path.resolve(import.meta.dirname, ".."),
  dist = path.join(site, ".vitepress/dist");
// A current rebuild must not remove already-generated immutable release bundles.
const prefix = path.join(site, ".vitepress/.archives-"),
  saved = fs.mkdtempSync(prefix);
const archives = ["versions", "versions.json"];
try {
  for (const name of archives)
    if (fs.existsSync(path.join(dist, name))) {
      const source = path.resolve(dist, name),
        target = path.resolve(saved, name);
      const boundary = path.resolve(site, ".vitepress") + path.sep;
      if (!source.startsWith(boundary) || !target.startsWith(boundary))
        throw Error("Unexpected archive path");
      fs.renameSync(source, target);
    }
  const result = spawnSync(
    process.execPath,
    ["node_modules/vitepress/bin/vitepress.js", "build", "."],
    { cwd: site, stdio: "inherit" },
  );
  if (result.error) throw result.error;
  process.exitCode = result.status ?? 1;
} finally {
  fs.mkdirSync(dist, { recursive: true });
  for (const name of archives)
    if (fs.existsSync(path.join(saved, name)))
      fs.renameSync(path.join(saved, name), path.join(dist, name));
  if (!path.resolve(saved).startsWith(path.resolve(prefix)))
    throw Error("Unexpected archive temporary directory");
  fs.rmdirSync(saved);
}
