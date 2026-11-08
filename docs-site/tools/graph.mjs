import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
const site = path.resolve(import.meta.dirname, "..");
const fd = fs.openSync(path.join(site, ".source-graph.json"), "w");
const result = spawnSync(
  "go",
  ["run", "./tools/source-graph/main.go", process.env.DOCS_SOURCE_ROOT || ".."],
  { cwd: site, stdio: ["ignore", fd, "inherit"] },
);
fs.closeSync(fd);
if (result.status !== 0) process.exit(result.status || 1);
