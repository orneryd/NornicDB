#!/usr/bin/env python3
"""Author: credentials lane (Codex). Check the existing ANTLR generator output.

Run in the lab, for example with sig/tnrun.sh py. The Makefile remains the
single generator owner. This checker uses an isolated copy, compares every
GENERATED_FILES entry with its committed bytes, and never edits source files.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import tarfile
import tempfile
import urllib.request


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def fetch(url):
    with urllib.request.urlopen(url, timeout=180) as response:
        return response.read()


def java_binary(bootstrap):
    configured = os.environ.get("JAVA_HOME")
    candidates = ([Path(configured) / "bin/java"] if configured else [])
    found = shutil.which("java")
    if found:
        candidates.append(Path(found))
    cache_root = Path.home() / ".cache" / "nornic-antlr-runtime"
    candidates.extend(sorted(cache_root.glob("*/bin/java")))
    # Reuse the previous lab generator's verified runtime cache when present.
    candidates.extend(sorted((Path.home() / ".cache").glob("codex-r73-antlr-*/bin/java")))
    for candidate in candidates:
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return candidate.resolve(), {"source": "configured or cached Java", "path": str(candidate)}
    if not bootstrap:
        raise RuntimeError("Java is required; set JAVA_HOME or use --bootstrap-java in the lab")
    if platform.system() != "Linux" or platform.machine() not in ("x86_64", "amd64"):
        raise RuntimeError("The lab Java bootstrap supports Linux x64; supply JAVA_HOME elsewhere")

    metadata_url = "https://api.adoptium.net/v3/assets/latest/17/hotspot"
    assets = json.loads(fetch(metadata_url))
    binaries = []
    for asset in assets:
        if asset.get("binary"):
            binaries.append(asset["binary"])
        binaries.extend(asset.get("binaries", []))
    binary = next((value for value in binaries if value.get("architecture") == "x64"
                   and value.get("os") == "linux" and value.get("image_type") == "jre"), None)
    if binary is None:
        raise RuntimeError("Adoptium metadata has no Linux x64 Java 17 runtime")
    package = binary["package"]
    checksum = package["checksum"].lower()
    if len(checksum) != 64 or any(ch not in "0123456789abcdef" for ch in checksum):
        raise RuntimeError("The runtime metadata has no SHA256")
    archive = fetch(package["link"])
    if digest(archive) != checksum:
        raise RuntimeError("The Java archive does not match its published SHA256")
    cache_root.mkdir(parents=True, exist_ok=True)
    destination = cache_root / checksum
    if not (destination / "bin/java").is_file():
        with tempfile.TemporaryDirectory(prefix="java-", dir=cache_root) as folder:
            staging = Path(folder)
            archive_path = staging / "runtime.tar.gz"
            archive_path.write_bytes(archive)
            unpacked = staging / "unpacked"
            unpacked.mkdir()
            with tarfile.open(archive_path, "r:gz") as bundle:
                bundle.extractall(unpacked, filter="data")
            entries = list(unpacked.iterdir())
            if len(entries) != 1 or not (entries[0] / "bin/java").is_file():
                raise RuntimeError("The verified Java archive has an unexpected layout")
            if not destination.exists():
                entries[0].rename(destination)
    return destination / "bin/java", {"source": metadata_url, "archive": package["link"], "sha256": checksum}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-java", action="store_true",
                        help="prepare a SHA256-verified Java 17 runtime in the lab user cache when absent")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    source = root / "pkg/cypher/antlr"
    head = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
    java, java_source = java_binary(args.bootstrap_java)
    version = subprocess.run([str(java), "-version"], check=True, capture_output=True, text=True)
    inputs = ("Makefile", "CypherLexer.g4", "CypherParser.g4")
    report = {"author": "credentials lane (Codex): isolated generator qualification",
              "head": head, "inputs": {name: digest((source / name).read_bytes()) for name in inputs},
              "java": java_source, "java_version": (version.stderr or version.stdout).strip(),
              "files": [], "status": "not qualified"}
    with tempfile.TemporaryDirectory(prefix="nornic-antlr-check-") as folder:
        work = Path(folder)
        for name in inputs:
            shutil.copyfile(source / name, work / name)
        # Read the output set from the generator owner instead of duplicating it.
        manifest = work / "check-manifest.mk"
        manifest.write_text(".PHONY: __nornic_generated_files\n"
                            "__nornic_generated_files:\n\t@printf '%s\\n' $(GENERATED_FILES)\n",
                            encoding="utf-8", newline="\n")
        listed = subprocess.check_output(["make", "--no-print-directory", "-s", "-C", str(work),
                                          "-f", "Makefile", "-f", str(manifest),
                                          "__nornic_generated_files"], text=True).splitlines()
        if not listed or len(listed) != len(set(listed)):
            raise RuntimeError("The generator output manifest is empty or duplicated")
        expected = {}
        for name in listed:
            path = Path(name)
            if path.is_absolute() or len(path.parts) != 1:
                raise RuntimeError("A generator output is not a local artifact name: " + name)
            expected[name] = subprocess.check_output(["git", "-C", str(root), "show",
                                                       head + ":pkg/cypher/antlr/" + name])
        command = ["make", "--no-print-directory", "-C", str(work), "JAVA=" + str(java), "generate"]
        print("GENERATOR_COMMAND " + json.dumps(command), flush=True)
        subprocess.run(command, check=True)
        jar = next(work.glob("antlr-*-complete.jar"))
        report["antlr_jar_sha256"] = digest(jar.read_bytes())
        for name in listed:
            actual = (work / name).read_bytes()
            report["files"].append({"path": "pkg/cypher/antlr/" + name,
                                     "expected_sha256": digest(expected[name]), "actual_sha256": digest(actual),
                                     "identical": actual == expected[name]})
    report["status"] = "identical" if all(row["identical"] for row in report["files"]) else "different"
    print("ANTLR_GENERATION_RESULT " + json.dumps(report, sort_keys=True), flush=True)
    return 0 if report["status"] == "identical" else 1


if __name__ == "__main__":
    raise SystemExit(main())
