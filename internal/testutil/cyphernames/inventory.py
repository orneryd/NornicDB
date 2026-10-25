"""Conservative source census, separate from semantic execution.

Pins executable-name construction cues, including globals/templates and runtime
peers. This is an admission ledger, not an arbitrary-data-flow safety analysis.
Every new cue requires a subject or a written source disposition in the ledger.
"""
import hashlib
import json
import pathlib
import re
import subprocess

ROOTS = {"apoc", "internal", "pkg", "plugins", "scripts", "ui", "macos", "cmd", "lib", "resolvers", "neural", "experiments"}
EXTENSIONS = {".go", ".mjs", ".js", ".ts", ".tsx", ".py", ".swift"}
QUERY = re.compile(r"[\"'`]\s*(?:MATCH|MERGE|CREATE|RETURN|SET|REMOVE|DROP|WITH|UNWIND|CALL|REQUIRE|YIELD)\b")
OWNER = re.compile(r"\b(?:QuoteName|NameText|quoteName|quote_name|quoteCypherIdent\w*|quoteSchemaName|projectionVariableText|remoteQuotedName|labelExpressionNameText)\s*\(")
SLOT = re.compile(r"(?:\w[:.]\s*(?:\$\{|\{[a-zA-Z_])|(?:AS|IS)\s+(?:\$\{|%[sv]|\{[a-zA-Z_]))")
# Cheap standalone fragment cues, deliberately not arbitrary data-flow analysis.
FRAGMENT = re.compile(r"(?:['\"](?:\[:|:|`)['\"]\s*\+\s*[A-Za-z_$]|[A-Za-z_$][\w.$]*\s*\+\s*['\"](?:\[:|:|`)['\"])")


def candidates(path, source):
    hits = {}
    for number, line in enumerate(source.splitlines(), 1):
        text = line.strip()
        if text.startswith(("//", "#", "*")):
            continue
        construction = any(c in text for c in ("+", "${", "fmt.", "Sprintf", "Fprintf", "WriteString", 'f"', "f'", ".format(", "join(", "Join(", "{{", "\\("))
        if OWNER.search(text) or SLOT.search(text) or (path.endswith((".mjs", ".ts", ".py")) and FRAGMENT.search(text)) or (QUERY.search(text) and construction and any(c in text for c in ('"', "'", '`'))):
            # Keep the complete source cue, so a changed key/alias/global within
            # an already inventoried file cannot inherit the old admission.
            key = hashlib.sha256((path + "\0" + text).encode()).hexdigest()
            hits[key] = {"line": number, "source": text}
    return hits


def discover(root):
    files = subprocess.check_output(["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=root).decode().split("\0")
    found = {}
    for file in sorted(set(files)):
        p = pathlib.PurePosixPath(file)
        if p.suffix not in EXTENSIONS or not (p.parts[0] in ROOTS or len(p.parts) == 1):
            continue
        if any(part in {"node_modules", "vendor", "antlr", "testdata", "__pycache__", "dist"} for part in p.parts):
            continue
        if any(part in {"testing", "testutil"} for part in p.parts) or re.search(r"(?:_test\.go$|(?:^|/)(?:test_|.*_test\.|.*_probe\.)|\.d\.ts$)", file):
            continue
        hits = candidates(file, (root / file).read_text(encoding="utf-8"))
        if hits:
            found[file] = hits
    return found


def validate(root, manifest):
    errors = []
    groups = manifest["groups"]
    if [g["id"] for g in groups] != list(range(1, 80)):
        errors.append("the r5 population must contain exactly groups 1..79")
    for group in groups:
        if not group.get("files") or any(not (root / f).is_file() for f in group["files"]):
            errors.append(f"group {group['id']} has a missing source file")
        if not group.get("subject") and not group.get("disposition"):
            errors.append(f"group {group['id']} has neither subject nor disposition")
        # Historical rows are source dispositions only. Execution columns live
        # exclusively in the generated runtime ledger, never in test symbols.
        if "subject" in group or "proof_scope" in group:
            errors.append(f"group {group['id']} contains obsolete hand-written execution evidence")
    actual = discover(root)
    admitted = manifest["discovery"]
    for file in sorted(set(actual) | set(admitted)):
        record = admitted.get(file, {})
        if any(g not in range(1, 80) for g in record.get("groups", [])):
            errors.append(f"{file}: invalid population group")
        if not record.get("groups") and not record.get("disposition"):
            errors.append(f"{file}: discovered construction has no group or disposition")
        for key in sorted(set(actual.get(file, {})) - set(record.get("cues", {}))):
            hit = actual[file][key]
            errors.append(f"{file}:{hit['line']}: unadmitted construction: {hit['source']}")
        for key in set(record.get("cues", {})) - set(actual.get(file, {})):
            errors.append(f"{file}: stale construction {key}; reconcile removal with its subject")
    return errors


if __name__ == "__main__":
    root = pathlib.Path(__file__).resolve().parents[3]
    manifest = json.loads((root / "internal/cyphertext/name_emission_inventory.json").read_text())
    errors = validate(root, manifest)
    if errors:
        raise SystemExit("\n".join(errors))
    print(f"79 groups; {len(manifest['discovery'])} source files; all discovered cues admitted")
