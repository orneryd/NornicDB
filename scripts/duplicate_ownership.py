"""Current-source Go ownership evidence; never a similarity or exclusion rule."""
import atexit
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
from functools import lru_cache

import graphify_local as gi


@lru_cache(maxsize=2)
def _helper_binary(fingerprint):
    # This caches a compiler tool, never a check result or a reuse decision.
    directory = Path(tempfile.mkdtemp(prefix="nornic-go-bindings-"))
    atexit.register(shutil.rmtree, directory, ignore_errors=True)
    helper = Path(__file__).parent / "go_owner_bindings"
    binary = directory / "bindings"
    sources = [p.name for p in sorted(helper.glob("*.go")) if not p.name.endswith("_test.go")]
    result = subprocess.run(["go", "build", "-o", str(binary), *sources], cwd=helper,
                            capture_output=True, text=True, timeout=120)
    if result.returncode:
        raise RuntimeError("Go ownership helper build failed: " + result.stderr[:2000])
    return binary


def _key(target):
    return (target.get("file"), target.get("name"), target.get("line"),
            target.get("receiver", "").lstrip("*"))


def _object(value):
    return (value.get("file"), value.get("pos"))


class GoOwnership:
    """Bind staged symbols and keep every accepted ownership proof inspectable.

    An inferred Graphify edge is never sufficient. Compiler identities supplement
    extracted edges; copied bodies and forwarding wrappers remain disqualifying.
    """
    def __init__(self, root, graph_path, candidates, similarity, wrapper):
        self.root = Path(root)
        self.similarity, self.wrapper = similarity, wrapper
        self.functions, self.nodes, self.calls = {}, {}, {}
        self._source = {}
        self._bodies, self._pipeline_cache = {}, {}
        paths = {c.source_file for c in candidates
                 if c.source_file.endswith(".go") and (self.root / c.source_file).is_file()}
        if not paths:
            return
        # An extracted owner's surviving caller need not itself cross the
        # added-line threshold. Include that current declaration for reverse
        # ownership proof, without adding it to the candidate/search population.
        candidate_ids = {c.id for c in candidates if c.source_file in paths}
        caller_files = {n["id"]: n.get("source_file") for n in gi.graph_items(graph_path, "nodes")
                        if gi.symbol_kind(n) == "function"}
        for edge in gi.graph_items(graph_path, gi.graph_link_key(graph_path)):
            file = edge.get("source_file")
            if (edge.get("target") in candidate_ids and edge.get("relation") == "calls"
                    and edge.get("_origin") == "ast" and edge.get("confidence") == "EXTRACTED"
                    and file == caller_files.get(edge.get("source"))
                    and file and file.endswith(".go") and (self.root / file).is_file()):
                paths.add(file)
        helper = Path(__file__).parent / "go_owner_bindings"
        fingerprint = hashlib.sha256(b"".join(p.name.encode() + p.read_bytes()
            for p in sorted(helper.glob("*.go")) if not p.name.endswith("_test.go"))).hexdigest()
        result = subprocess.run([str(_helper_binary(fingerprint))],
                                input=json.dumps({"root": str(self.root), "files": sorted(paths)}),
                                capture_output=True, text=True, timeout=300)
        if result.returncode:
            raise RuntimeError("Go ownership binding failed: " + result.stderr[:2000])
        document = json.loads(result.stdout)
        bad_dirs = {str(Path(e.get("path", "")).parent) for e in document.get("errors", [])}
        by_file = {}
        for package in document.get("packages", []):
            if package["dir"] in bad_dirs or any(e.get("kind") == "duplicate_declaration"
                                                  for e in package.get("diagnostics", [])):
                continue
            for source in package.get("files", []):
                for function in source.get("funcs", []):
                    if not function.get("body"):
                        continue
                    function = {**function, "file": source["path"], "package": package["import_path"]}
                    by_file.setdefault(source["path"], []).append(function)
        for node in gi.graph_items(graph_path, "nodes"):
            if gi.symbol_kind(node) != "function" or node.get("source_file") not in by_file:
                continue
            name = node.get("label", "").split("(", 1)[0].rsplit(".", 1)[-1]
            line = gi.parse_location(node.get("source_location"))
            found = [f for f in by_file[node["source_file"]] if f["name"] == name
                     and f["decl"]["line"] <= (line or 0) <= f["decl"]["end_line"]]
            if len(found) == 1:
                self.nodes[node["id"]] = {"id": node["id"], "file": node["source_file"],
                                          "label": node.get("label"), "line": line}
                self.functions[node["id"]] = found[0]
        identities = {}
        for identity, function in self.functions.items():
            identities.setdefault((function["file"], function["name"], function["decl"]["line"],
                                   function.get("receiver", "").lstrip("*")), []).append(identity)
        self._targets = {key: values[0] for key, values in identities.items() if len(values) == 1}
        for identity, function in self.functions.items():
            self.calls[identity] = {self.target(call): "L" + str(call["line"])
                                    for call in function["calls"] if self.target(call)}

    def target(self, call):
        if call.get("kind") != "free" or not call.get("target"):
            return None
        return self._targets.get(_key(call["target"]))

    def add_extracted_calls(self, calls):
        for identity, targets in calls.items():
            self.calls.setdefault(identity, {}).update(targets)

    def body(self, identity):
        if identity in self._bodies:
            return self._bodies[identity]
        function = self.functions[identity]
        path = function["file"]
        if path not in self._source:
            self._source[path] = (self.root / path).read_bytes()
        span = function["decl"]
        self._bodies[identity] = self._source[path][span["start"]:span["end"]].decode("utf-8")
        return self._bodies[identity]

    def proof(self, candidate, hit, threshold):
        if candidate.id not in self.functions or hit.id not in self.functions:
            return None
        evidence = None
        if (not self.functions[hit.id].get("is_method")
                and hit.id in self.calls.get(candidate.id, {})):
            evidence = {"kind": "current_owner_call", "call_location": self.calls[candidate.id][hit.id]}
        elif (not self.functions[candidate.id].get("is_method")
              and candidate.id in self.calls.get(hit.id, {})):
            evidence = {"kind": "extracted_owner", "call_location": self.calls[hit.id][candidate.id]}
        else:
            pipeline = self._shared_test_pipeline(candidate.id, hit.id)
            if pipeline:
                evidence = {"kind": "shared_test_execution", **pipeline}
        if evidence is None:
            return None
        left, right = self.body(candidate.id), self.body(hit.id)
        if self.wrapper(left) is not False or self.wrapper(right) is not False:
            return None
        if max(self.similarity(left, hit.body)["score"],
               self.similarity(left, right)["score"]) >= threshold:
            return None
        if evidence:
            return {"candidate": self.nodes[candidate.id], "match": self.nodes[hit.id], **evidence}
        return None

    @staticmethod
    def _framework(call):
        target = call.get("target") or {}
        package = call.get("import_path") or target.get("package")
        return package in ("testing", "github.com/stretchr/testify/require", "github.com/stretchr/testify/assert")

    def _pipelines(self, identity):
        if identity in self._pipeline_cache:
            return self._pipeline_cache[identity]
        function = self.functions[identity]
        calls = [c for c in function["calls"] if not c.get("in_func_literal")]
        writes = [_object(value) for value in function.get("writes", [])]
        found = {}
        for setup in calls:
            setup_id = self.target(setup)
            if not setup_id:
                continue
            for resource in setup.get("assigned", []):
                # A typed resource is constructed once and passed to the execution
                # owner. A same-name variable or intervening reassignment is no proof.
                if not resource.get("type", "").startswith("*") or writes.count(_object(resource)) != 1:
                    continue
                for execution in calls:
                    execution_id = self.target(execution)
                    if not execution_id or setup_id == execution_id or setup["line"] >= execution["line"]:
                        continue
                    if _object(resource) in [_object(arg) for arg in execution.get("args", [])]:
                        found[(setup_id, execution_id)] = {"setup": setup_id, "execution": execution_id,
                            "resource_type": resource["type"], "setup_line": setup["line"],
                            "execution_line": execution["line"]}
        self._pipeline_cache[identity] = found
        return found

    def _shared_test_pipeline(self, left, right):
        a, b = self.functions[left], self.functions[right]
        if {a.get("test_kind"), b.get("test_kind")} != {"test", "benchmark"}:
            return None
        if not a.get("test_signature_ok") or not b.get("test_signature_ok"):
            return None
        if a.get("unresolved_writes", 0) or b.get("unresolved_writes", 0):
            return None
        if ({_object(value) for value in a.get("state_writes", [])}
                & {_object(value) for value in b.get("state_writes", [])}):
            # Shared dummy calls must not hide repeated state changes that
            # still live outside the shared owners.
            return None
        pipelines_a, pipelines_b = self._pipelines(left), self._pipelines(right)
        common_calls = self.calls.get(left, {}).keys() & self.calls.get(right, {}).keys()
        for key in sorted(pipelines_a.keys() & pipelines_b.keys()):
            setup, execution = key
            if any(self.wrapper(self.body(owner)) is not False for owner in key):
                continue
            safe = True
            for function, pipeline in ((a, pipelines_a[key]), (b, pipelines_b[key])):
                for call in function["calls"]:
                    if call["kind"] == "builtin" or self._framework(call):
                        continue
                    target = self.target(call)
                    if target not in common_calls:
                        safe = False
                        break
                    if call.get("in_func_literal") and call.get("outer_call_line") != pipeline["setup_line"]:
                        safe = False
                        break
                if not safe:
                    break
            if safe and pipelines_a[key]["resource_type"] == pipelines_b[key]["resource_type"]:
                return {"owners": [self.nodes[setup], self.nodes[execution]],
                        "resource_type": pipelines_a[key]["resource_type"]}
        return None
