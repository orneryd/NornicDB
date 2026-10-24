#!/usr/bin/env python3
"""Exercise the installed runtime through an explicit MCP admission fixture.

Public NornicWorkflow tests separately verify T3 delegation authorization.
This script does not create real T3 parent/child receipts or call frontier models.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

EXPECTED_REPLY = "Verified assignment answer: NOR-10 — Todo."
WORKER_TASK = (
    "You are a delegated coding worker. Use native edit or write tools to fix "
    "math_ops.py so add(a,b) returns their sum. Read the existing test file and "
    "run python3 -m unittest -v test_math_ops.py. Change only math_ops.py. Return "
    "the actual test result. Do not route or delegate; admission is complete."
)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        if self.path != "/mcp":
            self.server.model_requests.append(self.path)
            self.send_error(500, "Completed replies must not request inference")
            return
        request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        method = request.get("method")
        if method == "initialize":
            result = {
                "protocolVersion": "2025-06-18", "capabilities": {"tools": {}},
                "serverInfo": {"name": "nornic-smoke-fixture", "version": "1"},
            }
        elif method == "tools/list":
            result = {"tools": [{
                "name": "nornic_workflow", "description": "Nornic admission fixture",
                "inputSchema": {"type": "object", "properties": {
                    "operation": {"type": "string"}, "requestId": {"type": "string"},
                }, "required": ["operation", "requestId"]},
            }]}
        elif method == "tools/call":
            self.server.operations.append(request["params"]["arguments"]["operation"])
            result = {
                "content": [{"type": "text", "text": json.dumps(self.server.receipt)}],
                "structuredContent": self.server.receipt,
            }
        else:
            result = {}
        data = json.dumps({"jsonrpc": "2.0", "id": request.get("id"), "result": result}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", choices=("workers", "intake-reply", "intake-native"), default="workers")
    scenario = parser.parse_args().scenario
    worker = scenario == "workers"
    root = Path(os.environ.get("SWARM_TOOLS_ROOT", "~/.local/share/nornicdb-swarm")).expanduser().resolve()
    extension = root / "worker-smoke-extension.ts"
    subprocess.run([
        "node", "--input-type=module", "-e",
        'import {PI_T3_MCP_EXTENSION_SOURCE as s} from "./packages/provider-pi/src/server/mcpExtensionSource.ts"; '
        'import {writeFileSync} from "node:fs"; writeFileSync(process.argv[1],s)', str(extension),
    ], cwd=root / "T3", check=True)
    models = [("nornicLocal", "nornic-ornith")]
    if worker:
        models.append(("ollamaRemote", "nornic-agent-remote"))
    failures, report = 0, []
    for provider, model in models:
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.operations, server.model_requests = [], []
        data = {"admission": "delegated-worker", "taskId": "fixture-task", "instructions": WORKER_TASK} if worker else {
            "kind": "question", "tier": "local", "allowedOperations": ["capabilities"],
            "instructions": "Reply exactly NATIVE_OK without any tools.",
        }
        if scenario == "intake-reply":
            data.update(completedRead={"outcome": "answered", "answer": EXPECTED_REPLY}, allowedOperations=[])
        server.receipt = {"ok": True, "code": "WORKER_ADMITTED" if worker else "OK", "version": 1,
                          "requestId": "fixture", "data": data}
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            with tempfile.TemporaryDirectory(prefix="nornic-runtime-smoke-", dir=root) as directory:
                workspace = Path(directory)
                (workspace / "math_ops.py").write_text("def add(a, b):\n    return a - b\n")
                (workspace / "test_math_ops.py").write_text(
                    "import unittest\nfrom math_ops import add\nclass Addition(unittest.TestCase):\n"
                    "    def test_values(self):\n        self.assertEqual(add(2, 3), 5)\n"
                    "        self.assertEqual(add(-4, 1), -3)\n"
                )
                agent_home = root / "pi-state"
                if scenario == "intake-reply":
                    agent_home = workspace / "agent-state"
                    agent_home.mkdir()
                    config = json.loads((root / "pi-state/models.json").read_text())
                    config["providers"][provider]["baseUrl"] = f"http://127.0.0.1:{server.server_port}/forbidden-inference"
                    (agent_home / "models.json").write_text(json.dumps(config))
                env = {key: value for key, value in os.environ.items() if key in ("HOME", "PATH", "TMPDIR", "LANG", "SHELL")}
                env.update(PI_CODING_AGENT_DIR=str(agent_home), T3_MCP_URL=f"http://127.0.0.1:{server.server_port}/mcp",
                           T3_MCP_BEARER_TOKEN="smoke-fixture-only", T3_PI_RUNTIME_MODE="full-access",
                           T3_PI_MCP_EXTENSION_PATH=str(extension))
                env["T3_NORNIC_DELEGATED_WORKER" if worker else "T3_NORNIC_LOCAL_WORKER"] = "1"
                prompt = "Fix the addition function in math_ops.py and verify the existing tests." if worker else ("Reply exactly NATIVE_OK." if scenario == "intake-native" else "check linear for my assigned ready to work issues")
                command = [str(root / "pi/node_modules/.bin/pi"), "--provider", provider, "--model", model,
                           "--no-skills", "--no-extensions", "--extension", str(extension), "--mode", "json"]
                if not worker:
                    command += ["--no-builtin-tools", "--no-context-files"]
                start = time.monotonic()
                result = subprocess.run([*command, "--print", prompt], cwd=workspace, env=env, capture_output=True, text=True, timeout=180)
                (root / f"{scenario}-smoke-{provider}.log").write_text(result.stdout + "\n" + result.stderr)
                events = []
                for line in result.stdout.splitlines():
                    try:
                        events.append(json.loads(line))
                    except ValueError:
                        pass
                tools = [event.get("toolName") for event in events if event.get("type") == "tool_execution_start"]
                messages = [event["message"] for event in events if event.get("type") == "message_end" and event.get("message", {}).get("role") == "assistant"]
                texts = ["".join(part.get("text", "") for part in message["content"] if part["type"] == "text") for message in messages]
                tokens = [message.get("usage", {}).get("totalTokens", 0) for message in messages]
                evidence = {"scenario": scenario, "provider": provider, "runtime_exit": result.returncode,
                            "elapsed_seconds": round(time.monotonic() - start, 2), "tools": tools,
                            "admission_operations": server.operations}
                if worker:
                    proof = subprocess.run(["python3", "-m", "unittest", "-v", "test_math_ops.py"], cwd=workspace, capture_output=True, text=True)
                    evidence.update(tests_passed=proof.returncode == 0, changed=(workspace / "math_ops.py").read_text() != "def add(a, b):\n    return a - b\n")
                    passed = proof.returncode == 0 and evidence["changed"] and server.operations == ["worker.admit"] and "bash" in tools and any(name in tools for name in ("edit", "write"))
                else:
                    expected = EXPECTED_REPLY if scenario == "intake-reply" else "NATIVE_OK"
                    evidence.update(exact_answer=texts == [expected], token_usage=tokens, unexpected_inference_requests=server.model_requests)
                    passed = texts == [expected] and server.operations == ["intent.route"] and not tools
                    passed = passed and (tokens == [0] and not server.model_requests if scenario == "intake-reply" else sum(tokens) > 0)
                failures += int(result.returncode != 0 or not passed)
                report.append(evidence)
                print(json.dumps(evidence), flush=True)
        except subprocess.TimeoutExpired:
            failures += 1
            print(json.dumps({"provider": provider, "error": "runtime timeout"}), flush=True)
        finally:
            server.shutdown()
            server.server_close()
    name = "worker" if worker else scenario
    (root / f"{name}-smoke-results.json").write_text(json.dumps(report, indent=2) + "\n")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
