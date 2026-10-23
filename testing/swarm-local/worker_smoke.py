#!/usr/bin/env python3
"""Run controlled native-tool tasks against both configured Unix model workers.

The MCP server is an explicit admission fixture. Public NornicWorkflow tests
separately verify T3 delegation-record authorization; this script makes no claim
to exercise a real frontier parent or create real T3 delegated-task receipts.
"""
import json, os, subprocess, tempfile, threading, time
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
root = Path(os.environ.get('SWARM_TOOLS_ROOT', '~/.local/share/nornicdb-swarm')).expanduser().resolve()
subprocess.run(['node', '--input-type=module', '-e', 'import {PI_T3_MCP_EXTENSION_SOURCE as s} from "./packages/provider-pi/src/server/mcpExtensionSource.ts"; import {writeFileSync} from "node:fs"; writeFileSync(process.argv[1],s)', str(root / 'worker-smoke-extension.ts')], cwd=root / 'T3', check=True)
failures = 0
report = []

class Handler(BaseHTTPRequestHandler):

    def log_message(self, *args):
        pass

    def do_POST(self):
        r = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        method = r.get('method')
        self.server.calls.append(method)
        if method == 'initialize':
            result = {'protocolVersion': '2025-06-18', 'capabilities': {'tools': {}}, 'serverInfo': {'name': 'worker-smoke-fixture', 'version': '1'}}
        elif method == 'tools/list':
            result = {'tools': [{'name': 'nornic_workflow', 'description': 'Nornic worker admission fixture', 'inputSchema': {'type': 'object', 'properties': {'operation': {'type': 'string'}, 'requestId': {'type': 'string'}}, 'required': ['operation', 'requestId']}}]}
        elif method == 'tools/call':
            operation = r.get('params', {}).get('arguments', {}).get('operation')
            self.server.operations.append(operation)
            response = {'ok': True, 'code': 'WORKER_ADMITTED', 'version': 0, 'requestId': 'fixture', 'data': {'admission': 'delegated-worker', 'taskId': 'fixture-task', 'instructions': 'You are a delegated coding worker. Use native tools only for this task: fix math_ops.py so add(a,b) returns their sum. Read the existing test file and run python3 -m unittest -v test_math_ops.py. Change only math_ops.py. Return the actual test result. Do not route or delegate; admission is already complete.'}}
            result = {'content': [{'type': 'text', 'text': json.dumps(response)}], 'structuredContent': response}
        else:
            result = {}
        data = json.dumps({'jsonrpc': '2.0', 'id': r.get('id'), 'result': result}).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)
for provider, model in [('nornicLocal', 'nornic-ornith'), ('ollamaRemote', 'nornic-agent-remote')]:
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    server.calls = []
    server.operations = []
    threading.Thread(target=server.serve_forever, daemon=True).start()
    with tempfile.TemporaryDirectory(prefix='nornic-worker-smoke-', dir=root) as d:
        workspace = Path(d)
        (workspace / 'math_ops.py').write_text('def add(a, b):\n    return a - b\n')
        (workspace / 'test_math_ops.py').write_text('import unittest\nfrom math_ops import add\nclass Addition(unittest.TestCase):\n    def test_values(self):\n        self.assertEqual(add(2, 3), 5)\n        self.assertEqual(add(-4, 1), -3)\n')
        env = {key: value for key, value in os.environ.items() if key in ('HOME', 'PATH', 'TMPDIR', 'LANG', 'SHELL')}
        env.update(PI_CODING_AGENT_DIR=str(root / 'pi-state'), T3_MCP_URL=f'http://127.0.0.1:{server.server_port}/mcp', T3_MCP_BEARER_TOKEN='smoke-fixture-only', T3_NORNIC_DELEGATED_WORKER='1', T3_PI_RUNTIME_MODE='full-access', T3_PI_MCP_EXTENSION_PATH=str(root / 'worker-smoke-extension.ts'))
        cmd = [str(root / 'pi/node_modules/.bin/pi'), '--provider', provider, '--model', model, '--no-skills', '--no-extensions', '--extension', str(root / 'worker-smoke-extension.ts'), '--mode', 'json', '--print', 'Fix the addition function in math_ops.py and verify the existing tests.']
        start = time.monotonic()
        try:
            r = subprocess.run(cmd, cwd=workspace, env=env, capture_output=True, text=True, timeout=180)
        except subprocess.TimeoutExpired:
            print(json.dumps({'provider': provider, 'error': 'runtime timeout'}), flush=True)
            failures += 1
            server.shutdown()
            continue
        (root / f'worker-smoke-{provider}.log').write_text(r.stdout + '\n' + r.stderr)
        events = []
        for line in r.stdout.splitlines():
            try:
                events.append(json.loads(line))
            except ValueError:
                pass
        tool_calls = [e.get('toolName') for e in events if e.get('type') == 'tool_execution_start']
        proof = subprocess.run(['python3', '-m', 'unittest', '-v', 'test_math_ops.py'], cwd=workspace, capture_output=True, text=True)
        result = {'provider': provider, 'runtime_exit': r.returncode, 'elapsed_seconds': round(time.monotonic() - start, 2), 'tools': tool_calls, 'admission_operations': server.operations, 'tests_passed': proof.returncode == 0, 'changed': (workspace / 'math_ops.py').read_text() != 'def add(a, b):\n    return a - b\n'}
        report.append(result)
        if r.returncode or proof.returncode or (not result['changed']) or (server.operations != ['worker.admit']) or ('bash' not in tool_calls or not any((name in tool_calls for name in ('edit', 'write')))):
            failures += 1
        print(json.dumps(result), flush=True)
    server.shutdown()
(root / 'worker-smoke-results.json').write_text(json.dumps(report, indent=2) + '\n')
raise SystemExit(1 if failures else 0)
