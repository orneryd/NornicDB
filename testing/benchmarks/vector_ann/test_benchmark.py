"""Contract regressions for the benchmark's isolation and published measurements."""
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import report
import run


class ReportContracts(unittest.TestCase):
    def fixture(self, root, mode="hnsw-fast+rerank", k=5):
        env = dict(k=k, queries=2, warmup=1, concurrency=1, nornic_version="test", nornic_commit="abc",
                   qdrant_version="test", batch=2, machine="test", nornic_gpu=False)
        corpus = dict(vectors=10, dimensions=2, model="synthetic", metric="cosine", roots=["fixture"],
                      kinds={"code": 10}, truth_k=10)
        measurement = dict(hnsw_ef=None, **{f"recall@{k}": 1, "recall@1": 1},
                           mean_ms=1, p50_ms=1, p95_ms=1, p99_ms=1, qps=10)
        result = dict(engine="nornicdb", mode=mode, title=mode, strategy="HNSW", runs=[measurement],
                      load_s=1, load_vectors_per_s=10, index_ready_s=1, disk_bytes=100,
                      rss_bytes_after_load=100, rss_bytes_after_queries=100)
        (root / "environment.json").write_text(json.dumps(env))
        (root / "meta.json").write_text(json.dumps(corpus))
        (root / "result.json").write_text(json.dumps(result))
        return result, env, corpus

    def test_tagged_results_are_in_all_reports(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root)
            with patch.object(sys, "argv", ["report.py", "--results", directory, "--corpus", directory]):
                report.main()
            for filename in ("comparison.md", "nornicdb.md"):
                self.assertIn("hnsw-fast+rerank", (root / filename).read_text())

    def test_nondefault_recall_column_matches_measured_k(self):
        with tempfile.TemporaryDirectory() as directory:
            result, env, corpus = self.fixture(Path(directory))
            text = report.engine_report("nornicdb", {("nornicdb", result["mode"]): result},
                                        [dict(name=result["mode"], title=result["title"])], env, corpus)
            self.assertIn("| recall@5 |", text)
            self.assertNotIn("| recall@10 |", text)


class RunnerContracts(unittest.TestCase):
    def test_inherited_engine_settings_do_not_change_mode(self):
        captured = {}
        class FakeServer:
            def __init__(self, cmd, env, data_dir, log, **kwargs):
                captured.update(env)
        args = type("Args", (), dict(nornic_grpc_port=16334, nornic_http_port=18474,
            nornic_bolt_port=18687, nornic_telemetry_port=19190, nornic_gpu=False, nornic_bin="nornicdb"))()
        with tempfile.TemporaryDirectory() as directory:
            engine = run.NornicEngine(args)
            with patch.dict(run.os.environ, {"NORNICDB_VECTOR_CPU_BRUTE_MAX_N": "999999999",
                "NORNICDB_VECTOR_ANN_QUALITY": "compressed", "NORNICDB_KMEANS_CLUSTERING_ENABLED": "true"}), \
                patch.object(run, "Server", FakeServer), patch.object(run, "wait_until"), \
                patch.object(engine, "stubs", return_value=(None, None)):
                engine.start(next(m for m in run.MODES if m["name"] == "hnsw-fast"), Path(directory), Path(directory)/"log")
        self.assertNotEqual("999999999", captured.get("NORNICDB_VECTOR_CPU_BRUTE_MAX_N"))
        self.assertNotEqual("true", captured.get("NORNICDB_KMEANS_CLUSTERING_ENABLED"))
        self.assertEqual("fast", captured["NORNICDB_VECTOR_ANN_QUALITY"])

    def test_preset_ef_is_explicit_on_qdrant(self):
        engine = run.QdrantEngine(None)
        mode = next(m for m in run.MODES if m["name"] == "hnsw-fast")
        self.assertEqual(50, engine.search_params(mode, None).hnsw_ef)

class ProcessIsolationContracts(unittest.TestCase):
    def test_occupied_port_cannot_measure_an_existing_service(self):
        import socket
        with socket.socket() as listener, tempfile.TemporaryDirectory() as directory:
            listener.bind(("127.0.0.1", 0))
            listener.listen()
            port = listener.getsockname()[1]
            with self.assertRaises(OSError):
                run.Server([sys.executable, "-c", "pass"], run.os.environ, Path(directory),
                           Path(directory)/"server.log", ports=(port,))

    def test_dead_child_is_terminal_even_if_health_would_succeed(self):
        with tempfile.TemporaryDirectory() as directory:
            server = run.Server([sys.executable, "-c", "raise SystemExit(7)"], run.os.environ,
                                Path(directory), Path(directory)/"server.log")
            try:
                server.proc.wait(5)
                with patch.object(run.requests, "get") as health:
                    with self.assertRaises(RuntimeError):
                        server.healthy("http://127.0.0.1:1/health", 1)
                    health.assert_not_called()
            finally:
                server.stop()

    def test_startup_failure_cleans_only_its_private_directory(self):
        import numpy as np
        class BrokenEngine:
            name = "nornicdb"
            mode_key = "nornic"
            server = None
            def start(self, mode, data_dir, log):
                self.data_dir = data_dir
                self.server = unittest.mock.Mock()
                raise RuntimeError("startup failed")
            def close(self):
                pass
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            other = root/"nornicdb-exact"
            other.mkdir()
            marker = other/"belongs-to-another-run"
            marker.write_text("keep")
            args = type("Args", (), dict(tag="", nornic_env=[], data_root=root, out=root, keep_data=False))()
            paths = []
            for _ in range(2):
                engine = BrokenEngine()
                with self.assertRaisesRegex(RuntimeError, "startup failed"):
                    run.run_one(engine, run.MODES[0], args, np.ones((2, 2)), None, None)
                engine.server.stop.assert_called_once()
                self.assertFalse(engine.data_dir.exists())
                paths.append(engine.data_dir)
            self.assertNotEqual(paths[0], paths[1])
            self.assertEqual("keep", marker.read_text())


class ArchiveIntegrityContracts(unittest.TestCase):
    def test_cached_archive_reverified_and_cached_binary_replaced(self):
        import hashlib
        import io
        import tarfile
        import fetch_qdrant
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = root/f"{fetch_qdrant.VERSION}-fixture.tar.gz"
            with tarfile.open(archive, "w:gz") as tar:
                member = tarfile.TarInfo("qdrant")
                member.size = 8
                tar.addfile(member, io.BytesIO(b"verified"))
            digest = hashlib.sha256(archive.read_bytes()).hexdigest()
            binary = root/"private"/"qdrant"
            binary.parent.mkdir()
            binary.write_text("tampered executable")
            fetch_qdrant.install(root, binary, fetch_qdrant.VERSION, "fixture.tar.gz", digest)
            self.assertEqual(b"verified", binary.read_bytes())
            archive.write_bytes(b"corrupt cached archive")
            with self.assertRaisesRegex(ValueError, "checksum mismatch"):
                fetch_qdrant.install(root, binary, fetch_qdrant.VERSION, "fixture.tar.gz", digest)

    def test_each_advertised_platform_has_its_own_digest(self):
        import fetch_qdrant
        self.assertEqual(4, len(fetch_qdrant.DIGESTS))
        self.assertEqual(4, len(set(fetch_qdrant.DIGESTS.values())))
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "explicit QDRANT_SHA256"):
                fetch_qdrant.install(Path(directory), Path(directory)/"qdrant", "v-other", "asset")


class ProvenanceContracts(unittest.TestCase):
    def test_source_revision_comes_from_binary_build_settings(self):
        output = "binary: go1.26.6\n\tbuild\tvcs.revision=actual-binary-revision\n\tbuild\tvcs.modified=true\n"
        completed = run.subprocess.CompletedProcess([], 0, stdout=output, stderr="")
        with patch.object(run.subprocess, "run", return_value=completed) as call:
            settings = run.go_build_settings("/private/build/nornicdb")
        self.assertEqual("actual-binary-revision", settings["vcs.revision"])
        self.assertEqual("true", settings["vcs.modified"])
        self.assertEqual(["go", "version", "-m", "/private/build/nornicdb"], call.call_args.args[0])

    def test_both_api_listeners_belong_to_the_child(self):
        server = object.__new__(run.Server)
        server.proc = unittest.mock.Mock()
        server.proc.poll.return_value = None
        server.ps = unittest.mock.Mock()
        def listener(port):
            return type("Connection", (), {"status": run.psutil.CONN_LISTEN,
                                           "laddr": type("Address", (), {"port": port})()})()
        server.ps.net_connections.return_value = [listener(18001)]
        with patch.object(run.requests, "get", return_value=unittest.mock.Mock(ok=True)):
            self.assertFalse(server.healthy("http://127.0.0.1:18001", 18001, 18002))
            server.ps.net_connections.return_value.append(listener(18002))
            self.assertTrue(server.healthy("http://127.0.0.1:18001", 18001, 18002))


if __name__ == "__main__":
    unittest.main()
