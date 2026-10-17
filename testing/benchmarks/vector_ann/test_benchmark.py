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
        self.assertEqual("none", captured["NORNICDB_VECTOR_HNSW_QUANTIZATION"])

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

class QuantizationContracts(unittest.TestCase):
    def test_both_engines_request_identical_quantization_search_parameters(self):
        for name in ("sq-int8", "pq-x4", "pq-x8", "pq-x16", "pq-x32", "pq-x64", "bq", "bq-asym4", "bq-asym8"):
            mode = next(m for m in run.MODES if m["name"] == name)
            self.assertIn("nornic", mode)
            self.assertIsNotNone(run.quantization_config(mode))
            for ef in (None, 16, 512):
                self.assertEqual(run.NornicEngine(None).search_params(mode, ef),
                                 run.QdrantEngine(None).search_params(mode, ef))

    def test_pending_or_uncompressed_mode_cannot_publish_compressed_result(self):
        args = type("Args", (), dict(nornic_gpu=False))()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/"server.log"
            engine = run.NornicEngine(args)
            engine.log_path = path
            engine.effective_settings = {}
            for name, strategy in (("sq-int8", "HNSW scalar int8 (1028 encoded bytes)"),
                                   ("pq-x16", "HNSW PQ x16 (4352 encoded bytes)"),
                                   ("bq", "HNSW binary (128 encoded bytes)")):
                mode = next(m for m in run.MODES if m["name"] == name)
                for invalid in ("HNSW", "HNSW (quantization pending)"):
                    path.write_text(f"Vector search strategy: {invalid} (N=1000 vectors)\n")
                    with self.assertRaises(RuntimeError):
                        engine.describe(mode)
                path.write_text(f"Vector search strategy: {strategy} (N=1000 vectors)\n")
                self.assertEqual(strategy, engine.describe(mode))

    def test_rescoring_variant_is_applied_to_both_engines(self):
        args = type("Args", (), dict(quantization_oversampling=8))()
        for name in ("sq-int8", "pq-x16", "bq", "bq-asym8"):
            mode = next(m for m in run.MODES if m["name"] == name)
            for engine in (run.NornicEngine(args), run.QdrantEngine(args)):
                self.assertEqual(8, engine.search_params(mode, 128).quantization.oversampling)

class QuantizationEvidenceContracts(unittest.TestCase):
    def test_unstamped_binary_cannot_start_a_nornic_benchmark(self):
        import numpy as np
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            corpus = root/"corpus"
            corpus.mkdir()
            np.save(corpus/"vectors.npy", np.array([[1, 0]], dtype=np.float32))
            np.save(corpus/"queries.npy", np.array([[1, 0]], dtype=np.float32))
            np.save(corpus/"truth.npy", np.array([[0]], dtype=np.int64))
            (corpus/"meta.json").write_text("{}")
            binary = root/"binary"
            binary.write_text("fixture")
            argv = ["run.py", "--corpus", str(corpus), "--out", str(root/"out"),
                    "--data-root", str(root/"data"), "--nornic-bin", str(binary),
                    "--qdrant-bin", str(binary), "--engines", "nornicdb", "--modes", "exact", "--queries", "1", "--k", "1"]
            with patch.object(sys, "argv", argv), \
                    patch.object(run.subprocess, "run", return_value=SimpleNamespace(stdout="binary: go1.26.6\n\tbuild\tCGO_ENABLED=1\n")), \
                    patch.object(report, "machine", return_value="fixture"), \
                    patch.object(run, "run_one") as start:
                with self.assertRaisesRegex(RuntimeError, "embedded VCS"):
                    run.main()
                start.assert_not_called()
                self.assertFalse((root/"out"/".runner.lock").exists())

    def test_runner_caption_uses_explicit_rescoring_factor(self):
        import numpy as np
        class StopBeforeStartup:
            name, mode_key = "nornicdb", "nornic"
            server = None
            def start(self, mode, data_dir, log):
                self.mode = mode
                raise RuntimeError("stop before startup")
            def close(self):
                pass
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = type("Args", (), dict(tag="rescore8", nornic_env=[], data_root=root, out=root,
                                         keep_data=False, quantization_oversampling=8))()
            engine = StopBeforeStartup()
            mode = next(m for m in run.MODES if m["name"] == "bq")
            with self.assertRaisesRegex(RuntimeError, "stop before startup"):
                run.run_one(engine, mode, args, np.ones((2, 2)), None, None)
            self.assertIn("8x oversampling", engine.mode["title"])
            self.assertNotIn("2x oversampling", engine.mode["title"])

    def test_report_caption_uses_recorded_parameters_without_rewriting_raw_results(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw = dict(engine="nornicdb", mode="bq+rescore8", title="binary (2x oversampling) [rescore8]",
                       runs=[dict(hnsw_ef=None, search_params=dict(quantization=dict(rescore=True, oversampling=8)))])
            path = root/"result.json"
            path.write_text(json.dumps(raw))
            displayed = report.load(root)[("nornicdb", "bq+rescore8")]
            self.assertIn("8x oversampling", displayed["title"])
            self.assertEqual(raw, json.loads(path.read_text()))
