#!/usr/bin/env python3
"""Serialized ANN benchmark: NornicDB vs Qdrant on one corpus, mode by mode.

Each (engine, mode) run starts its own server on a freshly wiped data directory, loads the corpus
over the Qdrant gRPC API, waits until the index is ready, then measures recall against exact ground
truth (corpus.py), single-client latency and concurrent throughput, optionally over an hnsw_ef sweep.
Only one server runs at a time. Both engines are driven through the same raw Qdrant gRPC calls
(Upsert, Search), so client and protocol costs are the same.

A mode an engine has no equivalent for is not run on it; the report shows N/A.

Writes <out>/<engine>-<mode>.json per run.
"""
import argparse
import concurrent.futures
import json
import os
import re
import shutil
import signal
import subprocess
import time
from pathlib import Path

import grpc
import numpy as np
import psutil
import requests
from qdrant_client import grpc as qg

# NornicDB modes are server environments (NORNICDB_VECTOR_ANN_QUALITY and friends): the strategy is
# chosen at startup. Qdrant modes are collection configurations plus search parameters.
HNSW_PRESETS = {  # NornicDB's quality presets (pkg/search/hnsw_config.go): M, efConstruction, efSearch
    "fast": (16, 100, 50),
    "balanced": (16, 200, 100),
    "accurate": (32, 400, 200),
}

MODES = [
    {"name": "exact", "title": "Exact (brute force)",
     "nornic": {"NORNICDB_VECTOR_CPU_BRUTE_MAX_N": "100000000"},
     "qdrant": {"hnsw": HNSW_PRESETS["fast"][:2], "search": {"exact": True}}},
] + [
    {"name": f"hnsw-{preset}", "title": f"HNSW {preset} (M={m}, efConstruction={efc}, efSearch={efs})", "sweep": True,
     "nornic": {"NORNICDB_VECTOR_ANN_QUALITY": preset},
     "qdrant": {"hnsw": (m, efc), "default_ef": efs}}
    for preset, (m, efc, efs) in HNSW_PRESETS.items()
] + [
    # k-means clustering runs while the index warms up at startup, so a database loaded after
    # startup is clustered by restarting the server on it.
    {"name": "ivf-hnsw", "title": "IVF (k-means routing, IVF-HNSW)", "restart_after_load": True,
     "nornic": {"NORNICDB_KMEANS_CLUSTERING_ENABLED": "true", "NORNICDB_VECTOR_IVF_HNSW_ENABLED": "true"}},
    {"name": "ivf-pq", "title": "IVF-PQ (compressed)",
     "nornic": {"NORNICDB_VECTOR_ANN_QUALITY": "compressed", "NORNICDB_PERSIST_SEARCH_INDEXES": "true"}},
    {"name": "sq-int8", "title": "HNSW + scalar int8 quantization (rescore)", "sweep": True,
     "qdrant": {"hnsw": HNSW_PRESETS["fast"][:2], "quantization": "scalar", "default_ef": 50}},
    {"name": "pq-x16", "title": "HNSW + product quantization x16 (rescore)", "sweep": True,
     "qdrant": {"hnsw": HNSW_PRESETS["fast"][:2], "quantization": "product", "default_ef": 50}},
    {"name": "bq", "title": "HNSW + binary quantization (rescore, 2x oversampling)", "sweep": True,
     "qdrant": {"hnsw": HNSW_PRESETS["fast"][:2], "quantization": "binary", "default_ef": 50}},
]

COLLECTION = "bench"
GRPC_MAX = 256 * 1024 * 1024


def dir_bytes(path: Path) -> int:
    """Bytes allocated on disk under path. Both engines preallocate sparse files (Badger's value log,
    Qdrant's WAL), so apparent file sizes overstate what is stored."""
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            try:
                total += os.lstat(os.path.join(root, name)).st_blocks * 512
            except OSError:
                pass
    return total


def wait_until(check, timeout, what, interval=0.5):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if check():
                return
        except Exception:  # noqa: BLE001 - the server may still be starting
            pass
        time.sleep(interval)
    raise TimeoutError(f"timed out waiting for {what}")


class Server:
    """A server process on its own data directory and ports."""

    def __init__(self, cmd, env, data_dir: Path, log: Path):
        self.data_dir = data_dir
        self.log = open(log, "w")
        # In its data directory: Qdrant writes a .qdrant-initialized marker to its working directory.
        self.proc = subprocess.Popen(cmd, env=env, stdout=self.log, stderr=subprocess.STDOUT, cwd=data_dir)
        self.ps = psutil.Process(self.proc.pid)

    def rss(self) -> int:
        try:
            return self.ps.memory_info().rss
        except psutil.Error:
            return 0

    def stop(self):
        self.proc.send_signal(signal.SIGTERM)
        try:
            self.proc.wait(120)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
        self.log.close()


class Engine:
    name = ""

    def __init__(self, args):
        self.args = args
        self.channel = None

    def stubs(self, port):
        self.channel = grpc.insecure_channel(f"127.0.0.1:{port}", options=[
            ("grpc.max_send_message_length", GRPC_MAX), ("grpc.max_receive_message_length", GRPC_MAX)])
        return qg.PointsStub(self.channel), qg.CollectionsStub(self.channel)

    def close(self):
        if self.channel is not None:
            self.channel.close()


class NornicEngine(Engine):
    name = "nornicdb"
    mode_key = "nornic"

    def start(self, mode, data_dir: Path, log: Path):
        cfg = data_dir.parent / f"{data_dir.name}.empty-config.yaml"
        cfg.write_text("{}\n")  # never pick up ~/.nornicdb/config.yaml
        env = {**os.environ,
               "NORNICDB_CONFIG": str(cfg), "NORNICDB_EMBEDDING_ENABLED": "false", "NORNICDB_MCP_ENABLED": "false",
               "NORNICDB_HEIMDALL_ENABLED": "false", "NORNICDB_QDRANT_GRPC_ENABLED": "true",
               "NORNICDB_QDRANT_GRPC_LISTEN_ADDR": f"127.0.0.1:{self.args.nornic_grpc_port}",
               "NORNICDB_QDRANT_GRPC_MAX_TOP_K": "10000",
               # Off the default :9090, which another process may hold.
               "NORNICDB_TELEMETRY_LISTEN": f"127.0.0.1:{self.args.nornic_telemetry_port}"}
        if not self.args.nornic_gpu:
            env.update({"NORNICDB_VECTOR_GPU_BRUTE_MAX_N": "0", "NORNICDB_HNSW_BUILD_GPU_ENABLED": "false"})
        env.update(mode["nornic"])
        cmd = [self.args.nornic_bin, "serve", "--data-dir", str(data_dir), "--address", "127.0.0.1",
               "--http-port", str(self.args.nornic_http_port), "--bolt-port", str(self.args.nornic_bolt_port),
               "--no-auth", "--headless"]
        self.server = Server(cmd, env, data_dir, log)
        self.log_path = log
        wait_until(lambda: requests.get(f"http://127.0.0.1:{self.args.nornic_http_port}/health", timeout=1).ok,
                   300, "NornicDB to start")
        return self.stubs(self.args.nornic_grpc_port)

    def create(self, collections, mode, dims):
        collections.Create(qg.CreateCollection(collection_name=COLLECTION, vectors_config=qg.VectorsConfig(
            params=qg.VectorParams(size=dims, distance=qg.Distance.Cosine))), timeout=60)

    def wait_ready(self, points, query):
        def ready():
            info = requests.get(f"http://127.0.0.1:{self.args.nornic_http_port}/db/{COLLECTION}", timeout=10).json()
            return info.get("searchReady") and not info.get("searchBuilding")
        wait_until(ready, 3600, "NornicDB's search index")
        points.Search(qg.SearchPoints(collection_name=COLLECTION, vector=query, limit=1), timeout=3600)  # builds the pipeline

    def describe(self, mode):
        text = Path(self.log_path).read_text(errors="replace")
        found = re.findall(r"Vector search strategy: (.*?) \(N=(\d+) vectors\)", text)
        built = re.findall(r"\[IVFPQ\] ✅ built \| ([^\n]*?) event_id", text)
        strategy = found[-1][0] if found else "unknown"
        return strategy + (f" [{built[-1].strip()}]" if built else "")

    def search_params(self, mode, ef):
        return qg.SearchParams(hnsw_ef=ef) if ef else None


class QdrantEngine(Engine):
    name = "qdrant"
    mode_key = "qdrant"

    def start(self, mode, data_dir: Path, log: Path):
        env = {**os.environ,
               "QDRANT__STORAGE__STORAGE_PATH": str(data_dir / "storage"),
               "QDRANT__STORAGE__SNAPSHOTS_PATH": str(data_dir / "snapshots"),
               "QDRANT__SERVICE__HOST": "127.0.0.1",
               "QDRANT__SERVICE__HTTP_PORT": str(self.args.qdrant_http_port),
               "QDRANT__SERVICE__GRPC_PORT": str(self.args.qdrant_grpc_port),
               "QDRANT__SERVICE__MAX_REQUEST_SIZE_MB": "256",
               "QDRANT__TELEMETRY_DISABLED": "true"}
        self.server = Server([self.args.qdrant_bin], env, data_dir, log)
        wait_until(lambda: requests.get(f"http://127.0.0.1:{self.args.qdrant_http_port}/readyz", timeout=1).ok,
                   300, "Qdrant to start")
        return self.stubs(self.args.qdrant_grpc_port)

    def create(self, collections, mode, dims):
        cfg = mode["qdrant"]
        m, efc = cfg["hnsw"]
        quantization = None
        if cfg.get("quantization") == "scalar":
            quantization = qg.QuantizationConfig(scalar=qg.ScalarQuantization(type=qg.QuantizationType.Int8, always_ram=True))
        elif cfg.get("quantization") == "product":
            quantization = qg.QuantizationConfig(product=qg.ProductQuantization(compression=qg.CompressionRatio.x16, always_ram=True))
        elif cfg.get("quantization") == "binary":
            quantization = qg.QuantizationConfig(binary=qg.BinaryQuantization(always_ram=True))
        collections.Create(qg.CreateCollection(
            collection_name=COLLECTION,
            vectors_config=qg.VectorsConfig(params=qg.VectorParams(size=dims, distance=qg.Distance.Cosine)),
            hnsw_config=qg.HnswConfigDiff(m=m, ef_construct=efc),
            # Index every segment: by default Qdrant keeps segments under ~20 MB as a plain
            # (brute-force) index, which would mix exact search into the HNSW measurements.
            optimizers_config=qg.OptimizersConfigDiff(indexing_threshold=1),
            quantization_config=quantization), timeout=60)

    def wait_ready(self, points, query):
        url = f"http://127.0.0.1:{self.args.qdrant_http_port}/collections/{COLLECTION}"

        def ready():
            result = requests.get(url, timeout=10).json()["result"]
            return (result["status"] == "green" and result.get("optimizer_status") == "ok"
                    and (result.get("indexed_vectors_count") or 0) >= (result.get("points_count") or 0))
        wait_until(ready, 3600, "Qdrant's index (status green, every vector indexed)", interval=1)

    def describe(self, mode):
        cfg = mode["qdrant"]
        m, efc = cfg["hnsw"]
        text = f"HNSW m={m} ef_construct={efc}"
        if cfg.get("quantization"):
            text += f" + {cfg['quantization']} quantization"
        if cfg.get("search", {}).get("exact"):
            text = "exact search (exact=true)"
        return text

    def search_params(self, mode, ef):
        cfg = mode["qdrant"]
        params = qg.SearchParams()
        if cfg.get("search", {}).get("exact"):
            params.exact = True
        if ef:
            params.hnsw_ef = ef
        if cfg.get("quantization"):
            params.quantization.CopyFrom(qg.QuantizationSearchParams(
                rescore=True, oversampling=2.0 if cfg["quantization"] == "binary" else 1.0))
        return params


def upsert(points, vectors, batch):
    for start in range(0, len(vectors), batch):
        part = vectors[start:start + batch]
        points.Upsert(qg.UpsertPoints(collection_name=COLLECTION, wait=True, points=[
            qg.PointStruct(id=qg.PointId(num=start + i), vectors=qg.Vectors(vector=qg.Vector(data=vec.tolist())))
            for i, vec in enumerate(part)]), timeout=600)


def measure(points, queries, truth, k, params, warmup, concurrency):
    q_lists = [q.tolist() for q in queries]

    def one(i):
        resp = points.Search(qg.SearchPoints(collection_name=COLLECTION, vector=q_lists[i], limit=k, params=params), timeout=600)
        return [p.id.num for p in resp.result]

    for i in range(min(warmup, len(q_lists))):
        one(i)
    latencies, results = [], []
    for i in range(len(q_lists)):
        t = time.perf_counter()
        results.append(one(i))
        latencies.append((time.perf_counter() - t) * 1000)
    recall = {}
    for at in sorted({1, 10, k}):
        if at > k:
            continue
        hits = sum(len(set(r[:at]) & set(truth[i, :at].tolist())) for i, r in enumerate(results))
        recall[f"recall@{at}"] = hits / (len(results) * at)
    t = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(concurrency) as pool:
        list(pool.map(one, range(len(q_lists))))
    qps = len(q_lists) / (time.perf_counter() - t)
    lat = np.array(latencies)
    return {**recall, "mean_ms": float(lat.mean()), "p50_ms": float(np.percentile(lat, 50)),
            "p95_ms": float(np.percentile(lat, 95)), "p99_ms": float(np.percentile(lat, 99)),
            "qps": qps, "concurrency": concurrency, "queries": len(q_lists)}


def run_one(engine, mode, args, vectors, queries, truth):
    if args.tag:
        mode = {**mode, "name": f"{mode['name']}+{args.tag}", "title": f"{mode['title']} [{args.tag}]"}
    if engine.mode_key == "nornic" and args.nornic_env:
        mode = {**mode, "nornic": {**mode["nornic"], **dict(kv.split("=", 1) for kv in args.nornic_env)}}
    data_dir = args.data_root / f"{engine.name}-{mode['name']}"
    shutil.rmtree(data_dir, ignore_errors=True)
    data_dir.mkdir(parents=True)
    log = args.out / f"{engine.name}-{mode['name']}.log"
    print(f"== {engine.name} / {mode['name']}: {mode['title']}", flush=True)
    t0 = time.time()
    points, collections = engine.start(mode, data_dir, log)
    try:
        engine.create(collections, mode, vectors.shape[1])
        t = time.time()
        upsert(points, vectors, args.batch)
        load_s = time.time() - t
        t = time.time()
        if mode.get("restart_after_load"):
            engine.close()
            engine.server.stop()
            points, collections = engine.start(mode, data_dir, log.with_suffix(".restart.log"))
        engine.wait_ready(points, queries[0].tolist())
        ready_s = time.time() - t
        result = {
            "engine": engine.name, "mode": mode["name"], "title": mode["title"], "strategy": engine.describe(mode),
            "vectors": int(len(vectors)), "dimensions": int(vectors.shape[1]), "k": args.k,
            "load_s": load_s, "load_vectors_per_s": len(vectors) / load_s, "index_ready_s": ready_s,
            "rss_bytes_after_load": engine.server.rss(), "disk_bytes": dir_bytes(data_dir), "runs": [],
        }
        efs = [None]
        if mode.get("sweep"):
            efs += [ef for ef in args.ef_sweep if ef >= args.k]
        for ef in efs:
            params = engine.search_params(mode, ef)
            stats = measure(points, queries, truth, args.k, params, args.warmup, args.concurrency)
            stats["hnsw_ef"] = ef
            result["runs"].append(stats)
            label = "server default" if ef is None else f"hnsw_ef={ef}"
            print(f"   {label:16} recall@{args.k} {stats[f'recall@{args.k}']:.4f}  p50 {stats['p50_ms']:7.3f} ms  "
                  f"p95 {stats['p95_ms']:7.3f} ms  {stats['qps']:8.1f} qps", flush=True)
        result["rss_bytes_after_queries"] = engine.server.rss()
        result["strategy"] = engine.describe(mode)
        result["wall_s"] = time.time() - t0
        print(f"   load {load_s:.1f}s ({result['load_vectors_per_s']:.0f}/s), index ready {ready_s:.1f}s, "
              f"disk {result['disk_bytes'] / 1e6:.0f} MB, RSS {result['rss_bytes_after_queries'] / 1e6:.0f} MB, "
              f"strategy: {result['strategy']}", flush=True)
        (args.out / f"{engine.name}-{mode['name']}.json").write_text(json.dumps(result, indent=2) + "\n")
    finally:
        engine.close()
        engine.server.stop()
        if not args.keep_data:
            shutil.rmtree(data_dir, ignore_errors=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--corpus", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--data-root", type=Path, required=True, help="parent of the per-run data directories (wiped per run)")
    ap.add_argument("--nornic-bin", required=True)
    ap.add_argument("--qdrant-bin", required=True)
    ap.add_argument("--engines", default="nornicdb,qdrant")
    ap.add_argument("--modes", default=",".join(m["name"] for m in MODES))
    ap.add_argument("--queries", type=int, default=1000)
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--warmup", type=int, default=100)
    ap.add_argument("--concurrency", type=int, default=8)
    ap.add_argument("--batch", type=int, default=500)
    ap.add_argument("--ef-sweep", default="16,32,64,128,256,512")
    ap.add_argument("--nornic-gpu", action="store_true", help="leave NornicDB's GPU brute force and GPU HNSW build on")
    ap.add_argument("--keep-data", action="store_true")
    ap.add_argument("--nornic-env", action="append", default=[], metavar="KEY=VALUE",
                    help="extra NornicDB environment for a variant run (repeatable); use with --tag")
    ap.add_argument("--tag", default="", help="suffix for the run names of a variant (e.g. rerank200)")
    ap.add_argument("--nornic-http-port", type=int, default=18474)
    ap.add_argument("--nornic-bolt-port", type=int, default=18687)
    ap.add_argument("--nornic-grpc-port", type=int, default=16334)
    ap.add_argument("--nornic-telemetry-port", type=int, default=19190)
    ap.add_argument("--qdrant-http-port", type=int, default=17333)
    ap.add_argument("--qdrant-grpc-port", type=int, default=17334)
    args = ap.parse_args()
    args.ef_sweep = [int(x) for x in args.ef_sweep.split(",") if x]
    # Absolute paths: each server runs in its own data directory.
    args.corpus, args.out, args.data_root = args.corpus.resolve(), args.out.resolve(), args.data_root.resolve()
    args.nornic_bin, args.qdrant_bin = str(Path(args.nornic_bin).resolve()), str(Path(args.qdrant_bin).resolve())
    args.out.mkdir(parents=True, exist_ok=True)
    args.data_root.mkdir(parents=True, exist_ok=True)

    vectors = np.load(args.corpus / "vectors.npy")
    queries = np.load(args.corpus / "queries.npy")[:args.queries]
    truth = np.load(args.corpus / "truth.npy")[:args.queries]
    if truth.shape[1] < args.k:
        raise SystemExit(f"ground truth has top-{truth.shape[1]}, need k={args.k}")
    def version(cmd):
        try:
            return subprocess.run(cmd, capture_output=True, text=True, timeout=30).stdout.strip().splitlines()[-1].split()[-1]
        except Exception:  # noqa: BLE001
            return "unknown"
    commit = subprocess.run(["git", "-C", str(Path(__file__).resolve().parent), "rev-parse", "--short", "HEAD"],
                            capture_output=True, text=True).stdout.strip()
    import report
    (args.out / "environment.json").write_text(json.dumps({
        "nornic_version": version([args.nornic_bin, "version"]), "nornic_commit": commit,
        "qdrant_version": version([args.qdrant_bin, "--version"]), "queries": int(len(queries)), "k": args.k,
        "warmup": args.warmup, "concurrency": args.concurrency, "batch": args.batch, "nornic_gpu": args.nornic_gpu,
        "ef_sweep": args.ef_sweep, "machine": report.machine(),
    }, indent=2) + "\n")
    wanted_modes = set(args.modes.split(","))
    engines = {"nornicdb": NornicEngine(args), "qdrant": QdrantEngine(args)}
    for mode in MODES:
        if mode["name"] not in wanted_modes:
            continue
        for engine_name in args.engines.split(","):
            engine = engines[engine_name]
            if engine.mode_key not in mode:  # no equivalent on this engine: N/A in the report
                continue
            run_one(engine, mode, args, vectors, queries, truth)


if __name__ == "__main__":
    main()
