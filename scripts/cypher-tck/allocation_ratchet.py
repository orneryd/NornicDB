import argparse
import json
import math
import os
from pathlib import Path
import statistics
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


WORKLOADS = {
    "cypher": (
        "BenchmarkExecuteReturn_NoBindings",
        "BenchmarkExecuteReturn_BoundValues",
        "BenchmarkExecuteInternal_ScalarParameter",
        "BenchmarkFilterBindingsByWhere_CompiledJoin",
        "BenchmarkFilterBindingsByWhere_SharedExpressionPlan",
    ),
    "storage": ("BenchmarkBadger_GetNode_CacheHit",),
}
METRICS = ("ns/op", "B/op", "allocs/op")
BENCHMARK_ALIASES = {
    "BenchmarkFilterBindingsByWhere_GenericFallback": "BenchmarkFilterBindingsByWhere_SharedExpressionPlan",
}


def resolve_baseline(checkout, reference):
    reference = reference.strip()
    commands = []
    revision = "HEAD^{commit}~1"
    if reference and set(reference) != {"0"}:
        commands.append(["git", "-C", str(checkout), "fetch", "--no-tags", "--depth=1", "--", "origin", reference])
        revision = "FETCH_HEAD^{commit}"
    commands.append(["git", "-C", str(checkout), "rev-parse", "--verify", revision])
    for command in commands:
        completed = subprocess.run(command, capture_output=True, text=True, timeout=60,
                                   env=dict(os.environ, GIT_TERMINAL_PROMPT="0"))
        if completed.returncode:
            raise RuntimeError(f"cannot resolve allocation baseline: {completed.stderr.strip()}")
    commit = completed.stdout.strip()
    if len(commit) not in (40, 64) or any(character not in "0123456789abcdefABCDEF" for character in commit):
        raise RuntimeError("allocation baseline did not resolve to a commit hash")
    return commit


def parse_samples(text, names, operations, samples):
    records = {name: [] for name in names}
    source_names = {}
    output = []
    packages = set()
    completed = []
    final_event = {}
    for line in text.splitlines():
        event = json.loads(line)
        if not isinstance(event, dict):
            raise ValueError("benchmark event must be an object")
        final_event = event
        if event.get("Package"):
            packages.add(event["Package"])
        if event.get("Action") in ("fail", "build-fail"):
            raise ValueError(f"benchmark run failed: {event}")
        if event.get("Action") == "pass" and not event.get("Test"):
            completed.append(event.get("Package"))
        if event.get("Action") == "output":
            output.append(event.get("Output", ""))
    if len(packages) != 1 or completed != list(packages) or final_event.get("Action") != "pass" or final_event.get("Test"):
        raise ValueError("benchmark run must finish with one successful package completion")
    for line in "".join(output).splitlines():
        fields = line.split()
        if not fields or not fields[0].startswith("Benchmark") or len(fields) < 8:
            continue
        source = fields[0].rsplit("-", 1)[0]
        name = BENCHMARK_ALIASES.get(source, source)
        if name not in records or int(fields[1]) != operations:
            raise ValueError(f"unexpected benchmark or operation count: {fields}")
        if source_names.setdefault(name, source) != source:
            raise ValueError(f"mixed benchmark aliases for {name}")
        metrics = dict(zip(fields[3::2], map(float, fields[2::2])))
        if any(metric not in metrics or not math.isfinite(metrics[metric])
               or metrics[metric] < 0 for metric in METRICS):
            raise ValueError(f"invalid metrics for {name}: {metrics}")
        records[name].append(metrics)
    for name, values in records.items():
        if len(values) != samples:
            raise ValueError(f"{name}: expected {samples} samples, got {len(values)}")
    return {name: {metric: statistics.median(value[metric] for value in values)
                   for metric in METRICS} for name, values in records.items()}


def compare_allocations(base, head, exceptions, tolerance_percent=0.0):
    """Gate allocation medians against the baseline.

    B/op ceilings accept a relative noise tolerance (percent) so byte-level
    binary-layout jitter on shared runners cannot trip the gate; allocs/op
    counts stay exact and per-workload exception ceilings always win.
    """
    if not isinstance(exceptions, dict):
        raise ValueError("allocation exceptions must be an object")
    if not isinstance(tolerance_percent, (int, float)) or isinstance(tolerance_percent, bool) \
            or not math.isfinite(tolerance_percent) or tolerance_percent < 0:
        raise ValueError("allocation tolerance must be a non-negative number")
    if base.keys() != head.keys() or not base or exceptions.keys() - head.keys():
        raise ValueError("baseline, head or exception benchmark coverage differs")
    failures = []
    for name, current in head.items():
        allowance = exceptions.get(name, {})
        if not isinstance(allowance, dict) or allowance.keys() - {"reason", *METRICS[1:]}:
            raise ValueError(f"{name}: invalid allocation exception fields")
        if allowance and (not isinstance(allowance.get("reason"), str) or not allowance["reason"].strip()):
            raise ValueError(f"{name}: allocation exception needs a reason")
        for metric in METRICS[1:]:
            if metric in allowance:
                ceiling = allowance[metric]
            elif metric == "B/op":
                ceiling = base[name][metric] * (1 + tolerance_percent / 100)
            else:
                ceiling = base[name][metric]
            if not isinstance(ceiling, (int, float)) or isinstance(ceiling, bool) or not math.isfinite(ceiling) or ceiling < base[name][metric]:
                raise ValueError(f"{name}: invalid {metric} ceiling")
            if current[metric] > ceiling:
                failures.append(f"{name}: {metric} {base[name][metric]} -> {current[metric]} (ceiling {ceiling})")
    return failures


def run_benchmarks(checkout, flavor, artifacts, parser, operations, samples):
    results = {}
    environment = dict(os.environ, NORNICDB_PARSER=parser)
    for package, names in WORKLOADS.items():
        stem = artifacts / f"{flavor}-{package}"
        selected = names + tuple(old for old, current in BENCHMARK_ALIASES.items() if current in names)
        command = [
            "go", "-C", str(checkout), "test", "-json", "-tags=noui,nolocalllm",
            f"./pkg/{package}", "-run=^$", "-bench=^(" + "|".join(selected) + ")$",
            "-benchmem", f"-benchtime={operations}x", f"-count={samples}", "-cpu=1",
            "-timeout=10m", f"-o={stem}.test",
        ]
        runs = ((command, stem),
                (command + [f"-cpuprofile={stem}.cpu.pprof"], Path(f"{stem}-profile")))
        for invocation, output in runs:
            completed = subprocess.run(invocation, env=environment, capture_output=True,
                                       text=True, timeout=660)
            Path(f"{output}.jsonl").write_text(completed.stdout)
            Path(f"{output}.stderr").write_text(completed.stderr)
            if completed.returncode:
                raise RuntimeError(f"{flavor}/{package} failed; see {output}.stderr and .jsonl")
            measured = parse_samples(completed.stdout, names, operations, samples)
            if output == stem:
                results.update(measured)
    return results


class AllocationRatchetTests(unittest.TestCase):
    def test_baseline_parent_is_resolved_in_head_checkout(self):
        commit = "a" * 40
        for reference in ("", "0" * 40):
            with self.subTest(reference=reference), patch("subprocess.run", return_value=
                    subprocess.CompletedProcess([], 0, commit + "\n", "")) as run:
                self.assertEqual(resolve_baseline(Path("head"), reference), commit)
                self.assertEqual(run.call_args.args[0],
                                 ["git", "-C", "head", "rev-parse", "--verify", "HEAD^{commit}~1"])

    def test_explicit_baseline_is_fetched_then_resolved(self):
        commit = "b" * 40
        completed = [subprocess.CompletedProcess([], 0, "", ""),
                     subprocess.CompletedProcess([], 0, commit + "\n", "")]
        with patch("subprocess.run", side_effect=completed) as run:
            self.assertEqual(resolve_baseline(Path("head"), "refs/heads/main"), commit)
            self.assertEqual(run.call_args_list[0].args[0],
                             ["git", "-C", "head", "fetch", "--no-tags", "--depth=1", "--", "origin", "refs/heads/main"])
            self.assertEqual(run.call_args_list[1].args[0],
                             ["git", "-C", "head", "rev-parse", "--verify", "FETCH_HEAD^{commit}"])

    def test_failed_or_invalid_baseline_is_rejected(self):
        for completed in (subprocess.CompletedProcess([], 128, "", "missing parent"),
                          subprocess.CompletedProcess([], 0, "not a commit\n", "")):
            with self.subTest(completed=completed), patch("subprocess.run", return_value=completed):
                with self.assertRaises(RuntimeError):
                    resolve_baseline(Path("head"), "")

    def test_historical_benchmark_names_are_reported_canonically(self):
        old, current = next(iter(BENCHMARK_ALIASES.items()))
        output = json.dumps({"Action": "output", "Package": "sample", "Output":
            f"{old}-1 100 4 ns/op 8 B/op 2 allocs/op\n"})
        completion = json.dumps({"Action": "pass", "Package": "sample"})
        self.assertEqual(parse_samples(output + "\n" + completion, [current], 100, 1)[current]["B/op"], 8)
        second = json.dumps({"Action": "output", "Package": "sample", "Output":
            f"{current}-1 100 4 ns/op 8 B/op 2 allocs/op\n"})
        with self.assertRaises(ValueError):
            parse_samples("\n".join([output, second, completion]), [current], 100, 2)

    def test_failed_or_truncated_package_cannot_supply_samples(self):
        output = json.dumps({"Action": "output", "Package": "sample", "Output":
            "BenchmarkSample-1 100 4 ns/op 8 B/op 2 allocs/op\n"})
        endings = [[], [{"Action": "fail", "Package": "sample"}],
                   [{"Action": "skip", "Package": "sample"}],
                   [{"Action": "fail", "Package": "sample", "Test": "TestFailure"},
                    {"Action": "pass", "Package": "sample"}]]
        for events in endings:
            with self.subTest(events=events), self.assertRaises(ValueError):
                parse_samples("\n".join([output, *(json.dumps(event) for event in events)]),
                              ["BenchmarkSample"], 100, 1)

    def test_profile_allocations_do_not_enter_gate(self):
        def complete(command, **kwargs):
            package = next(package for package in WORKLOADS if f"./pkg/{package}" in command)
            profiled = any(argument.startswith("-cpuprofile=") for argument in command)
            output = "".join(json.dumps({"Action": "output", "Output":
                f"{name}-1 1000 4 ns/op {999 if profiled else 8} B/op 2 allocs/op\n"}) + "\n"
                for name in WORKLOADS[package])
            output += json.dumps({"Action": "pass", "Package": package}) + "\n"
            return subprocess.CompletedProcess(command, 0, output, "")

        with tempfile.TemporaryDirectory() as directory, patch("subprocess.run", side_effect=complete) as run:
            results = run_benchmarks(Path(directory), "head", Path(directory), "antlr", 1000, 1)
            self.assertEqual(len(results), sum(map(len, WORKLOADS.values())))
            self.assertTrue(all(result["B/op"] == 8 for result in results.values()))
            self.assertEqual(run.call_count, 2 * len(WORKLOADS))
            self.assertTrue(all(call.kwargs["env"]["NORNICDB_PARSER"] == "antlr" for call in run.call_args_list))

    def test_parse_and_complete_coverage(self):
        event = {"Action": "output", "Output": "BenchmarkSample-1 100 4 ns/op 8 B/op 2 allocs/op\n"}
        completion = "\n" + json.dumps({"Action": "pass", "Package": "sample"})
        text = json.dumps(event) + completion
        parsed = parse_samples(text, ["BenchmarkSample"], 100, 1)
        self.assertEqual(parsed["BenchmarkSample"]["B/op"], 8)
        fragmented = "\n".join(json.dumps({"Action": "output", "Output": part}) for part in ["BenchmarkSample-1\t", "100 4 ns/op 8 B/op 2 allocs/op\n"])
        self.assertEqual(parse_samples(fragmented + completion, ["BenchmarkSample"], 100, 1), parsed)
        for operations, samples, names in [(99, 1, ["BenchmarkSample"]), (100, 2, ["BenchmarkSample"]), (100, 1, ["BenchmarkMissing"])]:
            with self.assertRaises(ValueError):
                parse_samples(text, names, operations, samples)
        event["Output"] = "BenchmarkSample-1 100 nan ns/op 8 B/op 2 allocs/op\n"
        with self.assertRaises(ValueError):
            parse_samples(json.dumps(event) + completion, ["BenchmarkSample"], 100, 1)

    def test_only_allocations_are_gated(self):
        base = {"sample": {"ns/op": 1, "B/op": 8, "allocs/op": 2}}
        slower = {"sample": {"ns/op": 1000, "B/op": 8, "allocs/op": 2}}
        self.assertEqual(compare_allocations(base, slower, {}), [])
        for metric in METRICS[1:]:
            head = {"sample": dict(slower["sample"], **{metric: 9})}
            self.assertEqual(len(compare_allocations(base, head, {})), 1)
            self.assertEqual(compare_allocations(base, head, {"sample": {"reason": "declared workload change", metric: 9}}), [])
        for exceptions in [{"sample": {"B/op": 9}}, {"missing": {"reason": "unknown"}}, {"sample": {"reason": "invalid", "B/op": -1}}, {"sample": {"reason": None, "B/op": 9}}, {"sample": {"reason": "typo", "bytes": 9}}, []]:
            with self.assertRaises(ValueError):
                compare_allocations(base, slower, exceptions)
        with self.assertRaises(ValueError):
            compare_allocations(base, {}, {})

    def test_bytes_noise_tolerance_margins(self):
        base = {"sample": {"ns/op": 1, "B/op": 6187, "allocs/op": 12}}
        bumped = {"sample": {"ns/op": 1, "B/op": 6188, "allocs/op": 12}}
        self.assertEqual(compare_allocations(base, bumped, {}, 1.0), [])
        self.assertEqual(len(compare_allocations(base, bumped, {})), 1)
        over = {"sample": {"ns/op": 1, "B/op": 6310, "allocs/op": 12}}
        self.assertEqual(len(compare_allocations(base, over, {}, 1.0)), 1)
        extra_alloc = {"sample": {"ns/op": 1, "B/op": 6187, "allocs/op": 13}}
        self.assertEqual(len(compare_allocations(base, extra_alloc, {}, 1.0)), 1)
        for tolerance in (-1.0, float("nan"), float("inf"), True):
            with self.subTest(tolerance=tolerance), self.assertRaises(ValueError):
                compare_allocations(base, bumped, {}, tolerance)


def main():
    parser = argparse.ArgumentParser(description="Fixed-count allocation gate; timing is reported, never gated.")
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--resolve-baseline", action="store_true")
    parser.add_argument("--reference", default="")
    parser.add_argument("--base", type=Path)
    parser.add_argument("--head", type=Path)
    parser.add_argument("--artifacts", type=Path)
    parser.add_argument("--parser", choices=("nornic", "antlr"), default="nornic")
    parser.add_argument("--operations", type=int, default=1000)
    parser.add_argument("--samples", type=int, default=5)
    parser.add_argument("--tolerance-bytes-pct", type=float, default=1.0,
                        help="relative B/op noise tolerance in percent (default 1.0)")
    arguments = parser.parse_args()
    if arguments.self_test:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(AllocationRatchetTests)
        return 0 if unittest.TextTestRunner().run(suite).wasSuccessful() else 1
    if arguments.resolve_baseline:
        if not arguments.head:
            parser.error("head checkout is required to resolve the baseline")
        print(f"sha={resolve_baseline(arguments.head.resolve(), arguments.reference)}")
        return 0
    if not all((arguments.base, arguments.head, arguments.artifacts)) or arguments.operations < 1 or arguments.samples < 1:
        parser.error("positive operations/samples and base, head, artifacts paths are required")
    artifacts = arguments.artifacts.resolve()
    artifacts.mkdir(parents=True, exist_ok=True)
    base = run_benchmarks(arguments.base.resolve(), "base", artifacts, arguments.parser, arguments.operations, arguments.samples)
    head = run_benchmarks(arguments.head.resolve(), "head", artifacts, arguments.parser, arguments.operations, arguments.samples)
    exception_path = arguments.head / "scripts/cypher-tck/allocation-exceptions.json"
    exceptions = json.loads(exception_path.read_text()) if exception_path.exists() else {}
    failures = compare_allocations(base, head, exceptions, arguments.tolerance_bytes_pct)
    report = {"parser": arguments.parser, "operations": arguments.operations,
              "samples": arguments.samples, "base": base, "head": head,
              "exceptions": exceptions, "failures": failures}
    (artifacts / "allocation-report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
        print(f"allocation gate failed: {error}", file=sys.stderr)
        sys.exit(1)