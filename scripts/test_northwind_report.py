import unittest

from northwind_report import (
    benchmark_throughput,
    parser_mode_lines,
    render_comparison,
    render_single_report,
    render_sweep,
    rules_for_label,
)


def make_query(name, mean_ms=20.0, latencies_ms=None):
    latencies = latencies_ms if latencies_ms is not None else [mean_ms, mean_ms]
    return {
        "name": name,
        "description": f"{name} workload",
        "cypher": f"RETURN '{name}' AS name",
        "iterations": len(latencies),
        "latencies_ms": latencies,
        "mean_ms": mean_ms,
        "median_ms": mean_ms,
        "p95_ms": mean_ms,
        "p99_ms": mean_ms,
        "min_ms": min(latencies),
        "max_ms": max(latencies),
        "stddev_ms": 0.0,
        "ops_per_second": 1000.0 / mean_ms,
        "row_count": 1,
        "result_hash": "abc123",
    }


def make_run(queries, operations, duration_ms):
    return {
        "results": {
            "queries": queries,
            "total_benchmark_operations": operations,
            "total_benchmark_duration_ms": duration_ms,
            "overall_ops_per_second": 10.56,
            "iterations_per_query": 2,
            "warmup_iterations": 1,
            "seed_batch_size": 500,
            "seed_parallelism": 4,
            "seed_counts": {},
        },
        "power": {},
        "wall_seconds": 1.0,
        "memory": {},
        "storage": {
            "totals": {
                "raw_data": 0,
                "index": 0,
                "logs": 0,
                "meta": 0,
                "skip": 0,
                "other": 0,
            }
        },
        "disk_total_bytes": 0,
    }


class NorthwindReportTests(unittest.TestCase):
    def test_seed_phases_and_legacy_rates(self):
        nornic = make_run([make_query("shared")], 2, 100.0)
        neo4j = make_run([make_query("shared")], 2, 100.0)
        for run in (nornic, neo4j):
            run["results"].update(seed_duration_ms=1000, seed_wipe_ms=100,
                                  seed_index_ms=200, seed_ingestion_ms=700,
                                  seed_nodes=700, seed_relationships=1400)
        nornic["label"] = "nornicdb"
        nornic["storage"]["files"] = {"raw_data": []}
        args = dict(iterations=2, warmup=1, batch_size=500, parallel=4, products=10, orders=10)

        single = render_single_report(nornic, **args)
        comparison = render_comparison({"nornicdb": nornic, "neo4j": neo4j}, **args)
        self.assertIn("Index setup duration: **200.00 ms**", single)
        self.assertIn("Ingestion nodes/sec: **1,000.00**", single)
        self.assertIn("| Ingestion duration (ms) |", comparison)
        self.assertIn("| Ingestion nodes/sec | 1,000.00 | 1,000.00 |", comparison)

        del neo4j["results"]["seed_ingestion_ms"]
        legacy = render_comparison({"nornicdb": nornic, "neo4j": neo4j}, **args)
        self.assertIn("Seed nodes/sec (total incl. setup, legacy)", legacy)
        self.assertNotIn("| Ingestion nodes/sec |", legacy)

    def test_throughput_uses_measured_suite_duration(self):
        per_operation_ms = 1000.0 / 10.56
        result = {
            "total_benchmark_operations": 40,
            "total_benchmark_duration_ms": 5239.733,
            "overall_ops_per_second": 10.56,
            "queries": [make_query("legacy", latencies_ms=[per_operation_ms] * 40)],
        }

        throughput = benchmark_throughput(result)

        self.assertAlmostEqual(throughput["end_to_end_ops_per_second"], 7.63399, places=4)
        self.assertAlmostEqual(throughput["query_latency_ops_per_second"], 10.56, places=6)

    def test_comparison_includes_union_of_query_names(self):
        nornic = make_run([make_query("nornic_only"), make_query("shared")], 4, 100.0)
        neo4j = make_run([make_query("shared"), make_query("neo4j_only")], 4, 100.0)

        report = render_comparison(
            {"nornicdb": nornic, "neo4j": neo4j},
            iterations=2,
            warmup=1,
            batch_size=500,
            parallel=4,
            products=10,
            orders=10,
        )

        self.assertIn("### `nornic_only`", report)
        self.assertIn("### `neo4j_only`", report)
        self.assertIn("| Neo4j | not run |", report)
        self.assertIn("| NornicDB | not run |", report)
        self.assertIn("End-to-end query-loop throughput (ops/sec)", report)
        self.assertIn("Query-latency-only aggregate throughput (ops/sec)", report)

    def test_comparison_flags_unequal_workload_configuration(self):
        nornic = make_run([make_query("shared")], 2, 100.0)
        neo4j = make_run([make_query("shared")], 3, 100.0)
        neo4j["results"]["random_seed"] = 99
        nornic["results"]["random_seed"] = 42

        report = render_comparison(
            {"nornicdb": nornic, "neo4j": neo4j},
            iterations=2, warmup=1, batch_size=500, parallel=4, products=10, orders=10,
        )

        self.assertIn("Invalid workload comparison", report)
        self.assertIn("random_seed: NornicDB=42 Neo4j=99", report)
        self.assertIn("measured operations: NornicDB=2 Neo4j=3", report)

    ARGS = dict(iterations=2, warmup=1, batch_size=500, parallel=4, products=10, orders=10)

    def three_runs(self, antlr_mean=40.0, antlr_hash="abc123"):
        nornic = make_run([make_query("a", 10.0), make_query("b", 20.0)], 4, 100.0)
        neo4j = make_run([make_query("a", 30.0), make_query("b", 60.0)], 4, 100.0)
        antlr = make_run([make_query("a", antlr_mean), make_query("b", antlr_mean * 2)], 4, 400.0)
        antlr["results"]["queries"][1]["result_hash"] = antlr_hash
        return {"nornicdb": nornic, "neo4j": neo4j, "nornicdb-antlr": antlr}

    def test_comparison_without_an_antlr_run_has_no_antlr_content(self):
        runs = self.three_runs()
        del runs["nornicdb-antlr"]
        report = render_comparison(runs, **self.ARGS)
        self.assertNotIn("ANTLR", report)
        self.assertNotIn("Parser Modes", report)

    def test_antlr_run_adds_rows_and_a_section_without_changing_existing_figures(self):
        runs = self.three_runs()
        without = dict(runs)
        del without["nornicdb-antlr"]
        base = render_comparison(without, **self.ARGS)
        report = render_comparison(runs, **self.ARGS)

        # every line of the original report is still there, in order
        remaining = iter(report.splitlines())
        for line in base.splitlines():
            self.assertTrue(any(line == candidate for candidate in remaining), f"lost or reordered: {line!r}")

        self.assertIn("## NornicDB Parser Modes: default vs ANTLR", report)
        antlr_rows = [line for line in report.splitlines() if line.startswith("| NornicDB (ANTLR) |")]
        self.assertEqual(len(antlr_rows), 2)  # one extra row per query
        self.assertIn("Mean-latency ratio (NornicDB ANTLR / default): **4.00×**", report)

    def test_parser_mode_section_ratios_and_parity(self):
        runs = self.three_runs(antlr_mean=40.0)
        text = "\n".join(parser_mode_lines(runs["nornicdb"], runs["nornicdb-antlr"]))
        # latency: ANTLR 40 ms vs default 10 ms
        self.assertIn("| `a` | 10.00 | 40.00 | 4.00× |", text)
        # throughput: default does 4 ops in 100 ms, ANTLR in 400 ms, so ANTLR is 4× slower
        self.assertIn("| End-to-end query-loop throughput (ops/sec) | 40.00 | 10.00 | -75.0% | 4.00× |", text)
        self.assertIn("Both modes returned identical results for all 2 queries.", text)

        mismatch = "\n".join(parser_mode_lines(*[self.three_runs(antlr_hash="zzz")[k] for k in ("nornicdb", "nornicdb-antlr")]))
        self.assertIn("Result mismatch between parser modes", mismatch)
        self.assertIn("| ❌ |", mismatch)

    def test_parser_mode_section_flags_unequal_workloads(self):
        runs = self.three_runs()
        runs["nornicdb-antlr"]["results"]["random_seed"] = 7
        runs["nornicdb"]["results"]["random_seed"] = 42
        text = "\n".join(parser_mode_lines(runs["nornicdb"], runs["nornicdb-antlr"]))
        self.assertIn("Invalid parser-mode comparison", text)

    def test_rules_for_label_dispatches_per_engine(self):
        self.assertIs(rules_for_label("nornicdb"), rules_for_label("nornicdb-antlr"))
        self.assertIsNot(rules_for_label("falkor"), rules_for_label("memgraph"))
        self.assertIsNot(rules_for_label("ladybug"), rules_for_label("neo4j"))
        # The falkor rules classify unknown files as raw data (catch-all last).
        from northwind_report import FALKOR_RULES, MEMGRAPH_RULES, LADYBUG_RULES

        self.assertEqual(FALKOR_RULES[-1][0], "raw_data")
        self.assertEqual(MEMGRAPH_RULES[-1][0], "raw_data")
        self.assertEqual(LADYBUG_RULES[-1][0], "raw_data")

    def test_sweep_renders_all_engines_and_flags_row_mismatch(self):
        def run_for(engine, row_count):
            run = make_run([make_query("shared", mean_ms=20.0)], 2, 100.0)
            run["results"]["queries"][0]["row_count"] = row_count
            run["results"].update(
                seed_duration_ms=1000,
                seed_nodes=700,
                seed_relationships=1400,
                seed_counts={
                    "categories": 8,
                    "suppliers": 12,
                    "customers": 20,
                    "products": 10,
                    "orders": 10,
                    "part_of_edges": 10,
                    "supplies_edges": 10,
                    "purchased_edges": 10,
                    "orders_edges": 0,
                },
            )
            run["power"] = {"package_power_mw_avg": 12000.0, "energy_joules": 12.5}
            return run

        nornic = run_for("nornicdb", 1)
        neo4j = run_for("neo4j", 1)
        falkor = run_for("falkor", 2)  # intentionally disagrees with the others
        memgraph = run_for("memgraph", 1)
        ladybug = run_for("ladybug", 1)

        sweep = render_sweep(
            {
                "nornicdb": nornic,
                "neo4j": neo4j,
                "falkor": falkor,
                "memgraph": memgraph,
                "ladybug": ladybug,
            }
        )

        self.assertIn("# Northwind Benchmark Sweep — All Engines", sweep)
        self.assertIn("**NornicDB**", sweep)
        self.assertIn("**Neo4j**", sweep)
        self.assertIn("**FalkorDB**", sweep)
        self.assertIn("**Memgraph**", sweep)
        self.assertIn("**LadybugDB**", sweep)
        self.assertIn("## Seed Counts Cross-Check", sweep)
        self.assertIn("| Category | 8 | 8 | 8 | 8 | 8 |", sweep)
        self.assertIn("## Query Result Cross-Check", sweep)
        # FalkorDB's row count differs, so agreement must be flagged.
        self.assertIn("| ❌ |", sweep)

        # Fewer than two engines → no sweep report.
        self.assertEqual(render_sweep({"nornicdb": nornic}), "")


if __name__ == "__main__":
    unittest.main()
