#!/usr/bin/env python3
"""List structural convergence candidates from a Graphify code graph."""

import argparse
import json
from collections import defaultdict
from itertools import combinations
from pathlib import Path

import ijson


def graph_items(path, key):
    with path.open("rb") as graph_file:
        yield from ijson.items(graph_file, f"{key}.item")


def link_key(path):
    """Return the edge-array key of the graph export.

    Graphify >=0.9.69 exports ``links``; older graphs used ``edges``. Probe
    the links key first and fall back to edges when it is absent.
    """
    for _ in graph_items(path, "links"):
        return "links"
    return "edges"


def analyze(graph_path, components):
    symbols = {}
    groups = defaultdict(list)
    node_counts = defaultdict(int)
    calls = defaultdict(set)
    call_counts = defaultdict(int)

    for node in graph_items(graph_path, "nodes"):
        source_file = node.get("source_file", "")
        component = source_file.rsplit("/", 1)[0]
        if component not in components or not source_file.endswith(".go") or source_file.endswith("_test.go"):
            continue
        node_counts[component] += 1
        label = node.get("label", "")
        if not label.endswith("()"):
            continue
        symbol = {"id": node["id"], "file": source_file, "location": node.get("source_location", "")}
        symbols[node["id"]] = symbol
        groups[(component, label)].append(symbol)

    for edge in graph_items(graph_path, link_key(graph_path)):
        if edge.get("relation") != "calls":
            continue
        caller = edge.get("source")
        callee = edge.get("target")
        if caller not in symbols:
            continue
        component = symbols[caller]["file"].rsplit("/", 1)[0]
        call_counts[component] += 1
        calls[caller].add(callee)

    candidates = []
    for (component, label), members in sorted(groups.items()):
        files = {member["file"] for member in members}
        if len(files) < 2:
            continue
        members.sort(key=lambda member: (member["file"], member["location"], member["id"]))
        pairs = []
        for left, right in combinations(members, 2):
            if left["file"] == right["file"]:
                continue
            shared = calls[left["id"]] & calls[right["id"]]
            if shared:
                pairs.append({"left": left, "right": right, "shared_callee_ids": sorted(shared)})
        if not pairs:
            continue
        candidates.append({
            "component": component,
            "label": label,
            "pairs": pairs,
        })

    return {
        "components": [{"name": component, "nodes": node_counts[component], "call_edges": call_counts[component]}
                       for component in sorted(components)],
        "candidates": candidates,
    }


def compare_candidate_groups(old, current):
    old_keys = {f"{candidate['component']}|{candidate['label']}" for candidate in old["candidates"]}
    current_keys = {f"{candidate['component']}|{candidate['label']}" for candidate in current["candidates"]}
    return {
        "old_count": len(old_keys),
        "current_count": len(current_keys),
        "retained": len(old_keys & current_keys),
        "old_only": sorted(old_keys - current_keys),
        "current_only": sorted(current_keys - old_keys),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--graph", type=Path, default=Path("graphify-out/graph.json"))
    parser.add_argument("--compare-graph", type=Path, help="historical graph to compare using the same candidate filter")
    parser.add_argument("--component", action="append", required=True, help="Go package path, e.g. pkg/cypher")
    args = parser.parse_args()
    if not args.graph.is_file():
        parser.error(f"graph not found: {args.graph}")
    if args.compare_graph is not None and not args.compare_graph.is_file():
        parser.error(f"graph not found: {args.compare_graph}")
    current = analyze(args.graph, set(args.component))
    if args.compare_graph is None:
        result = current
    else:
        old = analyze(args.compare_graph, set(args.component))
        result = {
            "old_components": old["components"],
            "current_components": current["components"],
            **compare_candidate_groups(old, current),
        }
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()