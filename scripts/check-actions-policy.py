#!/usr/bin/env python3
"""Check the metered Actions policy; requires PyYAML (pip install pyyaml).

Run from any directory with ``python3 scripts/check-actions-policy.py``.
PR events and schedules are forbidden; every runner job must wait on the
administrator environment unless it comes from an existing publishing push.
"""
from pathlib import Path
import sys

import yaml

ROOT = Path(__file__).resolve().parents[1]
GATE = "${{ github.event_name == 'push' && 'automatic-publishing' || 'admin-approved-actions' }}"
PUSH_WORKFLOWS = {
    "cd.yml": {"tags": ["*"]},
    "release-macos.yml": {"tags": ["*"]},
    "cd-llama-cpu.yml": {"branches": ["main"], "paths": ["docker/Dockerfile.llama-cpu"]},
    "cd-llama-cuda.yml": {"branches": ["main"], "paths": ["docker/Dockerfile.llama-cuda"]},
    "docs-pages.yml": {
        "branches": ["main"],
        "paths": ["docs/**", "mkdocs.yml", ".github/workflows/docs-pages.yml", "requirements-docs.txt"],
    },
}


def violations(name, workflow):
    """Return trigger or runner-gate violations in a parsed workflow."""
    errors = []
    # YAML 1.1 parsers interpret the unquoted Actions key `on` as True.
    triggers = workflow.get("on", workflow.get(True, {}))
    if not isinstance(triggers, dict):
        return [f"{name}: triggers must be a mapping"]
    if set(triggers) - {"push", "workflow_dispatch", "workflow_call"}:
        errors.append(f"{name}: automatic non-push event is forbidden")
    if "push" in triggers and (name not in PUSH_WORKFLOWS or triggers["push"] != PUSH_WORKFLOWS[name]):
        errors.append(f"{name}: push differs from approved main/tag publishing triggers")
    for job_name, job in workflow.get("jobs", {}).items():
        if "runs-on" in job and job.get("environment") != GATE:
            errors.append(f"{name}/{job_name}: runner lacks administrator approval gate")
        if "uses" in job and not job["uses"].startswith("./.github/workflows/"):
            errors.append(f"{name}/{job_name}: unaudited reusable workflow")
    return errors


def self_test():
    """Demonstrate that regressions fail and manual and publishing routes pass."""
    base = {"on": {"workflow_dispatch": None}, "jobs": {"test": {"runs-on": "ubuntu-latest", "environment": GATE}}}
    assert not violations("ci.yml", base)
    for event in ("pull_request", "pull_request_target", "schedule", "workflow_run", "repository_dispatch"):
        assert violations("ci.yml", {**base, "on": {event: None}})
    assert violations("ci.yml", {**base, "jobs": {"test": {"runs-on": "ubuntu-latest"}}})
    assert violations("ci.yml", {**base, "on": {"push": {"branches": ["develop"]}}})
    for name, push in PUSH_WORKFLOWS.items():
        assert not violations(name, {**base, "on": {"push": push}})
    assert violations("ci.yml", {**base, "jobs": {"other": {"uses": "someone/repo/.github/workflows/ci.yml@main"}}})


if __name__ == "__main__":
    self_test()
    errors = []
    for path in sorted((ROOT / ".github/workflows").glob("*.y*ml")):
        errors.extend(violations(path.name, yaml.safe_load(path.read_text())))
    if errors:
        print("\n".join(errors), file=sys.stderr)
        sys.exit(1)
    print("Actions policy: all workflows passed; regression checks passed")
