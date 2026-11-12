#!/usr/bin/env python3
"""Check the metered Actions policy; requires PyYAML (pip install pyyaml).

Run from any directory with ``python3 scripts/check-actions-policy.py``.
Only same-repository release-to-main PR verification may run automatically;
other runner jobs require approval except publishing and PR-closing governance.
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
        "paths": ["CONTRIBUTING.md", "docs/**", "docs-site/**", ".github/workflows/docs-pages.yml"],
    },
}


def violations(name, workflow):
    """Return trigger or runner-gate violations in a parsed workflow."""
    errors = []
    # YAML 1.1 parsers interpret the unquoted Actions key `on` as True.
    triggers = workflow.get("on", workflow.get(True, {}))
    if not isinstance(triggers, dict):
        return [f"{name}: triggers must be a mapping"]
    if name == "release-verification.yml":
        expected = {"pull_request_target": {"branches": ["main"], "types": ["opened", "reopened", "synchronize"]}}
        jobs = workflow.get("jobs", {})
        expected["workflow_dispatch"] = {"inputs": {key: {"description": description, "required": True, "type": "string"} for key, description in (("head", "Full commit SHA to verify"), ("base", "Full ancestor commit SHA used as the verification baseline"))}}
        expected_jobs = {"authorize", "prepare", "build", "tests", "lint", "checks", "verify"}
        guard = "${{ always() && ((github.event_name == 'workflow_dispatch' && needs.authorize.result == 'success') || (github.event_name == 'pull_request_target' && github.event.pull_request.head.repo.full_name == github.repository && startsWith(github.head_ref, 'release/'))) }}"
        authorize = jobs.get("authorize", {})
        guarded = (jobs.get("prepare", {}).get("if") == guard and jobs.get("prepare", {}).get("needs") == "authorize"
                   and authorize.get("if") == "${{ github.event_name == 'workflow_dispatch' }}"
                   and authorize.get("environment") == GATE)
        dependents = (all(jobs.get(name, {}).get("needs") == "prepare" for name in ("build", "tests", "lint"))
                      and set(jobs.get("checks", {}).get("needs", [])) == {"prepare", "build", "lint"}
                      and jobs.get("checks", {}).get("if") == "${{ !cancelled() && needs.prepare.result == 'success' }}")
        final = jobs.get("verify", {})
        final_guard = final.get("if", "") == "${{ always() && (github.event_name == 'workflow_dispatch' || (github.event.pull_request.head.repo.full_name == github.repository && startsWith(github.head_ref, 'release/'))) }}"
        final_needs = set(final.get("needs", [])) == expected_jobs - {"verify", "authorize"}
        if (triggers != expected or set(jobs) != expected_jobs or not guarded or not dependents
                or not final_guard or not final_needs or any("environment" in job for name, job in jobs.items() if name != "authorize")
                or workflow.get("permissions") != {"contents": "read"}):
            errors.append(f"{name}: invalid release-to-main verification policy")
        return errors
    if name == "release-graphify-pr.yml":
        jobs = workflow.get("jobs", {})
        expected = {"pull_request_target": {"branches": ["release/**"], "types": ["opened", "reopened", "synchronize", "edited"]},
                    "issue_comment": {"types": ["created"]}}
        authorize, extract, check = (jobs.get(key, {}) for key in ("authorize", "extract", "check"))
        permission = {"contents": "read", "issues": "write", "pull-requests": "write"}
        gate = any(step.get("run") == "python .github/scripts/graphify_gate.py" for step in authorize.get("steps", []))
        default_checkout = any(step.get("with", {}).get("ref") == "${{ github.event.repository.default_branch }}" for step in authorize.get("steps", []))
        if (triggers != expected or workflow.get("permissions") != {} or set(jobs) != {"authorize", "extract", "check"}
                or authorize.get("permissions") != permission or check.get("permissions") != permission
                or extract.get("permissions") != {"contents": "read"} or not gate or not default_checkout
                or extract.get("needs") != "authorize" or extract.get("if") != "${{ needs.authorize.outputs.authorized == 'true' }}"
                or check.get("needs") != ["authorize", "extract"]
                or check.get("if") != "${{ always() && needs.authorize.outputs.authorized == 'true' }}"
                or "${{ secrets." in str(extract)):
            errors.append(f"{name}: invalid trusted Graphify PR policy")
        return errors
    if name == "release-graphify-ingest.yml":
        jobs = workflow.get("jobs", {})
        select, ingest = jobs.get("select", {}), jobs.get("ingest", {})
        dispatch = triggers.get("workflow_dispatch", {})
        branch = (dispatch.get("inputs") or {}).get("branch", {}) if isinstance(dispatch, dict) else {}
        gate = any(step.get("run") == "python .github/scripts/graphify_ingest_gate.py" for step in select.get("steps", []))
        if (set(triggers) != {"pull_request_target", "workflow_dispatch"}
                or triggers.get("pull_request_target") != {"branches": ["release/**"], "types": ["closed"]}
                or branch.get("required") is not True or branch.get("type") != "string"
                or workflow.get("permissions") != {"contents": "read"} or set(jobs) != {"select", "ingest"}
                or select.get("if") != "${{ github.event_name == 'workflow_dispatch' || github.event.pull_request.merged == true }}"
                or not gate or ingest.get("needs") != "select"
                or ingest.get("concurrency") != {"group": "release-graphify-rolling-index", "cancel-in-progress": True}):
            errors.append(f"{name}: invalid trusted Graphify ingestion policy")
        return errors
    if name == "reject-develop-pr.yml":
        expected = {"pull_request_target": {"branches": ["develop"], "types": ["opened", "reopened", "edited"]}}
        jobs = workflow.get("jobs", {})
        job = jobs.get("close", {})
        steps = job.get("steps", [])
        if (triggers != expected or workflow.get("permissions") != {"contents": "read", "pull-requests": "write"}
                or set(jobs) != {"close"} or job.get("runs-on") != "ubuntu-latest"
                or len(steps) != 1 or "uses" in steps[0]
                or "gh api --method PATCH" not in steps[0].get("run", "")
                or "state=closed" not in steps[0].get("run", "")):
            errors.append(f"{name}: invalid develop PR governance workflow")
        return errors
    if set(triggers) - {"push", "workflow_dispatch", "workflow_call"}:
        errors.append(f"{name}: automatic non-push event is forbidden")
    if "push" in triggers and (name not in PUSH_WORKFLOWS or triggers["push"] != PUSH_WORKFLOWS[name]):
        errors.append(f"{name}: push differs from approved main/tag publishing triggers")
    if name == "ci.yml" and "coverage-plan" in workflow.get("jobs", {}):
        jobs = workflow["jobs"]
        expected_needs = {
            "test": {"coverage-plan"}, "coverage-groups": {"coverage-plan"},
            "coverage": {"coverage-plan", "coverage-groups", "test"},
            "make-test-shards": {"coverage-plan"}, "make-test-lint": {"coverage-plan"},
            "make-test": {"coverage-plan", "make-test-shards", "make-test-lint"},
        }
        expected_conditions = {
            "test": "${{ inputs.test_only != true }}", "coverage-groups": "${{ inputs.test_only != true }}",
            "coverage": "${{ always() && inputs.test_only != true }}",
            "make-test-shards": "${{ inputs.test_only == true }}", "make-test-lint": "${{ inputs.test_only == true }}",
            "make-test": "${{ always() && inputs.test_only == true }}",
        }
        if (set(jobs) != set(expected_needs) | {"coverage-plan"}
                or jobs["coverage-plan"].get("environment") != GATE or jobs["coverage-plan"].get("needs")):
            errors.append(f"{name}: CI plan lacks its administrator approval gate")
        for job_name, expected_job_needs in expected_needs.items():
            job = jobs.get(job_name, {})
            needs = job.get("needs", [])
            needs = [needs] if isinstance(needs, str) else needs
            if (set(needs) != expected_job_needs or job.get("if") != expected_conditions[job_name]
                    or (job_name in ("coverage", "make-test") and job.get("environment") != GATE)):
                errors.append(f"{name}/{job_name}: runner can bypass the approved CI plan")
        return errors
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
    assert violations("release-verification.yml", {**base, "on": {"push": {"branches": ["**"]}}})
    assert violations("release-verification.yml", {**base, "on": {"push": {"tags": ["*"]}}})
    for name, push in PUSH_WORKFLOWS.items():
        assert not violations(name, {**base, "on": {"push": push}})
    # Docs publishing publishes on main pushes only; the portal paths follow the
    # VitePress docs-site, and every runner keeps the administrator gate.
    docs_push = {"branches": ["main"], "paths": ["CONTRIBUTING.md", "docs/**", "docs-site/**", ".github/workflows/docs-pages.yml"]}
    docs = {**base, "on": {"push": docs_push, "workflow_dispatch": None}}
    assert not violations("docs-pages.yml", docs)
    assert violations("docs-pages.yml", {**docs, "on": {"pull_request": {"branches": ["main"]}, "push": docs_push}})
    assert violations("docs-pages.yml", {**docs, "on": {"push": {**docs_push, "paths": ["docs/**", "mkdocs.yml", "requirements-docs.txt"]}}})
    assert violations("docs-pages.yml", {**docs, "jobs": {"build": {"runs-on": "ubuntu-latest"}}})
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
