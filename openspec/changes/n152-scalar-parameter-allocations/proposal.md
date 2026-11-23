# Avoid redundant bound-type admission for parameter-only results

## Problem and observed evidence

Release CI run 44994469696 failed its unchanged allocation ratchet at f2e264f1f against main b9036d7e. Its identical `RETURN $value AS value` fixture increased from 15 to 33 allocations internally and 28 to 46 allocations through public Execute. The request-local semantic walk added by #1926 is required for parameters that feed type-sensitive consumers, including WITH, CALL and UNION. Returning a bare parameter has no such consumer.

Local paired binaries with the exact ratchet fixture reproduce those counts. An alloc_objects profile attributes approximately 18 allocations per request to validateMatchSemanticScopesUncached and its clause/scope/projection readers. This is measured redundant work for this output-only contract, not a reason to remove parameter admission generally.

## Design

The existing immutable returnProjectionPlan retains a conservative parameterPassthrough proof. Its existing parameter reader must cover every projected expression completely, with no DISTINCT, wildcard or modifiers. The existing pipeline reader must prove exactly one whole RETURN in one branch. This proof is computed before the projection cache write lock and retains no request values. The bound admission entry point uses it only for RETURN-headed source, after all direct checks and successful value-independent semantic admission. Other statements retain the full request-local walk.

The cache is text-keyed. Bare complete parameter tokens and one plain RETURN are invariant between Cypher 5 and 25; public controls alternate dialects and parameter types against the same cached text. UNION, preceding clauses, property access, grouped expressions and tails are deliberately excluded. No new scanner, expression evaluator, cache, threshold or exception is introduced.

## Verification and non-goals

The ratchet's source and thresholds remain unchanged. Exact scalar before/after measurements, the existing cached WITH rebinding benchmark, public changing-type/missing-parameter controls, and original static/NOR79 cases establish the scoped claim. Full release qualification and Linux CI are separate gates.

DeepSeek job 7e1986936b6b4c5a9da7048f766565ac completed read-only in 185.823 seconds. Its graph retrieval was incomplete and nonapproved helper reads were unavailable. Parent profiling independently established attribution; broader scope-program/pooling suggestions were not adopted. Duplicate analysis reuses the existing projection/pipeline/parameter readers; service-backed staged checking remains separately reported.
