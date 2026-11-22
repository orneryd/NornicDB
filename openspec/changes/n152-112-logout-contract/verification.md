# Verification

Base f77ec7706de099588ac232b32b6908860511d3da. Documentation-only release correction; no runtime or changelog edits.

- `make verify-list BASE=origin/release/v1.5.2` passed (plan, not test qualification).
- `make verify BASE=origin/release/v1.5.2 ONLY=docs FETCH=0` passed, docs step 38s: npm ci, VitePress build, corpus/search/graph contract tests; zero skipped test cases. Other verifier steps explicitly unselected: localization, UI, native libraries, build, tests, allocations, conformance, differential. Browser/tagged docs journeys not run.
- Build regenerated the corpus manifest, including preexisting stale hashes for generated entity census and quick-start sources; no corresponding source change was made by this task.
- Existing real-router macOS/Linux fail-first evidence remains on N152-112. It is historical evidence of the old documentation contradiction, not a current-source runtime pass. No new passing test asserts revocation.
- Parent traced handleLogout, ValidateTokenWithFineGrainedProvider, CurrentClaimsContext, JWT expiry and token cache owners at this base. Both cache-hit and signature-validation paths refresh account claims. Exp=0 is non-expiring. The unauthenticated logout route can omit audit principal; wording is conditional.
- DeepSeek read-only job 5ece851cd5c54e28a492abb78a35e0a4 completed in82.589s. It verified handler/cache/middleware, but its tool refused auth.go and the compliance page as potential secrets; graph retrieval also failed. Parent independently read these locally; no guard bypass or resubmission occurred. Worker proposal was not adopted wholesale: account-role caveat was resolved by canonical owner tracing; universal expiry, external-provider semantics and cookie Secure identity assertions were not accepted.
- Security triage recorded locally in ignored docs/.flagged_security.md and archived; this does not score severity or publish an advisory. Prior actor possession of a valid token is required for demonstrated continued access.
- Full release qualification, runtime revocation, auth-verify, pooled reauthentication and receipts remain pending. No performance claim.
