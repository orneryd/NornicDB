# Deferred Items — Phase 01-observability-foundation-skeleton

Items discovered during plan execution that are out-of-scope for the
current task and intentionally NOT fixed. The verifier or a follow-up
plan should address these.

## Plan 01-04 (executor: 2026-04-30)

### Pre-existing race in pkg/nornicdb/embed_queue tests
- **Tests:** `TestEmbedWorkerConcurrency` (subtest
  `reset_close_overlap_no_waitgroup_reuse_panic`) and
  `TestEmbedQueueDebounceAndHelpers`.
- **Status:** Race detected under `go test -race`; reproduces on baseline
  `06dbddf` (Plan 01-03 HEAD) — confirmed pre-existing, NOT caused by
  Plan 01-04 changes.
- **Triage:** Concurrent reset/close on the embed worker WaitGroup;
  outside Phase 1 observability scope. Likely an issue for a future
  pkg/nornicdb refactor.
- **Action:** Leave alone. Plan 01-04 verifies its own tests under
  `-race` and does not modify embed_queue.

### Plugin/build-tag environmental flakes
- **Tests:** `TestLoadPluginsFromDir`,
  `TestPluginLoadAndProcedureExtractionHelpers`.
- **Status:** Fail in local dev environment because the test compiles a
  Go plugin which transitively needs `lib/llama_darwin_arm64`. This
  library is not part of the source tree and presumably gets built by
  CI's docker image.
- **Triage:** Environmental, not a code defect. Outside Phase 1 scope.
- **Action:** Document; CI will pass these with the llama lib present.

### ui/dist build prerequisite
- **File:** `ui/embed.go:16` (`//go:embed all:dist`).
- **Status:** `go build ./cmd/nornicdb/...` fails locally without
  `ui/dist/` populated (CI/Docker runs `npm run build` first).
- **Triage:** Build prerequisite, not a defect. The executor stubbed an
  empty `ui/dist/.gitkeep` for local verification only — NOT committed.
- **Action:** None. CI tooling handles this.
