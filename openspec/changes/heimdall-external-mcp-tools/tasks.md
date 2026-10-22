# Tasks

- [x] Implement opt-in action contract and FIFO collision handling.
- [x] Wire combined public discovery and invocation after plugin loading.
- [x] Document the contract and annotate default plugin examples without exposing them.
- [x] Add deterministic registration, protocol, context and inference regression tests.
- [x] Execute focused unit tests with CGO_ENABLED=0 (all three affected packages pass).
- [x] Execute focused race checks with `-tags nolocalllm` across Heimdall, MCP, server and plugin loader.
- [ ] Execute native llama.cpp model checks (library unavailable).
- [ ] Execute release-base verification successfully.

The user authorized submitting the PR if validation cannot run. Unexecuted or
blocked checks must be identified in the PR; they are not passes.

- [x] Move native generators into the host-only `pkg/heimdall/local` adapter.
- [x] Initialize configured plugins when the harness is disabled or inference startup fails.
- [x] Compile a real plugin without local native libraries and load it in a nolocalllm host with the harness disabled.
- [ ] Verify native inference with the local adapter (native library unavailable here).

## PR review corrections

- [x] Reproduce MCP drain deadlock, retained failed-start registration and stale exported tools before fixing.
- [x] Roll back failed starts and invoke plugin cleanup before external publication.
- [x] Remove owned MCP exports and reset FIFO state on unregister/shutdown.
- [x] Stop and shut down server-owned plugins, including plugin-only mode.
- [x] Correct plugin-only startup logs, catalog text and documentation.
- [x] Validate lifecycle regressions and focused race checks; full Heimdall/MCP suites pass. All 646 server top-level tests were accounted for in isolated batches: 642 pass, one previously reported base failure, three Neo4j-dependent skips. Record the localization verification blocker and single-process suite timeout on the PR.
