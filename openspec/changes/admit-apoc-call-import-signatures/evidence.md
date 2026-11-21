# Independent verification and ownership evidence

Base: `1a6eb031471688a71966a68147740ead6c43d6cb`.

DeepSeek job `b409f80798c4499da16ca65d02c583c9` proposed the 12-line policy correction in about 601 seconds. The parent independently reviewed the canonical policy, callable invocation and actual handler implementations. The two new metadata entries reuse the existing binding and checked invocation owner; no per-name dispatcher or second reflection invoker was added. The export signature plans serve distinct handler types, while plugin_exports procedure classification answers a different question.

The parent recovered the original 579-line regression from N152-56 and ran it on the unchanged release base. It failed in the reported admission path (0.449s, exit 1; `/tmp/n152-56-baseline.log`). This original suite was retained instead of the worker's replacement fixture: Initialize is once-only and could leave the worker fixture disconnected from the active registry. The original SetStorage helper binds and restores the actual backend; its sentinel records all operations and the real-node fixture uses that bound backend.

With the proposed policy, the original suite and policy controls passed independently:
- `GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -p=1 -tags=noui,nolocalllm ./apoc ./apoc/registry -run 'TestAPOCCall|TestCallable|TestArgument|TestAPOC' -count=1 -timeout=3m`: pass, 0.585s / 0.337s.
- `GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -p=1 -tags=noui,nolocalllm ./apoc/... -count=1 -timeout=5m`: all packages passed; packages without tests are reported as such.
- Same focused root/registry checks with `-race`: pass, 2.566s / 1.396s.
- Final branch root/registry full tests with the retained original fixture: pass, 0.621s / 0.325s.

The tested production file was byte-identical to the final branch. Parent logs: `/tmp/n152-56-original-fixed.log`, `/tmp/n152-56-apoc-all.log`, `/tmp/n152-56-race.log`, `/tmp/n152-56-final.log`.

`make verify-list BASE=origin/release/v1.5.2 FETCH=0` ran. `make verify BASE=origin/release/v1.5.2 FETCH=0 SKIP='allocations conformance differential'` completed as **Nothing to run**, not a suite pass: current verifier CI/docs path filters do not select apoc-only code, and the OpenSpec-selected allocation/conformance/differential checks were explicitly unselected for this bounded admission change. Full release/build/CI qualification and benchmarks remain unexecuted. The independent APOC tests above supply the actual affected-package evidence.

The existing `scripts/code-intelligence-hook.sh check-staged` was run after staging and exited 1: local endpoint `http://localhost:7474` returned an analysis/ingestion failure. The earlier worker Graph RAG receipt also reported timeouts and an unavailable code baseline. No clean duplicate result is claimed. No checker or hook was modified, disabled or installed. CONTRIBUTING.md identifies local hooks as optional tooling and permits candidate publication before full qualification; this PR remains a draft with graph analysis pending.

No import execution, query execution or database writes are implemented or inferred: these existing handlers return argument-derived summaries/placeholders. This correction only admits their two registered signatures through exported apoc.Call. Other signatures and the public Cypher route are outside its claimed scope.
