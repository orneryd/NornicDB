# N152-64 forwarded preaccept context and transport ordering

- [x] Parent: correct the forwarding transport so a forwarded preaccept inherits
      the signed absolute caller deadline and the connection cancellation, while
      accepted durable work keeps its replicator owner (`transport.go`,
      `handlers.go`, `transport_forward.go`).
- [x] Parent: record the fail-first baseline — the two existing TCP forwarding
      tests failed against `18b36a85d` (cancel and deadline leaves), and the
      corrected head passes the focused run (`/tmp/n152-64-parent-first.log`).
- [x] Unit-test lane: add `pkg/replication/transport_forward_context_test.go`
      covering the payload contract, signed deadline enforcement, cancellation
      lifetime, accepted-work ownership and the connection ordering/idle/join
      contracts, reusing the existing fixtures and helpers.
- [x] Unit-test lane: focused run green
      (`GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -p=1 -tags=noui,nolocalllm
      -count=1 -run '<the eleven new tests>' ./pkg/replication`), see
      `evidence.md`.
- [x] Unit-test lane: focused `-race` run green, and the complete
      `./pkg/replication` package run green with the same resource limits.
- [x] Parent: reviewed and amended cleanup ordering; independent focused race run passed (4.161s).
- [x] Parent: verify-list completed; `make verify BASE=origin/release/v1.5.2 ONLY=localization` passed (8s). UI, llama, build, full tests, allocations, conformance, differential and docs skipped; full qualification remains pending.
- [ ] Parent: release-branch integration, PR and full qualification.
