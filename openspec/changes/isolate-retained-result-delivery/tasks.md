# Retained result ownership: NOR-306 / NOR-307 / NOR-310

Base: release/v1.5.0 bea145d518c9ae2c9df7ced456b245321cad9565.

- [x] Read current tickets and source retention boundaries; preserve separate AC.
- [x] Delegate APOC correction, query-cache correction and statement-view audit.
- [x] Identify existing structural caller-value copy as shared owner.
- [ ] Reproduce APOC graph/warmup mutation through supported public APIs.
- [ ] Reproduce ordinary/smart query-cache mutation through public execution.
- [ ] Establish NOR-310 reachability independently of NOR-307.
- [ ] Integrate reuse without compatibility wrappers or duplicate copying code.
- [ ] Preserve result values/types, ordering, explicit writes, nil and lifecycle.
- [ ] Profile and measure representative ownership-boundary workloads.
- [ ] Run affected public, race and integration checks and resolve failures.
- [ ] Update Linear AC evidence; open and push release PR.
- [ ] Complete exact-head make-test CI and disposition duplicate findings.
- [ ] Clean completed task resources when merge-ready.

No storage-format, Badger key, WAL or query syntax changes. No blanket
collection census or speculative mutation API. No duplicate-checker changes.
