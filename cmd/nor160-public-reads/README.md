# NOR-160 public storage-read census fixture

Author: Codex, 29 October 2026. Analysis-only client, not product code.

Consumes maintained `internal/workcensus.LoadProgram` at base commit
`b013bdfd4b8064725d63b29f3d2e9467408b3ae7`. No loader, SSA builder or generic
extractor is copied or modified. The separate fixture branch keeps the client
out of NOR-160's product delta. Remove this temporary client/fixture branch
when the lane's evidence and upstream consumer obligations are settled.

Run only on TrueNAS after exact analysis admission:

```text
go run ./cmd/nor160-public-reads git:110509e49c8ca510c58e2500bb40a3cbd3a1f7ad -
```

`NOR160_CENSUS_CONTROL=1` adds a virtual outside-storage file with typed
visitor/alias and erased abstract-Engine reads. Compare outputs and require
both new methods. No physical product overlay is written.

The lab worker shares Git objects with earlier pushed110 jobs. `git:SHA`
archives that exact tree into a generated temporary directory, analyzes it,
then removes only that directory. It does not change the tool checkout. A
missing object fails visibly; no fetch, alternate source or private loader.

Go1.26.9+, CGO_ENABLED=0, GOWORK=off, GOMAXPROCS=2, GOMEMLIMIT=3GiB,
GOFLAGS=-p=2. Compile-only preflight both parsers before analysis. One lane
analysis at a time; request6GiB admission and30m timeout. Prior equivalent
loads peaked2.15–2.91GiB. No product tests or main/init functions execute.

Scope: storage public node/edge reads across every engine/adapter, transaction
and buffer getter. Broader discovery candidates remain for classification.
Cypher result caches, Progressive pages, search materialization/continuations
and APOC-owned images are explicitly out of NOR-160 scope and are not edited.
