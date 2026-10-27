# Follow-up acceptance TODO

- [x] Merge checkpoint PR1515, pull release head, verify identical tree.
- [x] Preserve original acceptance contract and all audit inventories.
- [ ] Complete and verify code-graph ingestion at merged release head.
- [ ] Run enabled duplicate checker against refreshed release graph; resolve every reported finding.
- [ ] Reconcile every original issue1513 acceptance criterion, including credential precedence, actual authenticated remote lifecycle HTTP/Bolt implicit/explicit open/query/commit/rollback, persistence, upgrade/read-only/unkeyed and backup behavior. Preserve approved contract changes explicitly.
- [ ] Requalify all earlier evidence against integrated source; cover public callers and both parser modes.
- [ ] Complete the 67-finding tracker and 210-row batch5 inventory without treating either as superseding the other; retain whole-declaration and decoded-name open populations until refreshed and reviewed.
- [ ] Finish deferred workload-specific performance qualification, including LongASCII Lex, imported/scoped policy/transport, lookup workloads and stored-user-record mapping.
- [ ] Reconcile private advisory evidence and review-ready drafts without publishing.
- [ ] Run uncached make test after behavioral corrections and retain full logs; fix all regressions.
- [ ] Publish exact-head evidence and update linked GitHub/Linear criteria; leave unproved criteria open.

No API migration or behavioral change is made by this inventory-only commit.

## Current acceptance proof

- [x] Re-run real authenticated remote lifecycle matrix with native parser.
- [x] Re-run same matrix with ANTLR runtime parser.
- [x] Re-run focused persistence/runtime/wire checks on unchanged merged source.
- [ ] Establish successful authenticated remote-write durable commit/rollback proof separately from read lifecycle.

See evidence.md for commands, results and limits; these do not close the broader acceptance checklist.
