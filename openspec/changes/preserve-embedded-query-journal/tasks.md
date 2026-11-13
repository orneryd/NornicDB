- [x] Reproduce durable embedded data loss on current release; retain async control.
- [x] Reuse native WAL mutation publication at the existing transaction capability.
- [x] Assert complete embedded/explicit CRUD replay and rollback exclusion.
- [x] Assert foreign/canceled/closed-journal admission rejects before publication.
- [x] Run focused storage/Cypher/nornicdb checks with default parser and ANTLR.
- [x] Publish the line-item PR against release/v1.5.0 and link from #1803.
- [ ] Obtain exact-head CI and advisory duplicate-review results.
- [ ] Integrate and reverify the combined release candidate before shipping.

## Shared release CI correction

- [x] Collect all initial CI shard failures and reproduce deterministic failures on release baseline.
- [x] Restore temporal procedure localization metadata and verify both approved output schemas.
- [x] Correct projected label-stream filtering through the existing mandatory stream owner.
- [x] Correct snapshot regression ordering to assert the exact pre-write snapshot excludes the later node.
- [x] Register test-owned storage with existing fixture lifetime owners.
- [x] Align epoch grammar coverage and test query-body rejection without schema/graph effects.
- [x] Integrate approved exact lexical, counter and local-endpoint inventory updates with rejection controls.
- [x] Refresh the generated full census report from exact-head CI output; no local full census run.
- [ ] Push shared correction and obtain exact-head regular make-test CI success.
