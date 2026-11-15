# Repeated-endpoint traversal limits

- [x] Reproduce 1,000 adjacency reads for one requested self-loop row on release a492f2acd.
- [x] Enforce repeated endpoint identity in the canonical walk before a path consumes the limit.
- [x] Verify exact rows, LIMIT/SKIP windows, non-loop rejection and chained repeated-node identity under both parsers.
- [x] Run focused race checks and paired allocation/CPU profiles on the identical generated fixture.
- [ ] Publish the candidate against release/v1.5.0 and run exact-head CI.
