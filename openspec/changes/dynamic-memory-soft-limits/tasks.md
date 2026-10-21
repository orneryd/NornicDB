## 1. Draft planning evidence

- [x] Record maintainer decisions, soft-limit boundaries and pending operator choices.
- [x] Inventory release GC/context/storage owners and develop transaction tracker.
- [x] Link lane A #1615/#1616 and memory implementation #1368 plus follow-ups.
- [ ] Validate OpenSpec artifacts and run required PR verification; post exact SHAs/results.
- [ ] Publish a draft PR containing only this proposal and its implementation plan.

## 2. Before implementation

- [ ] Resolve the explicit setting/sizing/telemetry/victim-policy decisions in design.md.
- [ ] Establish lane A's integrated passing cancellation evidence without relaxing deadlines.
- [ ] Inventory every memory charge site, lifetime, transport and storage acceptance/commit fence.
- [ ] Reproduce the 8 GiB container / 7168 MiB GC-target OOM with immutable baseline,
      actual peak memory, query, server survival, cgroup events and restart evidence.
- [ ] Reproduce current compatibility errors against pinned Neo4j 5.26 and preserve results.

## 3. One integrated implementation

- [ ] Port/reuse the complete reviewed develop tracker and follow-ups with transaction defaults zero.
- [ ] Add one application policy owner using existing constraint/measurement infrastructure.
- [ ] Implement provisional startup sizing, resource-transition updates, periodic reconciliation,
      separate persistent/active/background attribution and adaptive headroom.
- [ ] Integrate cached pressure decisions and cancellation causes into the canonical execution gate;
      migrate all lane A consumers and remove superseded competing checks.
- [ ] Add approximate pre-growth reservations with correct transaction/result retention and releases.
- [ ] Integrate HTTP/Bolt admission, safe active-transaction cancellation, background budgets and
      commit/rollback/flush completion capacity without patching Badger or changing durability policy.
- [ ] Expose current limit/baseline/reserve/usage, sample age, rejection/cancellation reasons,
      transaction identity and actual storage outcome through existing diagnostics.

## 4. Integrated proof before readiness

- [ ] Add fail-first deterministic injected-sampler/allocator/clock tests: baseline adaptation,
      no concurrent double charging, limit shrink, hysteresis, stale samples and first-error precedence.
- [ ] Verify original single-query OOM without new admissions; measure cancellation latency and overshoot.
- [ ] Verify concurrent transaction/pool exhaustion, no victim cascade, cleanup and subsequent service recovery.
- [ ] Cover sort/collect/DISTINCT/projection/path/subquery growth, final validation, results/backpressure,
      explicit retained writes, both parsers and every supported public execution consumer.
- [ ] Test HTTP 503 admission and Bolt/HTTP transaction errors, rollback/reset and fresh-session side effects.
- [ ] Exercise Badger direct/buffered/async modes and configured durability modes under slow flush/fsync,
      dirty-page/compaction pressure, native/model work, failures and restart durability checks.
- [ ] Prove pre-fence cancellation leaves no writes and post-fence pressure cannot report fictitious rollback.
- [ ] Run race and memory/retention tests, disabled/permissive/restrictive paired benchmarks with CPU/heap
      profiles across workload sizes; revert regressions or complexity without measured benefit.
- [ ] Publish support/limitations documentation explaining soft limits, accounting coverage, residual OOM
      risk and thresholds below container ceilings; do not advertise a hard memory bound.
- [ ] Run final required make verify against the actual integration base and post exact head/base,
      result, cached evidence, skips and remaining machine-specific tests before implementation readiness.
