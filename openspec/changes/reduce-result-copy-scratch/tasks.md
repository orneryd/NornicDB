# Reduce result-copy scratch

- [x] Preserve baseline copy implementation and compile matching benchmark binaries.
- [x] Profile wide-result allocation cost and locate identity-table growth.
- [x] Replace full-record table growth with stable blocks and compact handles in the canonical copy owner.
- [x] Preserve source/destination identity, concrete types, cycles, vector detachment and pool clearing.
- [x] Run focused race checks and interleaved memory/time measurements.
- [x] Document measured time tradeoff and remaining vector-output cost.
- [ ] Run full CI and duplicate review on the pushed head.
- [ ] Complete the remaining B31 workload qualification; this scratch correction alone does not close the whole report.
