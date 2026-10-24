# PR 1612 release readiness

- [x] Confirm NOR-22 assignment and exclude T3; audit reviews, inline and conversation comments.
- [x] Rebase onto current release with strict admission before canonical schema binding.
- [x] Reproduce the remaining lexical/preamble, completion and transport findings.
- [x] Verify shared lexical facts and public Execute/HTTP/Bolt corrections.
- [ ] Run make test and resolve PR regressions without skipping failures.
- [ ] Prepare exact-head/base receipt and Linear publication tracking.

The maintainer-selected local gate is make test. Broader checks remain for CI.
