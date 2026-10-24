# PR 1612 release readiness

- [x] Confirm NOR-22 assignment and exclude T3; audit reviews, inline and conversation comments.
- [x] Rebase onto current release with strict admission before canonical schema binding.
- [x] Reproduce the remaining lexical/preamble, completion and transport findings.
- [x] Verify shared lexical facts and public Execute/HTTP/Bolt corrections.
- [x] Preserve the superseded make test as incomplete; prepare the integrated current-release gate.
- [x] Prepare exact-head/base receipt and current Linear tracking for publication.

The maintainer-selected local gate is make test. Broader checks remain for CI.

The release advanced from 384a8e505 to f91dac08c during verification. The older full run was canceled and is not a pass. Final acceptance and publication are recorded in the PR result comment and NOR-22; this checklist records implementation and preparation, not an unexecuted gate result.

- [x] Restore grammar-owned source-column preservation in strict normalization; all failed public matrix roots pass, and exact-release controls also pass.
