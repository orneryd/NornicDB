# PR 1646 release acceptance

Scope: accept uppercase Z as UTC in temporal map construction through the shared offset parser. Other existing timezone incompatibilities remain out of scope. Target release base: eb16d6003afc6b39adefba8ae06f2531eabbeaff.

- [x] Read Linear NOR-57 and all PR review and conversation feedback.
- [x] Confirm the two-file fix is directly on the current release, with no unrelated stack.
- [ ] Reproduce the public Z-map failure on the exact release base.
- [ ] Verify temporal regression and component behavior on default and ANTLR parsers with race detection.
- [ ] Run make test and classify any failures against the release base.
- [ ] Review and publish the candidate and updated validation evidence.

Maintainer instruction: make test is the only local verification gate; broader allocation and compatibility checks belong in CI. The existing release search lock-staging failures were independently established during PR 1571 acceptance; no temporal change reaches the search package dependency closure.
