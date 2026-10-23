# PR 1646 release acceptance

Scope: accept uppercase Z as UTC in temporal map construction through the shared offset parser. Other existing timezone incompatibilities remain out of scope. Target release base: 7b7b12536b8fc8997f420324baad0fd5a08ceb0d.

- [x] Read Linear NOR-57 and all PR review and conversation feedback.
- [x] Confirm the two-file fix is directly on the current release, with no unrelated stack.
- [x] Reproduce the public Z-map failure on the exact release base.
- [x] Verify temporal regression and component behavior on default and ANTLR parsers with race detection.
- [ ] Run make test and classify any failures against the release base.
- [ ] Review and publish the candidate and updated validation evidence.

Maintainer instruction: make test is the only local verification gate; broader allocation and compatibility checks belong in CI. The existing release search lock-staging failures were independently established during PR 1571 acceptance; no temporal change reaches the search package dependency closure.

The public reproduction failed on the preceding release base. Both parser temporal families passed with race detection on the two-file temporal correction. PR 1571 then landed on the release; the temporal candidate was rebased cleanly to include it. Earlier make test attempts were canceled for missing generated UI assets and then superseded release ancestry, respectively; neither is a complete pass. The final make test checks the integrated release source.
