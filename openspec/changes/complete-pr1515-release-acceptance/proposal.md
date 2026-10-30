# Complete PR1515 release acceptance

PR1515 merged into release/v1.4.4 at `04c87de3e232fece79ebf3eb5af78e5b3fe5e09f`, tree `8afe9eb5b31969ef5f41a2539fc03ca4f9f53592`. This follow-up retains all original criteria without current proof and all remaining convergence work. The linked issue1513 and NOR-6 stay open.

The checkpoint passed uncached `GOFLAGS=-count=1 make test`: 121 packages, exit0, 970.966s, plus both lint gates. Four later gofmt whitespace-only changes have identical Go scanner token streams. The squash merge preserved the tested candidate tree. This supersedes historical full-suite failures only; it does not qualify unexercised acceptance criteria.

The checker was bypassed only for the authorized checkpoint commit/push; normal local configuration is restored. Benchmarking was deferred until push. Remaining findings are not waived or dismissed. Use a newly ingested immutable release baseline, review every finding, and establish public-path evidence before resolving criteria.

The companion original-contract.md preserves the original public issue/PR descriptions and full carry-forward inventory. Its evidence labels describe the earlier inventory snapshot; reconcile each with current source and receipts rather than treating unchecked rows or retired declarations as completion. Do not publish private advisory details.
