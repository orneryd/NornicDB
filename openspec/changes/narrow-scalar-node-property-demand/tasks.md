# Narrow scalar node property demand

- [x] Reproduce the over-read: 13/14 scalar/header-only statements demand the
      unrelated 128 KiB property body (`TestScalarNodeUsesDoNotDemandUnusedPropertyBodies`,
      baseline failures retained: every subtest failed on
      `require.NotNil(t, scan.projection)`).
- [x] Replace the "any bare reference reads the whole node" rule with an
      observation analysis in the existing owner
      (`pipelineNodePredicateProperties`), not a parallel parser, substring
      allowlist or query-specific bypass; reuse the existing expression
      structure owners and the `traversalSafeArgumentFunctions` table.
- [x] Follow the variable across `WITH` from the retained clause syntax: a
      rename is followed, a second pass-through and a binding to anything else
      keep the whole demand (`TestScalarNodeDemandPreservesEveryAliasEscape`,
      `TestPipelineLabelScanProjection`).
- [x] Read a clause's other row consumers from the same retained syntax:
      projections, ORDER BY, SKIP, LIMIT and a WITH's attached WHERE
      (`TestWithPaginationThenWhere`, `TestBug_WithAttachedWhere*`).
- [x] Keep the escapes whole: a returned node, `properties()`, a list or map
      escape, a native callback, an unknown call and a dynamic subscript
      (`TestNodeImageEscapesKeepWholePropertyBodies`).
- [x] Keep delivered images detached from storage and the storage header
      contract (`TestDeliveredNodeImagesStayDetachedFromStorage`,
      `TestProjectedLabelScanDeliversHeaderWithoutProperties`).
- [x] Cover quoted, commented, nested and computed occurrences
      (`TestPipelineLabelScanProjection`,
      `TestProjectedPropertiesRetainReferenceBoundaries`).
- [x] Prove the public path reaches the narrowed read: the recording label scan
      sees `[]` / `["b"]` projections for header-only and single-property
      statements (`TestLabelScanReadsOnlyUsedProperties`).
- [x] Verify the affected owners: `go vet ./pkg/cypher`, `go fmt ./pkg/cypher`,
      and the focused Pipeline / LabelScan / Projected / Traversal /
      Row / Return / With / Aggregat / Function / Subscript / Semantic /
      Static / Expression / Match / Scan / Count / Order / Unwind tests.
- [x] Converge label and traversal tail demands on pipelineNodePropertyDemand
      and pipelineTailDemand; retain the path-value whole-image boundary.
- [x] Migrate fn.Register to a typed input-image declaration independent of
      PROFILE policy, including metadata-only argument inputs.
- [x] Preserve static custom replacements and implicit binding reads.
- [x] Reproduce and repair full-vector callback input loss through the canonical
      statement image owner; retain metadata-only ordinary reads.
- [x] Retire the duplicate row identity evaluator and dispatch through the
      registered canonical implementation instead.
- [ ] Rebase onto current release and repeat affected focused/package checks.
- [ ] Qualify remaining allocation checks on the updated release baseline.
- [ ] Publish and complete exact-head CI; review duplicate/audit findings.
