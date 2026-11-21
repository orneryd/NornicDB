# TODO

- [x] Read original/release NOR-280 problem, complete AC and reproduction source.
- [x] Preserve the exact original reproduction before edits; check primary Neo4j format references.
- [x] Trace public exporter/CLI and shared CSV writer; compare early graph candidates to current source.
- [x] Verify current-base fail-first original cases and controls: original four failures/six controls on macOS0.726s.
- [x] Check delimiter positions, Unicode delimiters, empty/edge-whitespace normalization and protected existing output; expanded original-export overlay fails14cases/8controls0.942s.
- [x] Implement complete label preflight in existing exportNodesCSV before writeEntityCSV; no new encoder or property-array behavior.
- [x] Preserve localized actionable errors/ExitUnsupported6 through real Cobra command, default/custom delimiters and whitespace; English/Spanish/pseudo-locale rendering and no-file checks pass1.938s.
- [x] macOS final22case focused pass1.387s; affected adminimport2.177s/localization4.377s/adminCLI2.736s pass before the final test-only roundtrip assertion refinement.
- [x] Review the two-new-diagnostic proposal: neither per-ID constructor is reusable elsewhere, and that read-only round changed no source.
- [x] Consolidate the one label-representability invariant into one diagnostic: AdminExportLabelUnsupported(label, delimiter), ID adminimport.export_label_unsupported, one catalog entry per active locale, one conditional in the existing exportNodesCSV preflight.
- [x] Re-run current-source checks after consolidation: NOR-280 public22case, real Cobra localized error cases, adminimport/localization/adminCLI/localization_catalog/localization_inventory, catalog -check, vet, gofmt.
- [x] Reuse the existing bounded before/after cost evidence; the consolidated predicate keeps the same per-label work, so no new benchmark was requested or run.
- [ ] Parent-owned final gate after this source freeze: unchanged canonical duplicate checker, Linux cross-check and the staged release gate.

Consolidation rationale: the delimiter, empty-label and surrounding-whitespace
conditions are one Neo4j CSV `:LABEL` representability contract, so they belong
to one decision, one constructor, one ID and one message. Nothing is masked -
the rejected label set is the union of the former branches and every public
path, exit class and output ordering is unchanged. The second owner of the same
invariant is gone; the finding stays reported.

Unrelated surfaces are untouched: no importer, encoder, CLI error class,
checker, index, suppression or receipt change. The canonical duplicate checker,
reingestion and the staged gate remain with the parent after this source freeze.

## Parent acceptance on integrated release 7c61a23f5

- [x] Reviewed all original AC and full public export/import/CLI/copy boundaries; independently accepted the single representability predicate and localized diagnostic after DeepSeek175434d0747f42f0be4fb92ac5b17d2f.
- [x] Preserved fail-first on current release through unchanged-exporter overlay:14 failures/8 controls on macOS0.847s and Linux0.881s. The exporter/importer are unchanged from shipped v1.5.1 through this integration base.
- [x] Final parent macOS five affected packages/tools PASS: adminimport1.918s,localization4.172s,adminCLI2.739s,catalog0.327s,inventory0.260s; affected vet and catalog -check PASS.
- [x] Final parent Linux same five packages/tools PASS:4.714s/8.447s/4.921s/0.061s/0.010s, one2CPU/4GiB Radeon container, exit0/notOOM. Logs/inspect preserved; completed container removed normally. The earlier pre-consolidation Linux three-package pass is separate evidence.
- [x] Complete expanded public/CLI source and current commands/results are inline in existing Plane tickets; original historical source retained, no attachments or duplicate issues.
- [ ] Unchanged canonical duplicate gate on this frozen staged tree. Its result remains parent-owned; no checker/index/threshold/receipt edits.
- [ ] Publish to release/v1.5.2, milestonev1.5.2; exact-head approved CI. Full local repository verification, race/stress, allocation ratchet, differential and docs-site build remain unexecuted under focused/package policy.
