# N152-122: declared procedure YIELD admission

## Approved outcome

Reject an unknown declared procedure output before invoking its handler for RUN and PROFILE, and reject the same statement during EXPLAIN, on both parsers. Preserve valid calls, nil-result normalization, explicit empty results, handler errors, arity, dynamic/undeclared headers and participant-owned signatures.

Base: `f77ec7706de099588ac232b32b6908860511d3da`, latest fetched release/v1.5.2. Parent owns architecture approval, publication and full qualification. No second registry or syntax walker.

## Owner and proposed correction

The ticket names an older helper; current `boundCompilation.procedure` in `result_columns_compile.go` is the canonical compilation owner. It already resolves the registration and validates arity in the retained CST traversal, but `projectedProcedureNodeNames` treats explicit YIELD names as outputs without checking the declaration. Runtime `applyYieldFilter` then rejects after execution.

Use authoritative nonempty `ProcedureSpec.Returns`, `columnNames` and the existing `validateYieldColumnsExist` in that compiler owner for locally owned registrations. Keep participant-owned calls delegated to the selected graph. Keep actual-result projection validation: a value-dependent header can legally contain only part of the union of declared YIELD fields.

## Declaration boundaries

`ProcedureSpec.Returns` has no explicit nil-versus-empty schema marker. Both absent and empty output metadata remain runtime-validated. The ticket's declared-empty control is a result with declared column value and zero rows, not a zero-column declaration. Signature-only metadata is also runtime-validated: CREATE PROCEDURE intentionally stores no Outputs but builds a display signature containing `(value :: ANY)`, while its body may return other names. `procedureDeclaredReturns` still serves presentation/inference and nil-result normalization; its signature fallback is not a declaration-error admission guarantee. The first broader validation run exposed this difference through four existing Fabric procedure-write controls; the parent approved the authoritative Returns gate already used by pipeline admission.

`ResultSchemas` describes closed runtime header alternatives; Returns still declares all legal YIELD fields. `db.temporal.asOf` and retrieval procedures already document these alternatives. Do not reject a legal union field merely because one runtime alternative omits it.

## Reuse and remaining runtime boundary

Reuse the existing authoritative Returns metadata, column-name conversion, YIELD name validator and compiler traversal. Runtime actual-header validation remains necessary for dynamic and value-dependent outputs. `pipelineApplyProcedureCall` also validates a fake declared header; direct internal/test callers bypass compilation, so complete caller convergence has not been established. Preserve that existing boundary guard in this scoped fix; add no new duplicate implementation. Full indexed duplicate review remains a separate gate.

## Verification

Preserve the two exact test source blocks from Plane N152-122: original eight RED cells plus valid/nil/empty/error/arity controls. Add nested and empty-input pipeline no-handler assertions, signature-only display metadata, persisted CREATE PROCEDURE outputs, aliases, undeclared metadata and value-dependent output controls. Run bounded fail-first, focused normal/race/vet and applicable localization checks. Report all evidence and limits without implying full qualification.

## Integration boundary

The user assigned rebasing and existing PR review to another agent while this work was active. No rebase occurred; current evidence remains on f77ec7706 plus task edits. Combined current-release verification belongs to the integration owner.
