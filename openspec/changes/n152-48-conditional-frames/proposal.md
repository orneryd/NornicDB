# N152-48: preserve inherited outputs through generated conditional frames

## Contract

The retained NOR-144 matrix pins the reference's explicit inherited-column
projection and its wildcard rejection. A conditional arm returning the inherited
value unchanged must succeed without changing the existing rejection of a
user-written nested CALL returning that name. Conditional guards must remain
outside each arm's CALL, so aggregates in unselected arms cannot manufacture rows.

## Owner and implementation

The existing query-structure lowerer keeps its CASE selection, outer dispatch
CALL, and guarded per-arm CALL. At the emission site it records only generated
CALL keyword spans. The existing rewrite application adds their accepted offsets
to queryRewrite; withSourceRewrite retains those marks on the request's source
view. The source mapper's ancestor-relative operation follows exact windows and
unchanged token spans through later rewrites. Quoted-variable normalization keeps
that lineage when a conditional frame exists.

The existing callSubqueryReturnsOuterUnchanged owner still rejects a user nested
CALL exporting the name. Its only recursion exception is a CALL whose exact token
has the retained conditional-emission provenance. It still checks every branch's
unchanged-value contract; a generated frame is not a blanket rebinding exemption.
No parser, generated-name heuristic, shared mutable syntax annotation, or alternate
scope validator was introduced.

Semantic verdict caches omit annotated contexts because their existing keys are
text-only. Otherwise a valid generated lowering could authorize a user-pasted copy.
Syntax and projection-facts caches remain shared: neither owns this semantic rule.

## Reuse and duplicate investigation

Reviewed the lowerer, applyLabelRewriteEdits, withSourceRewrite, retained Source
Rewrite/Window ancestry, canonical SyntaxNode CALL token spans, and the existing
CALL scope walk. Source.SpanInAncestor reuses the existing OriginalSpan mapper for
each lineage step instead of adding a second offset algorithm. Provenance is
request-local and belongs to exact source identity. Existing CALL validation is
reused recursively only at generated frames. The broader worker relaxation was
superseded; its patch is preserved at /tmp/n152-48-superseded-wide-call.patch.

The duplicate graph/checker and broad release qualification are parent-owned and
not claimed passed here. No performance claim is made from focused tests.

## Evidence

The explicit user nested-CALL regression failed against the superseded worker
candidate: expected error, got nil (1.116s), /tmp/n152-48-user-nested-red.log.
Further exact command/results and limitations are tracked in tasks.md.
