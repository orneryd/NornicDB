# Narrow a label scan's node property demand

## Why

A labelled `MATCH` reads a node image for every row. The image's identity and
labels are always available; its property bodies are decoded only when a row
reads them. The pipeline already narrows a scan to the properties the statement
names (`pipelineLabelScanProjection`, `pipelineNodePredicateProperties`), but it
treated *any* bare reference to the pattern variable as a whole-node read, so
statements that read only identity, labels, existence or a list's size still
decoded every stored property body.

Measured on the reported fixture (one `:EIBig` node with a 1-byte property and
an unrelated 128 KiB property), 13 of 14 scalar/header-only statements demanded
the 128 KiB body: `id(n)`, `elementId(n)`, `labels(n)`, `n IS NULL`,
`n IS NOT NULL`, `WHERE n IS NOT NULL`, `size([n])`, `size(collect(n))`,
`WITH n RETURN 1`, `n['small']`, `(n).small`, `head([n]).small` and the combined
identity/scalar projection. Only `RETURN n.small` was already narrow.

## What changes

`pipelineNodePredicateProperties` gains the observation analysis: each
expression position is read as a consumer of the value (identity/labels only,
one named property, a list's element count, or the whole body) instead of
"bare reference means everything". It reuses the existing structure owners
(`forEachSemanticExpressionReference`, `stripEnclosingExpressionParentheses`,
`simpleSemanticIdentifier`, `splitPostfixPropertyAccess`, `rowSubscriptStart`,
`isListLiteral`, `splitTopLevelComma`, `inspectFunctionCall`,
`hasSuffixFoldASCII`, `decodeCypherStringLiteral`) and the existing
property-agnostic call table (`traversalSafeArgumentFunctions`). A form it does
not read keeps the previous conservative walk, so an unknown call, a map or
list escape, a dynamic subscript or an operator over the variable still reads
the whole body.

`pipelineLabelScanProjection` reads the read-only clauses from their retained
syntax (`projectionItemsFromSyntax`, the projection body's ORDER BY / SKIP /
LIMIT and a WITH's attached WHERE) instead of their whole text, and follows the
variable across a `WITH`: the item that projects it on its own carries it under
that item's name, so a rename is followed and a WITH that binds anything else
ends the demand. `pipelineMatchPhysicalHint.readTail` and its consumers carry
the clauses, not their texts; the traversal endpoint read keeps its existing
whole-clause analysis.

## Out of scope

`pipelineLabelScanArguments`, the temporal-viewport contract, mutation and
native full-image contracts, and exported API are unchanged. A traversal
endpoint (`traversalSourceProperties`) still analyses each clause's text, so
`MATCH (a)-[r]->(b) RETURN id(a)` still decodes `a`'s body; converging it needs
one shared tail-demand owner, which the lexical-population inventory would
record as a new member.

## Evidence

The failing baseline is retained: `TestScalarNodeUsesDoNotDemandUnusedPropertyBodies`
failed 13/14 subtests on the un-narrowed projection ("the read demanded every
stored property body, including the unused 128 KiB one"). The correction is
verified by the scalar demand, escape, detachment and storage-header tests plus
the projection unit tests and the nearby projection/row/function suites.
