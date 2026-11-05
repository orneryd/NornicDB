# PR #1688: delivered-head advisory dispositions

Analyzed source: `2444cb481ac084bc1b00218446453937379b3fcc`; [CI advisory run](https://github.com/orneryd/NornicDB/actions/runs/42064128213). The indexed reference remains `35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6`, not base `8f5835d7f`. This supplements, rather than discards, the [original 225-record ledger](pr1688-duplicate-dispositions.md).

CI displayed 74 refactor targets and 53 reuse candidates from 508 analyzed functions. Expanding the same artifact with unchanged rules/thresholds and no presentation caps returned 74 refactor / 55 reuse candidates: two additional behavioral matches appeared on the live requery. This ledger deliberately includes the expanded superset: **129 candidates / 515 matched pairs, 133 current-source ownership records and one AST call (263 records)**. The live requery is not represented as byte-identical to the earlier CI response. Zero cap skips; stale-baseline warning retained.

The 97 generated candidates map to the same original generated declarations; their file is unchanged by this follow-up. Per-entry grammar/context ownership decisions remain in the original ledger. Handwritten decisions below name current calls, required boundaries and concrete consolidation. No checker/rules/thresholds/exclusions/hooks are modified.

## All candidates and matched identities

### N001 — [.Expression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L22116)

Reported **refactor**; 16 matched declarations:

- [.OrderSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L15171) — code; similarity 1.0
- [.LetSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16407) — code; similarity 1.0
- [.RemoveSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L18012) — code; similarity 1.0
- [.SetSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19840) — code; similarity 1.0
- [.Pattern() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 1.0
- [.XorExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22172) — code; similarity 1.0
- [.AndExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22350) — code; similarity 1.0
- [.PowerExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23473) — code; similarity 1.0
- [.ExpressionType() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L25526) — code; similarity 1.0
- [.PropertyExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26248) — code; similarity 1.0
- [.InvocationName() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30402) — code; similarity 1.0
- [.ExpressionChain() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32100) — code; similarity 1.0
- [.ComparisonExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22771) — code; similarity 0.961
- [.LabelExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L20637) — code; similarity 0.881
- [.YieldItems() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19158) — code; similarity 0.876
- [.AddSubExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23087) — code; similarity 0.818

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) Expression() (localctx IExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F001 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N002 — [.XorExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L22294)

Reported **refactor**; 16 matched declarations:

- [.LetSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16407) — code; similarity 1.0
- [.SetSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19840) — code; similarity 1.0
- [.Pattern() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 1.0
- [.Expression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 1.0
- [.AndExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22350) — code; similarity 1.0
- [.PowerExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23473) — code; similarity 1.0
- [.ExpressionType() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L25526) — code; similarity 1.0
- [.PropertyOrLabelExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26053) — code; similarity 1.0
- [.PropertyExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26248) — code; similarity 1.0
- [.InvocationName() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30402) — code; similarity 1.0
- [.ExpressionChain() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32100) — code; similarity 1.0
- [.ComparisonExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22771) — code; similarity 0.961
- [.LabelExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L20637) — code; similarity 0.881
- [.YieldItems() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19158) — code; similarity 0.876
- [.LabelConjunction() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L20845) — code; similarity 0.818
- [.AddSubExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23087) — code; similarity 0.818

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) XorExpression() (localctx IXorExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F002 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N003 — [.AndExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L22472)

Reported **refactor**; 16 matched declarations:

- [.OrderSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L15171) — code; similarity 1.0
- [.LetSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16407) — code; similarity 1.0
- [.SetSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19840) — code; similarity 1.0
- [.Pattern() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 1.0
- [.Expression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 1.0
- [.XorExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22172) — code; similarity 1.0
- [.PowerExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23473) — code; similarity 1.0
- [.ExpressionType() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L25526) — code; similarity 1.0
- [.PropertyOrLabelExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26053) — code; similarity 1.0
- [.PropertyExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26248) — code; similarity 1.0
- [.InvocationName() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30402) — code; similarity 1.0
- [.ExpressionChain() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32100) — code; similarity 1.0
- [.ComparisonExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22771) — code; similarity 0.961
- [.LabelExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L20637) — code; similarity 0.881
- [.YieldItems() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19158) — code; similarity 0.876
- [.AddSubExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23087) — code; similarity 0.818

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) AndExpression() (localctx IAndExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F003 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N004 — [.PropertyExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L26370)

Reported **refactor**; 16 matched declarations:

- [.OrderSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L15171) — code; similarity 1.0
- [.LetSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16407) — code; similarity 1.0
- [.RemoveSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L18012) — code; similarity 1.0
- [.SetSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19840) — code; similarity 1.0
- [.Pattern() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 1.0
- [.Expression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 1.0
- [.AndExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22350) — code; similarity 1.0
- [.PowerExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23473) — code; similarity 1.0
- [.ExpressionType() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L25526) — code; similarity 1.0
- [.PropertyOrLabelExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26053) — code; similarity 1.0
- [.InvocationName() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30402) — code; similarity 1.0
- [.ExpressionChain() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32100) — code; similarity 1.0
- [.ComparisonExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22771) — code; similarity 0.961
- [.LabelExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L20637) — code; similarity 0.881
- [.YieldItems() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19158) — code; similarity 0.876
- [.AddSubExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23087) — code; similarity 0.818

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) PropertyExpression() (localctx IPropertyExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F004 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N005 — [.MapLit() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L8795)

Reported **refactor**; 15 matched declarations:

- [.MapLit() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L2845) — code; similarity 1.0
- [.ParenExpressionChain() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8700) — code; similarity 1.0
- [.Symbol() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8716) — code; similarity 1.0
- [.NodePattern() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8768) — code; similarity 1.0
- [.RelationshipsChainPattern() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8784) — code; similarity 1.0
- [.ExpressionChain() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8808) — code; similarity 1.0
- [.FunctionInvocation() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8840) — code; similarity 1.0
- [.ConstraintRequirement() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8864) — code; similarity 1.0
- [.Expression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8888) — code; similarity 1.0
- [.PropertyTypeName() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8916) — code; similarity 1.0
- [.ListLit() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L10378) — code; similarity 1.0
- [.MapLit() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L27927) — code; similarity 1.0
- [.MapProjection() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L28366) — code; similarity 1.0
- [.NumLit() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L33244) — code; similarity 1.0
- [.MapLit() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L33312) — code; similarity 1.0

**Disposition — retain generator ownership.** This is the generated context accessor `func (s *SchemaCommandContext) MapLit() IMapLitContext`. The receiver and asserted child-context type are part of the generated tree API; retain it rather than hand-patching generated getters. Original evidence: F005 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N006 — [.Parameter() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L8811)

Reported **refactor**; 15 matched declarations:

- [.Parameter() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L7234) — code; similarity 1.0
- [.ParenExpressionChain() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8700) — code; similarity 1.0
- [.Symbol() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8716) — code; similarity 1.0
- [.MapLit() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8740) — code; similarity 1.0
- [.NodePattern() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8768) — code; similarity 1.0
- [.RelationshipsChainPattern() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8784) — code; similarity 1.0
- [.ExpressionChain() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8808) — code; similarity 1.0
- [.FunctionInvocation() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8840) — code; similarity 1.0
- [.ConstraintRequirement() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8864) — code; similarity 1.0
- [.Expression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8888) — code; similarity 1.0
- [.PropertyTypeName() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8916) — code; similarity 1.0
- [.Parameter() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L27943) — code; similarity 1.0
- [.Parameter() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L28350) — code; similarity 1.0
- [.Parameter() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L39041) — code; similarity 1.0
- [.Parameter() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L39239) — code; similarity 1.0

**Disposition — retain generator ownership.** This is the generated context accessor `func (s *SchemaCommandContext) Parameter() IParameterContext`. The receiver and asserted child-context type are part of the generated tree API; retain it rather than hand-patching generated getters. Original evidence: F006 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N007 — [.RemoveSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L18134)

Reported **refactor**; 15 matched declarations:

- [.OrderSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L15171) — code; similarity 1.0
- [.LetSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16407) — code; similarity 1.0
- [.SetSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19840) — code; similarity 1.0
- [.Pattern() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 1.0
- [.Expression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 1.0
- [.XorExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22172) — code; similarity 1.0
- [.AndExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22350) — code; similarity 1.0
- [.PowerExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23473) — code; similarity 1.0
- [.ExpressionType() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L25526) — code; similarity 1.0
- [.PropertyOrLabelExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26053) — code; similarity 1.0
- [.PropertyExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26248) — code; similarity 1.0
- [.InvocationName() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30402) — code; similarity 1.0
- [.ExpressionChain() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32100) — code; similarity 1.0
- [.ComparisonExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22771) — code; similarity 0.961
- [.YieldItems() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19158) — code; similarity 0.876

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) RemoveSt() (localctx IRemoveStContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F007 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N008 — [.ExpressionType() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L25648)

Reported **refactor**; 15 matched declarations:

- [.LetSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16407) — code; similarity 1.0
- [.RemoveSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L18012) — code; similarity 1.0
- [.MergeSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19516) — code; similarity 1.0
- [.SetSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19840) — code; similarity 1.0
- [.Pattern() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 1.0
- [.Expression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 1.0
- [.AndExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22350) — code; similarity 1.0
- [.PowerExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23473) — code; similarity 1.0
- [.PropertyOrLabelExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26053) — code; similarity 1.0
- [.PropertyExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26248) — code; similarity 1.0
- [.InvocationName() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30402) — code; similarity 1.0
- [.ExpressionChain() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32100) — code; similarity 1.0
- [.LabelExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L20637) — code; similarity 0.881
- [.YieldItems() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19158) — code; similarity 0.876
- [.AddSubExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23087) — code; similarity 0.818

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) ExpressionType() (localctx IExpressionTypeContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F008 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N009 — [.ExpressionChain() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L32222)

Reported **refactor**; 15 matched declarations:

- [.OrderSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L15171) — code; similarity 1.0
- [.LetSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16407) — code; similarity 1.0
- [.RemoveSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L18012) — code; similarity 1.0
- [.SetSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19840) — code; similarity 1.0
- [.Pattern() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 1.0
- [.Expression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 1.0
- [.AndExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22350) — code; similarity 1.0
- [.PowerExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23473) — code; similarity 1.0
- [.ExpressionType() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L25526) — code; similarity 1.0
- [.PropertyExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26248) — code; similarity 1.0
- [.InvocationName() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30402) — code; similarity 1.0
- [.ComparisonExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22771) — code; similarity 0.961
- [.LabelExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L20637) — code; similarity 0.881
- [.YieldItems() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19158) — code; similarity 0.876
- [.AddSubExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23087) — code; similarity 0.818

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) ExpressionChain() (localctx IExpressionChainContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F009 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N010 — [.ComparisonExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L22893)

Reported **refactor**; 15 matched declarations:

- [.LetSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16407) — code; similarity 0.961
- [.MergeSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19516) — code; similarity 0.961
- [.SetSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19840) — code; similarity 0.961
- [.Pattern() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 0.961
- [.Expression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 0.961
- [.XorExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22172) — code; similarity 0.961
- [.AndExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22350) — code; similarity 0.961
- [.PowerExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23473) — code; similarity 0.961
- [.ExpressionType() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L25526) — code; similarity 0.961
- [.PropertyExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26248) — code; similarity 0.961
- [.InvocationName() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30402) — code; similarity 0.961
- [.ExpressionChain() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32100) — code; similarity 0.961
- [.LabelExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L20637) — code; similarity 0.87
- [.YieldItems() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19158) — code; similarity 0.844
- [.AddSubExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23087) — code; similarity 0.808

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) ComparisonExpression() (localctx IComparisonExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F010 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N011 — [.MergeSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L19638)

Reported **refactor**; 14 matched declarations:

- [.LetSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16407) — code; similarity 1.0
- [.RemoveSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L18012) — code; similarity 1.0
- [.SetSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19840) — code; similarity 1.0
- [.Pattern() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 1.0
- [.Expression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 1.0
- [.XorExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22172) — code; similarity 1.0
- [.AndExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22350) — code; similarity 1.0
- [.PowerExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23473) — code; similarity 1.0
- [.ExpressionType() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L25526) — code; similarity 1.0
- [.PropertyExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26248) — code; similarity 1.0
- [.InvocationName() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30402) — code; similarity 1.0
- [.ExpressionChain() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32100) — code; similarity 1.0
- [.ComparisonExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22771) — code; similarity 0.961
- [.YieldItems() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19158) — code; similarity 0.876

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) MergeSt() (localctx IMergeStContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F011 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N012 — [.OrderSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L15293)

Reported **refactor**; 12 matched declarations:

- [.LetSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16407) — code; similarity 1.0
- [.RemoveSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L18012) — code; similarity 1.0
- [.SetSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19840) — code; similarity 1.0
- [.Pattern() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 1.0
- [.Expression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 1.0
- [.PowerExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23473) — code; similarity 1.0
- [.ExpressionType() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L25526) — code; similarity 1.0
- [.PropertyExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26248) — code; similarity 1.0
- [.InvocationName() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30402) — code; similarity 1.0
- [.ExpressionChain() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32100) — code; similarity 1.0
- [.LabelExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L20637) — code; similarity 0.881
- [.YieldItems() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19158) — code; similarity 0.876

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) OrderSt() (localctx IOrderStContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F012 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N013 — [.PropertyOrLabelExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L26175)

Reported **refactor**; 12 matched declarations:

- [.LetSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16407) — code; similarity 1.0
- [.SetSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19840) — code; similarity 1.0
- [.Pattern() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 1.0
- [.Expression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 1.0
- [.AndExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22350) — code; similarity 1.0
- [.PowerExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23473) — code; similarity 1.0
- [.ExpressionType() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L25526) — code; similarity 1.0
- [.PropertyExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26248) — code; similarity 1.0
- [.InvocationName() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30402) — code; similarity 1.0
- [.ExpressionChain() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32100) — code; similarity 1.0
- [.LabelExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L20637) — code; similarity 0.881
- [.YieldItems() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19158) — code; similarity 0.876

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) PropertyOrLabelExpression() (localctx IPropertyOrLabelExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F013 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N014 — [.Pattern() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L21938)

Reported **refactor**; 11 matched declarations:

- [.LetSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16407) — code; similarity 1.0
- [.MergeSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19516) — code; similarity 1.0
- [.SetSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19840) — code; similarity 1.0
- [.Expression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 1.0
- [.XorExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22172) — code; similarity 1.0
- [.AndExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22350) — code; similarity 1.0
- [.PowerExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23473) — code; similarity 1.0
- [.ExpressionType() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L25526) — code; similarity 1.0
- [.PropertyExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26248) — code; similarity 1.0
- [.InvocationName() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30402) — code; similarity 1.0
- [.ExpressionChain() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32100) — code; similarity 1.0

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) Pattern() (localctx IPatternContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F014 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N015 — [.StringExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L24972)

Reported **refactor**; 11 matched declarations:

- [.PropertyTypeName() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L11134) — code; similarity 0.957
- [.LetItem() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16579) — code; similarity 0.904
- [.MapPair() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L34682) — code; similarity 0.904
- [.ReturnSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L13801) — code; similarity 0.875
- [.LimitSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L14300) — code; similarity 0.875
- [.CreateSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21415) — code; similarity 0.875
- [.Where() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21667) — code; similarity 0.875
- [.LabelPredicate() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L24366) — code; similarity 0.875
- [.Lhs() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L29295) — code; similarity 0.875
- [.DynamicPropertyExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L20278) — code; similarity 0.85
- [.StringLit() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L34104) — code; similarity 0.827

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) StringExpression() (localctx IStringExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F015 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N016 — [.LetSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L16529)

Reported **refactor**; 10 matched declarations:

- [.RemoveSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L18012) — code; similarity 1.0
- [.MergeSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19516) — code; similarity 1.0
- [.SetSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19840) — code; similarity 1.0
- [.Pattern() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 1.0
- [.Expression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 1.0
- [.AndExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22350) — code; similarity 1.0
- [.PowerExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23473) — code; similarity 1.0
- [.ExpressionType() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L25526) — code; similarity 1.0
- [.ExpressionChain() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32100) — code; similarity 1.0
- [.YieldItems() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19158) — code; similarity 0.876

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) LetSt() (localctx ILetStContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F016 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N017 — [.PowerExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L23595)

Reported **refactor**; 10 matched declarations:

- [.LetSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16407) — code; similarity 1.0
- [.Pattern() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 1.0
- [.Expression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 1.0
- [.XorExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22172) — code; similarity 1.0
- [.AndExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22350) — code; similarity 1.0
- [.ExpressionType() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L25526) — code; similarity 1.0
- [.PropertyExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26248) — code; similarity 1.0
- [.ExpressionChain() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32100) — code; similarity 1.0
- [.YieldItems() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19158) — code; similarity 0.876
- [.AddSubExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23087) — code; similarity 0.818

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) PowerExpression() (localctx IPowerExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F017 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N018 — [.InvocationName() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L30524)

Reported **refactor**; 10 matched declarations:

- [.LetSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16407) — code; similarity 1.0
- [.SetSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19840) — code; similarity 1.0
- [.Pattern() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 1.0
- [.Expression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 1.0
- [.AndExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22350) — code; similarity 1.0
- [.PowerExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23473) — code; similarity 1.0
- [.ExpressionType() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L25526) — code; similarity 1.0
- [.PropertyExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26248) — code; similarity 1.0
- [.ExpressionChain() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32100) — code; similarity 1.0
- [.YieldItems() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19158) — code; similarity 0.876

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) InvocationName() (localctx IInvocationNameContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F018 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N019 — [.SetSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L19962)

Reported **refactor**; 9 matched declarations:

- [.LetSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16407) — code; similarity 1.0
- [.RemoveSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L18012) — code; similarity 1.0
- [.Pattern() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 1.0
- [.Expression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 1.0
- [.PowerExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23473) — code; similarity 1.0
- [.ExpressionType() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L25526) — code; similarity 1.0
- [.InvocationName() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30402) — code; similarity 1.0
- [.ExpressionChain() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32100) — code; similarity 1.0
- [.YieldItems() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19158) — code; similarity 0.876

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) SetSt() (localctx ISetStContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F019 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N020 — [.PropertyTypeName() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L11256)

Reported **refactor**; 9 matched declarations:

- [.ReturnSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L13801) — code; similarity 0.891
- [.SkipSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L14182) — code; similarity 0.891
- [.LimitSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L14300) — code; similarity 0.891
- [.CreateSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21415) — code; similarity 0.891
- [.Where() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21667) — code; similarity 0.891
- [.LabelPredicate() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L24366) — code; similarity 0.891
- [.StringLit() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L34104) — code; similarity 0.86
- [.CharLit() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L34201) — code; similarity 0.86
- [.Lhs() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L29295) — code; similarity 0.837

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) PropertyTypeName() (localctx IPropertyTypeNameContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F020 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N021 — [.Lhs() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L29417)

Reported **refactor**; 8 matched declarations:

- [.ParenthesizedExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30889) — code; similarity 0.971
- [.StringLit() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L34104) — code; similarity 0.95
- [.CharLit() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L34201) — code; similarity 0.95
- [.LetItem() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16579) — code; similarity 0.916
- [.ReturnSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L13801) — code; similarity 0.888
- [.LabelPredicate() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L24366) — code; similarity 0.888
- [.PropertyTypeName() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L11134) — code; similarity 0.837
- [.ShowCommand() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L4238) — behavior; similarity 0.32

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) Lhs() (localctx ILhsContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F021 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N022 — [.PatternWhere() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L21667)

Reported **refactor**; 8 matched declarations:

- [.WithSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L13936) — code; similarity 0.947
- [.ProjectionItem() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L14854) — code; similarity 0.947
- [.UnwindSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16229) — code; similarity 0.926
- [.ProjectionBody() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L14469) — code; similarity 0.871
- [.FilterSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16706) — code; similarity 0.843
- [.UseClause() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L3715) — code; similarity 0.812
- [.PatternElemStart() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L27069) — code; similarity 0.81
- [.YieldItem() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19343) — code; similarity 0.803

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) PatternWhere() (localctx IPatternWhereContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F022 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N023 — [.AddSubExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L23209)

Reported **refactor**; 8 matched declarations:

- [.Pattern() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 0.818
- [.Expression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 0.818
- [.AndExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22350) — code; similarity 0.818
- [.PowerExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23473) — code; similarity 0.818
- [.ExpressionType() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L25526) — code; similarity 0.818
- [.PropertyExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26248) — code; similarity 0.818
- [.ExpressionChain() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32100) — code; similarity 0.818
- [.LabelExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L20637) — code; similarity 0.815

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) AddSubExpression() (localctx IAddSubExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F023 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N024 — [.Parameter() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L8572)

Reported **refactor**; 7 matched declarations:

- [.Parameter() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L7156) — code; similarity 1.0
- [.Parameter() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L27889) — code; similarity 1.0
- [.Parameter() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L28266) — code; similarity 1.0
- [.Name() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L33003) — code; similarity 1.0
- [.NumLit() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L33004) — code; similarity 1.0
- [.Parameter() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L38963) — code; similarity 1.0
- [.Parameter() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L39161) — code; similarity 1.0

**Disposition — retain generator ownership.** This occurrence is the generated interface member `Parameter() IParameterContext`, a required typed tree declaration with no executable implementation body. Its generated context implementation remains the owner; do not replace the interface contract with a hand-written forwarding layer. Original evidence: F024 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N025 — [.SkipSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L14304)

Reported **refactor**; 7 matched declarations:

- [.ReturnSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L13801) — code; similarity 1.0
- [.LimitSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L14300) — code; similarity 1.0
- [.CreateSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21415) — code; similarity 1.0
- [.Where() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21667) — code; similarity 1.0
- [.ForSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16862) — code; similarity 0.971
- [.PropertyTypeName() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L11134) — code; similarity 0.891
- [.StringExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L24850) — code; similarity 0.875

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) SkipSt() (localctx ISkipStContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F025 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N026 — [.LabelExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L20759)

Reported **refactor**; 7 matched declarations:

- [.Pattern() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 0.881
- [.Expression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 0.881
- [.ExpressionType() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L25526) — code; similarity 0.881
- [.PropertyOrLabelExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26053) — code; similarity 0.881
- [.ExpressionChain() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32100) — code; similarity 0.881
- [.LabelConjunction() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L20845) — code; similarity 0.815
- [.PatternElem() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26867) — code; similarity 0.806

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) LabelExpression() (localctx ILabelExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F026 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N027 — [.LimitSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L14422)

Reported **refactor**; 6 matched declarations:

- [.ReturnSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L13801) — code; similarity 1.0
- [.SkipSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L14182) — code; similarity 1.0
- [.CreateSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21415) — code; similarity 1.0
- [.ForSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16862) — code; similarity 0.971
- [.PropertyTypeName() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L11134) — code; similarity 0.891
- [.StringExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L24850) — code; similarity 0.875

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) LimitSt() (localctx ILimitStContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F027 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N028 — [.CharLit() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L34323)

Reported **refactor**; 6 matched declarations:

- [.StringLit() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L34104) — code; similarity 1.0
- [.Lhs() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L29295) — code; similarity 0.95
- [.ParenthesizedExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30889) — code; similarity 0.923
- [.LetItem() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16579) — code; similarity 0.869
- [.StringExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L24850) — code; similarity 0.827
- [.ShowCommand() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L4238) — behavior; similarity 0.304

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) CharLit() (localctx ICharLitContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F028 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N029 — [.DynamicPropertyExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L20400)

Reported **refactor**; 5 matched declarations:

- [.ParenthesizedExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30889) — code; similarity 1.0
- [.ReduceExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32600) — code; similarity 1.0
- [.AllReduceExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32886) — code; similarity 1.0
- [.StringExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L24850) — code; similarity 0.85
- [.PropertyExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26248) — behavior; similarity 0.645

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) DynamicPropertyExpression() (localctx IDynamicPropertyExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F029 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N030 — [.LabelConjunction() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L20967)

Reported **refactor**; 5 matched declarations:

- [.AddSubExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23087) — code; similarity 1.0
- [.Pattern() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 0.818
- [.Expression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 0.818
- [.PropertyOrLabelExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26053) — code; similarity 0.818
- [.LabelExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L20637) — code; similarity 0.815

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) LabelConjunction() (localctx ILabelConjunctionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F030 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N031 — [.CreateSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L21537)

Reported **refactor**; 5 matched declarations:

- [.ReturnSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L13801) — code; similarity 1.0
- [.SkipSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L14182) — code; similarity 1.0
- [.ForSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16862) — code; similarity 0.971
- [.PropertyTypeName() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L11134) — code; similarity 0.891
- [.StringExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L24850) — code; similarity 0.875

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) CreateSt() (localctx ICreateStContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F031 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N032 — [.StringLit() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L34226)

Reported **refactor**; 5 matched declarations:

- [.CharLit() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L34201) — code; similarity 1.0
- [.Lhs() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L29295) — code; similarity 0.95
- [.PropertyTypeName() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L11134) — code; similarity 0.86
- [.StringExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L24850) — code; similarity 0.827
- [.ShowCommand() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L4238) — behavior; similarity 0.304

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) StringLit() (localctx IStringLitContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F032 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N033 — [.ListLit() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L34442)

Reported **refactor**; 5 matched declarations:

- [.ParenExpressionChain() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L18972) — code; similarity 1.0
- [.MapProjection() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L28902) — code; similarity 0.897
- [.PatternComprehension() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L31240) — code; similarity 0.874
- [.MapLit() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L34499) — code; similarity 0.859
- [.UseGraphArguments() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L39444) — code; similarity 0.833

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) ListLit() (localctx IListLitContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F033 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N034 — [.SingleQuery() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L11548)

Reported **refactor**; 5 matched declarations:

- [.RemoveItem() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L18213) — code; similarity 0.97
- [.ShellCommandElement() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L2909) — code; similarity 0.941
- [.PatternElemStart() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L27069) — code; similarity 0.831
- [.SubqueryExist() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30219) — code; similarity 0.824
- [.SyntaxFragment() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L37849) — code; similarity 0.814

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) SingleQuery() (localctx ISingleQueryContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F034 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N035 — [.YieldItems() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L19280)

Reported **refactor**; 5 matched declarations:

- [.SetSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19840) — code; similarity 0.876
- [.Pattern() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 0.876
- [.Expression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 0.876
- [.PowerExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23473) — code; similarity 0.876
- [.ExpressionChain() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32100) — code; similarity 0.876

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) YieldItems() (localctx IYieldItemsContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F035 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N036 — [.ReturnSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L13923)

Reported **refactor**; 4 matched declarations:

- [.SkipSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L14182) — code; similarity 1.0
- [.LimitSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L14300) — code; similarity 1.0
- [.CreateSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21415) — code; similarity 1.0
- [.ForSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16862) — code; similarity 0.971

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) ReturnSt() (localctx IReturnStContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F036 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N037 — [.Literal() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L33470)

Reported **refactor**; 4 matched declarations:

- [.CompositeValue() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L7270) — code; similarity 0.979
- [.CredentialValue() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L39077) — code; similarity 0.979
- [.CredentialURL() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L39275) — code; similarity 0.979
- [.Properties() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L27979) — code; similarity 0.884

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) Literal() (localctx ILiteralContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F037 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N038 — [.ProjectionItem() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L14976)

Reported **refactor**; 4 matched declarations:

- [.UnwindSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16229) — code; similarity 0.978
- [.ProjectionBody() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L14469) — code; similarity 0.88
- [.FilterSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16706) — code; similarity 0.853
- [.YieldItem() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19343) — code; similarity 0.814

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) ProjectionItem() (localctx IProjectionItemContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F038 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N039 — [.ForSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L16984)

Reported **refactor**; 4 matched declarations:

- [.SkipSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L14182) — code; similarity 0.971
- [.LimitSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L14300) — code; similarity 0.971
- [.CreateSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21415) — code; similarity 0.971
- [.Where() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21667) — code; similarity 0.971

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) ForSt() (localctx IForStContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F039 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N040 — [.RelationshipTypes() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L30048)

Reported **refactor**; 4 matched declarations:

- [.MergeAction() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19673) — code; similarity 0.933
- [.QueryPrefix() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L3843) — code; similarity 0.907
- [.BoolLit() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L33698) — code; similarity 0.907
- [.PathFunction() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26646) — code; similarity 0.872

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) RelationshipTypes() (localctx IRelationshipTypesContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F040 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N041 — [.LetItem() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L16701)

Reported **refactor**; 4 matched declarations:

- [.Lhs() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L29295) — code; similarity 0.916
- [.StringExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L24850) — code; similarity 0.904
- [.CharLit() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L34201) — code; similarity 0.869
- [.PropertyTypeName() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L11134) — code; similarity 0.865

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) LetItem() (localctx ILetItemContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F041 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N042 — [.YieldItem() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L19465)

Reported **refactor**; 4 matched declarations:

- [.ProjectionBody() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L14469) — code; similarity 0.9
- [.UnwindSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16229) — code; similarity 0.834
- [.ProjectionItem() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L14854) — code; similarity 0.814
- [.YieldItems() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19158) — behavior; similarity 0.643

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) YieldItem() (localctx IYieldItemContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F042 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N043 — [.ParenExpressionChain() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L19094)

Reported **refactor**; 4 matched declarations:

- [.MapProjection() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L28902) — code; similarity 0.897
- [.PatternComprehension() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L31240) — code; similarity 0.874
- [.MapLit() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L34499) — code; similarity 0.859
- [.UseGraphArguments() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L39444) — code; similarity 0.833

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) ParenExpressionChain() (localctx IParenExpressionChainContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F043 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N044 — [.MapLit() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L8571)

Reported **refactor**; 3 matched declarations:

- [.MapLit() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L2803) — code; similarity 1.0
- [.MapLit() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L27888) — code; similarity 1.0
- [.MapLit() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L33190) — code; similarity 1.0

**Disposition — retain generator ownership.** This occurrence is the generated interface member `MapLit() IMapLitContext`, a required typed tree declaration with no executable implementation body. Its generated context implementation remains the owner; do not replace the interface contract with a hand-written forwarding layer. Original evidence: F044 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N045 — [.CountSubquery() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L12343)

Reported **refactor**; 3 matched declarations:

- [.ExistsSubquery() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L11986) — code; similarity 1.0
- [.SubqueryExist() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30219) — code; similarity 0.815
- [.RelationshipQuantifier() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L27738) — code; similarity 0.811

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) CountSubquery() (localctx ICountSubqueryContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F045 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N046 — [.DeleteSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L17960)

Reported **refactor**; 3 matched declarations:

- [.MatchSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16052) — code; similarity 1.0
- [.CypherPreamble() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L2220) — code; similarity 0.826
- [.RemoveSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L18012) — behavior; similarity 0.641

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) DeleteSt() (localctx IDeleteStContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F046 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N047 — [.ReduceExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L32722)

Reported **refactor**; 3 matched declarations:

- [.ParenthesizedExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30889) — code; similarity 1.0
- [.AllReduceExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32886) — code; similarity 1.0
- [.CountAll() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L31931) — code; similarity 0.962

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) ReduceExpression() (localctx IReduceExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F047 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N048 — [.WithSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L14058)

Reported **refactor**; 3 matched declarations:

- [.UnwindSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16229) — code; similarity 0.978
- [.ProjectionBody() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L14469) — code; similarity 0.88
- [.FilterSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16706) — code; similarity 0.853

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) WithSt() (localctx IWithStContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F048 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N049 — [.UnwindSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L16351)

Reported **refactor**; 3 matched declarations:

- [.WithSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L13936) — code; similarity 0.978
- [.FilterSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16706) — code; similarity 0.836
- [.YieldItem() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19343) — code; similarity 0.834

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) UnwindSt() (localctx IUnwindStContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F049 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N050 — [.Where() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L21789)

Reported **refactor**; 3 matched declarations:

- [.ForSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16862) — code; similarity 0.971
- [.PropertyTypeName() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L11134) — code; similarity 0.891
- [.StringExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L24850) — code; similarity 0.875

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) Where() (localctx IWhereContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F050 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N051 — [.CountAll() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L32053)

Reported **refactor**; 3 matched declarations:

- [.ParenthesizedExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30889) — code; similarity 0.962
- [.ReduceExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32600) — code; similarity 0.962
- [.AllReduceExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32886) — code; similarity 0.962

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) CountAll() (localctx ICountAllContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F051 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N052 — [.MergeAction() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L19795)

Reported **refactor**; 3 matched declarations:

- [.PathFunction() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26646) — code; similarity 0.936
- [.RelationshipTypes() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L29926) — code; similarity 0.933
- [.DynamicLabel() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21258) — code; similarity 0.818

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) MergeAction() (localctx IMergeActionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F052 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N053 — [.UnionSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L30176)

Reported **refactor**; 3 matched declarations:

- [.MatchSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16052) — code; similarity 0.915
- [.DeleteSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L17838) — code; similarity 0.915
- [.RegularQuery() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L11282) — code; similarity 0.822

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) UnionSt() (localctx IUnionStContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F053 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N054 — [.FilterSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L16828)

Reported **refactor**; 3 matched declarations:

- [.ProjectionBody() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L14469) — code; similarity 0.861
- [.WithSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L13936) — code; similarity 0.853
- [.UnwindSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16229) — code; similarity 0.836

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) FilterSt() (localctx IFilterStContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F054 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N055 — [.LoadCSVSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L17195)

Reported **refactor**; 3 matched declarations:

- [.QueryCallSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L18726) — code; similarity 0.85
- [.ShowTail() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L5187) — code; similarity 0.805
- [.SchemaCommand() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8976) — behavior; similarity 0.595

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) LoadCSVSt() (localctx ILoadCSVStContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F055 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N056 — [.ExistsSubquery() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L12108)

Reported **refactor**; 2 matched declarations:

- [.CountSubquery() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L12221) — code; similarity 1.0
- [.RelationshipQuantifier() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L27738) — code; similarity 0.811

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) ExistsSubquery() (localctx IExistsSubqueryContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F056 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N057 — [.ParenthesizedExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L31011)

Reported **refactor**; 2 matched declarations:

- [.ReduceExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32600) — code; similarity 1.0
- [.CountAll() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L31931) — code; similarity 0.962

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) ParenthesizedExpression() (localctx IParenthesizedExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F057 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N058 — [.AllReduceExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L33008)

Reported **refactor**; 2 matched declarations:

- [.ParenthesizedExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30889) — code; similarity 1.0
- [.ReduceExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32600) — code; similarity 1.0

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) AllReduceExpression() (localctx IAllReduceExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F058 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N059 — [.Properties() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L28101)

Reported **refactor**; 2 matched declarations:

- [.UpdatingStatement() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L17675) — code; similarity 0.964
- [.Literal() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L33348) — code; similarity 0.884

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) Properties() (localctx IPropertiesContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F060 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N060 — [.ReadingStatement() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L17539)

Reported **refactor**; 2 matched declarations:

- [.Symbol() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L35808) — code; similarity 0.955
- [.PatternElemStart() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L27069) — code; similarity 0.831

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) ReadingStatement() (localctx IReadingStatementContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F061 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N061 — [.RegularQuery() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L11404)

Reported **refactor**; 2 matched declarations:

- [.CypherPreamble() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L2220) — code; similarity 0.951
- [.UnionSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30054) — code; similarity 0.822

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) RegularQuery() (localctx IRegularQueryContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F062 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N062 — [.MapProjection() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L29024)

Reported **refactor**; 2 matched declarations:

- [.MapLit() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L34499) — code; similarity 0.933
- [.PatternComprehension() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L31240) — code; similarity 0.879

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) MapProjection() (localctx IMapProjectionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F066 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N063 — [.MapLit() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L34621)

Reported **refactor**; 2 matched declarations:

- [.MapProjection() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L28902) — code; similarity 0.933
- [.PatternComprehension() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L31240) — code; similarity 0.91

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) MapLit() (localctx IMapLitContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F067 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N064 — [.UpdatingStatement() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L17797)

Reported **refactor**; 2 matched declarations:

- [.Literal() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L33348) — code; similarity 0.918
- [.StringExpPrefix() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L24962) — code; similarity 0.845

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) UpdatingStatement() (localctx IUpdatingStatementContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F068 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N065 — [.RemoveItem() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L18335)

Reported **refactor**; 2 matched declarations:

- [.ShellCommandElement() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L2909) — code; similarity 0.914
- [.PatternElemStart() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L27069) — code; similarity 0.81

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) RemoveItem() (localctx IRemoveItemContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F069 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N066 — [.PatternComprehension() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L31362)

Reported **refactor**; 2 matched declarations:

- [.MapLit() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L34499) — code; similarity 0.91
- [.ParenExpressionChain() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L18972) — code; similarity 0.874

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) PatternComprehension() (localctx IPatternComprehensionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F070 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N067 — [.MapPair() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L34804)

Reported **refactor**; 2 matched declarations:

- [.StringExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L24850) — code; similarity 0.904
- [.PropertyTypeName() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L11134) — code; similarity 0.865

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) MapPair() (localctx IMapPairContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F071 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N068 — [.IntegerLit() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L33937)

Reported **refactor**; 2 matched declarations:

- [.DynamicLabel() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21258) — code; similarity 0.892
- [.CypherGroup() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L2384) — code; similarity 0.824

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) IntegerLit() (localctx IIntegerLitContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F072 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N069 — [.LabelPredicate() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L24488)

Reported **refactor**; 2 matched declarations:

- [.PropertyTypeName() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L11134) — code; similarity 0.891
- [.StringExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L24850) — code; similarity 0.875

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) LabelPredicate() (localctx ILabelPredicateContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F073 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N070 — [.ProjectionBody() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L14591)

Reported **refactor**; 2 matched declarations:

- [.WithSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L13936) — code; similarity 0.88
- [.ProjectionItem() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L14854) — code; similarity 0.88

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) ProjectionBody() (localctx IProjectionBodyContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F074 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N071 — [.FilterExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L31917)

Reported **refactor**; 2 matched declarations:

- [.NullExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L25120) — code; similarity 0.867
- [.DeleteSt() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L17838) — code; similarity 0.859

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) FilterExpression() (localctx IFilterExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F075 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N072 — [.ComparisonSigns() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L23049)

Reported **refactor**; 2 matched declarations:

- [.FilterWith() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L31040) — code; similarity 0.863
- [.ComparisonExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22771) — behavior; similarity 0.485

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) ComparisonSigns() (localctx IComparisonSignsContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F076 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N073 — [.SubqueryExist() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L30341)

Reported **refactor**; 2 matched declarations:

- [.Name() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L34881) — code; similarity 0.815
- [.Symbol() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L35808) — code; similarity 0.801

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) SubqueryExist() (localctx ISubqueryExistContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F079 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N074 — [.PatternElem() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L26989)

Reported **refactor**; 2 matched declarations:

- [.NotExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22519) — code; similarity 0.8
- [.PatternElemPart() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L27239) — behavior; similarity 0.496

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) PatternElem() (localctx IPatternElemContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F080 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N075 — [.RelationDetail() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L29851)

Reported **reuse**; 1 matched declarations:

- [.NodePattern() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L28165) — code; similarity 1.0

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) RelationDetail() (localctx IRelationDetailContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F081 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N076 — [.CollectSubquery() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L12527)

Reported **reuse**; 1 matched declarations:

- [.CountAll() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L31931) — code; similarity 0.962

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) CollectSubquery() (localctx ICollectSubqueryContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F082 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N077 — [TestBoltFactoryTransactionPinsBeginDatabaseAndRejectsCrossDatabaseRun() / pkg/bolt/transaction_lifecycle_early_return_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/transaction_lifecycle_early_return_test.go#L212)

Reported **reuse**; 1 matched declarations:

- [TestBoltRawTransactionPinsBeginDatabaseAndRejectsCrossDatabaseRun() / pkg/bolt/transaction_lifecycle_early_return_test.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/bolt/transaction_lifecycle_early_return_test.go#L243) — code; similarity 0.936

**Disposition — current-source review.** Consolidate with the raw-executor case in TestBoltTransactionPinsBeginDatabaseAndRejectsCrossDatabaseRun: one table owns every wire action and assertion; factory/raw capability and 8/1 connection budgets remain explicit fresh fixtures. Parent removed identity/function-forwarding table adapters. Focused race population passes (3.123s).

### N078 — [TestBoltRawTransactionPinsBeginDatabaseAndRejectsCrossDatabaseRun() / pkg/bolt/transaction_lifecycle_early_return_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/transaction_lifecycle_early_return_test.go#L243)

Reported **reuse**; 1 matched declarations:

- [TestBoltFactoryTransactionPinsBeginDatabaseAndRejectsCrossDatabaseRun() / pkg/bolt/transaction_lifecycle_early_return_test.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/bolt/transaction_lifecycle_early_return_test.go#L212) — code; similarity 0.936

**Disposition — current-source review.** Same concrete correction as N077: preserve both raw/factory subcases and retire the copied test body, not either capability. No production routing change.

### N079 — [.NormalizationPredicate() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L24325)

Reported **reuse**; 1 matched declarations:

- [.CypherOption() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L2554) — code; similarity 0.904

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) NormalizationPredicate() (localctx INormalizationPredicateContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F083 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N080 — [.GetNode() / pkg/storage/badger_transaction_graph_reads.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/badger_transaction_graph_reads.go#L25)

Reported **reuse**; 1 matched declarations:

- [.GetStoredNode() / pkg/storage/badger_transaction_stored_reads.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/storage/badger_transaction_stored_reads.go#L9) — code; similarity 0.879

**Disposition — S2 — retain shared owner.** GetNode and GetStoredNode both call readTransactionEntity with the same node state/clone/load callbacks. Only visibleRead versus storedRead differs. Engine and StoredEntityReader require the two public capabilities; filtering suppressed/decayed data out of the integrity reader would break its contract. Admission, snapshot/pending selection and completion have one implementation already. Original evidence: F084 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N081 — [.GetStoredNode() / pkg/storage/badger_transaction_stored_reads.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/badger_transaction_stored_reads.go#L9)

Reported **reuse**; 1 matched declarations:

- [.GetNode() / pkg/storage/badger_transaction_graph_reads.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/storage/badger_transaction_graph_reads.go#L25) — code; similarity 0.879

**Disposition — S2 — retain shared owner.** Reverse of F084: public stored-inclusive capability intentionally bypasses user visibility; both selectors use readTransactionEntity, not copied read bodies. No exported Go API migration is introduced to rename those required capability methods. Original evidence: F085 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N082 — [.PathFunction() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L26768)

Reported **reuse**; 1 matched declarations:

- [.DynamicLabel() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21258) — code; similarity 0.872

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) PathFunction() (localctx IPathFunctionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F086 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N083 — [.parseCreateConstraintTemporalDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L1556)

Reported **reuse**; 1 matched declarations:

- [.parseCreateConstraintDomainDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/schema.go#L1628) — code; similarity 0.863

**Disposition — C1 — retain distinct predicate decoders.** Both parsers reuse parseCreateConstraintForRequireDDL for the head, pattern and OPTIONS. Temporal parses a property tuple plus IS TEMPORAL/NO OVERLAP and returns key/interval properties; domain parses one qualified property IN a value list and returns AllowedValues input. Their storage validators and typed outputs differ. Shared head/pattern parsing is consolidated separately; no new callback wrapper is needed around these distinct predicate decisions. Original evidence: F087 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N084 — [.NullExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L25242)

Reported **reuse**; 1 matched declarations:

- [.RangeLit() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L33548) — code; similarity 0.861

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) NullExpression() (localctx INullExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F088 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N085 — [.DynamicLabel() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L21380)

Reported **reuse**; 1 matched declarations:

- [.CypherGroup() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L2384) — code; similarity 0.856

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) DynamicLabel() (localctx IDynamicLabelContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F089 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N086 — [.Parameter() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L33229)

Reported **reuse**; 1 matched declarations:

- [.Atom() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L28610) — code; similarity 0.854

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) Parameter() (localctx IParameterContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F090 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N087 — [schemaOptionsNotMapError() / pkg/cypher/schema_options.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema_options.go#L138)

Reported **reuse**; 1 matched declarations:

- [invalidSubscriptTypeError() / pkg/cypher/row_subscript_validation.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/row_subscript_validation.go#L288) — code; similarity 0.853

**Disposition — C5 — retain composed errors.** Both use newSemanticError, the shared semantic-error constructor. OPTIONS must emit Neo.ClientError.General.InvalidArguments with Neo4j value representation and expected-map text; subscript failure emits Neo.ClientError.Statement.TypeError/InvalidArgumentType with a Cypher type name. Combining the message builders would erase distinct wire code/type/message contracts; common error construction already has one owner. Original evidence: F091 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N088 — [.SetItem() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L20190)

Reported **reuse**; 1 matched declarations:

- [.NumLit() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L33965) — code; similarity 0.845

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) SetItem() (localctx ISetItemContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F092 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N089 — [.GetOutgoingEdgesVisibleAt() / pkg/storage/badger_mvcc.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/badger_mvcc.go#L1197)

Reported **reuse**; 1 matched declarations:

- [.GetIncomingEdgesVisibleAt() / pkg/storage/badger_mvcc.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/storage/badger_mvcc.go#L1201) — code; similarity 0.841

**Disposition — S3 — retain direction selectors.** Outgoing and Incoming are required public adjacency capabilities. Both call readAdjacentEdgesAtWithView with the typed Outgoing/Incoming value, the same snapshot and view owner. There is no second historical traversal, lookup, cancellation or reader-release body. Original evidence: F093 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N090 — [.GetIncomingEdgesVisibleAt() / pkg/storage/badger_mvcc.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/badger_mvcc.go#L1201)

Reported **reuse**; 1 matched declarations:

- [.GetOutgoingEdgesVisibleAt() / pkg/storage/badger_mvcc.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/storage/badger_mvcc.go#L1197) — code; similarity 0.841

**Disposition — S3 — retain direction selectors.** Reverse of F093; one directional historical-read owner, two required interface selectors. A new public API migration is unnecessary to eliminate an implementation that is already shared. Original evidence: F094 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N091 — [.GetEdge() / pkg/storage/badger_transaction_graph_reads.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/badger_transaction_graph_reads.go#L188)

Reported **reuse**; 1 matched declarations:

- [.GetStoredEdge() / pkg/storage/badger_transaction_stored_reads.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/storage/badger_transaction_stored_reads.go#L14) — code; similarity 0.841

**Disposition — S2 — retain shared owner.** GetEdge/GetStoredEdge select visibleRead/storedRead on the same readTransactionEntity, edge state, clone and load callbacks. Visible relationship filtering and stored integrity reads are different published capabilities; overlay, locking and snapshot work are already shared. Original evidence: F095 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N092 — [.GetStoredEdge() / pkg/storage/badger_transaction_stored_reads.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/badger_transaction_stored_reads.go#L14)

Reported **reuse**; 1 matched declarations:

- [.GetEdge() / pkg/storage/badger_transaction_graph_reads.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/storage/badger_transaction_graph_reads.go#L185) — code; similarity 0.841

**Disposition — S2 — retain shared owner.** Reverse of F095; no duplicate point-read implementation remains beyond selecting the capability policy. Original evidence: F096 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N093 — [.UseReferencePrefix() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L39844)

Reported **reuse**; 1 matched declarations:

- [.MapProjection() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L28902) — code; similarity 0.835

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) UseReferencePrefix() (localctx IUseReferencePrefixContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F097 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N094 — [.parseCreateConstraintDomainDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L1593)

Reported **reuse**; 1 matched declarations:

- [.parseCreateConstraintTemporalDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/schema.go#L1591) — code; similarity 0.832

**Disposition — C1 — retain distinct predicate decoders.** Reverse of F087. Common DDL head/pattern/options parsing remains shared, while domain allowed-value decoding and temporal interval decoding keep their distinct typed results and validators. Original evidence: F098 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N095 — [.StringExpPrefix() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L25084)

Reported **reuse**; 1 matched declarations:

- [.Literal() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L33348) — code; similarity 0.825

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) StringExpPrefix() (localctx IStringExpPrefixContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F099 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N096 — [.QuantifiedPath() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L27542)

Reported **reuse**; 1 matched declarations:

- [.RangeLit() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L33548) — code; similarity 0.823

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) QuantifiedPath() (localctx IQuantifiedPathContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F100 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N097 — [.PatternElemStart() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L27191)

Reported **reuse**; 1 matched declarations:

- [.PatternWhere() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21545) — code; similarity 0.81

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) PatternElemStart() (localctx IPatternElemStartContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F101 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N098 — [.parseCreateConstraintForRequireParts() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L1386)

Reported **reuse**; 1 matched declarations:

- [.parseCreateConstraintForRequireHead() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/schema.go#L1399) — code; similarity 0.808

**Disposition — current-source review.** Retain current canonical prefix reuse. The indexed match is the former parseCreateConstraintForRequireHead implementation. Current Head calls Parts directly, then alone performs splitDDLOptionsTail/schemaOptionsTail for predicate DDL. Contract-block DDL calls Parts and rejects a nonempty block tail instead; making it consume predicate OPTIONS would admit currently rejected syntax. Both-parser no-mutation and private exact-error controls pass. No second prefix decoder exists in current Head.

### N099 — [.TypePredicate() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L25404)

Reported **reuse**; 1 matched declarations:

- [.RelationshipPattern() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L29428) — code; similarity 0.804

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) TypePredicate() (localctx ITypePredicateContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F103 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N100 — [.NotExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L22641)

Reported **reuse**; 1 matched declarations:

- [.PatternElem() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26867) — code; similarity 0.8

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) NotExpression() (localctx INotExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F104 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N101 — [.MultDivExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L23412)

Reported **reuse**; 1 matched declarations:

- [.PowerExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23473) — behavior; similarity 0.736

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) MultDivExpression() (localctx IMultDivExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F105 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N102 — [.UnaryAddSubExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L23747)

Reported **reuse**; 1 matched declarations:

- [.AddSubExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23087) — behavior; similarity 0.663

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) UnaryAddSubExpression() (localctx IUnaryAddSubExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F107 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N103 — [TestBoltBeginCleanupFailureClosesAndQuarantinesRawExecutor() / pkg/bolt/transaction_cleanup_failure_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/transaction_cleanup_failure_test.go#L331)

Reported **reuse**; 1 matched declarations:

- [TestBoltQuarantinesSingleConnectionRawExecutorAfterCleanupFailure() / pkg/bolt/transaction_cleanup_failure_test.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/bolt/transaction_cleanup_failure_test.go#L278) — behavior; similarity 0.654

**Disposition — current-source review.** Preserve partial-BEGIN compensation versus established-transaction ROLLBACK quarantine: the former asserts one compensation and a rejected new BEGIN; the latter asserts a rejected autocommit RUN with zero executor execution. Consolidate their repeated wait/new-connection/failure/closed probe, also used by commit-error/panic cases; retain distinct per-executor counters and terminal error assertions.

### N104 — [TestBoltBenchmarkCreateDeleteRelationship_LargeDataset() / pkg/bolt/integration_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/integration_test.go#L1141)

Reported **reuse**; 1 matched declarations:

- [TestBoltBenchmarkCreateDeleteRelationship() / pkg/bolt/integration_test.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/bolt/integration_test.go#L1143) — behavior; similarity 0.625

**Disposition — current-source review.** Keep the 250-node memory population distinct from the two-node case. Consolidate seed/checked wire iterations and server configuration through shared owners; preserve born/released versus plain-property populations explicitly. Assert seed counts and zero surviving TEMP_REL outside timing. No ns/op performance claim follows from this fixture cleanup.

### N105 — [TestExplicitTransactionStreamsResultsByQID() / pkg/bolt/streaming_qid_compatibility_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/streaming_qid_compatibility_test.go#L13)

Reported **reuse**; 1 matched declarations:

- [TestExplicitTransactionRejectsUnknownQIDUntilReset() / pkg/bolt/streaming_qid_compatibility_test.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/bolt/streaming_qid_compatibility_test.go#L67) — behavior; similarity 0.607

**Disposition — B4 — reuse, retain different scenarios.** Both tests already use shared explicit transaction startup, runStatementAndQID and PULL helpers. One asserts four simultaneously live streams, selective discard, remaining rows and ID reset after commit; the other asserts InvalidFormat for unknown QID, IGNORED until RESET and recovery. B4 removes duplicated PULL decoding; merging these state machines would change their preconditions. Original evidence: F108 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N106 — [.SinglePartQ() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L15593)

Reported **reuse**; 1 matched declarations:

- [.MultiPartQ() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L15868) — behavior; similarity 0.588

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) SinglePartQ() (localctx ISinglePartQContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F109 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N107 — [.PatternPart() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L26568)

Reported **reuse**; 1 matched declarations:

- [.PatternElemPart() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L27239) — behavior; similarity 0.56

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) PatternPart() (localctx IPatternPartContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F110 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N108 — [.RelationshipsChainPattern() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L31568)

Reported **reuse**; 1 matched declarations:

- [.RelationshipPattern() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L29428) — behavior; similarity 0.555

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) RelationshipsChainPattern() (localctx IRelationshipsChainPatternContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F111 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N109 — [TestBoltExpiryBeforeRunClaimNeverExecutesAndResetRecovers() / pkg/bolt/transaction_lifecycle_early_return_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/transaction_lifecycle_early_return_test.go#L113)

Reported **reuse**; 1 matched declarations:

- [TestBoltExpiryBeforeRunClaimJoinsCleanupBeforeFailure() / pkg/bolt/transaction_lifecycle_early_return_test.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/bolt/transaction_lifecycle_early_return_test.go#L161) — behavior; similarity 0.543

**Disposition — current-source review.** Keep both ordering directions: cleanup must complete while RUN is blocked before claim, and the public failure must wait for in-flight rollback. The first also verifies IGNORED/RESET/reuse and zero execution. Consolidate the shared resolver-barrier/timeout/server fixture, with idempotent release registered for cleanup; preserve each executor and distinct ordering assertions.

### N110 — [TestLegacyDateTimeWireMetadataSignatureNegotiation() / pkg/bolt/legacy_datetime_negotiation_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/legacy_datetime_negotiation_test.go#L85)

Reported **reuse**; 1 matched declarations:

- [TestLegacyDateTimeWireSignatureNegotiation() / pkg/bolt/legacy_datetime_negotiation_test.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/bolt/legacy_datetime_negotiation_test.go#L14) — behavior; similarity 0.53

**Disposition — B5 — reuse, retain different wire locations.** Both use dateTimeWireConnect/dateTimeWireQuery and the independent dateTimeWire encoder. Scalar parameter/result signatures versus node/relationship metadata properties exercise different wire locations and rejection/recovery assertions. Preserve both populations; share local fixture setup and raw protocol owners. Original evidence: F112 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N111 — [.FunctionInvocation() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L30766)

Reported **reuse**; 1 matched declarations:

- [.SchemaCommand() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8976) — behavior; similarity 0.504

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) FunctionInvocation() (localctx IFunctionInvocationContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F113 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N112 — [.NodePattern() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L28287)

Reported **reuse**; 1 matched declarations:

- [.SchemaCommand() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8976) — behavior; similarity 0.481

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) NodePattern() (localctx INodePatternContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F114 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N113 — [TestDatabaseCommands_CreateDatabase_WhitespacePermutations() / pkg/cypher/database_commands_whitespace_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/database_commands_whitespace_test.go#L60)

Reported **reuse**; 1 matched declarations:

- [TestDatabaseCommands_DropDatabase_WhitespacePermutations() / pkg/cypher/database_commands_whitespace_test.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/database_commands_whitespace_test.go#L104) — behavior; similarity 0.474

**Disposition — C6 — retain shared fixture and input generator.** Both reuse newDatabaseCommandFixture and whitespacePermutations. CREATE substitutes unique names and asserts empty results plus catalog presence; DROP recreates targets between permutations, tests IF EXISTS separately and asserts name output/catalog absence. Only generic t.Run/iteration scaffolding remains alike; parsing, execution, fixture ownership and permutation generation are not reimplemented. Original evidence: F115 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N114 — [TestDatabaseManagerBoltExecuteWriteDeletesUnwindMatchedRelationship() / pkg/bolt/execute_write_relationship_delete_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/execute_write_relationship_delete_test.go#L16)

Reported **reuse**; 1 matched declarations:

- [TestDatabaseManagerBoltExplicitTransactionPreservesBindingsAcrossUnwindMutationPipeline() / pkg/bolt/execute_write_relationship_delete_test.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/bolt/execute_write_relationship_delete_test.go#L205) — behavior; similarity 0.471

**Disposition — current-source review.** Keep delete counters/idempotency/rollback/endpoints and returned binding values as distinct production oracles. Reuse the existing newDatabaseManagerBoltWriteSession owner rather than adding a second bootstrap helper; preserve explicit 8-connection configuration versus existing default callers, driver connectivity and checked cleanup.

### N115 — [.ListExpression() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L24717)

Reported **reuse**; 1 matched declarations:

- [.SchemaCommand() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8976) — behavior; similarity 0.432

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) ListExpression() (localctx IListExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F116 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N116 — [TestBoltZeroRowCorrelatedCallAggregation() / pkg/bolt/zero_row_correlated_aggregation_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/zero_row_correlated_aggregation_test.go#L10)

Reported **reuse**; 1 matched declarations:

- [TestZeroRowCorrelatedCallAggregation() / pkg/cypher/zero_row_correlated_aggregation_test.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/zero_row_correlated_aggregation_test.go#L16) — behavior; similarity 0.401

**Disposition — current-source review.** Retain both direct executor and Bolt TCP/PackStream transaction boundaries. Consolidate identical seed, params and exact three-column row oracle in the cross-package test fixture owner. Each public test retains its 2 fixtures × 2 transaction modes × 2 input populations; Bolt uses the existing result collector. The query fixtures are read-only, so reuse does not depend on hidden writes.

### N117 — [TestBoltBenchmarkCreateDeleteRelationship() / pkg/bolt/integration_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/integration_test.go#L1089)

Reported **reuse**; 1 matched declarations:

- [TestBoltBenchmarkCreateDeleteRelationship_Badger() / pkg/bolt/integration_test.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/bolt/integration_test.go#L1280) — behavior; similarity 0.395

**Disposition — current-source review.** Keep the two-node memory case and persistent 250-node Badger/profile case. Share checked seed/query-iteration/server owners without replacing population/property/profile/skip policies. Badger retains its existing CI/Windows qualification boundary, and its engine close has one fixture owner; no benchmark or full CI pass is inferred for a skipped backend.

### N118 — [TestExplicitTransactionMergeUpdateViolationsFailTheStatement() / pkg/cypher/constraint_checked_writes_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/constraint_checked_writes_test.go#L156)

Reported **reuse**; 1 matched declarations:

- [TestExplicitTransactionConstraintViolationFailsTheStatement() / pkg/cypher/constraint_checked_writes_test.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/constraint_checked_writes_test.go#L93) — behavior; similarity 0.387

**Disposition — C7 — retain shared rejection lifecycle.** Both use constraintRejectedInExplicitTransaction for BEGIN, rejected statement and terminal operation. Keep the different node/edge constraint populations, mutation queries and COMMIT versus ROLLBACK/post-state oracles; no second transaction/error-check body remains. Original evidence: F117 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N119 — [.ReservedWord() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/cypher_parser.go#L37818)

Reported **reuse**; 1 matched declarations:

- [.ProcedurePrivilegePattern() / pkg/cypher/antlr/cypher_parser.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L38852) — behavior; similarity 0.38

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) ReservedWord() (localctx IReservedWordContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes. Original evidence: F118 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N120 — [TestBoltLazyResultEndsOnDiscardResetAndRun() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L165)

Reported **reuse**; 1 matched declarations:

- [TestBoltLazyResultDiscardReachesTheError() / pkg/bolt/lazy_result_edge_test.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/bolt/lazy_result_edge_test.go#L118) — behavior; similarity 0.352

**Disposition — current-source review.** Retain different oracles over the same shared lazy-result fixture/PULL/readBatch/sendDiscard primitives: successful DISCARD/reset/replacement termination and exact remaining row counts versus DISCARD reaching an arithmetic failure at row 2000. The latter must observe ArithmeticError rather than success/exhaustion. No second PULL decoder or discard implementation is needed; keep both scenarios.

### N121 — [TestBoltExplicitTransactionTimeoutCancelsActiveRunBeforeRollback() / pkg/bolt/transaction_lifecycle_active_run_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/transaction_lifecycle_active_run_test.go#L15)

Reported **reuse**; 1 matched declarations:

- [TestBoltExplicitTransactionRejectsInvalidTimeoutBeforeStorageAllocation() / pkg/bolt/transaction_lifecycle_compatibility_test.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/bolt/transaction_lifecycle_compatibility_test.go#L186) — behavior; similarity 0.307

**Disposition — current-source review.** Retain active RUN cancellation/join/reader release/no persisted write versus malformed BEGIN timeout rejected before engine allocation. Their state transitions are opposite (allocated then rolled back versus never allocated). Existing lifecycle fixture, BEGIN, wire failure and result owners are reused; combining assertions would erase one public phase boundary, not remove an alternate implementation.

### N122 — [TestCompositeRemoteCredentialOutput() / pkg/cypher/remote_credential_output_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/remote_credential_output_test.go#L73)

Reported **reuse**; 1 matched declarations:

- [TestCompositeRemoteCredentialErrorOutput() / pkg/cypher/remote_credential_output_test.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/remote_credential_output_test.go#L26) — behavior; similarity 0.285

**Disposition — C8 — retain separate output boundaries.** Malformed credential-bearing declarations assert the localized error identity, fallback/data redaction and public parse failure without creating a catalog. Successful encrypted declarations exercise CREATE/ALTER/SHOW typed and map projections, source immutability, slow-query logging and local nil URI. A shared success/error fixture would collapse incompatible setup/admission paths; no duplicate credential parser, renderer or redactor is implemented by these tests. Original evidence: F119 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N123 — [.parseConstraintContractPrimitive() / pkg/cypher/schema_contracts.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema_contracts.go#L258)

Reported **reuse**; 1 matched declarations:

- [.parseConstraintContractEntry() / pkg/cypher/schema_contracts.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/schema_contracts.go#L310) — behavior; similarity 0.263

**Disposition — current-source review.** Retain producer/consumer reuse: parseConstraintContractEntry invokes Primitive once, returns its typed constraint/error when recognized, otherwise tries the property-type owner then constructs a boolean entry. Primitive alone recognizes UNIQUE/EXISTS/node-key/temporal/relationship-key entries and constructs storage descriptors. The caller does not repeat primitive recognition; contract relationship-key syntax uses the shared predicate with its original parentheses admission. Focused schema and contract controls pass.

### N124 — [TestNumericWireBoltSignedAndReference() / pkg/bolt/numeric_wire_public_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/numeric_wire_public_test.go#L17)

Reported **reuse**; 1 matched declarations:

- [TestTypedNumericBoltBoundaries() / pkg/bolt/typed_numeric_wire_test.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/bolt/typed_numeric_wire_test.go#L15) — behavior; similarity 0.26

**Disposition — B8 — retain independent numeric populations.** Signed-boundary test checks eight exact 64-bit values, nested results and strings against an optional Neo4j driver reference while holding each explicit frame across the population. TypedNumericBoltBoundaries seeds typed properties and tests parameter/WITH/nested/written forms and expression results using listinputs and per-statement transaction ownership. Shared server/driver protocol owners remain; replacing one lifecycle with the other changes the test contract. Original evidence: F120 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N125 — [TestBoltConstraintIntegration_NewerConstraintTypes() / pkg/bolt/integration_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/integration_test.go#L414)

Reported **reuse**; 1 matched declarations:

- [TestBoltConstraintIntegration_ConstraintContracts() / pkg/bolt/integration_test.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/bolt/integration_test.go#L620) — behavior; similarity 0.227

**Disposition — current-source review.** Keep single-family DDL/SHOW CONSTRAINTS separate from grouped REQUIRE-block/SHOW CONSTRAINT CONTRACTS, whose record widths and primitive/boolean entry counts differ. Shared server/statement owners already remove transport duplication. Consolidate the repeated single-family subset with AllConstraintFamilies without losing the stronger row/type/entity assertions or its distinct statements; keep grouped contract expectations independent.

### N126 — [lazyResultTestConn() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L66)

Reported **reuse**; 1 matched declarations:

- [TestLazyResultEndsWhenTheConnectionGoes() / pkg/bolt/lazy_result_edge_test.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/bolt/lazy_result_edge_test.go#L79) — behavior; similarity 0.188

**Disposition — B9 — retain different fixture boundaries.** lazyResultTestConn drives real TCP/Bolt startup and wire-based seeding. TestLazyResultEndsWhenTheConnectionGoes injects readErrQueue/messageQueue failures into a mock Session after executor seeding, asserting lastLazy release and nextQueuedMessage errors. It cannot use a TCP connection fixture without losing the queue-injection seam; both exercise the same production lazy-result owner. Original evidence: F122 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N127 — [.streamNodesByLabelAtSnapshotWithView() / pkg/storage/badger_mvcc_indexed_reads.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/badger_mvcc_indexed_reads.go#L217)

Reported **reuse**; 1 matched declarations:

- [.streamNodesByLabelVisibleAtSnapshotWithView() / pkg/storage/badger_mvcc_indexed_reads.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/storage/badger_mvcc_indexed_reads.go#L218) — behavior; similarity 0.178

**Disposition — S5 — already converged in current source.** The indexed streamNodesByLabelVisibleAtSnapshotWithView body is a visibility-only forwarding wrapper in 35e3bddbf. It is absent at 38bd71f65 (declaration and callers); current callers pass an explicit visibility policy to streamNodesByLabelAtSnapshotWithView, which alone owns reader registration, indexed iteration and historical selection. Original evidence: F124 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N128 — [.streamNodesByLabelProjected() / pkg/storage/badger_transaction_graph_reads.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/badger_transaction_graph_reads.go#L462)

Reported **reuse**; 1 matched declarations:

- [.streamNodesByLabelAtSnapshotWithView() / pkg/storage/badger_mvcc_indexed_reads.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/storage/badger_mvcc_indexed_reads.go#L222) — behavior; similarity 0.154

**Disposition — S4 — corrective action / reuse.** Transaction stream already delegates historical committed population to streamNodesByLabelAtSnapshotWithView and latest buffered population to streamNodesByLabelProjectedInScope; it additionally overlays transaction-local pending/deleted nodes and unlocks around visitors. Remove its inert never-assigned overlay/overlayTouched/emitOverlay copy; retain those transaction responsibilities rather than duplicating the engine scan. Original evidence: F123 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

### N129 — [.execute() / pkg/cypher/composite_command_preparation.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/composite_command_preparation.go#L41)

Reported **reuse**; 1 matched declarations:

- [.executeCompositeCommand() / pkg/cypher/composite_commands.go](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/composite_commands.go#L315) — behavior; similarity 0.087

**Disposition — C9 — retain preparation/execution composition.** executeCompositeCommand owns manager availability, credential-redacted admission and CREATE/ALTER preparation, then invokes preparedCompositeCommand.execute. The latter alone applies the selected catalog mutation and formats its declared result. Metadata/EXPLAIN reuse preparation without executing. Folding side effects into the preparation wrapper would violate that no-mutation boundary; there are not two catalog mutation bodies. Original evidence: F125 in the linked ledger; apply the delivered correction status there, not an unimplemented future action.

## Every current-source ownership record

A verified call is evidence of actual reuse, not a waiver of surrounding copied code; candidate decisions above separately address residual overlap. These rows pin the current owner/caller and line.

| ID | Current candidate ↔ matched declaration | Binding | Decision / retained operation |
| --- | --- | --- | --- |
| Q001 | [.streamNodesByLabelProjected() / pkg/storage/badger_transaction_graph_reads.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/badger_transaction_graph_reads.go#L462) ↔ [streamVisitUnlocked() / pkg/storage/badger_transaction_graph_reads.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/badger_transaction_graph_reads.go#L422) | `current_owner_call` at `L495` | S4 — streamVisitUnlocked alone owns visitor unlock/retake and panic/error cleanup. Transaction streaming retains pending/deleted overlays and delegates committed populations to existing engine readers; remove its inert unassigned overlay path. |
| Q002 | [TestNumericPropertyIndexExactAroundFloatPrecision() / pkg/bolt/numeric_index_precision_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/numeric_index_precision_test.go#L19) ↔ [exactNumberOp() / pkg/bolt/numeric_index_precision_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/numeric_index_precision_test.go#L149) | `current_owner_call` at `L54` | Retain the independent math/big numeric oracle; use production transport but not the production numeric comparator for expected ordering around 2^53. Test scenarios call this one oracle, not copied comparison bodies. |
| Q003 | [TestTransactionUpdateEmbeddingStoragePolicy() / pkg/storage/embedding_text_policy_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/embedding_text_policy_test.go#L75) ↔ [sidecarTestNode() / pkg/storage/embedding_sidecar_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/embedding_sidecar_test.go#L20) | `current_owner_call` at `L93` | Retain shared sidecar fixture construction. The transaction test supplies its mutation/storage-policy cases and assertions; it does not rebuild the helper’s node/embedding source. |
| Q004 | [TestLegacyDateTimeWireMetadataSignatureNegotiation() / pkg/bolt/legacy_datetime_negotiation_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/legacy_datetime_negotiation_test.go#L85) ↔ [dateTimeWireConnect() / pkg/bolt/legacy_datetime_runtime_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/legacy_datetime_runtime_test.go#L55) | `current_owner_call` at `L110` | Retain the independent negotiation oracle: request exact Bolt4.4/UTC-patch/Bolt5 handshake bytes, assert the selected version and reference agent, and set the matching HELLO patch. Default openBoltTestConn negotiates its configured default and cannot supply those assertions. Socket I/O/PackStream primitives remain shared; a generic connection wrapper would not remove the oracle’s protocol decisions. |
| Q005 | [TestLegacyDateTimeWireMetadataSignatureNegotiation() / pkg/bolt/legacy_datetime_negotiation_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/legacy_datetime_negotiation_test.go#L85) ↔ [dateTimeWireQuery() / pkg/bolt/legacy_datetime_runtime_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/legacy_datetime_runtime_test.go#L142) | `current_owner_call` at `L152` | Retain one raw-record owner for these consumers: assert exactly one RECORD then SUCCESS while retaining encoded bytes for signature/location checks. Decoded readBatch cannot provide the raw struct-signature oracle. No second raw temporal-query body is introduced. |
| Q006 | [TestCreateDatabaseControls() / pkg/cypher/create_database_convergence_regression_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/create_database_convergence_regression_test.go#L178) ↔ [newCreateDatabaseRegressionExecutor() / pkg/cypher/create_database_convergence_regression_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/create_database_convergence_regression_test.go#L15) | `current_owner_call` at `L186` | Retain shared tracked executor/manager setup; creation controls vary names/existence/output/EXPLAIN expectations through that owner. There is no second manager-lifecycle body in the candidate. |
| Q007 | [TestBoltExplicitTransactionTerminalPathsRecordSinglePersistentWALAbort() / pkg/bolt/transaction_lifecycle_terminal_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/transaction_lifecycle_terminal_test.go#L74) ↔ [beginExplicitTransaction() / pkg/bolt/transaction_lifecycle_compatibility_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/transaction_lifecycle_compatibility_test.go#L47) | `current_owner_call` at `L131` | One BEGIN/SUCCESS owner drives transaction metadata. WAL/timeout consumers retain their separate durable-abort or deadline assertions, not copied BEGIN code. |
| Q008 | [ownRetainedValue() / pkg/storage/property_ownership.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/property_ownership.go#L38) ↔ [ownRetainedMap() / pkg/storage/property_ownership.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/property_ownership.go#L120) | `current_owner_call` at `L88` | S1 — remove canonicalPropertyValue/ownPropertyValue/ownedProperties/ownPropertyMap/codecImageProperties aliases and migrate consumers to ownRetainedValue/ownRetainedMap. Retain actual container/value owners: typed-list cloning preserves concrete shapes; canonicalPropertyList owns decoder/ingress homogeneous/mixed representation. Both are called by the same retained-value dispatcher, with no copied conversion bodies. |
| Q009 | [ownRetainedValue() / pkg/storage/property_ownership.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/property_ownership.go#L38) ↔ [retainedTypedPropertyList() / pkg/storage/property_ownership.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/property_ownership.go#L131) | `current_owner_call` at `L95` | S1 — remove canonicalPropertyValue/ownPropertyValue/ownedProperties/ownPropertyMap/codecImageProperties aliases and migrate consumers to ownRetainedValue/ownRetainedMap. Retain actual container/value owners: typed-list cloning preserves concrete shapes; canonicalPropertyList owns decoder/ingress homogeneous/mixed representation. Both are called by the same retained-value dispatcher, with no copied conversion bodies. |
| Q010 | [ownRetainedValue() / pkg/storage/property_ownership.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/property_ownership.go#L38) ↔ [canonicalPropertyList() / pkg/storage/property_ownership.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/property_ownership.go#L143) | `current_owner_call` at `L110` | S1 — remove canonicalPropertyValue/ownPropertyValue/ownedProperties/ownPropertyMap/codecImageProperties aliases and migrate consumers to ownRetainedValue/ownRetainedMap. Retain actual container/value owners: typed-list cloning preserves concrete shapes; canonicalPropertyList owns decoder/ingress homogeneous/mixed representation. Both are called by the same retained-value dispatcher, with no copied conversion bodies. |
| Q011 | [TestBoltZeroRowCorrelatedCallAggregation() / pkg/bolt/zero_row_correlated_aggregation_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/zero_row_correlated_aggregation_test.go#L10) ↔ [SendRun() / pkg/bolt/packstream_testutil_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/packstream_testutil_test.go#L257) | `current_owner_call` at `L41` | One RUN encoder accepts query, params and metadata. The aggregation consumer supplies its input population and asserts decoded results; it does not encode another RUN frame. |
| Q012 | [TestCallCancellationKeepsCallerErrorShape() / pkg/cypher/call_cancellation_error_shape_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/call_cancellation_error_shape_test.go#L105) ↔ [callCancellationCreateIndexes() / pkg/cypher/call_cancellation_error_shape_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/call_cancellation_error_shape_test.go#L90) | `current_owner_call` at `L123` | Retain shared index fixture/listing owners. Cancellation consumers add their own statement/COMMIT/discard/retry seam and exact no-publication/namespace assertions, not a second index catalog or seed decoder. |
| Q013 | [.parseCreateFulltextIndexDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L1093) ↔ [parseOptionalDDLName() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L734) | `current_owner_call` at `L1126` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| Q014 | [.parseCreateFulltextIndexDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L1093) ↔ [parseCreateFulltextForPattern() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L1068) | `current_owner_call` at `L1134` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| Q015 | [.executeCreateConstraintContract() / pkg/cypher/schema_contracts.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema_contracts.go#L72) ↔ [ValidateConstraintContractOnCreationForEngine() / pkg/storage/constraint_contracts.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/constraint_contracts.go#L341) | `current_owner_call` at `L116` | Retain the storage contract-validation owner. The Cypher consumer compiles typed entries and invokes validation before schema publication; it does not duplicate storage traversal or constraint evaluation. |
| Q016 | [.executeCreateConstraintContract() / pkg/cypher/schema_contracts.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema_contracts.go#L72) ↔ [RefreshUniqueConstraintValuesForConstraintsForEngine() / pkg/storage/constraint_validation.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/constraint_validation.go#L86) | `current_owner_call` at `L153` | Retain distinct post-admission UNIQUE-cache population using its storage owner; pre-creation contract validation is not a cache mutation. The Cypher caller sequences validation/bundle publication/cache refresh instead of implementing either storage algorithm. |
| Q017 | [TestListInputBoltRepresentations() / pkg/bolt/list_input_wire_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/list_input_wire_test.go#L45) ↔ [runListInputBoltStatement() / pkg/bolt/list_input_wire_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/list_input_wire_test.go#L18) | `current_owner_call` at `L63` | Retain the shared v5 driver statement/transaction lifecycle. The list-input consumer adds its own representations and exact numeric/scalar expectations; the v4.4 reference driver has a different API and explicit unpatched-protocol assertions. |
| Q018 | [TestLegacyDateTimeWireSignatureNegotiation() / pkg/bolt/legacy_datetime_negotiation_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/legacy_datetime_negotiation_test.go#L14) ↔ [dateTimeWireConnect() / pkg/bolt/legacy_datetime_runtime_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/legacy_datetime_runtime_test.go#L55) | `current_owner_call` at `L40` | Retain the independent negotiation oracle: request exact Bolt4.4/UTC-patch/Bolt5 handshake bytes, assert the selected version and reference agent, and set the matching HELLO patch. Default openBoltTestConn negotiates its configured default and cannot supply those assertions. Socket I/O/PackStream primitives remain shared; a generic connection wrapper would not remove the oracle’s protocol decisions. |
| Q019 | [TestLegacyDateTimeWireSignatureNegotiation() / pkg/bolt/legacy_datetime_negotiation_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/legacy_datetime_negotiation_test.go#L14) ↔ [dateTimeWireQuery() / pkg/bolt/legacy_datetime_runtime_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/legacy_datetime_runtime_test.go#L142) | `current_owner_call` at `L71` | Retain one raw-record owner for these consumers: assert exactly one RECORD then SUCCESS while retaining encoded bytes for signature/location checks. Decoded readBatch cannot provide the raw struct-signature oracle. No second raw temporal-query body is introduced. |
| Q020 | [.compileBound() / pkg/cypher/result_columns_compile.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/result_columns_compile.go#L81) ↔ [statementSyntaxQuery() / pkg/cypher/query_body_structure.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/query_body_structure.go#L447) | `current_owner_call` at `L99` | Retain canonical CST root selection. compileBound uses its typed query node for the binding/event walk rather than reparsing a body fragment; grammar-root selection has one implementation. |
| Q021 | [.compileBound() / pkg/cypher/result_columns_compile.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/result_columns_compile.go#L81) ↔ [ParseSyntax() / pkg/cypher/antlr/statement_syntax.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/antlr/statement_syntax.go#L207) | `current_owner_call` at `L91` | The accepted-source parser is called once by the compiler for the retained CST. Compilation walks the returned structure; it does not implement a second grammar or parse fragments through a fallback. |
| Q022 | [TestIndexDropProcedureExplicitTransactionRollbackKeepsStagedDrop() / pkg/cypher/index_drop_explicit_schema_visibility_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/index_drop_explicit_schema_visibility_test.go#L154) ↔ [indexDropNamesOf() / pkg/cypher/index_drop_explicit_schema_visibility_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/index_drop_explicit_schema_visibility_test.go#L222) | `current_owner_call` at `L214` | Retain shared index fixture/listing owners. Cancellation consumers add their own statement/COMMIT/discard/retry seam and exact no-publication/namespace assertions, not a second index catalog or seed decoder. |
| Q023 | [.parseCreateIndexDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L917) ↔ [parseOptionalDDLName() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L734) | `current_owner_call` at `L950` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| Q024 | [.parseCreateIndexDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L917) ↔ [parseCreateIndexForPattern() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L897) | `current_owner_call` at `L955` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| Q025 | [runBoltExplicitTxAttempt() / pkg/bolt/cross_session_merge_unique_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/cross_session_merge_unique_test.go#L98) ↔ [ReadMessage() / pkg/bolt/packstream_testutil_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/packstream_testutil_test.go#L167) | `current_owner_call` at `L150` | The shared PackStream framing reader supplies message kind/body; explicit transaction logic interprets terminal statuses rather than decoding framing again. |
| Q026 | [.parseCreateIndexLegacyDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L991) ↔ [parseOptionalDDLName() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L734) | `current_owner_call` at `L1015` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| Q027 | [TestIndexDropProcedureCancellationPublishesNothing() / pkg/cypher/index_drop_procedure_cancellation_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/index_drop_procedure_cancellation_test.go#L189) ↔ [newIndexDropFixture() / pkg/cypher/index_drop_procedure_cancellation_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/index_drop_procedure_cancellation_test.go#L144) | `current_owner_call` at `L205` | Retain shared index fixture/listing owners. Cancellation consumers add their own statement/COMMIT/discard/retry seam and exact no-publication/namespace assertions, not a second index catalog or seed decoder. |
| Q028 | [sameStoredValue() / pkg/storage/badger_embedding_sidecar.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/badger_embedding_sidecar.go#L295) ↔ [compareConstraintNumbers() / pkg/storage/constraint_list_equality.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/constraint_list_equality.go#L89) | `current_owner_call` at `L311` | Retain shared exact-number comparison: sameStoredValue calls compareConstraintNumbers rather than widening numeric values itself. Stored-image comparison additionally permits identical NaN bits and recursively compares maps/lists/time instants; constraint ordering keeps NaN unordered. Their different equality policies must not be erased; numeric decisions are already centralized. |
| Q029 | [TestNodeRuntimeCacheFillFormsPreserveMetadataAndIdentity() / pkg/storage/node_runtime_cache_contract_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/node_runtime_cache_contract_test.go#L185) ↔ [runtimeMetadataFull() / pkg/storage/node_runtime_cache_contract_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/node_runtime_cache_contract_test.go#L32) | `current_owner_call` at `L229` | Retain fixture-value reuse: one helper creates metadata fields and another synthetic runtime provenance/identity. Cache fill/store tests use those fixtures to assert copy/detach/policy behavior; they do not implement a second metadata or identity owner. |
| Q030 | [TestNodeRuntimeCacheFillFormsPreserveMetadataAndIdentity() / pkg/storage/node_runtime_cache_contract_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/node_runtime_cache_contract_test.go#L185) ↔ [runtimeSyntheticIdentity() / pkg/storage/node_runtime_cache_contract_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/node_runtime_cache_contract_test.go#L72) | `current_owner_call` at `L190` | Retain fixture-value reuse: one helper creates metadata fields and another synthetic runtime provenance/identity. Cache fill/store tests use those fixtures to assert copy/detach/policy behavior; they do not implement a second metadata or identity owner. |
| Q031 | [TestExplicitTransactionStreamsResultsByQID() / pkg/bolt/streaming_qid_compatibility_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/streaming_qid_compatibility_test.go#L13) ↔ [runStatementAndQID() / pkg/bolt/streaming_qid_compatibility_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/streaming_qid_compatibility_test.go#L157) | `current_owner_call` at `L21` | Retain one RUN/QID extraction owner. Consumers separately test live-stream isolation versus unknown-QID/IGNORED/RESET state; B4 shares subsequent PULL decoding. |
| Q032 | [TestLegacyDateTimeGraphPropertiesNeo4j526() / pkg/bolt/legacy_datetime_properties_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/legacy_datetime_properties_test.go#L29) ↔ [dateTimeWireConnect() / pkg/bolt/legacy_datetime_runtime_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/legacy_datetime_runtime_test.go#L55) | `current_owner_call` at `L48` | Retain the independent negotiation oracle: request exact Bolt4.4/UTC-patch/Bolt5 handshake bytes, assert the selected version and reference agent, and set the matching HELLO patch. Default openBoltTestConn negotiates its configured default and cannot supply those assertions. Socket I/O/PackStream primitives remain shared; a generic connection wrapper would not remove the oracle’s protocol decisions. |
| Q033 | [TestLegacyDateTimeGraphPropertiesNeo4j526() / pkg/bolt/legacy_datetime_properties_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/legacy_datetime_properties_test.go#L29) ↔ [dateTimeWireQuery() / pkg/bolt/legacy_datetime_runtime_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/legacy_datetime_runtime_test.go#L142) | `current_owner_call` at `L62` | Retain one raw-record owner for these consumers: assert exactly one RECORD then SUCCESS while retaining encoded bytes for signature/location checks. Decoded readBatch cannot provide the raw struct-signature oracle. No second raw temporal-query body is introduced. |
| Q034 | [TestLegacyDateTimeGraphPropertiesNeo4j526() / pkg/bolt/legacy_datetime_properties_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/legacy_datetime_properties_test.go#L29) ↔ [dateTimeWire() / pkg/bolt/legacy_datetime_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/legacy_datetime_test.go#L42) | `current_owner_call` at `L68` | Retain the independent expected wire-value builder (seconds/signature/zone) and its negative opposite-signature controls. It shares PackStream primitives, not the production temporal decision being tested. |
| Q035 | [parseForPatternTarget() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L852) ↔ [parseConstraintContractForClause() / pkg/cypher/schema_contracts.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema_contracts.go#L24) | `extracted_owner` at `L25` | One FOR node/relationship reader extracts variable and label/type; the contract consumer maps its entity enum and error, index consumers map label/type outputs. No second delimiter/pattern body remains in the consumers. |
| Q036 | [parseForPatternTarget() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L852) ↔ [parseCreateIndexForPattern() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L897) | `extracted_owner` at `L903` | One FOR node/relationship reader extracts variable and label/type; the contract consumer maps its entity enum and error, index consumers map label/type outputs. No second delimiter/pattern body remains in the consumers. |
| Q037 | [.parseCreateConstraintTypeDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L1841) ↔ [parseCreateIndexForPattern() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L897) | `current_owner_call` at `L1865` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| Q038 | [.parseCreateConstraintTypeDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L1841) ↔ [parseConstraintTypePredicate() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L1326) | `current_owner_call` at `L1870` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| Q039 | [.parseCreateConstraintRelationshipKeyOrCompositeUniqueDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L1796) ↔ [parseCreateIndexForPattern() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L897) | `current_owner_call` at `L1802` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| Q040 | [.parseCreateConstraintSimplePropertyDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L1891) ↔ [parseCreateIndexForPattern() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L897) | `current_owner_call` at `L1915` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| Q041 | [.parseCreateConstraintSimplePropertyDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L1891) ↔ [parseConstraintPredicate() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L1286) | `current_owner_call` at `L1920` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| Q042 | [TestBoltLazyResultDeliversEveryRowInBatches() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L80) ↔ [pullBatch() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L15) | `current_owner_call` at `L99` | One PULL transport/decoder returns records, summary and failure code. Success-only consumers assert empty failure codes; error/lifecycle consumers assert exact failures. This is not a second scoring, cursor or result owner. |
| Q043 | [TestBoltLazyResultErrorFailsThePullThatReachesIt() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L123) ↔ [pullBatch() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L15) | `current_owner_call` at `L127` | One PULL transport/decoder returns records, summary and failure code. Success-only consumers assert empty failure codes; error/lifecycle consumers assert exact failures. This is not a second scoring, cursor or result owner. |
| Q044 | [TestBoltLazyResultTimeoutSource() / pkg/bolt/deadline_source_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/deadline_source_test.go#L18) ↔ [pullBatch() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L15) | `current_owner_call` at `L47` | One PULL transport/decoder returns records, summary and failure code. Success-only consumers assert empty failure codes; error/lifecycle consumers assert exact failures. This is not a second scoring, cursor or result owner. |
| Q045 | [.parseCreateConstraintForRequireParts() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L1386) ↔ [parseOptionalDDLName() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L734) | `current_owner_call` at `L1414` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| Q046 | [TestBoltLazyResultEndsOnDiscardResetAndRun() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L165) ↔ [pullBatch() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L15) | `current_owner_call` at `L183` | One PULL transport/decoder returns records, summary and failure code. Success-only consumers assert empty failure codes; error/lifecycle consumers assert exact failures. This is not a second scoring, cursor or result owner. |
| Q047 | [TestBoltLazyResultEndsOnDiscardResetAndRun() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L165) ↔ [readBatch() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L21) | `current_owner_call` at `L172` | One batch decoder distinguishes RECORD/SUCCESS/FAILURE and retains summary/failure fields. SendPull constructs a request; readBatch consumes responses. Opposite protocol directions remain distinct and share PackStream primitives. |
| Q048 | [propertiesSegment() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L806) ↔ [.parseCreateIndexDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L917) | `extracted_owner` at `L960` | One ON-list/delimiter/OPTIONS splitter serves ordinary and fulltext index families with explicit syntax admission. Both-parser controls preserve native-only extension versus ANTLR rejection and no catalog mutation. |
| Q049 | [propertiesSegment() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L806) ↔ [.parseCreateFulltextIndexDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L1093) | `extracted_owner` at `L1151` | One ON-list/delimiter/OPTIONS splitter serves ordinary and fulltext index families with explicit syntax admission. Both-parser controls preserve native-only extension versus ANTLR rejection and no catalog mutation. |
| Q050 | [.parseCreateConstraintNodeKeyDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L1437) ↔ [parseCreateIndexForPattern() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L897) | `current_owner_call` at `L1461` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| Q051 | [TestNodeRuntimeCacheStorePreservesMetadataAndPolicy() / pkg/storage/node_runtime_cache_contract_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/node_runtime_cache_contract_test.go#L145) ↔ [runtimeSyntheticIdentity() / pkg/storage/node_runtime_cache_contract_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/node_runtime_cache_contract_test.go#L72) | `current_owner_call` at `L147` | Retain fixture-value reuse: one helper creates metadata fields and another synthetic runtime provenance/identity. Cache fill/store tests use those fixtures to assert copy/detach/policy behavior; they do not implement a second metadata or identity owner. |
| Q052 | [.parseCreateConstraintCardinalityDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L1661) ↔ [parseCardinalityRequireExpr() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L1640) | `current_owner_call` at `L1670` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| Q053 | [serveBoltIntegrationServer() / pkg/bolt/integration_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/integration_test.go#L179) ↔ [startBoltIntegrationServer() / pkg/bolt/integration_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/integration_test.go#L153) | `extracted_owner` at `L173` | B3 — shared constructor owns executor/configuration, serveBoltIntegrationServer owns public Serve/listen/join, and callers own special fixtures. Remove remaining copied startup/HELLO/close bodies where contracts match. The second fixture was traced through private serve to the same public Serve method and retired. All callers use this owner with their original optional shutdown observer; deliberate close failures remain asserted. |
| Q054 | [TestLegacyDateTimeWireSignatureFailurePreservesState() / pkg/bolt/legacy_datetime_failure_state_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/legacy_datetime_failure_state_test.go#L12) ↔ [dateTimeWireConnect() / pkg/bolt/legacy_datetime_runtime_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/legacy_datetime_runtime_test.go#L55) | `current_owner_call` at `L27` | Retain the independent negotiation oracle: request exact Bolt4.4/UTC-patch/Bolt5 handshake bytes, assert the selected version and reference agent, and set the matching HELLO patch. Default openBoltTestConn negotiates its configured default and cannot supply those assertions. Socket I/O/PackStream primitives remain shared; a generic connection wrapper would not remove the oracle’s protocol decisions. |
| Q055 | [TestLegacyDateTimeWireSignatureFailurePreservesState() / pkg/bolt/legacy_datetime_failure_state_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/legacy_datetime_failure_state_test.go#L12) ↔ [dateTimeWireQuery() / pkg/bolt/legacy_datetime_runtime_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/legacy_datetime_runtime_test.go#L142) | `current_owner_call` at `L35` | Retain one raw-record owner for these consumers: assert exactly one RECORD then SUCCESS while retaining encoded bytes for signature/location checks. Decoded readBatch cannot provide the raw struct-signature oracle. No second raw temporal-query body is introduced. |
| Q056 | [TestDatabaseCommands_CreateDatabase_WhitespacePermutations() / pkg/cypher/database_commands_whitespace_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/database_commands_whitespace_test.go#L60) ↔ [whitespacePermutations() / pkg/cypher/database_commands_whitespace_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/database_commands_whitespace_test.go#L21) | `current_owner_call` at `L73` | Retain one permutation generator used by CREATE/DROP tests; F115 records the distinct seed/state/output oracles. No duplicated whitespace reader is retained. |
| Q057 | [newDatabaseManagerBoltWriteSession() / pkg/bolt/server_dbmanager_integration_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/server_dbmanager_integration_test.go#L300) ↔ [TestDatabaseManagerBoltNeo4jDriverExecuteWritePrefixesGeneratedNodeIDs() / pkg/bolt/server_dbmanager_integration_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/server_dbmanager_integration_test.go#L365) | `extracted_owner` at `L373` | One DB-manager server/driver/connectivity/write-session fixture is reused directly. Callers keep their supplied manager, explicit/default connection budget and separate counter/rollback/binding assertions. |
| Q058 | [newDatabaseManagerBoltWriteSession() / pkg/bolt/server_dbmanager_integration_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/server_dbmanager_integration_test.go#L300) ↔ [TestDatabaseManagerBoltNeo4jDriverExecuteWriteRollsBackPriorWrites() / pkg/bolt/server_dbmanager_integration_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/server_dbmanager_integration_test.go#L330) | `extracted_owner` at `L338` | One DB-manager server/driver/connectivity/write-session fixture is reused directly. Callers keep their supplied manager, explicit/default connection budget and separate counter/rollback/binding assertions. |
| Q059 | [runBoltExplicitTxSessions() / pkg/bolt/cross_session_merge_unique_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/cross_session_merge_unique_test.go#L68) ↔ [TestBoltCrossSessionMergeUniqueConflict_Concurrent_ExplicitTx() / pkg/bolt/cross_session_merge_unique_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/cross_session_merge_unique_test.go#L359) | `extracted_owner` at `L382` | One goroutine/wait/error-collection owner drives session rounds and preserves session/round diagnostics. Callers retain original namespace, concurrency, queries and graph identity oracles. |
| Q060 | [runBoltExplicitTxSessions() / pkg/bolt/cross_session_merge_unique_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/cross_session_merge_unique_test.go#L68) ↔ [TestBoltCrossSessionMergeUniqueConflict_Concurrent_UnwindExplicitTx() / pkg/bolt/cross_session_merge_unique_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/cross_session_merge_unique_test.go#L446) | `extracted_owner` at `L468` | One goroutine/wait/error-collection owner drives session rounds and preserves session/round diagnostics. Callers retain original namespace, concurrency, queries and graph identity oracles. |
| Q061 | [runBoltExplicitTxSessions() / pkg/bolt/cross_session_merge_unique_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/cross_session_merge_unique_test.go#L68) ↔ [TestBoltCrossSessionMultiMergeUniqueConflict_Concurrent_ExplicitTx() / pkg/bolt/cross_session_merge_unique_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/cross_session_merge_unique_test.go#L411) | `extracted_owner` at `L424` | One goroutine/wait/error-collection owner drives session rounds and preserves session/round diagnostics. Callers retain original namespace, concurrency, queries and graph identity oracles. |
| Q062 | [runBoltExplicitTxSessions() / pkg/bolt/cross_session_merge_unique_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/cross_session_merge_unique_test.go#L68) ↔ [TestBoltConcurrentMergeOfUniqueKeyAfterOtherWrites() / pkg/bolt/merge_unique_after_writes_race_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/merge_unique_after_writes_race_test.go#L18) | `extracted_owner` at `L33` | One goroutine/wait/error-collection owner drives session rounds and preserves session/round diagnostics. Callers retain original namespace, concurrency, queries and graph identity oracles. |
| Q063 | [runBoltExplicitTxSessions() / pkg/bolt/cross_session_merge_unique_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/cross_session_merge_unique_test.go#L68) ↔ [runBoltExplicitTxWithRetry() / pkg/bolt/cross_session_merge_unique_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/cross_session_merge_unique_test.go#L47) | `current_owner_call` at `L77` | One retry owner applies the original transient-status filter, 20-attempt budget and backoff around runBoltExplicitTxAttempt; no query-only adapter or duplicate retry loop remains. |
| Q064 | [TestPipelineApplyProcedureCallEdges() / pkg/cypher/pipeline_procedure_call_coverage_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/pipeline_procedure_call_coverage_test.go#L56) ↔ [registerCoverageProcedures() / pkg/cypher/pipeline_procedure_call_coverage_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/pipeline_procedure_call_coverage_test.go#L15) | `current_owner_call` at `L57` | Retain cleanup-owned procedure registration shared by the procedure cases. Each case supplies a different pipeline/empty/error contract; it does not re-register the same procedure bodies itself. |
| Q065 | [projectCachedNodeForRead() / pkg/storage/badger_queries.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/badger_queries.go#L637) ↔ [getNodeProjectedThrough() / pkg/storage/wrapper_capabilities.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/wrapper_capabilities.go#L196) | `extracted_owner` at `L204` | Retain one cached-image projection owner called by engine cache hits and wrapper fallback; decoded storage reads use the existing projected decoder. Node-copy callbacks own metadata/vector detachment. ProjectedNodeReader constrains property keys, not mandatory vector absence, so the inspected fallback’s optional vectors do not violate that interface. |
| Q066 | [projectCachedNodeForRead() / pkg/storage/badger_queries.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/badger_queries.go#L637) ↔ [.getNodeProjected() / pkg/storage/badger_nodes.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/badger_nodes.go#L453) | `extracted_owner` at `L460` | Retain one cached-image projection owner called by engine cache hits and wrapper fallback; decoded storage reads use the existing projected decoder. Node-copy callbacks own metadata/vector detachment. ProjectedNodeReader constrains property keys, not mandatory vector absence, so the inspected fallback’s optional vectors do not violate that interface. |
| Q067 | [projectCachedNodeForRead() / pkg/storage/badger_queries.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/badger_queries.go#L637) ↔ [copyNodeWithoutEmbeddings() / pkg/storage/transaction.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/transaction.go#L152) | `current_owner_call` at `L653` | Retain one cached-image projection owner called by engine cache hits and wrapper fallback; decoded storage reads use the existing projected decoder. Node-copy callbacks own metadata/vector detachment. ProjectedNodeReader constrains property keys, not mandatory vector absence, so the inspected fallback’s optional vectors do not violate that interface. |
| Q068 | [schemaOptionsTail() / pkg/cypher/schema_options.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema_options.go#L50) ↔ [.parseCreateIndexLegacyDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L991) | `extracted_owner` at `L1039` | C2/C3 — the listed statement consumer calls the shared OPTIONS admission owner; it has no second map/parameter-tail decoder. Further converge adjacent ON/property/head scanning and retire family-only forwarding wrappers. Keep each family’s admitted syntax and error identity. |
| Q069 | [schemaOptionsTail() / pkg/cypher/schema_options.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema_options.go#L50) ↔ [.parseCreateIndexDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L917) | `extracted_owner` at `L964` | C2/C3 — the listed statement consumer calls the shared OPTIONS admission owner; it has no second map/parameter-tail decoder. Further converge adjacent ON/property/head scanning and retire family-only forwarding wrappers. Keep each family’s admitted syntax and error identity. |
| Q070 | [schemaOptionsTail() / pkg/cypher/schema_options.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema_options.go#L50) ↔ [.parseCreateConstraintForRequireHead() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L1424) | `extracted_owner` at `L1430` | C2/C3 — the listed statement consumer calls the shared OPTIONS admission owner; it has no second map/parameter-tail decoder. Further converge adjacent ON/property/head scanning and retire family-only forwarding wrappers. Keep each family’s admitted syntax and error identity. |
| Q071 | [schemaOptionsTail() / pkg/cypher/schema_options.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema_options.go#L50) ↔ [.parseCreateFulltextIndexDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L1093) | `extracted_owner` at `L1155` | C2/C3 — the listed statement consumer calls the shared OPTIONS admission owner; it has no second map/parameter-tail decoder. Further converge adjacent ON/property/head scanning and retire family-only forwarding wrappers. Keep each family’s admitted syntax and error identity. |
| Q072 | [.parseCreateConstraintContractDDL() / pkg/cypher/schema_contracts.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema_contracts.go#L35) ↔ [parseConstraintContractForClause() / pkg/cypher/schema_contracts.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema_contracts.go#L24) | `current_owner_call` at `L46` | This consumer calls parseForPatternTarget, then maps node/relationship to the typed storage contract entity. It owns that result/error mapping, not another FOR parser. |
| Q073 | [startBoltIntegrationServer() / pkg/bolt/integration_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/integration_test.go#L153) ↔ [TestBoltBenchmarkCreateDeleteRelationship() / pkg/bolt/integration_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/integration_test.go#L1089) | `extracted_owner` at `L1095` | B3 — shared constructor owns executor/configuration, serveBoltIntegrationServer owns public Serve/listen/join, and callers own special fixtures. Remove remaining copied startup/HELLO/close bodies where contracts match. The second fixture was traced through private serve to the same public Serve method and retired. All callers use this owner with their original optional shutdown observer; deliberate close failures remain asserted. |
| Q074 | [startBoltIntegrationServer() / pkg/bolt/integration_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/integration_test.go#L153) ↔ [TestBoltCypherIntegration() / pkg/bolt/integration_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/integration_test.go#L264) | `extracted_owner` at `L270` | B3 — shared constructor owns executor/configuration, serveBoltIntegrationServer owns public Serve/listen/join, and callers own special fixtures. Remove remaining copied startup/HELLO/close bodies where contracts match. The second fixture was traced through private serve to the same public Serve method and retired. All callers use this owner with their original optional shutdown observer; deliberate close failures remain asserted. |
| Q075 | [startBoltIntegrationServer() / pkg/bolt/integration_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/integration_test.go#L153) ↔ [serveBoltIntegrationServer() / pkg/bolt/integration_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/integration_test.go#L179) | `current_owner_call` at `L173` | B3 — shared constructor owns executor/configuration, serveBoltIntegrationServer owns public Serve/listen/join, and callers own special fixtures. Remove remaining copied startup/HELLO/close bodies where contracts match. The second fixture was traced through private serve to the same public Serve method and retired. All callers use this owner with their original optional shutdown observer; deliberate close failures remain asserted. |
| Q076 | [startBoltIntegrationServer() / pkg/bolt/integration_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/integration_test.go#L153) ↔ [boltLoadBinaryServer() / pkg/bolt/apoc_load_binary_wire_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/apoc_load_binary_wire_test.go#L109) | `extracted_owner` at `L116` | B3 — shared constructor owns executor/configuration, serveBoltIntegrationServer owns public Serve/listen/join, and callers own special fixtures. Remove remaining copied startup/HELLO/close bodies where contracts match. The second fixture was traced through private serve to the same public Serve method and retired. All callers use this owner with their original optional shutdown observer; deliberate close failures remain asserted. |
| Q077 | [runBoltQueryWithParams() / pkg/bolt/integration_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/integration_test.go#L232) ↔ [TestBoltIntegration_G2GVersionNodesFallbackRowShape_ServerStack() / pkg/bolt/integration_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/integration_test.go#L767) | `extracted_owner` at `L798` | One parameterized RUN/PULL/full-record collection owner serves callers with nil or populated maps. Consumers retain their exact result/error oracles; no no-parameter forwarding alias remains. |
| Q078 | [runBoltQueryWithParams() / pkg/bolt/integration_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/integration_test.go#L232) ↔ [TestBoltZeroRowCorrelatedCallAggregation() / pkg/bolt/zero_row_correlated_aggregation_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/zero_row_correlated_aggregation_test.go#L10) | `extracted_owner` at `L13` | One parameterized RUN/PULL/full-record collection owner serves callers with nil or populated maps. Consumers retain their exact result/error oracles; no no-parameter forwarding alias remains. |
| Q079 | [runBoltQueryWithParams() / pkg/bolt/integration_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/integration_test.go#L232) ↔ [TestBoltLazyResultDeliversEveryRowInBatches() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L80) | `extracted_owner` at `L114` | One parameterized RUN/PULL/full-record collection owner serves callers with nil or populated maps. Consumers retain their exact result/error oracles; no no-parameter forwarding alias remains. |
| Q080 | [runBoltQueryWithParams() / pkg/bolt/integration_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/integration_test.go#L232) ↔ [TestBoltLazyResultEndsOnDiscardResetAndRun() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L165) | `extracted_owner` at `L197` | One parameterized RUN/PULL/full-record collection owner serves callers with nil or populated maps. Consumers retain their exact result/error oracles; no no-parameter forwarding alias remains. |
| Q081 | [runBoltQueryWithParams() / pkg/bolt/integration_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/integration_test.go#L232) ↔ [TestBoltCountSubqueryCommentGap() / pkg/bolt/count_subquery_comment_protocol_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/count_subquery_comment_protocol_test.go#L18) | `extracted_owner` at `L45` | One parameterized RUN/PULL/full-record collection owner serves callers with nil or populated maps. Consumers retain their exact result/error oracles; no no-parameter forwarding alias remains. |
| Q082 | [parseCreateIndexForPattern() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L897) ↔ [.parseCreateConstraintRelationshipKeyOrCompositeUniqueDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L1796) | `extracted_owner` at `L1802` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| Q083 | [parseCreateIndexForPattern() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L897) ↔ [.parseCreateIndexDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L917) | `extracted_owner` at `L955` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| Q084 | [parseCreateIndexForPattern() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L897) ↔ [.parseCreateConstraintSimplePropertyDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L1891) | `extracted_owner` at `L1915` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| Q085 | [parseCreateIndexForPattern() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L897) ↔ [.parseCreateConstraintTypeDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L1841) | `extracted_owner` at `L1865` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| Q086 | [parseCreateIndexForPattern() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L897) ↔ [parseCreateFulltextForPattern() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L1068) | `extracted_owner` at `L1069` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| Q087 | [parseCreateIndexForPattern() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L897) ↔ [.parseCreateConstraintNodeKeyDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L1437) | `extracted_owner` at `L1461` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| Q088 | [TestLegacyDateTimeLocalServer() / pkg/bolt/legacy_datetime_runtime_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/legacy_datetime_runtime_test.go#L34) ↔ [startBoltIntegrationServer() / pkg/bolt/integration_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/integration_test.go#L153) | `current_owner_call` at `L45` | B3 — shared constructor owns executor/configuration, serveBoltIntegrationServer owns public Serve/listen/join, and callers own special fixtures. Remove remaining copied startup/HELLO/close bodies where contracts match. The second fixture was traced through private serve to the same public Serve method and retired. All callers use this owner with their original optional shutdown observer; deliberate close failures remain asserted. |
| Q089 | [roundTripLocalDateTimeScenario() / pkg/bolt/localdatetime_roundtrip_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/localdatetime_roundtrip_test.go#L103) ↔ [runBoltStatementNoRecords() / pkg/bolt/localdatetime_roundtrip_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/localdatetime_roundtrip_test.go#L124) | `current_owner_call` at `L111` | Retain the prebuilt-RUN-message consumer: temporal input tests must inject hand-encoded parameter bytes rather than reconstruct them through SendRun. It shares the underlying message I/O owner; decoded query/parameter collection is a different input contract. |
| Q090 | [roundTripLocalDateTimeScenario() / pkg/bolt/localdatetime_roundtrip_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/localdatetime_roundtrip_test.go#L103) ↔ [TestBoltIntegration_LocalDateTimeParamRoundTrip_ServerStack() / pkg/bolt/localdatetime_roundtrip_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/localdatetime_roundtrip_test.go#L71) | `extracted_owner` at `L75` | Retain one roundTripLocalDateTimeScenario for scalar and UNWIND prebuilt messages, preserving store readback and exact temporal values. Shared server/connection owners already own lifecycle. |
| Q091 | [TestMalformedMatchStatementsRejectedLikeNeo4j() / pkg/cypher/match_semantic_validation_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/match_semantic_validation_test.go#L108) ↔ [requireMatchSemanticDetail() / pkg/cypher/match_semantic_validation_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/match_semantic_validation_test.go#L202) | `current_owner_call` at `L117` | Retain one assertion owner for the typed semantic detail/localization identity. Test populations supply distinct malformed statements and expected identities; no second string/status classifier remains. |
| Q092 | [newBoltQuotaFixture() / pkg/bolt/transaction_quota_fixture_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/transaction_quota_fixture_test.go#L15) ↔ [TestBoltTransactionQuotas() / pkg/bolt/transaction_quotas_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/transaction_quotas_test.go#L19) | `extracted_owner` at `L76` | One quota fixture owns memory engine/namespace/capacity configuration; quota cases assert different admission/release boundaries using it, not duplicate quota implementations. |
| Q093 | [TestCypherMatchingLocalizedErrorsHaveTypedIdentity() / pkg/cypher/matching_localization_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/matching_localization_test.go#L24) ↔ [requireCypherMatchingLocalizedError() / pkg/cypher/matching_localization_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/matching_localization_test.go#L13) | `current_owner_call` at `L34` | Retain one assertion owner for the typed semantic detail/localization identity. Test populations supply distinct malformed statements and expected identities; no second string/status classifier remains. |
| Q094 | [.parseCreateConstraintPolicyDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L1779) ↔ [parsePolicyModeRequireExpr() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L1762) | `current_owner_call` at `L1788` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| Q095 | [.parseCreateConstraintPolicyDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L1779) ↔ [parsePolicyForClause() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L1708) | `current_owner_call` at `L1784` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| Q096 | [runBoltExplicitTxWithRetry() / pkg/bolt/cross_session_merge_unique_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/cross_session_merge_unique_test.go#L47) ↔ [runBoltExplicitTxAttempt() / pkg/bolt/cross_session_merge_unique_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/cross_session_merge_unique_test.go#L98) | `current_owner_call` at `L50` | One explicit BEGIN/parameterized RUN/PULL/terminal attempt owns the wire sequence; callers provide queries and parameters. Retry policy lives only in runBoltExplicitTxWithRetry. |
| Q097 | [startTransactionLifecycleServerWithStore() / pkg/bolt/transaction_lifecycle_test_helpers_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/transaction_lifecycle_test_helpers_test.go#L20) ↔ [startTransactionLifecycleServer() / pkg/bolt/transaction_lifecycle_compatibility_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/transaction_lifecycle_compatibility_test.go#L20) | `extracted_owner` at `L28` | One lifecycle test server owns the supplied store; the factory fixture selects that store and delegates actual startup to the shared Serve owner. |
| Q098 | [endpointCheckMatchesGetNode() / pkg/storage/nor160_endpoint_visibility_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/nor160_endpoint_visibility_test.go#L20) ↔ [TestBadgerTransactionRelationshipEndpointVisible() / pkg/storage/relationship_endpoint_checker_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/relationship_endpoint_checker_test.go#L91) | `extracted_owner` at `L147` | S6 — use endpointCheckMatchesGetNode for the answered/GetNode agreement assertion; retain each scenario’s explicit answered/visibility expectations. Remove the local copied assertion, not the peer/delete/reveal/decay populations. |
| Q099 | [TestBoltLazyResultClientTimeoutStatus() / pkg/bolt/deadline_status_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/deadline_status_test.go#L57) ↔ [pullBatch() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L15) | `current_owner_call` at `L67` | One PULL transport/decoder returns records, summary and failure code. Success-only consumers assert empty failure codes; error/lifecycle consumers assert exact failures. This is not a second scoring, cursor or result owner. |
| Q100 | [TestBoltLazyResultLeavesOtherStatementsWhole() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L205) ↔ [lazyResultTestConn() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L66) | `current_owner_call` at `L206` | Retain fixture composition: shared store/server/connection owners plus explicit wire-based row seeding. Each consumer keeps batch-size/exhaustion/timeout/discard/reset/RUN assertions. Mock Session queue-injection tests cannot use a TCP-only fixture without losing their failure seam (F122). |
| Q101 | [.parseCreateConstraintDomainDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L1593) ↔ [parseDomainConstraintPredicate() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema.go#L1568) | `current_owner_call` at `L1598` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| Q102 | [boltLoadBinaryServer() / pkg/bolt/apoc_load_binary_wire_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/apoc_load_binary_wire_test.go#L109) ↔ [startBoltIntegrationServer() / pkg/bolt/integration_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/integration_test.go#L153) | `current_owner_call` at `L116` | B3 — shared constructor owns executor/configuration, serveBoltIntegrationServer owns public Serve/listen/join, and callers own special fixtures. Remove remaining copied startup/HELLO/close bodies where contracts match. The second fixture was traced through private serve to the same public Serve method and retired. All callers use this owner with their original optional shutdown observer; deliberate close failures remain asserted. |
| Q103 | [boltLoadBinaryServer() / pkg/bolt/apoc_load_binary_wire_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/apoc_load_binary_wire_test.go#L109) ↔ [TestBoltLoadBinaryWireTransmitsBytes() / pkg/bolt/apoc_load_binary_wire_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/apoc_load_binary_wire_test.go#L123) | `extracted_owner` at `L128` | Retain specialized fixture composition: create the named APOC-load store through the shared server owner, then test the actual byte handler/serialized bytes. B1 removes its duplicated statement collector; the fixture does not reimplement server lifecycle. |
| Q104 | [lazyResultTestConn() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L66) ↔ [openBoltTestConn() / pkg/bolt/integration_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/integration_test.go#L209) | `current_owner_call` at `L70` | Retain fixture composition: shared store/server/connection owners plus explicit wire-based row seeding. Each consumer keeps batch-size/exhaustion/timeout/discard/reset/RUN assertions. Mock Session queue-injection tests cannot use a TCP-only fixture without losing their failure seam (F122). |
| Q105 | [lazyResultTestConn() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L66) ↔ [TestBoltLazyResultClientTimeoutStatus() / pkg/bolt/deadline_status_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/deadline_status_test.go#L57) | `extracted_owner` at `L58` | Retain fixture composition: shared store/server/connection owners plus explicit wire-based row seeding. Each consumer keeps batch-size/exhaustion/timeout/discard/reset/RUN assertions. Mock Session queue-injection tests cannot use a TCP-only fixture without losing their failure seam (F122). |
| Q106 | [lazyResultTestConn() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L66) ↔ [TestBoltLazyResultLeavesOtherStatementsWhole() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L205) | `extracted_owner` at `L206` | Retain fixture composition: shared store/server/connection owners plus explicit wire-based row seeding. Each consumer keeps batch-size/exhaustion/timeout/discard/reset/RUN assertions. Mock Session queue-injection tests cannot use a TCP-only fixture without losing their failure seam (F122). |
| Q107 | [lazyResultTestConn() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L66) ↔ [TestBoltLazyResultDeliversEveryRowInBatches() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L80) | `extracted_owner` at `L81` | Retain fixture composition: shared store/server/connection owners plus explicit wire-based row seeding. Each consumer keeps batch-size/exhaustion/timeout/discard/reset/RUN assertions. Mock Session queue-injection tests cannot use a TCP-only fixture without losing their failure seam (F122). |
| Q108 | [lazyResultTestConn() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L66) ↔ [TestBoltLazyResultEndsOnDiscardResetAndRun() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L165) | `extracted_owner` at `L166` | Retain fixture composition: shared store/server/connection owners plus explicit wire-based row seeding. Each consumer keeps batch-size/exhaustion/timeout/discard/reset/RUN assertions. Mock Session queue-injection tests cannot use a TCP-only fixture without losing their failure seam (F122). |
| Q109 | [lazyResultTestConn() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L66) ↔ [TestBoltLazyResultDiscardReachesTheError() / pkg/bolt/lazy_result_edge_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_edge_test.go#L118) | `extracted_owner` at `L119` | Retain fixture composition: shared store/server/connection owners plus explicit wire-based row seeding. Each consumer keeps batch-size/exhaustion/timeout/discard/reset/RUN assertions. Mock Session queue-injection tests cannot use a TCP-only fixture without losing their failure seam (F122). |
| Q110 | [fixedCommandScopeFacts() / pkg/cypher/result_columns_compile.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/result_columns_compile.go#L70) ↔ [.compileBound() / pkg/cypher/result_columns_compile.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/result_columns_compile.go#L81) | `extracted_owner` at `L89` | M1 — consolidate remaining inline privilege-command metadata into fixedCommandScopeFacts with an explicit scope-transfer policy. Preserve incoming execution names for privileges and declared output scope for CREATE/SHOW. Public StatementFacts checks cover all four fixed-schema families. |
| Q111 | [fixedCommandScopeFacts() / pkg/cypher/result_columns_compile.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/result_columns_compile.go#L70) ↔ [projectionScope() / pkg/cypher/result_columns_program.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/result_columns_program.go#L82) | `current_owner_call` at `L77` | M1 — consolidate remaining inline privilege-command metadata into fixedCommandScopeFacts with an explicit scope-transfer policy. Preserve incoming execution names for privileges and declared output scope for CREATE/SHOW. Public StatementFacts checks cover all four fixed-schema families. |
| Q112 | [parseConstraintContractForClause() / pkg/cypher/schema_contracts.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema_contracts.go#L24) ↔ [.parseCreateConstraintContractDDL() / pkg/cypher/schema_contracts.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema_contracts.go#L35) | `extracted_owner` at `L46` | This consumer calls parseForPatternTarget, then maps node/relationship to the typed storage contract entity. It owns that result/error mapping, not another FOR parser. |
| Q113 | [ownRetainedMap() / pkg/storage/property_ownership.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/property_ownership.go#L120) ↔ [ownRetainedValue() / pkg/storage/property_ownership.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/property_ownership.go#L38) | `current_owner_call` at `L124` | S1 — remove canonicalPropertyValue/ownPropertyValue/ownedProperties/ownPropertyMap/codecImageProperties aliases and migrate consumers to ownRetainedValue/ownRetainedMap. Retain actual container/value owners: typed-list cloning preserves concrete shapes; canonicalPropertyList owns decoder/ingress homogeneous/mixed representation. Both are called by the same retained-value dispatcher, with no copied conversion bodies. |
| Q114 | [constraintRejectedInExplicitTransaction() / pkg/cypher/constraint_checked_writes_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/constraint_checked_writes_test.go#L31) ↔ [TestExplicitTransactionConstraintViolationFailsTheStatement() / pkg/cypher/constraint_checked_writes_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/constraint_checked_writes_test.go#L101) | `extracted_owner` at `L131` | Retain the existing common BEGIN/rejected statement/terminal lifecycle; keep distinct constraint populations and COMMIT/ROLLBACK/post-state oracles. F117 names the pair; neither consumer has its own rejection lifecycle copy. |
| Q115 | [constraintRejectedInExplicitTransaction() / pkg/cypher/constraint_checked_writes_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/constraint_checked_writes_test.go#L31) ↔ [TestExplicitTransactionMergeUpdateViolationsFailTheStatement() / pkg/cypher/constraint_checked_writes_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/constraint_checked_writes_test.go#L156) | `extracted_owner` at `L184` | Retain the existing common BEGIN/rejected statement/terminal lifecycle; keep distinct constraint populations and COMMIT/ROLLBACK/post-state oracles. F117 names the pair; neither consumer has its own rejection lifecycle copy. |
| Q116 | [schemaOptionsParameter() / pkg/cypher/schema_options.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema_options.go#L90) ↔ [scanParameterReferences() / pkg/cypher/parameters.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/parameters.go#L141) | `current_owner_call` at `L91` | Retain direct reuse: scanParameterReferences alone owns quote-aware parameter-reference scanning; this consumer accepts only the one reference spanning the entire OPTIONS expression. It does not implement another reference scanner. |
| Q117 | [writtenEdgeSource() / pkg/storage/badger_transaction_images.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/badger_transaction_images.go#L239) ↔ [setEdgeImage() / pkg/storage/badger_transaction_images.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/badger_transaction_images.go#L251) | `current_owner_call` at `L245` | One retained image updater installs owned properties and compact envelope timestamps. Source selection/codec decoding callers do not reimplement image installation. |
| Q118 | [writtenEdgeSource() / pkg/storage/badger_transaction_images.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/badger_transaction_images.go#L239) ↔ [.codecEdgeImageLocked() / pkg/storage/badger_transaction_images.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/badger_transaction_images.go#L54) | `extracted_owner` at `L55` | One written-edge source owner obtains an exact retained property image or signals codec fallback; it calls setEdgeImage. The codec caller decides whether serialized bytes must be decoded, not another retained-value algorithm. |
| Q119 | [retainedTypedPropertyList() / pkg/storage/property_ownership.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/property_ownership.go#L131) ↔ [ownRetainedValue() / pkg/storage/property_ownership.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/property_ownership.go#L38) | `extracted_owner` at `L95` | S1 — remove canonicalPropertyValue/ownPropertyValue/ownedProperties/ownPropertyMap/codecImageProperties aliases and migrate consumers to ownRetainedValue/ownRetainedMap. Retain actual container/value owners: typed-list cloning preserves concrete shapes; canonicalPropertyList owns decoder/ingress homogeneous/mixed representation. Both are called by the same retained-value dispatcher, with no copied conversion bodies. |
| Q120 | [pullStatement() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L51) ↔ [pullBatch() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L15) | `current_owner_call` at `L53` | One PULL transport/decoder returns records, summary and failure code. Success-only consumers assert empty failure codes; error/lifecycle consumers assert exact failures. This is not a second scoring, cursor or result owner. |
| Q121 | [pullStatement() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L51) ↔ [TestExplicitTransactionStreamsResultsByQID() / pkg/bolt/streaming_qid_compatibility_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/streaming_qid_compatibility_test.go#L13) | `extracted_owner` at `L61` | This typed QID consumer invokes pullBatch with its own n/qid metadata and asserts success; it owns QID-specific inputs, not a second PULL or message decoder. |
| Q122 | [pullStatement() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L51) ↔ [TestSearchContinuationPageSizeBolt() / pkg/bolt/search_page_size_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/search_page_size_test.go#L15) | `extracted_owner` at `L70` | This typed QID consumer invokes pullBatch with its own n/qid metadata and asserts success; it owns QID-specific inputs, not a second PULL or message decoder. |
| Q123 | [schemaOptionsEndsStatement() / pkg/cypher/schema_options.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/schema_options.go#L78) ↔ [queryGapEnd() / pkg/cypher/keyword_scan.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/keyword_scan.go#L1056) | `current_owner_call` at `L81` | Retain direct reuse: queryGapEnd/readQueryGap own comments/trivia. This consumer checks that their returned end consumes the remaining statement; no second comment scanner is retained. |
| Q124 | [.writtenEdgeImage() / pkg/storage/badger_transaction_images.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/badger_transaction_images.go#L112) ↔ [setEdgeImage() / pkg/storage/badger_transaction_images.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/badger_transaction_images.go#L251) | `current_owner_call` at `L114` | One retained image updater installs owned properties and compact envelope timestamps. Source selection/codec decoding callers do not reimplement image installation. |
| Q125 | [codecNodeProperties() / pkg/storage/badger_transaction_images.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/badger_transaction_images.go#L222) ↔ [ownRetainedMap() / pkg/storage/property_ownership.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/property_ownership.go#L120) | `current_owner_call` at `L227` | S1 — remove canonicalPropertyValue/ownPropertyValue/ownedProperties/ownPropertyMap/codecImageProperties aliases and migrate consumers to ownRetainedValue/ownRetainedMap. Retain actual container/value owners: typed-list cloning preserves concrete shapes; canonicalPropertyList owns decoder/ingress homogeneous/mixed representation. Both are called by the same retained-value dispatcher, with no copied conversion bodies. |
| Q126 | [pullBatch() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L15) ↔ [readBatch() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L21) | `current_owner_call` at `L18` | One batch decoder distinguishes RECORD/SUCCESS/FAILURE and retains summary/failure fields. SendPull constructs a request; readBatch consumes responses. Opposite protocol directions remain distinct and share PackStream primitives. |
| Q127 | [pullBatch() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L15) ↔ [SendPull() / pkg/bolt/packstream_testutil_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/packstream_testutil_test.go#L264) | `current_owner_call` at `L17` | One PULL request encoder handles supplied n/qid fields. Consumers decide batch size/cursor behavior; response decoding remains readBatch, not a copied request encoder. |
| Q128 | [pullBatch() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L15) ↔ [TestBoltLazyResultDeliversEveryRowInBatches() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L80) | `extracted_owner` at `L99` | One PULL transport/decoder returns records, summary and failure code. Success-only consumers assert empty failure codes; error/lifecycle consumers assert exact failures. This is not a second scoring, cursor or result owner. |
| Q129 | [pullBatch() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L15) ↔ [TestBoltLazyResultErrorFailsThePullThatReachesIt() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L123) | `extracted_owner` at `L127` | One PULL transport/decoder returns records, summary and failure code. Success-only consumers assert empty failure codes; error/lifecycle consumers assert exact failures. This is not a second scoring, cursor or result owner. |
| Q130 | [pullBatch() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L15) ↔ [TestBoltLazyResultEndsOnDiscardResetAndRun() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L165) | `extracted_owner` at `L183` | One PULL transport/decoder returns records, summary and failure code. Success-only consumers assert empty failure codes; error/lifecycle consumers assert exact failures. This is not a second scoring, cursor or result owner. |
| Q131 | [pullBatch() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/lazy_result_test.go#L15) ↔ [TestBoltLazyResultClientTimeoutStatus() / pkg/bolt/deadline_status_test.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/bolt/deadline_status_test.go#L57) | `extracted_owner` at `L67` | One PULL transport/decoder returns records, summary and failure code. Success-only consumers assert empty failure codes; error/lifecycle consumers assert exact failures. This is not a second scoring, cursor or result owner. |
| Q132 | [ownedValueCopy() / pkg/storage/property_copy.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/property_copy.go#L28) ↔ [ownedEntries() / pkg/storage/property_ownership.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/property_ownership.go#L26) | `extracted_owner` at `L31` | Retain structural ownership composition: ownedValueCopy is the cycle/type-preserving value walker, ownedEntries owns map allocation/entries, and cloneConstraint copies the schema definition then replaces its reference fields using that walker. These are different input layers sharing the same value-copy implementation, not alternative copy algorithms. |
| Q133 | [ownedValueCopy() / pkg/storage/property_copy.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/property_copy.go#L28) ↔ [cloneConstraint() / pkg/storage/schema_ownership.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/storage/schema_ownership.go#L12) | `extracted_owner` at `L17` | Retain structural ownership composition: ownedValueCopy is the cycle/type-preserving value walker, ownedEntries owns map allocation/entries, and cloneConstraint copies the schema definition then replaces its reference fields using that walker. These are different input layers sharing the same value-copy implementation, not alternative copy algorithms. |

### A001 — [.canonicalConstraintRequirement() / pkg/cypher/constraint_requirement.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/cypher/constraint_requirement.go) ↔ [CypherSchemaConstraintEntityMismatch() / pkg/localization/messages_cypher_schema.go](https://github.com/orneryd/NornicDB/blob/2444cb481ac084bc1b00218446453937379b3fcc/pkg/localization/messages_cypher_schema.go)

Retain the typed localized-error owner: canonicalConstraintRequirement validates entity identity and calls CypherSchemaConstraintEntityMismatch to construct its exact error identity. The constructor does not parse the statement. Call evidence: L47.

## Final qualification

All named corrective actions are integrated in the follow-up tree. Parent verification passes on that combined source:

- Affected Bolt race checks: 22.412s, including both database-pinning modes, timeout/quarantine populations, delete/binding/session consumers, all constraint matrices and all three relationship workloads (Badger executed locally).
- Shared aggregation oracle through the direct Cypher public path under race: 13.923s; both query fixtures, both transaction modes and both populations retained.
- Normal blocking lexical population check: 2.151s.
- The small relationship fixture retains the original Test names, not indexed replacement names. The 250-node memory/Badger fixtures keep their different original property shapes; their server buffers are now explicitly aligned, so old and new informational timing logs are not paired performance evidence. No speed claim is made.
- One-line TEMP_REL count forwarding helper removed; callers use requireBoltCount directly. Both migrated DB-manager tests reuse the existing session fixture with connection limit 8; all three existing callers retain default 0. Checked driver/session cleanup remains.
- Barrier cleanup is registered inside the common fixture before its assertions, including the caller's optional blocking rollback release. An abort during setup cannot strand that release behind server shutdown.
- Shared zero-row seed/params/oracle runs through both public boundaries. The two fixture queries have no mutation clause; their nested CALL bodies are read-only.

Full make test remains a CI gate, not an inferred pass from these ownership records. The earlier 2444cb481 test run was still in progress when these test-only corrections were prepared; any superseded run is incomplete, not a final-head pass. The original terminated run and stale index remain documented; no generated code or checker policy was changed. Fresh source review covers the added test owners, without opening an indefinite new analyzer cycle.

## Full make-test follow-up: exact cancellation admission

Run 42070124669 on c7cbdf481 reported `TestCommitPreparationBufferedConflictAdmissionCancellation/plain` failing its exact stack-owner assertion. The unchanged local reproduction fails at that same assertion; the prepared case passes. This is not a memory-kill diagnosis.

The acknowledged-write correction added `validateNodeMutationPreimagesLocked` before the plain commit's buffered conflict scan. Holding the buffer mutex therefore stops an UPDATE at that earlier preimage read, before `commitsSince` can be reached. Prepared commits still first block at `observeBufferedCommitReceipt`.

The corrected single matrix retains all three exact public admissions: prepared receipt observation, plain UPDATE preimage validation, and a plain CREATE (no update preimage) reaching the original conflict scan. Every case uses real cancellation while the mutex remains held, checks cancellation/no handoff/no receipt/unmodified target and peer, and retries successfully. The CREATE case additionally proves its unaccepted node is absent and the same identity succeeds on retry. No production path, timeout bound, assertion owner or checker policy is weakened or changed.

Parent verification: five normal repetitions pass (2.098s); the integrated cancellation/receipt/bulk-poll/acknowledged-write race population passes three repetitions (64.437s). DeepSeek independently reproduced the same old assertion, traced the same three owners, and validated an equivalent matrix with discriminating swapped-owner negative controls. Parent retained the additional same-identity CREATE retry assertion. Full CI results remain separately required for the delivered correction.
