# PR #1688: complete duplicate dispositions

Inventory source: [the advisory comment](https://github.com/orneryd/NornicDB/pull/1688#issuecomment-6655948901). Reviewed source is `38bd71f65bfc191270af1f9774be934d042f83ac`; indexed reference is `35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6`, not the PR base `8f5835d7f`. Every indexed match was compared to current ownership rather than waived for age.

The comment hides 70 refactor targets and 36 reuse candidates. Recovery used the same CI graph artifact (`13424277048`), unchanged analyzer, same source/base/thresholds and `ignore=()`, expanding only report/match presentation caps. It reproduces 400 analyzed functions, 80 refactor targets, 46 reuse candidates, zero cap skips and the same stale-baseline warning. All **126 candidate entries / 518 matched pairs**, **98 ownership proofs**, and **one AST owner call** are listed below. IDs refer to this expanded inventory; file/label/line identifies the corresponding comment entry.

The later delivered-head advisory is accounted for in [the follow-up ledger](pr1688-duplicate-dispositions-followup.md), including its new matches and canonical test-fixture corrections. This original inventory remains preserved.

## Status and evidence

Corrective-action codes are explicit work items, not assertions that pending implementation or CI passed. The integration checklist and final PR comment record which are applied and verified. No analyzer, threshold, exclusion or local hook change is part of this work. Source review is not a full-suite pass.

Generated entries are retained under the grammar/generator owner. The Go file declares ANTLR 4.13.1 generation from CypherParser.g4. Required typed rule entry points, rule-context interfaces and child getters are not retired. Shared mechanics live in the ANTLR runtime/generator; rule indices, expected tokens, alternative decisions and context result types are grammar-specific. A hand-written common wrapper would be overwritten on regeneration and would not eliminate the generator-defined methods. This is a concrete ownership disposition, not a claim that generated output is dissimilar.

## Full candidate inventory

### F001 — [.Expression() in pkg/cypher/antlr/cypher_parser.go:22116](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L22116)

Reported as **refactor**; 16 matching declarations:

- [.OrderSt() in pkg/cypher/antlr/cypher_parser.go:15171](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L15171) — code; similarity 1.0
- [.LetSt() in pkg/cypher/antlr/cypher_parser.go:16407](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16407) — code; similarity 1.0
- [.RemoveSt() in pkg/cypher/antlr/cypher_parser.go:18012](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L18012) — code; similarity 1.0
- [.SetSt() in pkg/cypher/antlr/cypher_parser.go:19840](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19840) — code; similarity 1.0
- [.Pattern() in pkg/cypher/antlr/cypher_parser.go:21816](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 1.0
- [.XorExpression() in pkg/cypher/antlr/cypher_parser.go:22172](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22172) — code; similarity 1.0
- [.AndExpression() in pkg/cypher/antlr/cypher_parser.go:22350](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22350) — code; similarity 1.0
- [.PowerExpression() in pkg/cypher/antlr/cypher_parser.go:23473](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23473) — code; similarity 1.0
- [.ExpressionType() in pkg/cypher/antlr/cypher_parser.go:25526](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L25526) — code; similarity 1.0
- [.PropertyExpression() in pkg/cypher/antlr/cypher_parser.go:26248](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26248) — code; similarity 1.0
- [.InvocationName() in pkg/cypher/antlr/cypher_parser.go:30402](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30402) — code; similarity 1.0
- [.ExpressionChain() in pkg/cypher/antlr/cypher_parser.go:32100](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32100) — code; similarity 1.0
- [.ComparisonExpression() in pkg/cypher/antlr/cypher_parser.go:22771](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22771) — code; similarity 0.961
- [.LabelExpression() in pkg/cypher/antlr/cypher_parser.go:20637](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L20637) — code; similarity 0.881
- [.YieldItems() in pkg/cypher/antlr/cypher_parser.go:19158](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19158) — code; similarity 0.876
- [.AddSubExpression() in pkg/cypher/antlr/cypher_parser.go:23087](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23087) — code; similarity 0.818

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) Expression() (localctx IExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F002 — [.XorExpression() in pkg/cypher/antlr/cypher_parser.go:22294](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L22294)

Reported as **refactor**; 16 matching declarations:

- [.LetSt() in pkg/cypher/antlr/cypher_parser.go:16407](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16407) — code; similarity 1.0
- [.SetSt() in pkg/cypher/antlr/cypher_parser.go:19840](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19840) — code; similarity 1.0
- [.Pattern() in pkg/cypher/antlr/cypher_parser.go:21816](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 1.0
- [.Expression() in pkg/cypher/antlr/cypher_parser.go:21994](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 1.0
- [.AndExpression() in pkg/cypher/antlr/cypher_parser.go:22350](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22350) — code; similarity 1.0
- [.PowerExpression() in pkg/cypher/antlr/cypher_parser.go:23473](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23473) — code; similarity 1.0
- [.ExpressionType() in pkg/cypher/antlr/cypher_parser.go:25526](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L25526) — code; similarity 1.0
- [.PropertyOrLabelExpression() in pkg/cypher/antlr/cypher_parser.go:26053](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26053) — code; similarity 1.0
- [.PropertyExpression() in pkg/cypher/antlr/cypher_parser.go:26248](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26248) — code; similarity 1.0
- [.InvocationName() in pkg/cypher/antlr/cypher_parser.go:30402](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30402) — code; similarity 1.0
- [.ExpressionChain() in pkg/cypher/antlr/cypher_parser.go:32100](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32100) — code; similarity 1.0
- [.ComparisonExpression() in pkg/cypher/antlr/cypher_parser.go:22771](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22771) — code; similarity 0.961
- [.LabelExpression() in pkg/cypher/antlr/cypher_parser.go:20637](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L20637) — code; similarity 0.881
- [.YieldItems() in pkg/cypher/antlr/cypher_parser.go:19158](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19158) — code; similarity 0.876
- [.LabelConjunction() in pkg/cypher/antlr/cypher_parser.go:20845](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L20845) — code; similarity 0.818
- [.AddSubExpression() in pkg/cypher/antlr/cypher_parser.go:23087](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23087) — code; similarity 0.818

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) XorExpression() (localctx IXorExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F003 — [.AndExpression() in pkg/cypher/antlr/cypher_parser.go:22472](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L22472)

Reported as **refactor**; 16 matching declarations:

- [.OrderSt() in pkg/cypher/antlr/cypher_parser.go:15171](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L15171) — code; similarity 1.0
- [.LetSt() in pkg/cypher/antlr/cypher_parser.go:16407](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16407) — code; similarity 1.0
- [.SetSt() in pkg/cypher/antlr/cypher_parser.go:19840](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19840) — code; similarity 1.0
- [.Pattern() in pkg/cypher/antlr/cypher_parser.go:21816](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 1.0
- [.Expression() in pkg/cypher/antlr/cypher_parser.go:21994](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 1.0
- [.XorExpression() in pkg/cypher/antlr/cypher_parser.go:22172](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22172) — code; similarity 1.0
- [.PowerExpression() in pkg/cypher/antlr/cypher_parser.go:23473](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23473) — code; similarity 1.0
- [.ExpressionType() in pkg/cypher/antlr/cypher_parser.go:25526](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L25526) — code; similarity 1.0
- [.PropertyOrLabelExpression() in pkg/cypher/antlr/cypher_parser.go:26053](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26053) — code; similarity 1.0
- [.PropertyExpression() in pkg/cypher/antlr/cypher_parser.go:26248](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26248) — code; similarity 1.0
- [.InvocationName() in pkg/cypher/antlr/cypher_parser.go:30402](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30402) — code; similarity 1.0
- [.ExpressionChain() in pkg/cypher/antlr/cypher_parser.go:32100](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32100) — code; similarity 1.0
- [.ComparisonExpression() in pkg/cypher/antlr/cypher_parser.go:22771](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22771) — code; similarity 0.961
- [.LabelExpression() in pkg/cypher/antlr/cypher_parser.go:20637](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L20637) — code; similarity 0.881
- [.YieldItems() in pkg/cypher/antlr/cypher_parser.go:19158](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19158) — code; similarity 0.876
- [.AddSubExpression() in pkg/cypher/antlr/cypher_parser.go:23087](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23087) — code; similarity 0.818

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) AndExpression() (localctx IAndExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F004 — [.PropertyExpression() in pkg/cypher/antlr/cypher_parser.go:26370](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L26370)

Reported as **refactor**; 16 matching declarations:

- [.OrderSt() in pkg/cypher/antlr/cypher_parser.go:15171](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L15171) — code; similarity 1.0
- [.LetSt() in pkg/cypher/antlr/cypher_parser.go:16407](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16407) — code; similarity 1.0
- [.RemoveSt() in pkg/cypher/antlr/cypher_parser.go:18012](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L18012) — code; similarity 1.0
- [.SetSt() in pkg/cypher/antlr/cypher_parser.go:19840](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19840) — code; similarity 1.0
- [.Pattern() in pkg/cypher/antlr/cypher_parser.go:21816](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 1.0
- [.Expression() in pkg/cypher/antlr/cypher_parser.go:21994](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 1.0
- [.AndExpression() in pkg/cypher/antlr/cypher_parser.go:22350](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22350) — code; similarity 1.0
- [.PowerExpression() in pkg/cypher/antlr/cypher_parser.go:23473](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23473) — code; similarity 1.0
- [.ExpressionType() in pkg/cypher/antlr/cypher_parser.go:25526](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L25526) — code; similarity 1.0
- [.PropertyOrLabelExpression() in pkg/cypher/antlr/cypher_parser.go:26053](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26053) — code; similarity 1.0
- [.InvocationName() in pkg/cypher/antlr/cypher_parser.go:30402](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30402) — code; similarity 1.0
- [.ExpressionChain() in pkg/cypher/antlr/cypher_parser.go:32100](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32100) — code; similarity 1.0
- [.ComparisonExpression() in pkg/cypher/antlr/cypher_parser.go:22771](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22771) — code; similarity 0.961
- [.LabelExpression() in pkg/cypher/antlr/cypher_parser.go:20637](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L20637) — code; similarity 0.881
- [.YieldItems() in pkg/cypher/antlr/cypher_parser.go:19158](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19158) — code; similarity 0.876
- [.AddSubExpression() in pkg/cypher/antlr/cypher_parser.go:23087](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23087) — code; similarity 0.818

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) PropertyExpression() (localctx IPropertyExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F005 — [.MapLit() in pkg/cypher/antlr/cypher_parser.go:8795](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L8795)

Reported as **refactor**; 15 matching declarations:

- [.MapLit() in pkg/cypher/antlr/cypher_parser.go:2845](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L2845) — code; similarity 1.0
- [.ParenExpressionChain() in pkg/cypher/antlr/cypher_parser.go:8700](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8700) — code; similarity 1.0
- [.Symbol() in pkg/cypher/antlr/cypher_parser.go:8716](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8716) — code; similarity 1.0
- [.NodePattern() in pkg/cypher/antlr/cypher_parser.go:8768](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8768) — code; similarity 1.0
- [.RelationshipsChainPattern() in pkg/cypher/antlr/cypher_parser.go:8784](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8784) — code; similarity 1.0
- [.ExpressionChain() in pkg/cypher/antlr/cypher_parser.go:8808](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8808) — code; similarity 1.0
- [.FunctionInvocation() in pkg/cypher/antlr/cypher_parser.go:8840](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8840) — code; similarity 1.0
- [.ConstraintRequirement() in pkg/cypher/antlr/cypher_parser.go:8864](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8864) — code; similarity 1.0
- [.Expression() in pkg/cypher/antlr/cypher_parser.go:8888](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8888) — code; similarity 1.0
- [.PropertyTypeName() in pkg/cypher/antlr/cypher_parser.go:8916](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8916) — code; similarity 1.0
- [.ListLit() in pkg/cypher/antlr/cypher_parser.go:10378](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L10378) — code; similarity 1.0
- [.MapLit() in pkg/cypher/antlr/cypher_parser.go:27927](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L27927) — code; similarity 1.0
- [.MapProjection() in pkg/cypher/antlr/cypher_parser.go:28366](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L28366) — code; similarity 1.0
- [.NumLit() in pkg/cypher/antlr/cypher_parser.go:33244](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L33244) — code; similarity 1.0
- [.MapLit() in pkg/cypher/antlr/cypher_parser.go:33312](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L33312) — code; similarity 1.0

**Disposition — retain generator ownership.** This is the generated context accessor `func (s *SchemaCommandContext) MapLit() IMapLitContext`. The receiver and asserted child-context type are part of the generated tree API; retain it rather than hand-patching generated getters.

### F006 — [.Parameter() in pkg/cypher/antlr/cypher_parser.go:8811](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L8811)

Reported as **refactor**; 15 matching declarations:

- [.Parameter() in pkg/cypher/antlr/cypher_parser.go:7234](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L7234) — code; similarity 1.0
- [.ParenExpressionChain() in pkg/cypher/antlr/cypher_parser.go:8700](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8700) — code; similarity 1.0
- [.Symbol() in pkg/cypher/antlr/cypher_parser.go:8716](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8716) — code; similarity 1.0
- [.MapLit() in pkg/cypher/antlr/cypher_parser.go:8740](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8740) — code; similarity 1.0
- [.NodePattern() in pkg/cypher/antlr/cypher_parser.go:8768](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8768) — code; similarity 1.0
- [.RelationshipsChainPattern() in pkg/cypher/antlr/cypher_parser.go:8784](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8784) — code; similarity 1.0
- [.ExpressionChain() in pkg/cypher/antlr/cypher_parser.go:8808](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8808) — code; similarity 1.0
- [.FunctionInvocation() in pkg/cypher/antlr/cypher_parser.go:8840](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8840) — code; similarity 1.0
- [.ConstraintRequirement() in pkg/cypher/antlr/cypher_parser.go:8864](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8864) — code; similarity 1.0
- [.Expression() in pkg/cypher/antlr/cypher_parser.go:8888](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8888) — code; similarity 1.0
- [.PropertyTypeName() in pkg/cypher/antlr/cypher_parser.go:8916](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8916) — code; similarity 1.0
- [.Parameter() in pkg/cypher/antlr/cypher_parser.go:27943](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L27943) — code; similarity 1.0
- [.Parameter() in pkg/cypher/antlr/cypher_parser.go:28350](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L28350) — code; similarity 1.0
- [.Parameter() in pkg/cypher/antlr/cypher_parser.go:39041](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L39041) — code; similarity 1.0
- [.Parameter() in pkg/cypher/antlr/cypher_parser.go:39239](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L39239) — code; similarity 1.0

**Disposition — retain generator ownership.** This is the generated context accessor `func (s *SchemaCommandContext) Parameter() IParameterContext`. The receiver and asserted child-context type are part of the generated tree API; retain it rather than hand-patching generated getters.

### F007 — [.RemoveSt() in pkg/cypher/antlr/cypher_parser.go:18134](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L18134)

Reported as **refactor**; 15 matching declarations:

- [.OrderSt() in pkg/cypher/antlr/cypher_parser.go:15171](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L15171) — code; similarity 1.0
- [.LetSt() in pkg/cypher/antlr/cypher_parser.go:16407](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16407) — code; similarity 1.0
- [.SetSt() in pkg/cypher/antlr/cypher_parser.go:19840](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19840) — code; similarity 1.0
- [.Pattern() in pkg/cypher/antlr/cypher_parser.go:21816](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 1.0
- [.Expression() in pkg/cypher/antlr/cypher_parser.go:21994](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 1.0
- [.XorExpression() in pkg/cypher/antlr/cypher_parser.go:22172](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22172) — code; similarity 1.0
- [.AndExpression() in pkg/cypher/antlr/cypher_parser.go:22350](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22350) — code; similarity 1.0
- [.PowerExpression() in pkg/cypher/antlr/cypher_parser.go:23473](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23473) — code; similarity 1.0
- [.ExpressionType() in pkg/cypher/antlr/cypher_parser.go:25526](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L25526) — code; similarity 1.0
- [.PropertyOrLabelExpression() in pkg/cypher/antlr/cypher_parser.go:26053](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26053) — code; similarity 1.0
- [.PropertyExpression() in pkg/cypher/antlr/cypher_parser.go:26248](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26248) — code; similarity 1.0
- [.InvocationName() in pkg/cypher/antlr/cypher_parser.go:30402](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30402) — code; similarity 1.0
- [.ExpressionChain() in pkg/cypher/antlr/cypher_parser.go:32100](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32100) — code; similarity 1.0
- [.ComparisonExpression() in pkg/cypher/antlr/cypher_parser.go:22771](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22771) — code; similarity 0.961
- [.YieldItems() in pkg/cypher/antlr/cypher_parser.go:19158](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19158) — code; similarity 0.876

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) RemoveSt() (localctx IRemoveStContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F008 — [.ExpressionType() in pkg/cypher/antlr/cypher_parser.go:25648](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L25648)

Reported as **refactor**; 15 matching declarations:

- [.LetSt() in pkg/cypher/antlr/cypher_parser.go:16407](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16407) — code; similarity 1.0
- [.RemoveSt() in pkg/cypher/antlr/cypher_parser.go:18012](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L18012) — code; similarity 1.0
- [.MergeSt() in pkg/cypher/antlr/cypher_parser.go:19516](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19516) — code; similarity 1.0
- [.SetSt() in pkg/cypher/antlr/cypher_parser.go:19840](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19840) — code; similarity 1.0
- [.Pattern() in pkg/cypher/antlr/cypher_parser.go:21816](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 1.0
- [.Expression() in pkg/cypher/antlr/cypher_parser.go:21994](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 1.0
- [.AndExpression() in pkg/cypher/antlr/cypher_parser.go:22350](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22350) — code; similarity 1.0
- [.PowerExpression() in pkg/cypher/antlr/cypher_parser.go:23473](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23473) — code; similarity 1.0
- [.PropertyOrLabelExpression() in pkg/cypher/antlr/cypher_parser.go:26053](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26053) — code; similarity 1.0
- [.PropertyExpression() in pkg/cypher/antlr/cypher_parser.go:26248](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26248) — code; similarity 1.0
- [.InvocationName() in pkg/cypher/antlr/cypher_parser.go:30402](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30402) — code; similarity 1.0
- [.ExpressionChain() in pkg/cypher/antlr/cypher_parser.go:32100](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32100) — code; similarity 1.0
- [.LabelExpression() in pkg/cypher/antlr/cypher_parser.go:20637](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L20637) — code; similarity 0.881
- [.YieldItems() in pkg/cypher/antlr/cypher_parser.go:19158](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19158) — code; similarity 0.876
- [.AddSubExpression() in pkg/cypher/antlr/cypher_parser.go:23087](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23087) — code; similarity 0.818

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) ExpressionType() (localctx IExpressionTypeContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F009 — [.ExpressionChain() in pkg/cypher/antlr/cypher_parser.go:32222](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L32222)

Reported as **refactor**; 15 matching declarations:

- [.OrderSt() in pkg/cypher/antlr/cypher_parser.go:15171](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L15171) — code; similarity 1.0
- [.LetSt() in pkg/cypher/antlr/cypher_parser.go:16407](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16407) — code; similarity 1.0
- [.RemoveSt() in pkg/cypher/antlr/cypher_parser.go:18012](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L18012) — code; similarity 1.0
- [.SetSt() in pkg/cypher/antlr/cypher_parser.go:19840](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19840) — code; similarity 1.0
- [.Pattern() in pkg/cypher/antlr/cypher_parser.go:21816](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 1.0
- [.Expression() in pkg/cypher/antlr/cypher_parser.go:21994](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 1.0
- [.AndExpression() in pkg/cypher/antlr/cypher_parser.go:22350](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22350) — code; similarity 1.0
- [.PowerExpression() in pkg/cypher/antlr/cypher_parser.go:23473](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23473) — code; similarity 1.0
- [.ExpressionType() in pkg/cypher/antlr/cypher_parser.go:25526](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L25526) — code; similarity 1.0
- [.PropertyExpression() in pkg/cypher/antlr/cypher_parser.go:26248](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26248) — code; similarity 1.0
- [.InvocationName() in pkg/cypher/antlr/cypher_parser.go:30402](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30402) — code; similarity 1.0
- [.ComparisonExpression() in pkg/cypher/antlr/cypher_parser.go:22771](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22771) — code; similarity 0.961
- [.LabelExpression() in pkg/cypher/antlr/cypher_parser.go:20637](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L20637) — code; similarity 0.881
- [.YieldItems() in pkg/cypher/antlr/cypher_parser.go:19158](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19158) — code; similarity 0.876
- [.AddSubExpression() in pkg/cypher/antlr/cypher_parser.go:23087](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23087) — code; similarity 0.818

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) ExpressionChain() (localctx IExpressionChainContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F010 — [.ComparisonExpression() in pkg/cypher/antlr/cypher_parser.go:22893](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L22893)

Reported as **refactor**; 15 matching declarations:

- [.LetSt() in pkg/cypher/antlr/cypher_parser.go:16407](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16407) — code; similarity 0.961
- [.MergeSt() in pkg/cypher/antlr/cypher_parser.go:19516](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19516) — code; similarity 0.961
- [.SetSt() in pkg/cypher/antlr/cypher_parser.go:19840](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19840) — code; similarity 0.961
- [.Pattern() in pkg/cypher/antlr/cypher_parser.go:21816](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 0.961
- [.Expression() in pkg/cypher/antlr/cypher_parser.go:21994](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 0.961
- [.XorExpression() in pkg/cypher/antlr/cypher_parser.go:22172](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22172) — code; similarity 0.961
- [.AndExpression() in pkg/cypher/antlr/cypher_parser.go:22350](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22350) — code; similarity 0.961
- [.PowerExpression() in pkg/cypher/antlr/cypher_parser.go:23473](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23473) — code; similarity 0.961
- [.ExpressionType() in pkg/cypher/antlr/cypher_parser.go:25526](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L25526) — code; similarity 0.961
- [.PropertyExpression() in pkg/cypher/antlr/cypher_parser.go:26248](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26248) — code; similarity 0.961
- [.InvocationName() in pkg/cypher/antlr/cypher_parser.go:30402](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30402) — code; similarity 0.961
- [.ExpressionChain() in pkg/cypher/antlr/cypher_parser.go:32100](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32100) — code; similarity 0.961
- [.LabelExpression() in pkg/cypher/antlr/cypher_parser.go:20637](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L20637) — code; similarity 0.87
- [.YieldItems() in pkg/cypher/antlr/cypher_parser.go:19158](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19158) — code; similarity 0.844
- [.AddSubExpression() in pkg/cypher/antlr/cypher_parser.go:23087](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23087) — code; similarity 0.808

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) ComparisonExpression() (localctx IComparisonExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F011 — [.MergeSt() in pkg/cypher/antlr/cypher_parser.go:19638](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L19638)

Reported as **refactor**; 14 matching declarations:

- [.LetSt() in pkg/cypher/antlr/cypher_parser.go:16407](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16407) — code; similarity 1.0
- [.RemoveSt() in pkg/cypher/antlr/cypher_parser.go:18012](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L18012) — code; similarity 1.0
- [.SetSt() in pkg/cypher/antlr/cypher_parser.go:19840](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19840) — code; similarity 1.0
- [.Pattern() in pkg/cypher/antlr/cypher_parser.go:21816](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 1.0
- [.Expression() in pkg/cypher/antlr/cypher_parser.go:21994](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 1.0
- [.XorExpression() in pkg/cypher/antlr/cypher_parser.go:22172](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22172) — code; similarity 1.0
- [.AndExpression() in pkg/cypher/antlr/cypher_parser.go:22350](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22350) — code; similarity 1.0
- [.PowerExpression() in pkg/cypher/antlr/cypher_parser.go:23473](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23473) — code; similarity 1.0
- [.ExpressionType() in pkg/cypher/antlr/cypher_parser.go:25526](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L25526) — code; similarity 1.0
- [.PropertyExpression() in pkg/cypher/antlr/cypher_parser.go:26248](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26248) — code; similarity 1.0
- [.InvocationName() in pkg/cypher/antlr/cypher_parser.go:30402](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30402) — code; similarity 1.0
- [.ExpressionChain() in pkg/cypher/antlr/cypher_parser.go:32100](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32100) — code; similarity 1.0
- [.ComparisonExpression() in pkg/cypher/antlr/cypher_parser.go:22771](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22771) — code; similarity 0.961
- [.YieldItems() in pkg/cypher/antlr/cypher_parser.go:19158](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19158) — code; similarity 0.876

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) MergeSt() (localctx IMergeStContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F012 — [.OrderSt() in pkg/cypher/antlr/cypher_parser.go:15293](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L15293)

Reported as **refactor**; 12 matching declarations:

- [.LetSt() in pkg/cypher/antlr/cypher_parser.go:16407](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16407) — code; similarity 1.0
- [.RemoveSt() in pkg/cypher/antlr/cypher_parser.go:18012](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L18012) — code; similarity 1.0
- [.SetSt() in pkg/cypher/antlr/cypher_parser.go:19840](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19840) — code; similarity 1.0
- [.Pattern() in pkg/cypher/antlr/cypher_parser.go:21816](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 1.0
- [.Expression() in pkg/cypher/antlr/cypher_parser.go:21994](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 1.0
- [.PowerExpression() in pkg/cypher/antlr/cypher_parser.go:23473](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23473) — code; similarity 1.0
- [.ExpressionType() in pkg/cypher/antlr/cypher_parser.go:25526](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L25526) — code; similarity 1.0
- [.PropertyExpression() in pkg/cypher/antlr/cypher_parser.go:26248](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26248) — code; similarity 1.0
- [.InvocationName() in pkg/cypher/antlr/cypher_parser.go:30402](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30402) — code; similarity 1.0
- [.ExpressionChain() in pkg/cypher/antlr/cypher_parser.go:32100](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32100) — code; similarity 1.0
- [.LabelExpression() in pkg/cypher/antlr/cypher_parser.go:20637](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L20637) — code; similarity 0.881
- [.YieldItems() in pkg/cypher/antlr/cypher_parser.go:19158](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19158) — code; similarity 0.876

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) OrderSt() (localctx IOrderStContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F013 — [.PropertyOrLabelExpression() in pkg/cypher/antlr/cypher_parser.go:26175](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L26175)

Reported as **refactor**; 12 matching declarations:

- [.LetSt() in pkg/cypher/antlr/cypher_parser.go:16407](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16407) — code; similarity 1.0
- [.SetSt() in pkg/cypher/antlr/cypher_parser.go:19840](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19840) — code; similarity 1.0
- [.Pattern() in pkg/cypher/antlr/cypher_parser.go:21816](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 1.0
- [.Expression() in pkg/cypher/antlr/cypher_parser.go:21994](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 1.0
- [.AndExpression() in pkg/cypher/antlr/cypher_parser.go:22350](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22350) — code; similarity 1.0
- [.PowerExpression() in pkg/cypher/antlr/cypher_parser.go:23473](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23473) — code; similarity 1.0
- [.ExpressionType() in pkg/cypher/antlr/cypher_parser.go:25526](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L25526) — code; similarity 1.0
- [.PropertyExpression() in pkg/cypher/antlr/cypher_parser.go:26248](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26248) — code; similarity 1.0
- [.InvocationName() in pkg/cypher/antlr/cypher_parser.go:30402](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30402) — code; similarity 1.0
- [.ExpressionChain() in pkg/cypher/antlr/cypher_parser.go:32100](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32100) — code; similarity 1.0
- [.LabelExpression() in pkg/cypher/antlr/cypher_parser.go:20637](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L20637) — code; similarity 0.881
- [.YieldItems() in pkg/cypher/antlr/cypher_parser.go:19158](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19158) — code; similarity 0.876

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) PropertyOrLabelExpression() (localctx IPropertyOrLabelExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F014 — [.Pattern() in pkg/cypher/antlr/cypher_parser.go:21938](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L21938)

Reported as **refactor**; 11 matching declarations:

- [.LetSt() in pkg/cypher/antlr/cypher_parser.go:16407](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16407) — code; similarity 1.0
- [.MergeSt() in pkg/cypher/antlr/cypher_parser.go:19516](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19516) — code; similarity 1.0
- [.SetSt() in pkg/cypher/antlr/cypher_parser.go:19840](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19840) — code; similarity 1.0
- [.Expression() in pkg/cypher/antlr/cypher_parser.go:21994](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 1.0
- [.XorExpression() in pkg/cypher/antlr/cypher_parser.go:22172](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22172) — code; similarity 1.0
- [.AndExpression() in pkg/cypher/antlr/cypher_parser.go:22350](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22350) — code; similarity 1.0
- [.PowerExpression() in pkg/cypher/antlr/cypher_parser.go:23473](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23473) — code; similarity 1.0
- [.ExpressionType() in pkg/cypher/antlr/cypher_parser.go:25526](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L25526) — code; similarity 1.0
- [.PropertyExpression() in pkg/cypher/antlr/cypher_parser.go:26248](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26248) — code; similarity 1.0
- [.InvocationName() in pkg/cypher/antlr/cypher_parser.go:30402](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30402) — code; similarity 1.0
- [.ExpressionChain() in pkg/cypher/antlr/cypher_parser.go:32100](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32100) — code; similarity 1.0

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) Pattern() (localctx IPatternContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F015 — [.StringExpression() in pkg/cypher/antlr/cypher_parser.go:24972](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L24972)

Reported as **refactor**; 11 matching declarations:

- [.PropertyTypeName() in pkg/cypher/antlr/cypher_parser.go:11134](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L11134) — code; similarity 0.957
- [.LetItem() in pkg/cypher/antlr/cypher_parser.go:16579](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16579) — code; similarity 0.904
- [.MapPair() in pkg/cypher/antlr/cypher_parser.go:34682](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L34682) — code; similarity 0.904
- [.ReturnSt() in pkg/cypher/antlr/cypher_parser.go:13801](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L13801) — code; similarity 0.875
- [.LimitSt() in pkg/cypher/antlr/cypher_parser.go:14300](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L14300) — code; similarity 0.875
- [.CreateSt() in pkg/cypher/antlr/cypher_parser.go:21415](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21415) — code; similarity 0.875
- [.Where() in pkg/cypher/antlr/cypher_parser.go:21667](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21667) — code; similarity 0.875
- [.LabelPredicate() in pkg/cypher/antlr/cypher_parser.go:24366](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L24366) — code; similarity 0.875
- [.Lhs() in pkg/cypher/antlr/cypher_parser.go:29295](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L29295) — code; similarity 0.875
- [.DynamicPropertyExpression() in pkg/cypher/antlr/cypher_parser.go:20278](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L20278) — code; similarity 0.85
- [.StringLit() in pkg/cypher/antlr/cypher_parser.go:34104](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L34104) — code; similarity 0.827

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) StringExpression() (localctx IStringExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F016 — [.LetSt() in pkg/cypher/antlr/cypher_parser.go:16529](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L16529)

Reported as **refactor**; 10 matching declarations:

- [.RemoveSt() in pkg/cypher/antlr/cypher_parser.go:18012](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L18012) — code; similarity 1.0
- [.MergeSt() in pkg/cypher/antlr/cypher_parser.go:19516](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19516) — code; similarity 1.0
- [.SetSt() in pkg/cypher/antlr/cypher_parser.go:19840](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19840) — code; similarity 1.0
- [.Pattern() in pkg/cypher/antlr/cypher_parser.go:21816](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 1.0
- [.Expression() in pkg/cypher/antlr/cypher_parser.go:21994](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 1.0
- [.AndExpression() in pkg/cypher/antlr/cypher_parser.go:22350](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22350) — code; similarity 1.0
- [.PowerExpression() in pkg/cypher/antlr/cypher_parser.go:23473](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23473) — code; similarity 1.0
- [.ExpressionType() in pkg/cypher/antlr/cypher_parser.go:25526](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L25526) — code; similarity 1.0
- [.ExpressionChain() in pkg/cypher/antlr/cypher_parser.go:32100](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32100) — code; similarity 1.0
- [.YieldItems() in pkg/cypher/antlr/cypher_parser.go:19158](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19158) — code; similarity 0.876

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) LetSt() (localctx ILetStContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F017 — [.PowerExpression() in pkg/cypher/antlr/cypher_parser.go:23595](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L23595)

Reported as **refactor**; 10 matching declarations:

- [.LetSt() in pkg/cypher/antlr/cypher_parser.go:16407](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16407) — code; similarity 1.0
- [.Pattern() in pkg/cypher/antlr/cypher_parser.go:21816](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 1.0
- [.Expression() in pkg/cypher/antlr/cypher_parser.go:21994](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 1.0
- [.XorExpression() in pkg/cypher/antlr/cypher_parser.go:22172](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22172) — code; similarity 1.0
- [.AndExpression() in pkg/cypher/antlr/cypher_parser.go:22350](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22350) — code; similarity 1.0
- [.ExpressionType() in pkg/cypher/antlr/cypher_parser.go:25526](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L25526) — code; similarity 1.0
- [.PropertyExpression() in pkg/cypher/antlr/cypher_parser.go:26248](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26248) — code; similarity 1.0
- [.ExpressionChain() in pkg/cypher/antlr/cypher_parser.go:32100](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32100) — code; similarity 1.0
- [.YieldItems() in pkg/cypher/antlr/cypher_parser.go:19158](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19158) — code; similarity 0.876
- [.AddSubExpression() in pkg/cypher/antlr/cypher_parser.go:23087](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23087) — code; similarity 0.818

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) PowerExpression() (localctx IPowerExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F018 — [.InvocationName() in pkg/cypher/antlr/cypher_parser.go:30524](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L30524)

Reported as **refactor**; 10 matching declarations:

- [.LetSt() in pkg/cypher/antlr/cypher_parser.go:16407](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16407) — code; similarity 1.0
- [.SetSt() in pkg/cypher/antlr/cypher_parser.go:19840](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19840) — code; similarity 1.0
- [.Pattern() in pkg/cypher/antlr/cypher_parser.go:21816](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 1.0
- [.Expression() in pkg/cypher/antlr/cypher_parser.go:21994](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 1.0
- [.AndExpression() in pkg/cypher/antlr/cypher_parser.go:22350](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22350) — code; similarity 1.0
- [.PowerExpression() in pkg/cypher/antlr/cypher_parser.go:23473](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23473) — code; similarity 1.0
- [.ExpressionType() in pkg/cypher/antlr/cypher_parser.go:25526](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L25526) — code; similarity 1.0
- [.PropertyExpression() in pkg/cypher/antlr/cypher_parser.go:26248](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26248) — code; similarity 1.0
- [.ExpressionChain() in pkg/cypher/antlr/cypher_parser.go:32100](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32100) — code; similarity 1.0
- [.YieldItems() in pkg/cypher/antlr/cypher_parser.go:19158](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19158) — code; similarity 0.876

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) InvocationName() (localctx IInvocationNameContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F019 — [.SetSt() in pkg/cypher/antlr/cypher_parser.go:19962](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L19962)

Reported as **refactor**; 9 matching declarations:

- [.LetSt() in pkg/cypher/antlr/cypher_parser.go:16407](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16407) — code; similarity 1.0
- [.RemoveSt() in pkg/cypher/antlr/cypher_parser.go:18012](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L18012) — code; similarity 1.0
- [.Pattern() in pkg/cypher/antlr/cypher_parser.go:21816](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 1.0
- [.Expression() in pkg/cypher/antlr/cypher_parser.go:21994](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 1.0
- [.PowerExpression() in pkg/cypher/antlr/cypher_parser.go:23473](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23473) — code; similarity 1.0
- [.ExpressionType() in pkg/cypher/antlr/cypher_parser.go:25526](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L25526) — code; similarity 1.0
- [.InvocationName() in pkg/cypher/antlr/cypher_parser.go:30402](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30402) — code; similarity 1.0
- [.ExpressionChain() in pkg/cypher/antlr/cypher_parser.go:32100](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32100) — code; similarity 1.0
- [.YieldItems() in pkg/cypher/antlr/cypher_parser.go:19158](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19158) — code; similarity 0.876

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) SetSt() (localctx ISetStContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F020 — [.PropertyTypeName() in pkg/cypher/antlr/cypher_parser.go:11256](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L11256)

Reported as **refactor**; 9 matching declarations:

- [.ReturnSt() in pkg/cypher/antlr/cypher_parser.go:13801](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L13801) — code; similarity 0.891
- [.SkipSt() in pkg/cypher/antlr/cypher_parser.go:14182](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L14182) — code; similarity 0.891
- [.LimitSt() in pkg/cypher/antlr/cypher_parser.go:14300](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L14300) — code; similarity 0.891
- [.CreateSt() in pkg/cypher/antlr/cypher_parser.go:21415](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21415) — code; similarity 0.891
- [.Where() in pkg/cypher/antlr/cypher_parser.go:21667](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21667) — code; similarity 0.891
- [.LabelPredicate() in pkg/cypher/antlr/cypher_parser.go:24366](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L24366) — code; similarity 0.891
- [.StringLit() in pkg/cypher/antlr/cypher_parser.go:34104](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L34104) — code; similarity 0.86
- [.CharLit() in pkg/cypher/antlr/cypher_parser.go:34201](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L34201) — code; similarity 0.86
- [.Lhs() in pkg/cypher/antlr/cypher_parser.go:29295](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L29295) — code; similarity 0.837

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) PropertyTypeName() (localctx IPropertyTypeNameContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F021 — [.Lhs() in pkg/cypher/antlr/cypher_parser.go:29417](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L29417)

Reported as **refactor**; 8 matching declarations:

- [.ParenthesizedExpression() in pkg/cypher/antlr/cypher_parser.go:30889](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30889) — code; similarity 0.971
- [.StringLit() in pkg/cypher/antlr/cypher_parser.go:34104](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L34104) — code; similarity 0.95
- [.CharLit() in pkg/cypher/antlr/cypher_parser.go:34201](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L34201) — code; similarity 0.95
- [.LetItem() in pkg/cypher/antlr/cypher_parser.go:16579](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16579) — code; similarity 0.916
- [.ReturnSt() in pkg/cypher/antlr/cypher_parser.go:13801](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L13801) — code; similarity 0.888
- [.LabelPredicate() in pkg/cypher/antlr/cypher_parser.go:24366](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L24366) — code; similarity 0.888
- [.PropertyTypeName() in pkg/cypher/antlr/cypher_parser.go:11134](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L11134) — code; similarity 0.837
- [.ShowCommand() in pkg/cypher/antlr/cypher_parser.go:4238](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L4238) — behavior; similarity 0.32

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) Lhs() (localctx ILhsContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F022 — [.PatternWhere() in pkg/cypher/antlr/cypher_parser.go:21667](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L21667)

Reported as **refactor**; 8 matching declarations:

- [.WithSt() in pkg/cypher/antlr/cypher_parser.go:13936](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L13936) — code; similarity 0.947
- [.ProjectionItem() in pkg/cypher/antlr/cypher_parser.go:14854](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L14854) — code; similarity 0.947
- [.UnwindSt() in pkg/cypher/antlr/cypher_parser.go:16229](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16229) — code; similarity 0.926
- [.ProjectionBody() in pkg/cypher/antlr/cypher_parser.go:14469](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L14469) — code; similarity 0.871
- [.FilterSt() in pkg/cypher/antlr/cypher_parser.go:16706](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16706) — code; similarity 0.843
- [.UseClause() in pkg/cypher/antlr/cypher_parser.go:3715](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L3715) — code; similarity 0.812
- [.PatternElemStart() in pkg/cypher/antlr/cypher_parser.go:27069](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L27069) — code; similarity 0.81
- [.YieldItem() in pkg/cypher/antlr/cypher_parser.go:19343](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19343) — code; similarity 0.803

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) PatternWhere() (localctx IPatternWhereContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F023 — [.AddSubExpression() in pkg/cypher/antlr/cypher_parser.go:23209](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L23209)

Reported as **refactor**; 8 matching declarations:

- [.Pattern() in pkg/cypher/antlr/cypher_parser.go:21816](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 0.818
- [.Expression() in pkg/cypher/antlr/cypher_parser.go:21994](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 0.818
- [.AndExpression() in pkg/cypher/antlr/cypher_parser.go:22350](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22350) — code; similarity 0.818
- [.PowerExpression() in pkg/cypher/antlr/cypher_parser.go:23473](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23473) — code; similarity 0.818
- [.ExpressionType() in pkg/cypher/antlr/cypher_parser.go:25526](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L25526) — code; similarity 0.818
- [.PropertyExpression() in pkg/cypher/antlr/cypher_parser.go:26248](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26248) — code; similarity 0.818
- [.ExpressionChain() in pkg/cypher/antlr/cypher_parser.go:32100](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32100) — code; similarity 0.818
- [.LabelExpression() in pkg/cypher/antlr/cypher_parser.go:20637](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L20637) — code; similarity 0.815

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) AddSubExpression() (localctx IAddSubExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F024 — [.Parameter() in pkg/cypher/antlr/cypher_parser.go:8572](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L8572)

Reported as **refactor**; 7 matching declarations:

- [.Parameter() in pkg/cypher/antlr/cypher_parser.go:7156](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L7156) — code; similarity 1.0
- [.Parameter() in pkg/cypher/antlr/cypher_parser.go:27889](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L27889) — code; similarity 1.0
- [.Parameter() in pkg/cypher/antlr/cypher_parser.go:28266](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L28266) — code; similarity 1.0
- [.Name() in pkg/cypher/antlr/cypher_parser.go:33003](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L33003) — code; similarity 1.0
- [.NumLit() in pkg/cypher/antlr/cypher_parser.go:33004](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L33004) — code; similarity 1.0
- [.Parameter() in pkg/cypher/antlr/cypher_parser.go:38963](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L38963) — code; similarity 1.0
- [.Parameter() in pkg/cypher/antlr/cypher_parser.go:39161](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L39161) — code; similarity 1.0

**Disposition — retain generator ownership.** This occurrence is the generated interface member `Parameter() IParameterContext`, a required typed tree declaration with no executable implementation body. Its generated context implementation remains the owner; do not replace the interface contract with a hand-written forwarding layer.

### F025 — [.SkipSt() in pkg/cypher/antlr/cypher_parser.go:14304](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L14304)

Reported as **refactor**; 7 matching declarations:

- [.ReturnSt() in pkg/cypher/antlr/cypher_parser.go:13801](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L13801) — code; similarity 1.0
- [.LimitSt() in pkg/cypher/antlr/cypher_parser.go:14300](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L14300) — code; similarity 1.0
- [.CreateSt() in pkg/cypher/antlr/cypher_parser.go:21415](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21415) — code; similarity 1.0
- [.Where() in pkg/cypher/antlr/cypher_parser.go:21667](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21667) — code; similarity 1.0
- [.ForSt() in pkg/cypher/antlr/cypher_parser.go:16862](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16862) — code; similarity 0.971
- [.PropertyTypeName() in pkg/cypher/antlr/cypher_parser.go:11134](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L11134) — code; similarity 0.891
- [.StringExpression() in pkg/cypher/antlr/cypher_parser.go:24850](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L24850) — code; similarity 0.875

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) SkipSt() (localctx ISkipStContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F026 — [.LabelExpression() in pkg/cypher/antlr/cypher_parser.go:20759](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L20759)

Reported as **refactor**; 7 matching declarations:

- [.Pattern() in pkg/cypher/antlr/cypher_parser.go:21816](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 0.881
- [.Expression() in pkg/cypher/antlr/cypher_parser.go:21994](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 0.881
- [.ExpressionType() in pkg/cypher/antlr/cypher_parser.go:25526](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L25526) — code; similarity 0.881
- [.PropertyOrLabelExpression() in pkg/cypher/antlr/cypher_parser.go:26053](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26053) — code; similarity 0.881
- [.ExpressionChain() in pkg/cypher/antlr/cypher_parser.go:32100](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32100) — code; similarity 0.881
- [.LabelConjunction() in pkg/cypher/antlr/cypher_parser.go:20845](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L20845) — code; similarity 0.815
- [.PatternElem() in pkg/cypher/antlr/cypher_parser.go:26867](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26867) — code; similarity 0.806

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) LabelExpression() (localctx ILabelExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F027 — [.LimitSt() in pkg/cypher/antlr/cypher_parser.go:14422](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L14422)

Reported as **refactor**; 6 matching declarations:

- [.ReturnSt() in pkg/cypher/antlr/cypher_parser.go:13801](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L13801) — code; similarity 1.0
- [.SkipSt() in pkg/cypher/antlr/cypher_parser.go:14182](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L14182) — code; similarity 1.0
- [.CreateSt() in pkg/cypher/antlr/cypher_parser.go:21415](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21415) — code; similarity 1.0
- [.ForSt() in pkg/cypher/antlr/cypher_parser.go:16862](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16862) — code; similarity 0.971
- [.PropertyTypeName() in pkg/cypher/antlr/cypher_parser.go:11134](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L11134) — code; similarity 0.891
- [.StringExpression() in pkg/cypher/antlr/cypher_parser.go:24850](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L24850) — code; similarity 0.875

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) LimitSt() (localctx ILimitStContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F028 — [.CharLit() in pkg/cypher/antlr/cypher_parser.go:34323](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L34323)

Reported as **refactor**; 6 matching declarations:

- [.StringLit() in pkg/cypher/antlr/cypher_parser.go:34104](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L34104) — code; similarity 1.0
- [.Lhs() in pkg/cypher/antlr/cypher_parser.go:29295](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L29295) — code; similarity 0.95
- [.ParenthesizedExpression() in pkg/cypher/antlr/cypher_parser.go:30889](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30889) — code; similarity 0.923
- [.LetItem() in pkg/cypher/antlr/cypher_parser.go:16579](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16579) — code; similarity 0.869
- [.StringExpression() in pkg/cypher/antlr/cypher_parser.go:24850](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L24850) — code; similarity 0.827
- [.ShowCommand() in pkg/cypher/antlr/cypher_parser.go:4238](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L4238) — behavior; similarity 0.304

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) CharLit() (localctx ICharLitContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F029 — [.DynamicPropertyExpression() in pkg/cypher/antlr/cypher_parser.go:20400](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L20400)

Reported as **refactor**; 5 matching declarations:

- [.ParenthesizedExpression() in pkg/cypher/antlr/cypher_parser.go:30889](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30889) — code; similarity 1.0
- [.ReduceExpression() in pkg/cypher/antlr/cypher_parser.go:32600](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32600) — code; similarity 1.0
- [.AllReduceExpression() in pkg/cypher/antlr/cypher_parser.go:32886](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32886) — code; similarity 1.0
- [.StringExpression() in pkg/cypher/antlr/cypher_parser.go:24850](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L24850) — code; similarity 0.85
- [.PropertyExpression() in pkg/cypher/antlr/cypher_parser.go:26248](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26248) — behavior; similarity 0.645

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) DynamicPropertyExpression() (localctx IDynamicPropertyExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F030 — [.LabelConjunction() in pkg/cypher/antlr/cypher_parser.go:20967](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L20967)

Reported as **refactor**; 5 matching declarations:

- [.AddSubExpression() in pkg/cypher/antlr/cypher_parser.go:23087](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23087) — code; similarity 1.0
- [.Pattern() in pkg/cypher/antlr/cypher_parser.go:21816](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 0.818
- [.Expression() in pkg/cypher/antlr/cypher_parser.go:21994](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 0.818
- [.PropertyOrLabelExpression() in pkg/cypher/antlr/cypher_parser.go:26053](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26053) — code; similarity 0.818
- [.LabelExpression() in pkg/cypher/antlr/cypher_parser.go:20637](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L20637) — code; similarity 0.815

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) LabelConjunction() (localctx ILabelConjunctionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F031 — [.CreateSt() in pkg/cypher/antlr/cypher_parser.go:21537](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L21537)

Reported as **refactor**; 5 matching declarations:

- [.ReturnSt() in pkg/cypher/antlr/cypher_parser.go:13801](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L13801) — code; similarity 1.0
- [.SkipSt() in pkg/cypher/antlr/cypher_parser.go:14182](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L14182) — code; similarity 1.0
- [.ForSt() in pkg/cypher/antlr/cypher_parser.go:16862](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16862) — code; similarity 0.971
- [.PropertyTypeName() in pkg/cypher/antlr/cypher_parser.go:11134](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L11134) — code; similarity 0.891
- [.StringExpression() in pkg/cypher/antlr/cypher_parser.go:24850](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L24850) — code; similarity 0.875

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) CreateSt() (localctx ICreateStContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F032 — [.StringLit() in pkg/cypher/antlr/cypher_parser.go:34226](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L34226)

Reported as **refactor**; 5 matching declarations:

- [.CharLit() in pkg/cypher/antlr/cypher_parser.go:34201](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L34201) — code; similarity 1.0
- [.Lhs() in pkg/cypher/antlr/cypher_parser.go:29295](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L29295) — code; similarity 0.95
- [.PropertyTypeName() in pkg/cypher/antlr/cypher_parser.go:11134](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L11134) — code; similarity 0.86
- [.StringExpression() in pkg/cypher/antlr/cypher_parser.go:24850](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L24850) — code; similarity 0.827
- [.ShowCommand() in pkg/cypher/antlr/cypher_parser.go:4238](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L4238) — behavior; similarity 0.304

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) StringLit() (localctx IStringLitContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F033 — [.ListLit() in pkg/cypher/antlr/cypher_parser.go:34442](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L34442)

Reported as **refactor**; 5 matching declarations:

- [.ParenExpressionChain() in pkg/cypher/antlr/cypher_parser.go:18972](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L18972) — code; similarity 1.0
- [.MapProjection() in pkg/cypher/antlr/cypher_parser.go:28902](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L28902) — code; similarity 0.897
- [.PatternComprehension() in pkg/cypher/antlr/cypher_parser.go:31240](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L31240) — code; similarity 0.874
- [.MapLit() in pkg/cypher/antlr/cypher_parser.go:34499](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L34499) — code; similarity 0.859
- [.UseGraphArguments() in pkg/cypher/antlr/cypher_parser.go:39444](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L39444) — code; similarity 0.833

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) ListLit() (localctx IListLitContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F034 — [.SingleQuery() in pkg/cypher/antlr/cypher_parser.go:11548](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L11548)

Reported as **refactor**; 5 matching declarations:

- [.RemoveItem() in pkg/cypher/antlr/cypher_parser.go:18213](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L18213) — code; similarity 0.97
- [.ShellCommandElement() in pkg/cypher/antlr/cypher_parser.go:2909](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L2909) — code; similarity 0.941
- [.PatternElemStart() in pkg/cypher/antlr/cypher_parser.go:27069](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L27069) — code; similarity 0.831
- [.SubqueryExist() in pkg/cypher/antlr/cypher_parser.go:30219](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30219) — code; similarity 0.824
- [.SyntaxFragment() in pkg/cypher/antlr/cypher_parser.go:37849](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L37849) — code; similarity 0.814

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) SingleQuery() (localctx ISingleQueryContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F035 — [.YieldItems() in pkg/cypher/antlr/cypher_parser.go:19280](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L19280)

Reported as **refactor**; 5 matching declarations:

- [.SetSt() in pkg/cypher/antlr/cypher_parser.go:19840](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19840) — code; similarity 0.876
- [.Pattern() in pkg/cypher/antlr/cypher_parser.go:21816](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21816) — code; similarity 0.876
- [.Expression() in pkg/cypher/antlr/cypher_parser.go:21994](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21994) — code; similarity 0.876
- [.PowerExpression() in pkg/cypher/antlr/cypher_parser.go:23473](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23473) — code; similarity 0.876
- [.ExpressionChain() in pkg/cypher/antlr/cypher_parser.go:32100](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32100) — code; similarity 0.876

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) YieldItems() (localctx IYieldItemsContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F036 — [.ReturnSt() in pkg/cypher/antlr/cypher_parser.go:13923](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L13923)

Reported as **refactor**; 4 matching declarations:

- [.SkipSt() in pkg/cypher/antlr/cypher_parser.go:14182](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L14182) — code; similarity 1.0
- [.LimitSt() in pkg/cypher/antlr/cypher_parser.go:14300](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L14300) — code; similarity 1.0
- [.CreateSt() in pkg/cypher/antlr/cypher_parser.go:21415](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21415) — code; similarity 1.0
- [.ForSt() in pkg/cypher/antlr/cypher_parser.go:16862](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16862) — code; similarity 0.971

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) ReturnSt() (localctx IReturnStContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F037 — [.Literal() in pkg/cypher/antlr/cypher_parser.go:33470](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L33470)

Reported as **refactor**; 4 matching declarations:

- [.CompositeValue() in pkg/cypher/antlr/cypher_parser.go:7270](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L7270) — code; similarity 0.979
- [.CredentialValue() in pkg/cypher/antlr/cypher_parser.go:39077](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L39077) — code; similarity 0.979
- [.CredentialURL() in pkg/cypher/antlr/cypher_parser.go:39275](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L39275) — code; similarity 0.979
- [.Properties() in pkg/cypher/antlr/cypher_parser.go:27979](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L27979) — code; similarity 0.884

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) Literal() (localctx ILiteralContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F038 — [.ProjectionItem() in pkg/cypher/antlr/cypher_parser.go:14976](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L14976)

Reported as **refactor**; 4 matching declarations:

- [.UnwindSt() in pkg/cypher/antlr/cypher_parser.go:16229](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16229) — code; similarity 0.978
- [.ProjectionBody() in pkg/cypher/antlr/cypher_parser.go:14469](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L14469) — code; similarity 0.88
- [.FilterSt() in pkg/cypher/antlr/cypher_parser.go:16706](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16706) — code; similarity 0.853
- [.YieldItem() in pkg/cypher/antlr/cypher_parser.go:19343](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19343) — code; similarity 0.814

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) ProjectionItem() (localctx IProjectionItemContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F039 — [.ForSt() in pkg/cypher/antlr/cypher_parser.go:16984](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L16984)

Reported as **refactor**; 4 matching declarations:

- [.SkipSt() in pkg/cypher/antlr/cypher_parser.go:14182](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L14182) — code; similarity 0.971
- [.LimitSt() in pkg/cypher/antlr/cypher_parser.go:14300](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L14300) — code; similarity 0.971
- [.CreateSt() in pkg/cypher/antlr/cypher_parser.go:21415](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21415) — code; similarity 0.971
- [.Where() in pkg/cypher/antlr/cypher_parser.go:21667](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21667) — code; similarity 0.971

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) ForSt() (localctx IForStContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F040 — [.RelationshipTypes() in pkg/cypher/antlr/cypher_parser.go:30048](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L30048)

Reported as **refactor**; 4 matching declarations:

- [.MergeAction() in pkg/cypher/antlr/cypher_parser.go:19673](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19673) — code; similarity 0.933
- [.QueryPrefix() in pkg/cypher/antlr/cypher_parser.go:3843](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L3843) — code; similarity 0.907
- [.BoolLit() in pkg/cypher/antlr/cypher_parser.go:33698](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L33698) — code; similarity 0.907
- [.PathFunction() in pkg/cypher/antlr/cypher_parser.go:26646](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26646) — code; similarity 0.872

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) RelationshipTypes() (localctx IRelationshipTypesContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F041 — [.LetItem() in pkg/cypher/antlr/cypher_parser.go:16701](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L16701)

Reported as **refactor**; 4 matching declarations:

- [.Lhs() in pkg/cypher/antlr/cypher_parser.go:29295](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L29295) — code; similarity 0.916
- [.StringExpression() in pkg/cypher/antlr/cypher_parser.go:24850](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L24850) — code; similarity 0.904
- [.CharLit() in pkg/cypher/antlr/cypher_parser.go:34201](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L34201) — code; similarity 0.869
- [.PropertyTypeName() in pkg/cypher/antlr/cypher_parser.go:11134](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L11134) — code; similarity 0.865

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) LetItem() (localctx ILetItemContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F042 — [.YieldItem() in pkg/cypher/antlr/cypher_parser.go:19465](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L19465)

Reported as **refactor**; 4 matching declarations:

- [.ProjectionBody() in pkg/cypher/antlr/cypher_parser.go:14469](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L14469) — code; similarity 0.9
- [.UnwindSt() in pkg/cypher/antlr/cypher_parser.go:16229](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16229) — code; similarity 0.834
- [.ProjectionItem() in pkg/cypher/antlr/cypher_parser.go:14854](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L14854) — code; similarity 0.814
- [.YieldItems() in pkg/cypher/antlr/cypher_parser.go:19158](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19158) — behavior; similarity 0.643

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) YieldItem() (localctx IYieldItemContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F043 — [.ParenExpressionChain() in pkg/cypher/antlr/cypher_parser.go:19094](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L19094)

Reported as **refactor**; 4 matching declarations:

- [.MapProjection() in pkg/cypher/antlr/cypher_parser.go:28902](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L28902) — code; similarity 0.897
- [.PatternComprehension() in pkg/cypher/antlr/cypher_parser.go:31240](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L31240) — code; similarity 0.874
- [.MapLit() in pkg/cypher/antlr/cypher_parser.go:34499](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L34499) — code; similarity 0.859
- [.UseGraphArguments() in pkg/cypher/antlr/cypher_parser.go:39444](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L39444) — code; similarity 0.833

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) ParenExpressionChain() (localctx IParenExpressionChainContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F044 — [.MapLit() in pkg/cypher/antlr/cypher_parser.go:8571](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L8571)

Reported as **refactor**; 3 matching declarations:

- [.MapLit() in pkg/cypher/antlr/cypher_parser.go:2803](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L2803) — code; similarity 1.0
- [.MapLit() in pkg/cypher/antlr/cypher_parser.go:27888](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L27888) — code; similarity 1.0
- [.MapLit() in pkg/cypher/antlr/cypher_parser.go:33190](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L33190) — code; similarity 1.0

**Disposition — retain generator ownership.** This occurrence is the generated interface member `MapLit() IMapLitContext`, a required typed tree declaration with no executable implementation body. Its generated context implementation remains the owner; do not replace the interface contract with a hand-written forwarding layer.

### F045 — [.CountSubquery() in pkg/cypher/antlr/cypher_parser.go:12343](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L12343)

Reported as **refactor**; 3 matching declarations:

- [.ExistsSubquery() in pkg/cypher/antlr/cypher_parser.go:11986](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L11986) — code; similarity 1.0
- [.SubqueryExist() in pkg/cypher/antlr/cypher_parser.go:30219](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30219) — code; similarity 0.815
- [.RelationshipQuantifier() in pkg/cypher/antlr/cypher_parser.go:27738](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L27738) — code; similarity 0.811

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) CountSubquery() (localctx ICountSubqueryContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F046 — [.DeleteSt() in pkg/cypher/antlr/cypher_parser.go:17960](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L17960)

Reported as **refactor**; 3 matching declarations:

- [.MatchSt() in pkg/cypher/antlr/cypher_parser.go:16052](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16052) — code; similarity 1.0
- [.CypherPreamble() in pkg/cypher/antlr/cypher_parser.go:2220](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L2220) — code; similarity 0.826
- [.RemoveSt() in pkg/cypher/antlr/cypher_parser.go:18012](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L18012) — behavior; similarity 0.641

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) DeleteSt() (localctx IDeleteStContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F047 — [.ReduceExpression() in pkg/cypher/antlr/cypher_parser.go:32722](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L32722)

Reported as **refactor**; 3 matching declarations:

- [.ParenthesizedExpression() in pkg/cypher/antlr/cypher_parser.go:30889](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30889) — code; similarity 1.0
- [.AllReduceExpression() in pkg/cypher/antlr/cypher_parser.go:32886](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32886) — code; similarity 1.0
- [.CountAll() in pkg/cypher/antlr/cypher_parser.go:31931](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L31931) — code; similarity 0.962

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) ReduceExpression() (localctx IReduceExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F048 — [.WithSt() in pkg/cypher/antlr/cypher_parser.go:14058](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L14058)

Reported as **refactor**; 3 matching declarations:

- [.UnwindSt() in pkg/cypher/antlr/cypher_parser.go:16229](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16229) — code; similarity 0.978
- [.ProjectionBody() in pkg/cypher/antlr/cypher_parser.go:14469](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L14469) — code; similarity 0.88
- [.FilterSt() in pkg/cypher/antlr/cypher_parser.go:16706](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16706) — code; similarity 0.853

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) WithSt() (localctx IWithStContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F049 — [.UnwindSt() in pkg/cypher/antlr/cypher_parser.go:16351](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L16351)

Reported as **refactor**; 3 matching declarations:

- [.WithSt() in pkg/cypher/antlr/cypher_parser.go:13936](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L13936) — code; similarity 0.978
- [.FilterSt() in pkg/cypher/antlr/cypher_parser.go:16706](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16706) — code; similarity 0.836
- [.YieldItem() in pkg/cypher/antlr/cypher_parser.go:19343](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L19343) — code; similarity 0.834

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) UnwindSt() (localctx IUnwindStContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F050 — [.Where() in pkg/cypher/antlr/cypher_parser.go:21789](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L21789)

Reported as **refactor**; 3 matching declarations:

- [.ForSt() in pkg/cypher/antlr/cypher_parser.go:16862](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16862) — code; similarity 0.971
- [.PropertyTypeName() in pkg/cypher/antlr/cypher_parser.go:11134](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L11134) — code; similarity 0.891
- [.StringExpression() in pkg/cypher/antlr/cypher_parser.go:24850](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L24850) — code; similarity 0.875

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) Where() (localctx IWhereContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F051 — [.CountAll() in pkg/cypher/antlr/cypher_parser.go:32053](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L32053)

Reported as **refactor**; 3 matching declarations:

- [.ParenthesizedExpression() in pkg/cypher/antlr/cypher_parser.go:30889](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30889) — code; similarity 0.962
- [.ReduceExpression() in pkg/cypher/antlr/cypher_parser.go:32600](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32600) — code; similarity 0.962
- [.AllReduceExpression() in pkg/cypher/antlr/cypher_parser.go:32886](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32886) — code; similarity 0.962

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) CountAll() (localctx ICountAllContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F052 — [.MergeAction() in pkg/cypher/antlr/cypher_parser.go:19795](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L19795)

Reported as **refactor**; 3 matching declarations:

- [.PathFunction() in pkg/cypher/antlr/cypher_parser.go:26646](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26646) — code; similarity 0.936
- [.RelationshipTypes() in pkg/cypher/antlr/cypher_parser.go:29926](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L29926) — code; similarity 0.933
- [.DynamicLabel() in pkg/cypher/antlr/cypher_parser.go:21258](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21258) — code; similarity 0.818

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) MergeAction() (localctx IMergeActionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F053 — [.UnionSt() in pkg/cypher/antlr/cypher_parser.go:30176](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L30176)

Reported as **refactor**; 3 matching declarations:

- [.MatchSt() in pkg/cypher/antlr/cypher_parser.go:16052](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16052) — code; similarity 0.915
- [.DeleteSt() in pkg/cypher/antlr/cypher_parser.go:17838](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L17838) — code; similarity 0.915
- [.RegularQuery() in pkg/cypher/antlr/cypher_parser.go:11282](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L11282) — code; similarity 0.822

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) UnionSt() (localctx IUnionStContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F054 — [.FilterSt() in pkg/cypher/antlr/cypher_parser.go:16828](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L16828)

Reported as **refactor**; 3 matching declarations:

- [.ProjectionBody() in pkg/cypher/antlr/cypher_parser.go:14469](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L14469) — code; similarity 0.861
- [.WithSt() in pkg/cypher/antlr/cypher_parser.go:13936](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L13936) — code; similarity 0.853
- [.UnwindSt() in pkg/cypher/antlr/cypher_parser.go:16229](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L16229) — code; similarity 0.836

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) FilterSt() (localctx IFilterStContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F055 — [.LoadCSVSt() in pkg/cypher/antlr/cypher_parser.go:17195](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L17195)

Reported as **refactor**; 3 matching declarations:

- [.QueryCallSt() in pkg/cypher/antlr/cypher_parser.go:18726](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L18726) — code; similarity 0.85
- [.ShowTail() in pkg/cypher/antlr/cypher_parser.go:5187](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L5187) — code; similarity 0.805
- [.SchemaCommand() in pkg/cypher/antlr/cypher_parser.go:8976](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8976) — behavior; similarity 0.595

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) LoadCSVSt() (localctx ILoadCSVStContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F056 — [.ExistsSubquery() in pkg/cypher/antlr/cypher_parser.go:12108](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L12108)

Reported as **refactor**; 2 matching declarations:

- [.CountSubquery() in pkg/cypher/antlr/cypher_parser.go:12221](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L12221) — code; similarity 1.0
- [.RelationshipQuantifier() in pkg/cypher/antlr/cypher_parser.go:27738](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L27738) — code; similarity 0.811

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) ExistsSubquery() (localctx IExistsSubqueryContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F057 — [.ParenthesizedExpression() in pkg/cypher/antlr/cypher_parser.go:31011](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L31011)

Reported as **refactor**; 2 matching declarations:

- [.ReduceExpression() in pkg/cypher/antlr/cypher_parser.go:32600](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32600) — code; similarity 1.0
- [.CountAll() in pkg/cypher/antlr/cypher_parser.go:31931](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L31931) — code; similarity 0.962

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) ParenthesizedExpression() (localctx IParenthesizedExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F058 — [.AllReduceExpression() in pkg/cypher/antlr/cypher_parser.go:33008](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L33008)

Reported as **refactor**; 2 matching declarations:

- [.ParenthesizedExpression() in pkg/cypher/antlr/cypher_parser.go:30889](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30889) — code; similarity 1.0
- [.ReduceExpression() in pkg/cypher/antlr/cypher_parser.go:32600](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L32600) — code; similarity 1.0

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) AllReduceExpression() (localctx IAllReduceExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F059 — [ownPropertyMap() in pkg/storage/property_ownership.go:135](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/property_ownership.go#L135)

Reported as **refactor**; 2 matching declarations:

- [codecImageProperties() in pkg/storage/badger_transaction_images.go:259](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/storage/badger_transaction_images.go#L259) — code; similarity 1.0
- [ownRetainedMap() in pkg/storage/property_ownership.go:201](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/storage/property_ownership.go#L201) — forwarder; similarity 0.169

**Disposition — S1 — corrective action.** Remove ownPropertyMap, ownPropertyValue, ownedProperties, canonicalPropertyValue and codecImageProperties; migrate private callers directly to ownRetainedMap/ownRetainedValue. Keep exactness results at written-image admission. Update census owner identities to those same canonical boundaries, without changing assertions.

### F060 — [.Properties() in pkg/cypher/antlr/cypher_parser.go:28101](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L28101)

Reported as **refactor**; 2 matching declarations:

- [.UpdatingStatement() in pkg/cypher/antlr/cypher_parser.go:17675](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L17675) — code; similarity 0.964
- [.Literal() in pkg/cypher/antlr/cypher_parser.go:33348](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L33348) — code; similarity 0.884

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) Properties() (localctx IPropertiesContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F061 — [.ReadingStatement() in pkg/cypher/antlr/cypher_parser.go:17539](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L17539)

Reported as **refactor**; 2 matching declarations:

- [.Symbol() in pkg/cypher/antlr/cypher_parser.go:35808](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L35808) — code; similarity 0.955
- [.PatternElemStart() in pkg/cypher/antlr/cypher_parser.go:27069](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L27069) — code; similarity 0.831

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) ReadingStatement() (localctx IReadingStatementContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F062 — [.RegularQuery() in pkg/cypher/antlr/cypher_parser.go:11404](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L11404)

Reported as **refactor**; 2 matching declarations:

- [.CypherPreamble() in pkg/cypher/antlr/cypher_parser.go:2220](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L2220) — code; similarity 0.951
- [.UnionSt() in pkg/cypher/antlr/cypher_parser.go:30054](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L30054) — code; similarity 0.822

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) RegularQuery() (localctx IRegularQueryContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F063 — [TestLegacyDateTimeWire() in pkg/bolt/legacy_datetime_runtime_test.go:32](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/legacy_datetime_runtime_test.go#L32)

Reported as **refactor**; 2 matching declarations:

- [TestLegacyDateTimeRealDriver() in pkg/bolt/legacy_datetime_runtime_test.go:146](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/bolt/legacy_datetime_runtime_test.go#L146) — code; similarity 0.94
- [TestLegacyDateTimeWireNamedZoneTransitions() in pkg/bolt/legacy_datetime_transitions_test.go:13](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/bolt/legacy_datetime_transitions_test.go#L13) — code; similarity 0.94

**Disposition — B5 — corrective action.** Converge the three identical local temporal-server fixture bodies, preserving the wire, pinned v4.4 driver and named-zone scenario runners and their separate assertions. Existing scenario runners remain the one local/reference oracle bodies.

### F064 — [TestLegacyDateTimeRealDriver() in pkg/bolt/legacy_datetime_runtime_test.go:146](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/legacy_datetime_runtime_test.go#L146)

Reported as **refactor**; 2 matching declarations:

- [TestLegacyDateTimeWire() in pkg/bolt/legacy_datetime_runtime_test.go:32](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/bolt/legacy_datetime_runtime_test.go#L32) — code; similarity 0.94
- [TestLegacyDateTimeWireNamedZoneTransitions() in pkg/bolt/legacy_datetime_transitions_test.go:13](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/bolt/legacy_datetime_transitions_test.go#L13) — code; similarity 0.94

**Disposition — B5 — corrective action.** Same local-server fixture consolidation as F063; preserve the bolt:// URI needed by the real driver and its unpatched Bolt4.4 negotiation assertions.

### F065 — [TestLegacyDateTimeWireNamedZoneTransitions() in pkg/bolt/legacy_datetime_transitions_test.go:13](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/legacy_datetime_transitions_test.go#L13)

Reported as **refactor**; 2 matching declarations:

- [TestLegacyDateTimeWire() in pkg/bolt/legacy_datetime_runtime_test.go:32](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/bolt/legacy_datetime_runtime_test.go#L32) — code; similarity 0.94
- [TestLegacyDateTimeRealDriver() in pkg/bolt/legacy_datetime_runtime_test.go:146](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/bolt/legacy_datetime_runtime_test.go#L146) — code; similarity 0.94

**Disposition — B5 — corrective action.** Same local-server fixture consolidation as F063; preserve DST overlap/gap populations and independently constructed expected timestamps.

### F066 — [.MapProjection() in pkg/cypher/antlr/cypher_parser.go:29024](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L29024)

Reported as **refactor**; 2 matching declarations:

- [.MapLit() in pkg/cypher/antlr/cypher_parser.go:34499](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L34499) — code; similarity 0.933
- [.PatternComprehension() in pkg/cypher/antlr/cypher_parser.go:31240](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L31240) — code; similarity 0.879

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) MapProjection() (localctx IMapProjectionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F067 — [.MapLit() in pkg/cypher/antlr/cypher_parser.go:34621](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L34621)

Reported as **refactor**; 2 matching declarations:

- [.MapProjection() in pkg/cypher/antlr/cypher_parser.go:28902](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L28902) — code; similarity 0.933
- [.PatternComprehension() in pkg/cypher/antlr/cypher_parser.go:31240](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L31240) — code; similarity 0.91

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) MapLit() (localctx IMapLitContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F068 — [.UpdatingStatement() in pkg/cypher/antlr/cypher_parser.go:17797](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L17797)

Reported as **refactor**; 2 matching declarations:

- [.Literal() in pkg/cypher/antlr/cypher_parser.go:33348](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L33348) — code; similarity 0.918
- [.StringExpPrefix() in pkg/cypher/antlr/cypher_parser.go:24962](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L24962) — code; similarity 0.845

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) UpdatingStatement() (localctx IUpdatingStatementContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F069 — [.RemoveItem() in pkg/cypher/antlr/cypher_parser.go:18335](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L18335)

Reported as **refactor**; 2 matching declarations:

- [.ShellCommandElement() in pkg/cypher/antlr/cypher_parser.go:2909](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L2909) — code; similarity 0.914
- [.PatternElemStart() in pkg/cypher/antlr/cypher_parser.go:27069](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L27069) — code; similarity 0.81

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) RemoveItem() (localctx IRemoveItemContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F070 — [.PatternComprehension() in pkg/cypher/antlr/cypher_parser.go:31362](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L31362)

Reported as **refactor**; 2 matching declarations:

- [.MapLit() in pkg/cypher/antlr/cypher_parser.go:34499](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L34499) — code; similarity 0.91
- [.ParenExpressionChain() in pkg/cypher/antlr/cypher_parser.go:18972](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L18972) — code; similarity 0.874

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) PatternComprehension() (localctx IPatternComprehensionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F071 — [.MapPair() in pkg/cypher/antlr/cypher_parser.go:34804](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L34804)

Reported as **refactor**; 2 matching declarations:

- [.StringExpression() in pkg/cypher/antlr/cypher_parser.go:24850](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L24850) — code; similarity 0.904
- [.PropertyTypeName() in pkg/cypher/antlr/cypher_parser.go:11134](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L11134) — code; similarity 0.865

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) MapPair() (localctx IMapPairContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F072 — [.IntegerLit() in pkg/cypher/antlr/cypher_parser.go:33937](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L33937)

Reported as **refactor**; 2 matching declarations:

- [.DynamicLabel() in pkg/cypher/antlr/cypher_parser.go:21258](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21258) — code; similarity 0.892
- [.CypherGroup() in pkg/cypher/antlr/cypher_parser.go:2384](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L2384) — code; similarity 0.824

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) IntegerLit() (localctx IIntegerLitContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F073 — [.LabelPredicate() in pkg/cypher/antlr/cypher_parser.go:24488](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L24488)

Reported as **refactor**; 2 matching declarations:

- [.PropertyTypeName() in pkg/cypher/antlr/cypher_parser.go:11134](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L11134) — code; similarity 0.891
- [.StringExpression() in pkg/cypher/antlr/cypher_parser.go:24850](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L24850) — code; similarity 0.875

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) LabelPredicate() (localctx ILabelPredicateContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F074 — [.ProjectionBody() in pkg/cypher/antlr/cypher_parser.go:14591](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L14591)

Reported as **refactor**; 2 matching declarations:

- [.WithSt() in pkg/cypher/antlr/cypher_parser.go:13936](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L13936) — code; similarity 0.88
- [.ProjectionItem() in pkg/cypher/antlr/cypher_parser.go:14854](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L14854) — code; similarity 0.88

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) ProjectionBody() (localctx IProjectionBodyContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F075 — [.FilterExpression() in pkg/cypher/antlr/cypher_parser.go:31917](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L31917)

Reported as **refactor**; 2 matching declarations:

- [.NullExpression() in pkg/cypher/antlr/cypher_parser.go:25120](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L25120) — code; similarity 0.867
- [.DeleteSt() in pkg/cypher/antlr/cypher_parser.go:17838](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L17838) — code; similarity 0.859

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) FilterExpression() (localctx IFilterExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F076 — [.ComparisonSigns() in pkg/cypher/antlr/cypher_parser.go:23049](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L23049)

Reported as **refactor**; 2 matching declarations:

- [.FilterWith() in pkg/cypher/antlr/cypher_parser.go:31040](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L31040) — code; similarity 0.863
- [.ComparisonExpression() in pkg/cypher/antlr/cypher_parser.go:22771](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22771) — behavior; similarity 0.485

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) ComparisonSigns() (localctx IComparisonSignsContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F077 — [TestBoltCrossSessionMergeUniqueConflict_EshuDefaultNamespaceExplicitTx() in pkg/bolt/cross_session_merge_unique_namespace_test.go:23](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/cross_session_merge_unique_namespace_test.go#L23)

Reported as **refactor**; 2 matching declarations:

- [TestBoltCrossSessionMergeUniqueConflict_Concurrent_ExplicitTx() in pkg/bolt/cross_session_merge_unique_test.go:456](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/bolt/cross_session_merge_unique_test.go#L456) — code; similarity 0.82
- [TestBoltCrossSessionMergeUniqueConflict_Sequential() in pkg/bolt/cross_session_merge_unique_test.go:206](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/bolt/cross_session_merge_unique_test.go#L206) — behavior; similarity 0.274

**Disposition — B6 — corrective action.** Converge the concurrent explicit-MERGE scheduling/verification lifecycle with the test-namespace case, parameterizing namespace, UID and concurrency (nornic/8 versus test/2). Preserve both populations. Sequential MERGE retains its sequential visibility oracle and reuses the common transport owner.

### F078 — [TestBoltCrossSessionMergeUniqueConflict_Concurrent_ExplicitTx() in pkg/bolt/cross_session_merge_unique_test.go:415](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/cross_session_merge_unique_test.go#L415)

Reported as **refactor**; 2 matching declarations:

- [TestBoltCrossSessionMergeUniqueConflict_EshuDefaultNamespaceExplicitTx() in pkg/bolt/cross_session_merge_unique_namespace_test.go:23](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/bolt/cross_session_merge_unique_namespace_test.go#L23) — code; similarity 0.82
- [TestBoltCrossSessionMergeUniqueConflict_Sequential() in pkg/bolt/cross_session_merge_unique_test.go:206](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/bolt/cross_session_merge_unique_test.go#L206) — behavior; similarity 0.351

**Disposition — B6 — corrective action.** Same convergence as F077. Do not replace the sequential case with a concurrent one: it checks visibility on the next connection rather than retry arbitration.

### F079 — [.SubqueryExist() in pkg/cypher/antlr/cypher_parser.go:30341](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L30341)

Reported as **refactor**; 2 matching declarations:

- [.Name() in pkg/cypher/antlr/cypher_parser.go:34881](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L34881) — code; similarity 0.815
- [.Symbol() in pkg/cypher/antlr/cypher_parser.go:35808](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L35808) — code; similarity 0.801

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) SubqueryExist() (localctx ISubqueryExistContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F080 — [.PatternElem() in pkg/cypher/antlr/cypher_parser.go:26989](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L26989)

Reported as **refactor**; 2 matching declarations:

- [.NotExpression() in pkg/cypher/antlr/cypher_parser.go:22519](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L22519) — code; similarity 0.8
- [.PatternElemPart() in pkg/cypher/antlr/cypher_parser.go:27239](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L27239) — behavior; similarity 0.496

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) PatternElem() (localctx IPatternElemContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F081 — [.RelationDetail() in pkg/cypher/antlr/cypher_parser.go:29851](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L29851)

Reported as **reuse**; 1 matching declarations:

- [.NodePattern() in pkg/cypher/antlr/cypher_parser.go:28165](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L28165) — code; similarity 1.0

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) RelationDetail() (localctx IRelationDetailContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F082 — [.CollectSubquery() in pkg/cypher/antlr/cypher_parser.go:12527](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L12527)

Reported as **reuse**; 1 matching declarations:

- [.CountAll() in pkg/cypher/antlr/cypher_parser.go:31931](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L31931) — code; similarity 0.962

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) CollectSubquery() (localctx ICollectSubqueryContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F083 — [.NormalizationPredicate() in pkg/cypher/antlr/cypher_parser.go:24325](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L24325)

Reported as **reuse**; 1 matching declarations:

- [.CypherOption() in pkg/cypher/antlr/cypher_parser.go:2554](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L2554) — code; similarity 0.904

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) NormalizationPredicate() (localctx INormalizationPredicateContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F084 — [.GetNode() in pkg/storage/badger_transaction_graph_reads.go:25](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/badger_transaction_graph_reads.go#L25)

Reported as **reuse**; 1 matching declarations:

- [.GetStoredNode() in pkg/storage/badger_transaction_stored_reads.go:9](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/storage/badger_transaction_stored_reads.go#L9) — code; similarity 0.879

**Disposition — S2 — retain shared owner.** GetNode and GetStoredNode both call readTransactionEntity with the same node state/clone/load callbacks. Only visibleRead versus storedRead differs. Engine and StoredEntityReader require the two public capabilities; filtering suppressed/decayed data out of the integrity reader would break its contract. Admission, snapshot/pending selection and completion have one implementation already.

### F085 — [.GetStoredNode() in pkg/storage/badger_transaction_stored_reads.go:9](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/badger_transaction_stored_reads.go#L9)

Reported as **reuse**; 1 matching declarations:

- [.GetNode() in pkg/storage/badger_transaction_graph_reads.go:25](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/storage/badger_transaction_graph_reads.go#L25) — code; similarity 0.879

**Disposition — S2 — retain shared owner.** Reverse of F084: public stored-inclusive capability intentionally bypasses user visibility; both selectors use readTransactionEntity, not copied read bodies. No exported Go API migration is introduced to rename those required capability methods.

### F086 — [.PathFunction() in pkg/cypher/antlr/cypher_parser.go:26768](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L26768)

Reported as **reuse**; 1 matching declarations:

- [.DynamicLabel() in pkg/cypher/antlr/cypher_parser.go:21258](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21258) — code; similarity 0.872

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) PathFunction() (localctx IPathFunctionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F087 — [.parseCreateConstraintTemporalDDL() in pkg/cypher/schema.go:1563](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L1563)

Reported as **reuse**; 1 matching declarations:

- [.parseCreateConstraintDomainDDL() in pkg/cypher/schema.go:1628](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/schema.go#L1628) — code; similarity 0.863

**Disposition — C1 — retain distinct predicate decoders.** Both parsers reuse parseCreateConstraintForRequireDDL for the head, pattern and OPTIONS. Temporal parses a property tuple plus IS TEMPORAL/NO OVERLAP and returns key/interval properties; domain parses one qualified property IN a value list and returns AllowedValues input. Their storage validators and typed outputs differ. Shared head/pattern parsing is consolidated separately; no new callback wrapper is needed around these distinct predicate decisions.

### F088 — [.NullExpression() in pkg/cypher/antlr/cypher_parser.go:25242](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L25242)

Reported as **reuse**; 1 matching declarations:

- [.RangeLit() in pkg/cypher/antlr/cypher_parser.go:33548](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L33548) — code; similarity 0.861

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) NullExpression() (localctx INullExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F089 — [.DynamicLabel() in pkg/cypher/antlr/cypher_parser.go:21380](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L21380)

Reported as **reuse**; 1 matching declarations:

- [.CypherGroup() in pkg/cypher/antlr/cypher_parser.go:2384](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L2384) — code; similarity 0.856

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) DynamicLabel() (localctx IDynamicLabelContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F090 — [.Parameter() in pkg/cypher/antlr/cypher_parser.go:33229](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L33229)

Reported as **reuse**; 1 matching declarations:

- [.Atom() in pkg/cypher/antlr/cypher_parser.go:28610](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L28610) — code; similarity 0.854

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) Parameter() (localctx IParameterContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F091 — [schemaOptionsNotMapError() in pkg/cypher/schema_options.go:138](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema_options.go#L138)

Reported as **reuse**; 1 matching declarations:

- [invalidSubscriptTypeError() in pkg/cypher/row_subscript_validation.go:288](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/row_subscript_validation.go#L288) — code; similarity 0.853

**Disposition — C5 — retain composed errors.** Both use newSemanticError, the shared semantic-error constructor. OPTIONS must emit Neo.ClientError.General.InvalidArguments with Neo4j value representation and expected-map text; subscript failure emits Neo.ClientError.Statement.TypeError/InvalidArgumentType with a Cypher type name. Combining the message builders would erase distinct wire code/type/message contracts; common error construction already has one owner.

### F092 — [.SetItem() in pkg/cypher/antlr/cypher_parser.go:20190](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L20190)

Reported as **reuse**; 1 matching declarations:

- [.NumLit() in pkg/cypher/antlr/cypher_parser.go:33965](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L33965) — code; similarity 0.845

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) SetItem() (localctx ISetItemContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F093 — [.GetOutgoingEdgesVisibleAt() in pkg/storage/badger_mvcc.go:1197](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/badger_mvcc.go#L1197)

Reported as **reuse**; 1 matching declarations:

- [.GetIncomingEdgesVisibleAt() in pkg/storage/badger_mvcc.go:1201](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/storage/badger_mvcc.go#L1201) — code; similarity 0.841

**Disposition — S3 — retain direction selectors.** Outgoing and Incoming are required public adjacency capabilities. Both call readAdjacentEdgesAtWithView with the typed Outgoing/Incoming value, the same snapshot and view owner. There is no second historical traversal, lookup, cancellation or reader-release body.

### F094 — [.GetIncomingEdgesVisibleAt() in pkg/storage/badger_mvcc.go:1201](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/badger_mvcc.go#L1201)

Reported as **reuse**; 1 matching declarations:

- [.GetOutgoingEdgesVisibleAt() in pkg/storage/badger_mvcc.go:1197](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/storage/badger_mvcc.go#L1197) — code; similarity 0.841

**Disposition — S3 — retain direction selectors.** Reverse of F093; one directional historical-read owner, two required interface selectors. A new public API migration is unnecessary to eliminate an implementation that is already shared.

### F095 — [.GetEdge() in pkg/storage/badger_transaction_graph_reads.go:188](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/badger_transaction_graph_reads.go#L188)

Reported as **reuse**; 1 matching declarations:

- [.GetStoredEdge() in pkg/storage/badger_transaction_stored_reads.go:14](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/storage/badger_transaction_stored_reads.go#L14) — code; similarity 0.841

**Disposition — S2 — retain shared owner.** GetEdge/GetStoredEdge select visibleRead/storedRead on the same readTransactionEntity, edge state, clone and load callbacks. Visible relationship filtering and stored integrity reads are different published capabilities; overlay, locking and snapshot work are already shared.

### F096 — [.GetStoredEdge() in pkg/storage/badger_transaction_stored_reads.go:14](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/badger_transaction_stored_reads.go#L14)

Reported as **reuse**; 1 matching declarations:

- [.GetEdge() in pkg/storage/badger_transaction_graph_reads.go:185](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/storage/badger_transaction_graph_reads.go#L185) — code; similarity 0.841

**Disposition — S2 — retain shared owner.** Reverse of F095; no duplicate point-read implementation remains beyond selecting the capability policy.

### F097 — [.UseReferencePrefix() in pkg/cypher/antlr/cypher_parser.go:39844](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L39844)

Reported as **reuse**; 1 matching declarations:

- [.MapProjection() in pkg/cypher/antlr/cypher_parser.go:28902](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L28902) — code; similarity 0.835

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) UseReferencePrefix() (localctx IUseReferencePrefixContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F098 — [.parseCreateConstraintDomainDDL() in pkg/cypher/schema.go:1600](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L1600)

Reported as **reuse**; 1 matching declarations:

- [.parseCreateConstraintTemporalDDL() in pkg/cypher/schema.go:1591](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/schema.go#L1591) — code; similarity 0.832

**Disposition — C1 — retain distinct predicate decoders.** Reverse of F087. Common DDL head/pattern/options parsing remains shared, while domain allowed-value decoding and temporal interval decoding keep their distinct typed results and validators.

### F099 — [.StringExpPrefix() in pkg/cypher/antlr/cypher_parser.go:25084](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L25084)

Reported as **reuse**; 1 matching declarations:

- [.Literal() in pkg/cypher/antlr/cypher_parser.go:33348](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L33348) — code; similarity 0.825

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) StringExpPrefix() (localctx IStringExpPrefixContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F100 — [.QuantifiedPath() in pkg/cypher/antlr/cypher_parser.go:27542](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L27542)

Reported as **reuse**; 1 matching declarations:

- [.RangeLit() in pkg/cypher/antlr/cypher_parser.go:33548](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L33548) — code; similarity 0.823

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) QuantifiedPath() (localctx IQuantifiedPathContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F101 — [.PatternElemStart() in pkg/cypher/antlr/cypher_parser.go:27191](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L27191)

Reported as **reuse**; 1 matching declarations:

- [.PatternWhere() in pkg/cypher/antlr/cypher_parser.go:21545](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L21545) — code; similarity 0.81

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) PatternElemStart() (localctx IPatternElemStartContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F102 — [TestBoltCrossSessionMultiMergeUniqueConflict_Concurrent_ExplicitTx() in pkg/bolt/cross_session_merge_unique_test.go:474](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/cross_session_merge_unique_test.go#L474)

Reported as **reuse**; 1 matching declarations:

- [TestBoltConcurrentMergeOfUniqueKeyAfterOtherWrites() in pkg/bolt/merge_unique_after_writes_race_test.go:19](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/bolt/merge_unique_after_writes_race_test.go#L19) — code; similarity 0.805

**Disposition — B7 — corrective action / preserve scenarios.** Retain both graph oracles: one checks O/V computed keys plus two uploads, the other checks ten rounds of one key owned by six sources after preceding writes/deletes. Converge their concurrent transport/retry lifecycle through B2/B6; keep the different queries, rounds and exact result cardinalities.

### F103 — [.TypePredicate() in pkg/cypher/antlr/cypher_parser.go:25404](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L25404)

Reported as **reuse**; 1 matching declarations:

- [.RelationshipPattern() in pkg/cypher/antlr/cypher_parser.go:29428](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L29428) — code; similarity 0.804

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) TypePredicate() (localctx ITypePredicateContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F104 — [.NotExpression() in pkg/cypher/antlr/cypher_parser.go:22641](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L22641)

Reported as **reuse**; 1 matching declarations:

- [.PatternElem() in pkg/cypher/antlr/cypher_parser.go:26867](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L26867) — code; similarity 0.8

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) NotExpression() (localctx INotExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F105 — [.MultDivExpression() in pkg/cypher/antlr/cypher_parser.go:23412](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L23412)

Reported as **reuse**; 1 matching declarations:

- [.PowerExpression() in pkg/cypher/antlr/cypher_parser.go:23473](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23473) — behavior; similarity 0.736

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) MultDivExpression() (localctx IMultDivExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F106 — [TestBoltCrossSessionMergeUniqueConflict_Concurrent_UnwindExplicitTx() in pkg/bolt/cross_session_merge_unique_test.go:524](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/cross_session_merge_unique_test.go#L524)

Reported as **reuse**; 1 matching declarations:

- [TestBoltCrossSessionMergeUniqueConflict_Concurrent_ExplicitTx() in pkg/bolt/cross_session_merge_unique_test.go:456](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/bolt/cross_session_merge_unique_test.go#L456) — behavior; similarity 0.72

**Disposition — B2/B6 — corrective action.** UNWIND rows and scalar MERGE retain their original batch sizes and UID populations. Retire duplicate transaction-attempt/retry envelopes and use one query/parameter-aware owner; converge concurrent scheduling without changing the workload assertions.

### F107 — [.UnaryAddSubExpression() in pkg/cypher/antlr/cypher_parser.go:23747](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L23747)

Reported as **reuse**; 1 matching declarations:

- [.AddSubExpression() in pkg/cypher/antlr/cypher_parser.go:23087](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L23087) — behavior; similarity 0.663

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) UnaryAddSubExpression() (localctx IUnaryAddSubExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F108 — [TestExplicitTransactionStreamsResultsByQID() in pkg/bolt/streaming_qid_compatibility_test.go:13](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/streaming_qid_compatibility_test.go#L13)

Reported as **reuse**; 1 matching declarations:

- [TestExplicitTransactionRejectsUnknownQIDUntilReset() in pkg/bolt/streaming_qid_compatibility_test.go:67](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/bolt/streaming_qid_compatibility_test.go#L67) — behavior; similarity 0.607

**Disposition — B4 — reuse, retain different scenarios.** Both tests already use shared explicit transaction startup, runStatementAndQID and PULL helpers. One asserts four simultaneously live streams, selective discard, remaining rows and ID reset after commit; the other asserts InvalidFormat for unknown QID, IGNORED until RESET and recovery. B4 removes duplicated PULL decoding; merging these state machines would change their preconditions.

### F109 — [.SinglePartQ() in pkg/cypher/antlr/cypher_parser.go:15593](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L15593)

Reported as **reuse**; 1 matching declarations:

- [.MultiPartQ() in pkg/cypher/antlr/cypher_parser.go:15868](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L15868) — behavior; similarity 0.588

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) SinglePartQ() (localctx ISinglePartQContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F110 — [.PatternPart() in pkg/cypher/antlr/cypher_parser.go:26568](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L26568)

Reported as **reuse**; 1 matching declarations:

- [.PatternElemPart() in pkg/cypher/antlr/cypher_parser.go:27239](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L27239) — behavior; similarity 0.56

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) PatternPart() (localctx IPatternPartContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F111 — [.RelationshipsChainPattern() in pkg/cypher/antlr/cypher_parser.go:31568](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L31568)

Reported as **reuse**; 1 matching declarations:

- [.RelationshipPattern() in pkg/cypher/antlr/cypher_parser.go:29428](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L29428) — behavior; similarity 0.555

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) RelationshipsChainPattern() (localctx IRelationshipsChainPatternContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F112 — [TestLegacyDateTimeWireMetadataSignatureNegotiation() in pkg/bolt/legacy_datetime_negotiation_test.go:85](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/legacy_datetime_negotiation_test.go#L85)

Reported as **reuse**; 1 matching declarations:

- [TestLegacyDateTimeWireSignatureNegotiation() in pkg/bolt/legacy_datetime_negotiation_test.go:14](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/bolt/legacy_datetime_negotiation_test.go#L14) — behavior; similarity 0.53

**Disposition — B5 — reuse, retain different wire locations.** Both use dateTimeWireConnect/dateTimeWireQuery and the independent dateTimeWire encoder. Scalar parameter/result signatures versus node/relationship metadata properties exercise different wire locations and rejection/recovery assertions. Preserve both populations; share local fixture setup and raw protocol owners.

### F113 — [.FunctionInvocation() in pkg/cypher/antlr/cypher_parser.go:30766](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L30766)

Reported as **reuse**; 1 matching declarations:

- [.SchemaCommand() in pkg/cypher/antlr/cypher_parser.go:8976](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8976) — behavior; similarity 0.504

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) FunctionInvocation() (localctx IFunctionInvocationContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F114 — [.NodePattern() in pkg/cypher/antlr/cypher_parser.go:28287](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L28287)

Reported as **reuse**; 1 matching declarations:

- [.SchemaCommand() in pkg/cypher/antlr/cypher_parser.go:8976](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8976) — behavior; similarity 0.481

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) NodePattern() (localctx INodePatternContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F115 — [TestDatabaseCommands_CreateDatabase_WhitespacePermutations() in pkg/cypher/database_commands_whitespace_test.go:60](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/database_commands_whitespace_test.go#L60)

Reported as **reuse**; 1 matching declarations:

- [TestDatabaseCommands_DropDatabase_WhitespacePermutations() in pkg/cypher/database_commands_whitespace_test.go:104](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/database_commands_whitespace_test.go#L104) — behavior; similarity 0.474

**Disposition — C6 — retain shared fixture and input generator.** Both reuse newDatabaseCommandFixture and whitespacePermutations. CREATE substitutes unique names and asserts empty results plus catalog presence; DROP recreates targets between permutations, tests IF EXISTS separately and asserts name output/catalog absence. Only generic t.Run/iteration scaffolding remains alike; parsing, execution, fixture ownership and permutation generation are not reimplemented.

### F116 — [.ListExpression() in pkg/cypher/antlr/cypher_parser.go:24717](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L24717)

Reported as **reuse**; 1 matching declarations:

- [.SchemaCommand() in pkg/cypher/antlr/cypher_parser.go:8976](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L8976) — behavior; similarity 0.432

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) ListExpression() (localctx IListExpressionContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F117 — [TestExplicitTransactionMergeUpdateViolationsFailTheStatement() in pkg/cypher/constraint_checked_writes_test.go:156](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/constraint_checked_writes_test.go#L156)

Reported as **reuse**; 1 matching declarations:

- [TestExplicitTransactionConstraintViolationFailsTheStatement() in pkg/cypher/constraint_checked_writes_test.go:93](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/constraint_checked_writes_test.go#L93) — behavior; similarity 0.387

**Disposition — C7 — retain shared rejection lifecycle.** Both use constraintRejectedInExplicitTransaction for BEGIN, rejected statement and terminal operation. Keep the different node/edge constraint populations, mutation queries and COMMIT versus ROLLBACK/post-state oracles; no second transaction/error-check body remains.

### F118 — [.ReservedWord() in pkg/cypher/antlr/cypher_parser.go:37818](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/antlr/cypher_parser.go#L37818)

Reported as **reuse**; 1 matching declarations:

- [.ProcedurePrivilegePattern() in pkg/cypher/antlr/cypher_parser.go:38852](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/antlr/cypher_parser.go#L38852) — behavior; similarity 0.38

**Disposition — retain generator ownership.** This is the generated typed rule entry point `func (p *CypherParser) ReservedWord() (localctx IReservedWordContext)`. The matched rules have their own typed entry points and rule-state/token decisions; retain generated rule ownership and change grammar/regenerate for syntax changes.

### F119 — [TestCompositeRemoteCredentialOutput() in pkg/cypher/remote_credential_output_test.go:73](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/remote_credential_output_test.go#L73)

Reported as **reuse**; 1 matching declarations:

- [TestCompositeRemoteCredentialErrorOutput() in pkg/cypher/remote_credential_output_test.go:26](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/remote_credential_output_test.go#L26) — behavior; similarity 0.285

**Disposition — C8 — retain separate output boundaries.** Malformed credential-bearing declarations assert the localized error identity, fallback/data redaction and public parse failure without creating a catalog. Successful encrypted declarations exercise CREATE/ALTER/SHOW typed and map projections, source immutability, slow-query logging and local nil URI. A shared success/error fixture would collapse incompatible setup/admission paths; no duplicate credential parser, renderer or redactor is implemented by these tests.

### F120 — [TestNumericWireBoltSignedAndReference() in pkg/bolt/numeric_wire_public_test.go:17](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/numeric_wire_public_test.go#L17)

Reported as **reuse**; 1 matching declarations:

- [TestTypedNumericBoltBoundaries() in pkg/bolt/typed_numeric_wire_test.go:15](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/bolt/typed_numeric_wire_test.go#L15) — behavior; similarity 0.26

**Disposition — B8 — retain independent numeric populations.** Signed-boundary test checks eight exact 64-bit values, nested results and strings against an optional Neo4j driver reference while holding each explicit frame across the population. TypedNumericBoltBoundaries seeds typed properties and tests parameter/WITH/nested/written forms and expression results using listinputs and per-statement transaction ownership. Shared server/driver protocol owners remain; replacing one lifecycle with the other changes the test contract.

### F121 — [ownRetainedMap() in pkg/storage/property_ownership.go:139](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/property_ownership.go#L139)

Reported as **reuse**; 1 matching declarations:

- [ownPropertyMap() in pkg/storage/property_ownership.go:197](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/storage/property_ownership.go#L197) — behavior; similarity 0.214

**Disposition — S1 — corrective action.** Retire ownPropertyMap and migrate callers directly to ownRetainedMap. The reverse indexed match is resolved by removal of the alias, not by dismissing its old source location.

### F122 — [lazyResultTestConn() in pkg/bolt/lazy_result_test.go:53](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/lazy_result_test.go#L53)

Reported as **reuse**; 1 matching declarations:

- [TestLazyResultEndsWhenTheConnectionGoes() in pkg/bolt/lazy_result_edge_test.go:79](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/bolt/lazy_result_edge_test.go#L79) — behavior; similarity 0.189

**Disposition — B9 — retain different fixture boundaries.** lazyResultTestConn drives real TCP/Bolt startup and wire-based seeding. TestLazyResultEndsWhenTheConnectionGoes injects readErrQueue/messageQueue failures into a mock Session after executor seeding, asserting lastLazy release and nextQueuedMessage errors. It cannot use a TCP connection fixture without losing the queue-injection seam; both exercise the same production lazy-result owner.

### F123 — [.streamNodesByLabelProjected() in pkg/storage/badger_transaction_graph_reads.go:462](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/badger_transaction_graph_reads.go#L462)

Reported as **reuse**; 1 matching declarations:

- [.streamNodesByLabelAtSnapshotWithView() in pkg/storage/badger_mvcc_indexed_reads.go:222](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/storage/badger_mvcc_indexed_reads.go#L222) — behavior; similarity 0.188

**Disposition — S4 — corrective action / reuse.** Transaction stream already delegates historical committed population to streamNodesByLabelAtSnapshotWithView and latest buffered population to streamNodesByLabelProjectedInScope; it additionally overlays transaction-local pending/deleted nodes and unlocks around visitors. Remove its inert never-assigned overlay/overlayTouched/emitOverlay copy; retain those transaction responsibilities rather than duplicating the engine scan.

### F124 — [.streamNodesByLabelAtSnapshotWithView() in pkg/storage/badger_mvcc_indexed_reads.go:217](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/badger_mvcc_indexed_reads.go#L217)

Reported as **reuse**; 1 matching declarations:

- [.streamNodesByLabelVisibleAtSnapshotWithView() in pkg/storage/badger_mvcc_indexed_reads.go:218](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/storage/badger_mvcc_indexed_reads.go#L218) — behavior; similarity 0.178

**Disposition — S5 — already converged in current source.** The indexed streamNodesByLabelVisibleAtSnapshotWithView body is a visibility-only forwarding wrapper in 35e3bddbf. It is absent at 38bd71f65 (declaration and callers); current callers pass an explicit visibility policy to streamNodesByLabelAtSnapshotWithView, which alone owns reader registration, indexed iteration and historical selection.

### F125 — [.execute() in pkg/cypher/composite_command_preparation.go:41](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/composite_command_preparation.go#L41)

Reported as **reuse**; 1 matching declarations:

- [.executeCompositeCommand() in pkg/cypher/composite_commands.go:315](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/cypher/composite_commands.go#L315) — behavior; similarity 0.087

**Disposition — C9 — retain preparation/execution composition.** executeCompositeCommand owns manager availability, credential-redacted admission and CREATE/ALTER preparation, then invokes preparedCompositeCommand.execute. The latter alone applies the selected catalog mutation and formats its declared result. Metadata/EXPLAIN reuse preparation without executing. Folding side effects into the preparation wrapper would violate that no-mutation boundary; there are not two catalog mutation bodies.

### F126 — [ownPropertyValue() in pkg/storage/property_ownership.go:29](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/property_ownership.go#L29)

Reported as **reuse**; 1 matching declarations:

- [ownRetainedValue() in pkg/storage/property_ownership.go:69](https://github.com/orneryd/NornicDB/blob/35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6/pkg/storage/property_ownership.go#L69) — forwarder; similarity 0.021

**Disposition — S1 — corrective action.** Retire ownPropertyValue and use ownRetainedValue directly, including recursive list ingestion. Preserve the exactness result where consumed; do not retain a compatibility/forwarding alias.

## Every reported current-source ownership record

A compiler/AST call is supporting evidence, not a blanket waiver. These rows record the actual shared behavior and adjacent correction or distinct remaining contract. Source links intentionally pin the analyzed head; later retired callers are recorded as corrective actions, not silently dropped.

| ID | Candidate and matched owner | Reported binding evidence | Disposition / action |
| --- | --- | --- | --- |
| P001 | [.streamNodesByLabelProjected() / pkg/storage/badger_transaction_graph_reads.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/badger_transaction_graph_reads.go#L462) ↔ [streamVisitUnlocked() / pkg/storage/badger_transaction_graph_reads.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/badger_transaction_graph_reads.go#L422) | `current_owner_call`, `L495` | S4 — streamVisitUnlocked alone owns visitor unlock/retake and panic/error cleanup. Transaction streaming retains pending/deleted overlays and delegates committed populations to existing engine readers; remove its inert unassigned overlay path. |
| P002 | [TestNumericPropertyIndexExactAroundFloatPrecision() / pkg/bolt/numeric_index_precision_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/numeric_index_precision_test.go#L19) ↔ [exactNumberOp() / pkg/bolt/numeric_index_precision_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/numeric_index_precision_test.go#L149) | `current_owner_call`, `L54` | Retain the independent math/big numeric oracle; use production transport but not the production numeric comparator for expected ordering around 2^53. Test scenarios call this one oracle, not copied comparison bodies. |
| P003 | [TestTransactionUpdateEmbeddingStoragePolicy() / pkg/storage/embedding_text_policy_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/embedding_text_policy_test.go#L75) ↔ [sidecarTestNode() / pkg/storage/embedding_sidecar_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/embedding_sidecar_test.go#L20) | `current_owner_call`, `L93` | Retain shared sidecar fixture construction. The transaction test supplies its mutation/storage-policy cases and assertions; it does not rebuild the helper’s node/embedding source. |
| P004 | [TestLegacyDateTimeWireMetadataSignatureNegotiation() / pkg/bolt/legacy_datetime_negotiation_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/legacy_datetime_negotiation_test.go#L85) ↔ [dateTimeWireConnect() / pkg/bolt/legacy_datetime_runtime_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/legacy_datetime_runtime_test.go#L43) | `current_owner_call`, `L110` | Retain the independent negotiation oracle: request exact Bolt4.4/UTC-patch/Bolt5 handshake bytes, assert the selected version and reference agent, and set the matching HELLO patch. Default openBoltTestConn negotiates its configured default and cannot supply those assertions. Socket I/O/PackStream primitives remain shared; a generic connection wrapper would not remove the oracle’s protocol decisions. |
| P005 | [TestLegacyDateTimeWireMetadataSignatureNegotiation() / pkg/bolt/legacy_datetime_negotiation_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/legacy_datetime_negotiation_test.go#L85) ↔ [dateTimeWireQuery() / pkg/bolt/legacy_datetime_runtime_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/legacy_datetime_runtime_test.go#L130) | `current_owner_call`, `L152` | Retain one raw-record owner for these consumers: assert exactly one RECORD then SUCCESS while retaining encoded bytes for signature/location checks. Decoded readBatch cannot provide the raw struct-signature oracle. No second raw temporal-query body is introduced. |
| P006 | [TestCreateDatabaseControls() / pkg/cypher/create_database_convergence_regression_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/create_database_convergence_regression_test.go#L178) ↔ [newCreateDatabaseRegressionExecutor() / pkg/cypher/create_database_convergence_regression_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/create_database_convergence_regression_test.go#L15) | `current_owner_call`, `L186` | Retain shared tracked executor/manager setup; creation controls vary names/existence/output/EXPLAIN expectations through that owner. There is no second manager-lifecycle body in the candidate. |
| P007 | [.parseCreateConstraintTypeDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L1854) ↔ [parseConstraintTypePredicate() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L1310) | `current_owner_call`, `L1883` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| P008 | [.parseCreateConstraintTypeDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L1854) ↔ [parseCreateIndexForPattern() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L806) | `current_owner_call`, `L1878` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| P009 | [ownRetainedValue() / pkg/storage/property_ownership.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/property_ownership.go#L48) ↔ [ownRetainedMap() / pkg/storage/property_ownership.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/property_ownership.go#L139) | `current_owner_call`, `L98` | S1 — remove canonicalPropertyValue/ownPropertyValue/ownedProperties/ownPropertyMap/codecImageProperties aliases and migrate consumers to ownRetainedValue/ownRetainedMap. Retain actual container/value owners: typed-list cloning preserves concrete shapes; canonicalPropertyList owns decoder/ingress homogeneous/mixed representation. Both are called by the same retained-value dispatcher, with no copied conversion bodies. |
| P010 | [ownRetainedValue() / pkg/storage/property_ownership.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/property_ownership.go#L48) ↔ [retainedTypedPropertyList() / pkg/storage/property_ownership.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/property_ownership.go#L150) | `current_owner_call`, `L105` | S1 — remove canonicalPropertyValue/ownPropertyValue/ownedProperties/ownPropertyMap/codecImageProperties aliases and migrate consumers to ownRetainedValue/ownRetainedMap. Retain actual container/value owners: typed-list cloning preserves concrete shapes; canonicalPropertyList owns decoder/ingress homogeneous/mixed representation. Both are called by the same retained-value dispatcher, with no copied conversion bodies. |
| P011 | [ownRetainedValue() / pkg/storage/property_ownership.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/property_ownership.go#L48) ↔ [canonicalPropertyList() / pkg/storage/property_ownership.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/property_ownership.go#L162) | `current_owner_call`, `L120` | S1 — remove canonicalPropertyValue/ownPropertyValue/ownedProperties/ownPropertyMap/codecImageProperties aliases and migrate consumers to ownRetainedValue/ownRetainedMap. Retain actual container/value owners: typed-list cloning preserves concrete shapes; canonicalPropertyList owns decoder/ingress homogeneous/mixed representation. Both are called by the same retained-value dispatcher, with no copied conversion bodies. |
| P012 | [TestCallCancellationKeepsCallerErrorShape() / pkg/cypher/call_cancellation_error_shape_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/call_cancellation_error_shape_test.go#L105) ↔ [callCancellationCreateIndexes() / pkg/cypher/call_cancellation_error_shape_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/call_cancellation_error_shape_test.go#L90) | `current_owner_call`, `L123` | Retain shared index fixture/listing owners. Cancellation consumers add their own statement/COMMIT/discard/retry seam and exact no-publication/namespace assertions, not a second index catalog or seed decoder. |
| P013 | [.parseCreateFulltextIndexDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L1077) ↔ [parseOptionalDDLName() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L734) | `current_owner_call`, `L1110` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| P014 | [.parseCreateFulltextIndexDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L1077) ↔ [extractFulltextPropertiesSegment() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L1036) | `current_owner_call`, `L1135` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| P015 | [.parseCreateFulltextIndexDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L1077) ↔ [parseCreateFulltextForPattern() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L1011) | `current_owner_call`, `L1118` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| P016 | [.executeCreateConstraintContract() / pkg/cypher/schema_contracts.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema_contracts.go#L142) ↔ [ValidateConstraintContractOnCreationForEngine() / pkg/storage/constraint_contracts.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/constraint_contracts.go#L341) | `current_owner_call`, `L186` | Retain the storage contract-validation owner. The Cypher consumer compiles typed entries and invokes validation before schema publication; it does not duplicate storage traversal or constraint evaluation. |
| P017 | [.executeCreateConstraintContract() / pkg/cypher/schema_contracts.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema_contracts.go#L142) ↔ [RefreshUniqueConstraintValuesForConstraintsForEngine() / pkg/storage/constraint_validation.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/constraint_validation.go#L86) | `current_owner_call`, `L223` | Retain distinct post-admission UNIQUE-cache population using its storage owner; pre-creation contract validation is not a cache mutation. The Cypher caller sequences validation/bundle publication/cache refresh instead of implementing either storage algorithm. |
| P018 | [TestListInputBoltRepresentations() / pkg/bolt/list_input_wire_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/list_input_wire_test.go#L45) ↔ [runListInputBoltStatement() / pkg/bolt/list_input_wire_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/list_input_wire_test.go#L18) | `current_owner_call`, `L63` | Retain the shared v5 driver statement/transaction lifecycle. The list-input consumer adds its own representations and exact numeric/scalar expectations; the v4.4 reference driver has a different API and explicit unpatched-protocol assertions. |
| P019 | [.parseCreateConstraintSimplePropertyDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L1943) ↔ [parseConstraintPredicate() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L1270) | `current_owner_call`, `L1972` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| P020 | [.parseCreateConstraintSimplePropertyDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L1943) ↔ [parseOptionalDDLName() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L734) | `current_owner_call`, `L2000` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| P021 | [.parseCreateConstraintSimplePropertyDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L1943) ↔ [parseCreateIndexForPattern() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L806) | `current_owner_call`, `L1967` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| P022 | [.compileBound() / pkg/cypher/result_columns_compile.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/result_columns_compile.go#L69) ↔ [statementSyntaxQuery() / pkg/cypher/query_body_structure.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/query_body_structure.go#L447) | `current_owner_call`, `L87` | Retain canonical CST root selection. compileBound uses its typed query node for the binding/event walk rather than reparsing a body fragment; grammar-root selection has one implementation. |
| P023 | [TestLegacyDateTimeWireSignatureNegotiation() / pkg/bolt/legacy_datetime_negotiation_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/legacy_datetime_negotiation_test.go#L14) ↔ [dateTimeWireConnect() / pkg/bolt/legacy_datetime_runtime_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/legacy_datetime_runtime_test.go#L43) | `current_owner_call`, `L40` | Retain the independent negotiation oracle: request exact Bolt4.4/UTC-patch/Bolt5 handshake bytes, assert the selected version and reference agent, and set the matching HELLO patch. Default openBoltTestConn negotiates its configured default and cannot supply those assertions. Socket I/O/PackStream primitives remain shared; a generic connection wrapper would not remove the oracle’s protocol decisions. |
| P024 | [TestLegacyDateTimeWireSignatureNegotiation() / pkg/bolt/legacy_datetime_negotiation_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/legacy_datetime_negotiation_test.go#L14) ↔ [dateTimeWireQuery() / pkg/bolt/legacy_datetime_runtime_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/legacy_datetime_runtime_test.go#L130) | `current_owner_call`, `L71` | Retain one raw-record owner for these consumers: assert exactly one RECORD then SUCCESS while retaining encoded bytes for signature/location checks. Decoded readBatch cannot provide the raw struct-signature oracle. No second raw temporal-query body is introduced. |
| P025 | [.parseCreateConstraintNodeKeyDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L1409) ↔ [parseOptionalDDLName() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L734) | `current_owner_call`, `L1459` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| P026 | [.parseCreateConstraintNodeKeyDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L1409) ↔ [parseCreateIndexForPattern() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L806) | `current_owner_call`, `L1433` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| P027 | [TestIndexDropProcedureExplicitTransactionRollbackKeepsStagedDrop() / pkg/cypher/index_drop_explicit_schema_visibility_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/index_drop_explicit_schema_visibility_test.go#L154) ↔ [indexDropNamesOf() / pkg/cypher/index_drop_explicit_schema_visibility_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/index_drop_explicit_schema_visibility_test.go#L222) | `current_owner_call`, `L214` | Retain shared index fixture/listing owners. Cancellation consumers add their own statement/COMMIT/discard/retry seam and exact no-publication/namespace assertions, not a second index catalog or seed decoder. |
| P028 | [.parseCreateIndexDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L860) ↔ [parseOptionalDDLName() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L734) | `current_owner_call`, `L893` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| P029 | [.parseCreateIndexDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L860) ↔ [extractIndexPropertiesSegment() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L785) | `current_owner_call`, `L903` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| P030 | [.parseCreateIndexDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L860) ↔ [parseCreateIndexForPattern() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L806) | `current_owner_call`, `L898` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| P031 | [.parseCreateIndexLegacyDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L934) ↔ [parseOptionalDDLName() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L734) | `current_owner_call`, `L958` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| P032 | [TestSearchStreamLifecycleBolt() / pkg/bolt/search_stream_lifecycle_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/search_stream_lifecycle_test.go#L23) ↔ [pullStatement() / pkg/bolt/streaming_qid_compatibility_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/streaming_qid_compatibility_test.go#L167) | `current_owner_call`, `L54` | B4 — one PULL/readBatch decoder; preserve success-only assertions at strict consumers and return failure codes to lifecycle/error tests. QID and result-lifecycle scenarios remain distinct. |
| P033 | [TestBoltCrossSessionMergeUniqueConflict_Concurrent_UnwindExplicitTx() / pkg/bolt/cross_session_merge_unique_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/cross_session_merge_unique_test.go#L524) ↔ [runBoltUnwindMergeExplicitTx() / pkg/bolt/cross_session_merge_unique_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/cross_session_merge_unique_test.go#L332) | `current_owner_call`, `L560` | B2/B6 — retire the duplicate explicit transaction attempt/retry envelope and query-only adapter; actual callers construct their query/params and use the canonical retry owner. Preserve the UNWIND input population and concurrency. |
| P034 | [TestIndexDropProcedureCancellationPublishesNothing() / pkg/cypher/index_drop_procedure_cancellation_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/index_drop_procedure_cancellation_test.go#L189) ↔ [newIndexDropFixture() / pkg/cypher/index_drop_procedure_cancellation_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/index_drop_procedure_cancellation_test.go#L144) | `current_owner_call`, `L205` | Retain shared index fixture/listing owners. Cancellation consumers add their own statement/COMMIT/discard/retry seam and exact no-publication/namespace assertions, not a second index catalog or seed decoder. |
| P035 | [TestBoltExplicitTxStatementUniqueViolationCode() / pkg/bolt/cross_session_merge_unique_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/cross_session_merge_unique_test.go#L595) ↔ [runBoltQueryAndCollectRecords() / pkg/bolt/integration_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/integration_test.go#L226) | `current_owner_call`, `L652` | B1 — retire the no-parameter collector and its duplicate body. All listed scenarios call runBoltQueryWithParams directly (nil for absent params); retain their distinct queries and exact result assertions. |
| P036 | [sameStoredValue() / pkg/storage/badger_embedding_sidecar.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/badger_embedding_sidecar.go#L295) ↔ [compareConstraintNumbers() / pkg/storage/constraint_list_equality.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/constraint_list_equality.go#L89) | `current_owner_call`, `L311` | Retain shared exact-number comparison: sameStoredValue calls compareConstraintNumbers rather than widening numeric values itself. Stored-image comparison additionally permits identical NaN bits and recursively compares maps/lists/time instants; constraint ordering keeps NaN unordered. Their different equality policies must not be erased; numeric decisions are already centralized. |
| P037 | [TestNodeRuntimeCacheFillFormsPreserveMetadataAndIdentity() / pkg/storage/node_runtime_cache_contract_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/node_runtime_cache_contract_test.go#L185) ↔ [runtimeMetadataFull() / pkg/storage/node_runtime_cache_contract_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/node_runtime_cache_contract_test.go#L32) | `current_owner_call`, `L229` | Retain fixture-value reuse: one helper creates metadata fields and another synthetic runtime provenance/identity. Cache fill/store tests use those fixtures to assert copy/detach/policy behavior; they do not implement a second metadata or identity owner. |
| P038 | [TestNodeRuntimeCacheFillFormsPreserveMetadataAndIdentity() / pkg/storage/node_runtime_cache_contract_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/node_runtime_cache_contract_test.go#L185) ↔ [runtimeSyntheticIdentity() / pkg/storage/node_runtime_cache_contract_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/node_runtime_cache_contract_test.go#L72) | `current_owner_call`, `L190` | Retain fixture-value reuse: one helper creates metadata fields and another synthetic runtime provenance/identity. Cache fill/store tests use those fixtures to assert copy/detach/policy behavior; they do not implement a second metadata or identity owner. |
| P039 | [.parseCreateConstraintRelationshipKeyOrCompositeUniqueDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L1803) ↔ [parseCreateIndexForPattern() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L806) | `current_owner_call`, `L1809` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| P040 | [.parseCreateConstraintRelationshipKeyOrCompositeUniqueDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L1803) ↔ [parseConstraintQualifiedProperty() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L1250) | `current_owner_call`, `L1841` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| P041 | [TestExplicitTransactionStreamsResultsByQID() / pkg/bolt/streaming_qid_compatibility_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/streaming_qid_compatibility_test.go#L13) ↔ [pullStatement() / pkg/bolt/streaming_qid_compatibility_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/streaming_qid_compatibility_test.go#L167) | `current_owner_call`, `L26` | B4 — one PULL/readBatch decoder; preserve success-only assertions at strict consumers and return failure codes to lifecycle/error tests. QID and result-lifecycle scenarios remain distinct. |
| P042 | [TestExplicitTransactionStreamsResultsByQID() / pkg/bolt/streaming_qid_compatibility_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/streaming_qid_compatibility_test.go#L13) ↔ [runStatementAndQID() / pkg/bolt/streaming_qid_compatibility_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/streaming_qid_compatibility_test.go#L157) | `current_owner_call`, `L21` | Retain one RUN/QID extraction owner. Consumers separately test live-stream isolation versus unknown-QID/IGNORED/RESET state; B4 shares subsequent PULL decoding. |
| P043 | [TestLegacyDateTimeGraphPropertiesNeo4j526() / pkg/bolt/legacy_datetime_properties_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/legacy_datetime_properties_test.go#L29) ↔ [dateTimeWireConnect() / pkg/bolt/legacy_datetime_runtime_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/legacy_datetime_runtime_test.go#L43) | `current_owner_call`, `L48` | Retain the independent negotiation oracle: request exact Bolt4.4/UTC-patch/Bolt5 handshake bytes, assert the selected version and reference agent, and set the matching HELLO patch. Default openBoltTestConn negotiates its configured default and cannot supply those assertions. Socket I/O/PackStream primitives remain shared; a generic connection wrapper would not remove the oracle’s protocol decisions. |
| P044 | [TestLegacyDateTimeGraphPropertiesNeo4j526() / pkg/bolt/legacy_datetime_properties_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/legacy_datetime_properties_test.go#L29) ↔ [dateTimeWireQuery() / pkg/bolt/legacy_datetime_runtime_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/legacy_datetime_runtime_test.go#L130) | `current_owner_call`, `L62` | Retain one raw-record owner for these consumers: assert exactly one RECORD then SUCCESS while retaining encoded bytes for signature/location checks. Decoded readBatch cannot provide the raw struct-signature oracle. No second raw temporal-query body is introduced. |
| P045 | [TestLegacyDateTimeGraphPropertiesNeo4j526() / pkg/bolt/legacy_datetime_properties_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/legacy_datetime_properties_test.go#L29) ↔ [dateTimeWire() / pkg/bolt/legacy_datetime_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/legacy_datetime_test.go#L42) | `current_owner_call`, `L68` | Retain the independent expected wire-value builder (seconds/signature/zone) and its negative opposite-signature controls. It shares PackStream primitives, not the production temporal decision being tested. |
| P046 | [TestNumericZeroAndNaNIndexKeys() / pkg/bolt/numeric_zero_keys_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/numeric_zero_keys_test.go#L16) ↔ [runBoltQueryWithParams() / pkg/bolt/merge_admission_family_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/merge_admission_family_test.go#L101) | `current_owner_call`, `L52` | B1 — retire the no-parameter collector and its duplicate body. All listed scenarios call runBoltQueryWithParams directly (nil for absent params); retain their distinct queries and exact result assertions. |
| P047 | [TestNumericZeroAndNaNIndexKeys() / pkg/bolt/numeric_zero_keys_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/numeric_zero_keys_test.go#L16) ↔ [runBoltQueryAndCollectRecords() / pkg/bolt/integration_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/integration_test.go#L226) | `current_owner_call`, `L33` | B1 — retire the no-parameter collector and its duplicate body. All listed scenarios call runBoltQueryWithParams directly (nil for absent params); retain their distinct queries and exact result assertions. |
| P048 | [.parseCreateConstraintForRequireHead() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L1369) ↔ [parseOptionalDDLName() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L734) | `current_owner_call`, `L1397` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| P049 | [TestBoltCountSubqueryCommentGap() / pkg/bolt/count_subquery_comment_protocol_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/count_subquery_comment_protocol_test.go#L18) ↔ [runBoltQueryAndCollectRecords() / pkg/bolt/integration_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/integration_test.go#L226) | `current_owner_call`, `L45` | B1 — retire the no-parameter collector and its duplicate body. All listed scenarios call runBoltQueryWithParams directly (nil for absent params); retain their distinct queries and exact result assertions. |
| P050 | [TestMergeAdmissionTupleIntegerPrecision() / pkg/bolt/merge_admission_family_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/merge_admission_family_test.go#L63) ↔ [runBoltQueryWithParams() / pkg/bolt/merge_admission_family_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/merge_admission_family_test.go#L101) | `current_owner_call`, `L74` | B1 — retire the no-parameter collector and its duplicate body. All listed scenarios call runBoltQueryWithParams directly (nil for absent params); retain their distinct queries and exact result assertions. |
| P051 | [TestNodeRuntimeCacheStorePreservesMetadataAndPolicy() / pkg/storage/node_runtime_cache_contract_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/node_runtime_cache_contract_test.go#L145) ↔ [runtimeSyntheticIdentity() / pkg/storage/node_runtime_cache_contract_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/node_runtime_cache_contract_test.go#L72) | `current_owner_call`, `L147` | Retain fixture-value reuse: one helper creates metadata fields and another synthetic runtime provenance/identity. Cache fill/store tests use those fixtures to assert copy/detach/policy behavior; they do not implement a second metadata or identity owner. |
| P052 | [.parseCreateConstraintCardinalityDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L1668) ↔ [parseCardinalityRequireExpr() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L1647) | `current_owner_call`, `L1677` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| P053 | [TestBoltExplicitTransactionsCreateIndependentSameLabelNodes() / pkg/bolt/concurrent_same_label_transaction_e2e_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/concurrent_same_label_transaction_e2e_test.go#L11) ↔ [runBoltQueryAndCollectRecords() / pkg/bolt/integration_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/integration_test.go#L226) | `current_owner_call`, `L39` | B1 — retire the no-parameter collector and its duplicate body. All listed scenarios call runBoltQueryWithParams directly (nil for absent params); retain their distinct queries and exact result assertions. |
| P054 | [TestLegacyDateTimeWireSignatureFailurePreservesState() / pkg/bolt/legacy_datetime_failure_state_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/legacy_datetime_failure_state_test.go#L12) ↔ [dateTimeWireConnect() / pkg/bolt/legacy_datetime_runtime_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/legacy_datetime_runtime_test.go#L43) | `current_owner_call`, `L27` | Retain the independent negotiation oracle: request exact Bolt4.4/UTC-patch/Bolt5 handshake bytes, assert the selected version and reference agent, and set the matching HELLO patch. Default openBoltTestConn negotiates its configured default and cannot supply those assertions. Socket I/O/PackStream primitives remain shared; a generic connection wrapper would not remove the oracle’s protocol decisions. |
| P055 | [TestLegacyDateTimeWireSignatureFailurePreservesState() / pkg/bolt/legacy_datetime_failure_state_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/legacy_datetime_failure_state_test.go#L12) ↔ [dateTimeWireQuery() / pkg/bolt/legacy_datetime_runtime_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/legacy_datetime_runtime_test.go#L130) | `current_owner_call`, `L35` | Retain one raw-record owner for these consumers: assert exactly one RECORD then SUCCESS while retaining encoded bytes for signature/location checks. Decoded readBatch cannot provide the raw struct-signature oracle. No second raw temporal-query body is introduced. |
| P056 | [TestDatabaseCommands_CreateDatabase_WhitespacePermutations() / pkg/cypher/database_commands_whitespace_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/database_commands_whitespace_test.go#L60) ↔ [whitespacePermutations() / pkg/cypher/database_commands_whitespace_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/database_commands_whitespace_test.go#L21) | `current_owner_call`, `L73` | Retain one permutation generator used by CREATE/DROP tests; F115 records the distinct seed/state/output oracles. No duplicated whitespace reader is retained. |
| P057 | [TestPipelineApplyProcedureCallEdges() / pkg/cypher/pipeline_procedure_call_coverage_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/pipeline_procedure_call_coverage_test.go#L56) ↔ [registerCoverageProcedures() / pkg/cypher/pipeline_procedure_call_coverage_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/pipeline_procedure_call_coverage_test.go#L15) | `current_owner_call`, `L57` | Retain cleanup-owned procedure registration shared by the procedure cases. Each case supplies a different pipeline/empty/error contract; it does not re-register the same procedure bodies itself. |
| P058 | [projectCachedNodeForRead() / pkg/storage/badger_queries.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/badger_queries.go#L637) ↔ [getNodeProjectedThrough() / pkg/storage/wrapper_capabilities.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/wrapper_capabilities.go#L196) | `extracted_owner`, `L204` | Retain one cached-image projection owner called by engine cache hits and wrapper fallback; decoded storage reads use the existing projected decoder. Node-copy callbacks own metadata/vector detachment. ProjectedNodeReader constrains property keys, not mandatory vector absence, so the inspected fallback’s optional vectors do not violate that interface. |
| P059 | [projectCachedNodeForRead() / pkg/storage/badger_queries.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/badger_queries.go#L637) ↔ [.getNodeProjected() / pkg/storage/badger_nodes.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/badger_nodes.go#L453) | `extracted_owner`, `L460` | Retain one cached-image projection owner called by engine cache hits and wrapper fallback; decoded storage reads use the existing projected decoder. Node-copy callbacks own metadata/vector detachment. ProjectedNodeReader constrains property keys, not mandatory vector absence, so the inspected fallback’s optional vectors do not violate that interface. |
| P060 | [projectCachedNodeForRead() / pkg/storage/badger_queries.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/badger_queries.go#L637) ↔ [copyNodeWithoutEmbeddings() / pkg/storage/transaction.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/transaction.go#L152) | `current_owner_call`, `L653` | Retain one cached-image projection owner called by engine cache hits and wrapper fallback; decoded storage reads use the existing projected decoder. Node-copy callbacks own metadata/vector detachment. ProjectedNodeReader constrains property keys, not mandatory vector absence, so the inspected fallback’s optional vectors do not violate that interface. |
| P061 | [schemaOptionsTail() / pkg/cypher/schema_options.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema_options.go#L50) ↔ [.parseCreateIndexLegacyDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L934) | `extracted_owner`, `L982` | C2/C3 — the listed statement consumer calls the shared OPTIONS admission owner; it has no second map/parameter-tail decoder. Further converge adjacent ON/property/head scanning and retire family-only forwarding wrappers. Keep each family’s admitted syntax and error identity. |
| P062 | [schemaOptionsTail() / pkg/cypher/schema_options.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema_options.go#L50) ↔ [.parseCreateIndexDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L860) | `extracted_owner`, `L907` | C2/C3 — the listed statement consumer calls the shared OPTIONS admission owner; it has no second map/parameter-tail decoder. Further converge adjacent ON/property/head scanning and retire family-only forwarding wrappers. Keep each family’s admitted syntax and error identity. |
| P063 | [schemaOptionsTail() / pkg/cypher/schema_options.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema_options.go#L50) ↔ [.parseCreateConstraintForRequireHead() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L1369) | `extracted_owner`, `L1402` | C2/C3 — the listed statement consumer calls the shared OPTIONS admission owner; it has no second map/parameter-tail decoder. Further converge adjacent ON/property/head scanning and retire family-only forwarding wrappers. Keep each family’s admitted syntax and error identity. |
| P064 | [schemaOptionsTail() / pkg/cypher/schema_options.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema_options.go#L50) ↔ [.parseCreateFulltextIndexDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L1077) | `extracted_owner`, `L1139` | C2/C3 — the listed statement consumer calls the shared OPTIONS admission owner; it has no second map/parameter-tail decoder. Further converge adjacent ON/property/head scanning and retire family-only forwarding wrappers. Keep each family’s admitted syntax and error identity. |
| P065 | [schemaOptionsTail() / pkg/cypher/schema_options.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema_options.go#L50) ↔ [.parseCreateConstraintNodeKeyDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L1409) | `extracted_owner`, `L1469` | C2/C3 — the listed statement consumer calls the shared OPTIONS admission owner; it has no second map/parameter-tail decoder. Further converge adjacent ON/property/head scanning and retire family-only forwarding wrappers. Keep each family’s admitted syntax and error identity. |
| P066 | [serveBoltIntegrationServer() / pkg/bolt/integration_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/integration_test.go#L181) ↔ [startBoltIntegrationServer() / pkg/bolt/integration_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/integration_test.go#L153) | `extracted_owner`, `L173` | B3 — shared constructor owns executor/configuration, serveBoltIntegrationServer owns public Serve/listen/join, and callers own special fixtures. Remove remaining copied startup/HELLO/close bodies where contracts match. The second fixture was traced through private serve to the same public Serve method and retired. All callers use this owner with their original optional shutdown observer; deliberate close failures remain asserted. |
| P067 | [serveBoltIntegrationServer() / pkg/bolt/integration_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/integration_test.go#L181) ↔ [TestBoltCypherIntegration() / pkg/bolt/integration_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/integration_test.go#L258) | `extracted_owner`, `L277` | B3 — shared constructor owns executor/configuration, serveBoltIntegrationServer owns public Serve/listen/join, and callers own special fixtures. Remove remaining copied startup/HELLO/close bodies where contracts match. The second fixture was traced through private serve to the same public Serve method and retired. All callers use this owner with their original optional shutdown observer; deliberate close failures remain asserted. |
| P068 | [startBoltIntegrationServer() / pkg/bolt/integration_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/integration_test.go#L153) ↔ [serveBoltIntegrationServer() / pkg/bolt/integration_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/integration_test.go#L181) | `current_owner_call`, `L173` | B3 — shared constructor owns executor/configuration, serveBoltIntegrationServer owns public Serve/listen/join, and callers own special fixtures. Remove remaining copied startup/HELLO/close bodies where contracts match. The second fixture was traced through private serve to the same public Serve method and retired. All callers use this owner with their original optional shutdown observer; deliberate close failures remain asserted. |
| P069 | [startBoltIntegrationServer() / pkg/bolt/integration_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/integration_test.go#L153) ↔ [boltLoadBinaryServer() / pkg/bolt/apoc_load_binary_wire_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/apoc_load_binary_wire_test.go#L109) | `extracted_owner`, `L116` | B3 — shared constructor owns executor/configuration, serveBoltIntegrationServer owns public Serve/listen/join, and callers own special fixtures. Remove remaining copied startup/HELLO/close bodies where contracts match. The second fixture was traced through private serve to the same public Serve method and retired. All callers use this owner with their original optional shutdown observer; deliberate close failures remain asserted. |
| P070 | [roundTripLocalDateTimeScenario() / pkg/bolt/localdatetime_roundtrip_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/localdatetime_roundtrip_test.go#L103) ↔ [runBoltStatementNoRecords() / pkg/bolt/localdatetime_roundtrip_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/localdatetime_roundtrip_test.go#L124) | `current_owner_call`, `L111` | Retain the prebuilt-RUN-message consumer: temporal input tests must inject hand-encoded parameter bytes rather than reconstruct them through SendRun. It shares the underlying message I/O owner; decoded query/parameter collection is a different input contract. |
| P071 | [roundTripLocalDateTimeScenario() / pkg/bolt/localdatetime_roundtrip_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/localdatetime_roundtrip_test.go#L103) ↔ [TestBoltIntegration_LocalDateTimeParamRoundTrip_ServerStack() / pkg/bolt/localdatetime_roundtrip_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/localdatetime_roundtrip_test.go#L71) | `extracted_owner`, `L75` | Retain one roundTripLocalDateTimeScenario for scalar and UNWIND prebuilt messages, preserving store readback and exact temporal values. Shared server/connection owners already own lifecycle. |
| P072 | [TestMalformedMatchStatementsRejectedLikeNeo4j() / pkg/cypher/match_semantic_validation_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/match_semantic_validation_test.go#L108) ↔ [requireMatchSemanticDetail() / pkg/cypher/match_semantic_validation_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/match_semantic_validation_test.go#L202) | `current_owner_call`, `L117` | Retain one assertion owner for the typed semantic detail/localization identity. Test populations supply distinct malformed statements and expected identities; no second string/status classifier remains. |
| P073 | [TestCypherMatchingLocalizedErrorsHaveTypedIdentity() / pkg/cypher/matching_localization_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/matching_localization_test.go#L24) ↔ [requireCypherMatchingLocalizedError() / pkg/cypher/matching_localization_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/matching_localization_test.go#L13) | `current_owner_call`, `L34` | Retain one assertion owner for the typed semantic detail/localization identity. Test populations supply distinct malformed statements and expected identities; no second string/status classifier remains. |
| P074 | [.parseCreateConstraintPolicyDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L1786) ↔ [parsePolicyModeRequireExpr() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L1769) | `current_owner_call`, `L1795` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| P075 | [.parseCreateConstraintPolicyDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L1786) ↔ [parsePolicyForClause() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L1715) | `current_owner_call`, `L1791` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| P076 | [endpointCheckMatchesGetNode() / pkg/storage/nor160_endpoint_visibility_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/nor160_endpoint_visibility_test.go#L20) ↔ [TestBadgerTransactionRelationshipEndpointVisible() / pkg/storage/relationship_endpoint_checker_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/relationship_endpoint_checker_test.go#L91) | `extracted_owner`, `L147` | S6 — use endpointCheckMatchesGetNode for the answered/GetNode agreement assertion; retain each scenario’s explicit answered/visibility expectations. Remove the local copied assertion, not the peer/delete/reveal/decay populations. |
| P077 | [.parseCreateConstraintDomainDDL() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L1600) ↔ [parseDomainConstraintPredicate() / pkg/cypher/schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema.go#L1575) | `current_owner_call`, `L1605` | C2/C3 — retain one owner for each actual name/pattern/predicate decision, and converge the surrounding duplicated constraint heads/property-list extraction. Remove family-only forwarding adapters and unsupported ON/ASSERT branches; preserve current FOR/REQUIRE grammar and family-specific typed predicates/results (F087/F098). |
| P078 | [boltLoadBinaryServer() / pkg/bolt/apoc_load_binary_wire_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/apoc_load_binary_wire_test.go#L109) ↔ [startBoltIntegrationServer() / pkg/bolt/integration_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/integration_test.go#L153) | `current_owner_call`, `L116` | B3 — shared constructor owns executor/configuration, serveBoltIntegrationServer owns public Serve/listen/join, and callers own special fixtures. Remove remaining copied startup/HELLO/close bodies where contracts match. The second fixture was traced through private serve to the same public Serve method and retired. All callers use this owner with their original optional shutdown observer; deliberate close failures remain asserted. |
| P079 | [boltLoadBinaryServer() / pkg/bolt/apoc_load_binary_wire_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/apoc_load_binary_wire_test.go#L109) ↔ [TestBoltLoadBinaryWireTransmitsBytes() / pkg/bolt/apoc_load_binary_wire_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/apoc_load_binary_wire_test.go#L123) | `extracted_owner`, `L128` | Retain specialized fixture composition: create the named APOC-load store through the shared server owner, then test the actual byte handler/serialized bytes. B1 removes its duplicated statement collector; the fixture does not reimplement server lifecycle. |
| P080 | [lazyResultTestConn() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/lazy_result_test.go#L53) ↔ [openBoltTestConn() / pkg/bolt/integration_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/integration_test.go#L206) | `current_owner_call`, `L57` | Retain fixture composition: shared store/server/connection owners plus explicit wire-based row seeding. Each consumer keeps batch-size/exhaustion/timeout/discard/reset/RUN assertions. Mock Session queue-injection tests cannot use a TCP-only fixture without losing their failure seam (F122). |
| P081 | [lazyResultTestConn() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/lazy_result_test.go#L53) ↔ [TestBoltLazyResultClientTimeoutStatus() / pkg/bolt/deadline_status_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/deadline_status_test.go#L57) | `extracted_owner`, `L58` | Retain fixture composition: shared store/server/connection owners plus explicit wire-based row seeding. Each consumer keeps batch-size/exhaustion/timeout/discard/reset/RUN assertions. Mock Session queue-injection tests cannot use a TCP-only fixture without losing their failure seam (F122). |
| P082 | [lazyResultTestConn() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/lazy_result_test.go#L53) ↔ [TestBoltLazyResultDeliversEveryRowInBatches() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/lazy_result_test.go#L67) | `extracted_owner`, `L68` | Retain fixture composition: shared store/server/connection owners plus explicit wire-based row seeding. Each consumer keeps batch-size/exhaustion/timeout/discard/reset/RUN assertions. Mock Session queue-injection tests cannot use a TCP-only fixture without losing their failure seam (F122). |
| P083 | [lazyResultTestConn() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/lazy_result_test.go#L53) ↔ [TestBoltLazyResultLeavesOtherStatementsWhole() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/lazy_result_test.go#L192) | `extracted_owner`, `L193` | Retain fixture composition: shared store/server/connection owners plus explicit wire-based row seeding. Each consumer keeps batch-size/exhaustion/timeout/discard/reset/RUN assertions. Mock Session queue-injection tests cannot use a TCP-only fixture without losing their failure seam (F122). |
| P084 | [lazyResultTestConn() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/lazy_result_test.go#L53) ↔ [TestBoltLazyResultEndsOnDiscardResetAndRun() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/lazy_result_test.go#L152) | `extracted_owner`, `L153` | Retain fixture composition: shared store/server/connection owners plus explicit wire-based row seeding. Each consumer keeps batch-size/exhaustion/timeout/discard/reset/RUN assertions. Mock Session queue-injection tests cannot use a TCP-only fixture without losing their failure seam (F122). |
| P085 | [lazyResultTestConn() / pkg/bolt/lazy_result_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/lazy_result_test.go#L53) ↔ [TestBoltLazyResultDiscardReachesTheError() / pkg/bolt/lazy_result_edge_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/lazy_result_edge_test.go#L118) | `extracted_owner`, `L119` | Retain fixture composition: shared store/server/connection owners plus explicit wire-based row seeding. Each consumer keeps batch-size/exhaustion/timeout/discard/reset/RUN assertions. Mock Session queue-injection tests cannot use a TCP-only fixture without losing their failure seam (F122). |
| P086 | [ownRetainedMap() / pkg/storage/property_ownership.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/property_ownership.go#L139) ↔ [ownRetainedValue() / pkg/storage/property_ownership.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/property_ownership.go#L48) | `current_owner_call`, `L143` | S1 — remove canonicalPropertyValue/ownPropertyValue/ownedProperties/ownPropertyMap/codecImageProperties aliases and migrate consumers to ownRetainedValue/ownRetainedMap. Retain actual container/value owners: typed-list cloning preserves concrete shapes; canonicalPropertyList owns decoder/ingress homogeneous/mixed representation. Both are called by the same retained-value dispatcher, with no copied conversion bodies. |
| P087 | [constraintRejectedInExplicitTransaction() / pkg/cypher/constraint_checked_writes_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/constraint_checked_writes_test.go#L31) ↔ [TestExplicitTransactionConstraintViolationFailsTheStatement() / pkg/cypher/constraint_checked_writes_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/constraint_checked_writes_test.go#L101) | `extracted_owner`, `L131` | Retain the existing common BEGIN/rejected statement/terminal lifecycle; keep distinct constraint populations and COMMIT/ROLLBACK/post-state oracles. F117 names the pair; neither consumer has its own rejection lifecycle copy. |
| P088 | [constraintRejectedInExplicitTransaction() / pkg/cypher/constraint_checked_writes_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/constraint_checked_writes_test.go#L31) ↔ [TestExplicitTransactionMergeUpdateViolationsFailTheStatement() / pkg/cypher/constraint_checked_writes_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/constraint_checked_writes_test.go#L156) | `extracted_owner`, `L184` | Retain the existing common BEGIN/rejected statement/terminal lifecycle; keep distinct constraint populations and COMMIT/ROLLBACK/post-state oracles. F117 names the pair; neither consumer has its own rejection lifecycle copy. |
| P089 | [schemaOptionsParameter() / pkg/cypher/schema_options.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema_options.go#L90) ↔ [scanParameterReferences() / pkg/cypher/parameters.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/parameters.go#L141) | `current_owner_call`, `L91` | Retain direct reuse: scanParameterReferences alone owns quote-aware parameter-reference scanning; this consumer accepts only the one reference spanning the entire OPTIONS expression. It does not implement another reference scanner. |
| P090 | [retainedTypedPropertyList() / pkg/storage/property_ownership.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/property_ownership.go#L150) ↔ [ownRetainedValue() / pkg/storage/property_ownership.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/property_ownership.go#L48) | `extracted_owner`, `L105` | S1 — remove canonicalPropertyValue/ownPropertyValue/ownedProperties/ownPropertyMap/codecImageProperties aliases and migrate consumers to ownRetainedValue/ownRetainedMap. Retain actual container/value owners: typed-list cloning preserves concrete shapes; canonicalPropertyList owns decoder/ingress homogeneous/mixed representation. Both are called by the same retained-value dispatcher, with no copied conversion bodies. |
| P091 | [schemaOptionsEndsStatement() / pkg/cypher/schema_options.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/schema_options.go#L78) ↔ [queryGapEnd() / pkg/cypher/keyword_scan.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/keyword_scan.go#L1056) | `current_owner_call`, `L81` | Retain direct reuse: queryGapEnd/readQueryGap own comments/trivia. This consumer checks that their returned end consumes the remaining statement; no second comment scanner is retained. |
| P092 | [TestLegacyDateTimeWire() / pkg/bolt/legacy_datetime_runtime_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/legacy_datetime_runtime_test.go#L32) ↔ [testDateTimeWire() / pkg/bolt/legacy_datetime_runtime_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/legacy_datetime_runtime_test.go#L75) | `current_owner_call`, `L36` | B5 — consolidate the identical local store/server setup into one local target matrix; retain this scenario runner as the shared local/reference oracle. Keep v4.4 driver negotiation, raw wire fixtures and independent DST transition values distinct. |
| P093 | [TestLegacyDateTimeRealDriver() / pkg/bolt/legacy_datetime_runtime_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/legacy_datetime_runtime_test.go#L146) ↔ [testLegacyDateTimeDriver() / pkg/bolt/legacy_datetime_runtime_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/legacy_datetime_runtime_test.go#L179) | `current_owner_call`, `L150` | B5 — consolidate the identical local store/server setup into one local target matrix; retain this scenario runner as the shared local/reference oracle. Keep v4.4 driver negotiation, raw wire fixtures and independent DST transition values distinct. |
| P094 | [TestLegacyDateTimeWireNamedZoneTransitions() / pkg/bolt/legacy_datetime_transitions_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/legacy_datetime_transitions_test.go#L13) ↔ [testDateTimeWireNamedZoneTransitions() / pkg/bolt/legacy_datetime_transitions_test.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/bolt/legacy_datetime_transitions_test.go#L24) | `current_owner_call`, `L17` | B5 — consolidate the identical local store/server setup into one local target matrix; retain this scenario runner as the shared local/reference oracle. Keep v4.4 driver negotiation, raw wire fixtures and independent DST transition values distinct. |
| P095 | [fixedCommandScopeFacts() / pkg/cypher/result_columns_compile.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/result_columns_compile.go#L62) ↔ [.compileBound() / pkg/cypher/result_columns_compile.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/result_columns_compile.go#L69) | `extracted_owner`, `L77` | M1 — consolidate remaining inline privilege-command metadata into fixedCommandScopeFacts with an explicit scope-transfer policy. Preserve incoming execution names for privileges and declared output scope for CREATE/SHOW. Public StatementFacts checks cover all four fixed-schema families. |
| P096 | [fixedCommandScopeFacts() / pkg/cypher/result_columns_compile.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/result_columns_compile.go#L62) ↔ [projectionScope() / pkg/cypher/result_columns_program.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/result_columns_program.go#L82) | `current_owner_call`, `L65` | M1 — consolidate remaining inline privilege-command metadata into fixedCommandScopeFacts with an explicit scope-transfer policy. Preserve incoming execution names for privileges and declared output scope for CREATE/SHOW. Public StatementFacts checks cover all four fixed-schema families. |
| P097 | [ownedValueCopy() / pkg/storage/property_copy.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/property_copy.go#L28) ↔ [ownedEntries() / pkg/storage/property_ownership.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/property_ownership.go#L38) | `extracted_owner`, `L43` | Retain structural ownership composition: ownedValueCopy is the cycle/type-preserving value walker, ownedEntries owns map allocation/entries, and cloneConstraint copies the schema definition then replaces its reference fields using that walker. These are different input layers sharing the same value-copy implementation, not alternative copy algorithms. |
| P098 | [ownedValueCopy() / pkg/storage/property_copy.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/property_copy.go#L28) ↔ [cloneConstraint() / pkg/storage/schema_ownership.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/storage/schema_ownership.go#L12) | `extracted_owner`, `L17` | Retain structural ownership composition: ownedValueCopy is the cycle/type-preserving value walker, ownedEntries owns map allocation/entries, and cloneConstraint copies the schema definition then replaces its reference fields using that walker. These are different input layers sharing the same value-copy implementation, not alternative copy algorithms. |

### O001 — [.canonicalConstraintRequirement() / pkg/cypher/constraint_requirement.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/cypher/constraint_requirement.go) ↔ [CypherSchemaConstraintEntityMismatch() / pkg/localization/messages_cypher_schema.go](https://github.com/orneryd/NornicDB/blob/38bd71f65bfc191270af1f9774be934d042f83ac/pkg/localization/messages_cypher_schema.go)

**Retain the canonical typed error owner.** `canonicalConstraintRequirement` validates requested NODE/REL/RELATIONSHIP identity against the parsed FOR entity, then calls `CypherSchemaConstraintEntityMismatch` for the exact localized mismatch identity. The localized-message constructor does not parse or validate the statement. Shared error construction and grammar decisions already have separate single owners; no copied mismatch decision or forwarding compatibility wrapper is retained. Reported binding: `L47` / `extracted_ast_call`.

## Corrective action checklist and verification

| Code | Required correction | Verification / state |
| --- | --- | --- |
| S1 | Retire property normalization/exactness forwarding aliases; canonical callers only | Applied in the integration tree; source search finds no retired calls. Focused storage checks pass (2.083s). Census names follow the same canonical ownership boundary, not a new exemption. |
| S4/S6 | Remove inert transaction overlay duplicate; share endpoint agreement assertion | Applied; focused overlay/endpoint/stored-image/NaN checks pass (2.083s). |
| M1 | Share fixed-command metadata construction while preserving scope differences | Applied; public privilege/CREATE/composite/SHOW facts controls pass (1.005s). |
| C2/C3 | Converge schema head/pattern/property/OPTIONS reading; retire unsupported legacy parsing and family forwarding adapters | Applied; final schema checks pass (native 2.211s; ANTLR plus NOR-300/metadata 3.262s). Both-parser child processes pin public admission, rejection without mutation and private error contracts. Baseline 38bd confirms the parenthesized fulltext extension is native-only; no grammar widening or skipped case is used. |
| B1/B2/B3/B4 | One Bolt result collector, explicit transaction/retry envelope, compatible startup and PULL decoder | Applied; migrated canonical callers, removed both startup copies and retained shutdown observers. Combined Bolt checks pass (3.811s); expanded affected-fixture checks pass (60.140s). |
| B5/B6/B7 | Converge temporal local fixture and concurrent-test lifecycle populations | Applied; both namespace/concurrency populations and all three temporal runners pass. Each temporal case retains its own fresh store/server and checked close; rows, rounds and independent oracles are preserved. |
| CI diagnostics | Preserve package progress, memory evidence and logs on termination | `make test` remains the gate with identical Go flags/assertions. Flush grep output immediately, sample process RSS/cgroup counters, and attempt artifact upload even on cancellation. Workflow YAML and shell syntax checked; CI runtime outcome pending. |

The `38bd71f65` CI run ended 143 after 27m42s in its test step, with no test name
or completed-package result retained and no uploaded artifact. It is incomplete,
not an assertion failure attributed to a named test or proof of OOM. The previous
release run's actual NOR-136 invariant failure and its deterministic correction
remain separately recorded. Final combined `make test` belongs to CI; no local
full-suite pass or full `make verify` is claimed here.

The lexical inventory was reviewed for exactly three retired declarations, three new shared owners and five mechanism transitions. The already-required canonical delimiter owner remains in scanner_minimum; no classifier, rule, assertion or test was disabled. Lexical controls pass (1.938s); credential normalization and field-policy controls pass in their actual Cypher (1.445s) and observability (0.415s) packages. Compiled/ingress ownership population checks pass (41.513s).

The final public Serve fixture also passes its targeted race population (8.592s). Baseline admission was independently repeated on `38bd71f65` in isolated parser processes (1.170s): the fulltext parenthesized extension differs by parser, while a healthy REQUIRE block succeeds in both. New assertions preserve those outcomes and require malformed tails to leave the catalog unchanged. No new production syntax defect was inferred from an incorrect test expectation.
