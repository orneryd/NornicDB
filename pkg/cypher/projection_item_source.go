package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

type projectionSourceStage uint8

const (
	projectionAdmitted projectionSourceStage = iota
	projectionBeforeRewrite
)

// projectionItemSource retains source spelling separately from the admitted
// expression/name. An explicitly empty alias still has a nonnegative AS offset.
type projectionItemSource struct {
	expression, alias       string
	rawExpression, rawAlias string
	as                      int
}

// readProjectionItemSource owns alias selection products at both actual stages.
// Pre-rewrite grouping retains its permissive last-word/source-spelling policy;
// admitted consumers use the retained grammar AS and execution projection names.
func readProjectionItemSource(ctx context.Context, item string, stage projectionSourceStage) projectionItemSource {
	out := projectionItemSource{as: -1}
	var clause cypherantlr.SyntaxNode
	if stage == projectionBeforeRewrite {
		trimmed := strings.TrimSpace(item)
		leading := len(item) - len(strings.TrimLeftFunc(item, cyphertext.IsWhitespace))
		for _, word := range projectionWords(trimmed) {
			if word.depth == 0 && word.upper == "AS" {
				out.as = leading + word.start
			}
		}
	} else {
		const prefix = "RETURN "
		var ok bool
		clause, ok = canonicalQueryClause(ctx, prefix+item)
		if !ok {
			return out
		}
		items := clause.ChildRule(cypherantlr.CypherParserRULE_projectionBody).ChildRule(cypherantlr.CypherParserRULE_projectionItems)
		if items.Valid() {
			projection := items.ChildRule(cypherantlr.CypherParserRULE_projectionItem)
			if projection.Valid() {
				if token, found := projection.DirectToken(cypherantlr.CypherLexerAS); found {
					start, _ := token.Span()
					out.as = start - len(prefix)
				}
			}
		}
	}
	// Both stages share original source boundaries. Semantic code/decoded names
	// below must not replace raw alias text used by AST and diagnostic consumers.
	out.rawExpression = strings.TrimSpace(item)
	out.rawAlias = out.rawExpression
	if out.as >= 0 {
		out.rawExpression = strings.TrimSpace(item[:out.as])
		out.rawAlias = strings.TrimSpace(item[out.as+len("AS"):])
	}
	if stage == projectionBeforeRewrite {
		out.expression, out.alias = out.rawExpression, out.rawAlias
		if out.as >= 0 {
			if name, ok := cyphertext.ScanIdentifier(out.rawAlias, 0); ok && name.End == len(out.rawAlias) {
				out.alias = name.Value
			}
		}
		return out
	}
	plan := projectionPlanFromSyntax(clause)
	if plan.valid && plan.star && len(plan.starItems) == 0 {
		out.expression, out.alias = "*", "*"
	} else if plan.valid && !plan.star && len(plan.projections) == 1 {
		out.expression, out.alias = plan.projections[0].expr, plan.projections[0].alias
	}
	return out
}
