// Author: Codex R17 implementation agent, 29 October 2026; occurrence form:
// Claude R17 implementation agent, 30 October 2026.
package cypher

import (
	"context"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// pipelineClauseAt is the retained clause occurrence of statement at byte
// offset at (retainedClauseAt): text-side operators address a clause of a
// statement they hold, never a clause text they compose.
func pipelineClauseAt(ctx context.Context, statement string, at int, rules ...int) (pipelineClause, bool) {
	node, ok := retainedClauseAt(ctx, statement, at, rules...)
	if !ok {
		var absent pipelineClause
		return absent, false
	}
	return pipelineClauseFromSyntax(node)
}

// withClauseAt is the WITH occurrence of statement at offset at.
func withClauseAt(ctx context.Context, statement string, at int) (pipelineClause, bool) {
	clause, ok := pipelineClauseAt(ctx, statement, at, cypherantlr.CypherParserRULE_withSt)
	return clause, ok && clause.kind == pipelineClauseWith
}

// returnClauseAt is the RETURN occurrence of statement at offset at.
func returnClauseAt(ctx context.Context, statement string, at int) (pipelineClause, bool) {
	clause, ok := pipelineClauseAt(ctx, statement, at, cypherantlr.CypherParserRULE_returnSt)
	return clause, ok && clause.kind == pipelineClauseReturn
}
