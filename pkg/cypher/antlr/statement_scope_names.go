// Author: Codex R17 implementation lane, 1 November 2026.
package antlr

import (
	"context"
	"strings"
)

// ScopeNames is the scope-list fact of an existing grammar node. Nil means
// wildcard, while a non-nil empty list means an explicit empty import.
func (list SyntaxNode) ScopeNames() []string {
	if !list.Valid() {
		return []string{}
	}
	if _, all := list.DirectToken(CypherLexerMULT); all {
		return nil
	}
	names := []string{}
	for i := 0; i < list.ChildCount(); i++ {
		if name, ok := list.Child(i).Name(); ok {
			names = append(names, name)
		}
	}
	return names
}

// ReadScopeNames is the compatibility ingress for a scope-list value. Within
// an execution it reads facts of actual CALL nodes, including empty/repeated
// scopes. Import-name facts do not carry occurrence-specific binding IDs.
// It never fabricates a CALL wrapper or parses an overlapping source window.
func ReadScopeNames(ctx context.Context, statement Statement, cypher25 bool, stage SourceStage) ([]string, error) {
	if err := syntaxStageError(ctx, statement.text, stage); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if scope := statementSyntaxScopeFrom(ctx); scope != nil {
		key := statementSyntaxKey{source: statement.text, entry: ScopeListEntry, cypher25: cypher25, stage: stage}
		if err := scope.acquire(ctx, key); err != nil {
			return nil, err
		}
		scope.Lock()
		var data *statementSyntaxData
		if len(scope.roots) > 0 {
			data = scope.roots[0].data
		}
		scope.Unlock()
		if data == nil {
			return nil, unretainedSyntaxError()
		}
		var names []string
		found := false
		(StatementSyntax{data}).Walk(func(node SyntaxNode) func() {
			if found || node.Rule() != CypherParserRULE_callSubquery {
				return nil
			}
			open, hasOpen := node.DirectToken(CypherLexerLPAREN)
			close, hasClose := node.DirectToken(CypherLexerRPAREN)
			if !hasOpen || !hasClose {
				return nil
			}
			_, start := open.InputSpan()
			end, _ := close.InputSpan()
			if start < 0 || end < start || end > len(data.source) {
				return nil
			}
			if strings.TrimSpace(data.source[start:end]) == strings.TrimSpace(statement.text) {
				names = node.ChildRule(CypherParserRULE_scopeList).ScopeNames()
				found = true
			}
			return nil
		})
		if found {
			return names, nil
		}
		return nil, unretainedSyntaxError()
	}
	syntax, err := ParseSyntax(ctx, statement, ScopeListEntry, cypher25, stage)
	if err != nil {
		return nil, err
	}
	return syntax.Root().ScopeNames(), nil
}
