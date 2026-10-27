package antlr

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"
)

// UseReference is the shared grammar's graph selector product. End is a UTF-8
// byte offset in the original source, including the optional GRAPH word and
// excluding its follower. AcceptedEnd separately retains the grammar cursor.
// Args retain the supplied view's original spelling.
// Callers own routing policy, never selector text or clause boundaries.
type UseReference struct {
	Name        string
	Function    string
	Args        []string
	End         int
	AcceptedEnd int
}

func ReadUseReference(ctx context.Context, source *cyphertext.Source) (UseReference, error) {
	if source.Err != nil {
		return UseReference{}, source.Err
	}
	syntax, err := ParseSyntax(ctx, source.Text, UseEntry, false, AcceptedSource)
	if err != nil {
		if syntax.Valid() && syntax.data.useError != nil {
			return UseReference{}, syntax.data.useError
		}
		return UseReference{}, err
	}
	return syntax.Root().UseReference(source), nil
}

func (node SyntaxNode) UseReference(source *cyphertext.Source) UseReference {
	_, end := node.Span()
	reference := UseReference{End: end, AcceptedEnd: node.data.nodes[node.id].end}
	if source != nil {
		span, _ := source.OriginalSpan(cyphertext.Span{Start: 0, End: reference.AcceptedEnd}, cyphertext.SourceExtent)
		reference.End = span.End
	}
	name := node.ChildRule(CypherParserRULE_invocationName).QualifiedName()
	args := node.ChildRule(CypherParserRULE_useGraphArguments)
	if !args.Valid() {
		reference.Name = name
		return reference
	}
	reference.Function = name
	chain := args.ChildRule(CypherParserRULE_expressionChain)
	if chain.Valid() {
		for i := 0; i < chain.ChildCount(); i++ {
			child := chain.Child(i)
			if child.Rule() == CypherParserRULE_expression {
				text := child.Text()
				if source != nil {
					start, end := child.Span()
					span, ok := source.OriginalSpan(cyphertext.Span{Start: start, End: end}, cyphertext.SourceExtent)
					if ok {
						text = source.Original[span.Start:span.End]
					}
				}
				reference.Args = append(reference.Args, text)
			}
		}
	}
	return reference
}

// useGraphKeyword is the sole contextual GRAPH decision, invoked by the shared
// grammar for both full statements and leading selector views. A following
// clause's '(' cannot turn MATCH into a graph-reference function.
func (p *CypherParser) useGraphKeyword() bool {
	stream := p.GetTokenStream()
	if !strings.EqualFold(stream.LT(1).GetText(), "GRAPH") {
		return false
	}
	next := stream.LT(2)
	if next.GetTokenType() == -1 || next.GetTokenType() == CypherParserDOT || next.GetTokenType() == CypherParserLPAREN {
		return false
	}
	if !IsUseClauseFollower(next.GetText(), false) {
		return true
	}
	after := stream.LT(3)
	if after.GetTokenType() == CypherParserDOT {
		return true
	}
	return IsUseClauseFollower(after.GetText(), false)
}

func IsUseClauseFollower(word string, inSubquery bool) bool {
	followers := useClauseFollowers[:]
	if inSubquery {
		followers = subqueryUseClauseFollowers[:]
	}
	for _, follower := range followers {
		if strings.EqualFold(word, follower) {
			return true
		}
	}
	return false
}

// useClauseFollowers are the words Neo4j's grammar accepts after the graph
// reference of a top-level USE clause, in the order of Neo4j's SyntaxError
// "expected …" list (Neo4j 5.26). START / STOP DATABASE and ENABLE SERVER
// are listed by their first word.
var useClauseFollowers = [...]string{"FOREACH", "ALTER", "ORDER", "CALL", "CREATE", "LOAD", "START", "STOP",
	"DEALLOCATE", "DELETE", "DENY", "DETACH", "DROP", "DRYRUN", "FINISH", "GRANT", "INSERT", "LIMIT", "MATCH",
	"MERGE", "NODETACH", "OFFSET", "OPTIONAL", "REALLOCATE", "REMOVE", "RENAME", "RETURN", "REVOKE", "ENABLE",
	"SET", "SHOW", "SKIP", "TERMINATE", "UNION", "UNWIND", "USE", "WITH"}

// subqueryUseClauseFollowers are the words Neo4j's grammar accepts after
// the graph reference of a USE clause that starts a subquery (CALL { USE …
// }): the query clauses, no administration commands.
var subqueryUseClauseFollowers = [...]string{"FOREACH", "ORDER", "CALL", "CREATE", "LOAD", "DELETE", "DETACH",
	"FINISH", "INSERT", "LIMIT", "MATCH", "MERGE", "NODETACH", "OFFSET", "OPTIONAL", "REMOVE", "RETURN", "SET",
	"SKIP", "UNION", "UNWIND", "USE", "WITH"}

// UseReferenceError contains only grammar position/role, never source excerpts.
type UseReferenceError struct {
	Offset             int
	Function, NamePart bool
}

func (*UseReferenceError) Error() string { return "invalid USE reference" }

// A selector cannot stop before a dotted part or before its argument list.
func (p *CypherParser) useReferenceComplete() bool {
	next := p.GetTokenStream().LA(1)
	return next != CypherParserDOT && next != CypherParserLPAREN && !(next == CypherParserFLOAT && strings.HasPrefix(p.GetTokenStream().LT(1).GetText(), "."))
}
