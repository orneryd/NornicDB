package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// validateArrowOperators rejects a "->" that ends no relationship pattern.
// In Cypher an arrow head only closes a relationship: --> or -[…]->, so its
// '-' follows another '-' or a ']'. Anywhere else (an expression such as
// SET n.m = 'x' -> [..] or RETURN f(n) -> 1) Neo4j 5.26.30 fails the
// statement when it compiles it, with "Invalid input '>': expected an
// expression"; NornicDB wrote the value or failed only when a row was
// evaluated (#461, #523). Strings and comments are skipped.
func validateArrowOperators(query string) error {
	if !strings.Contains(query, "->") {
		return nil
	}
	for i := 0; i+1 < len(query); i++ {
		switch c := query[i]; cypherLexicalByte(c) {
		case cypherQuoteClass:
			i = skipCypherQuotedText(query, i, c) - 1
		case '/':
			if end := queryCommentEnd(query, i); end >= 0 {
				i = end - 1
			}
		case '-':
			if query[i+1] != '>' {
				continue
			}
			if i > 0 && (query[i-1] == '-' || query[i-1] == ']') {
				continue
			}
			return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
				localization.CypherCoreInvalidInputExpectedExpression(">"))
		}
	}
	return nil
}

// validatePlaceholderOperands rejects a '<' where an operand must start (after
// ':', '=', ',', '(', '[' or '{') followed by a name: <expr>, n:<Label>,
// {v: <x>}. A comparison or <> follows an operand and a pattern arrow follows
// ')' or ']', so no valid statement has one; Neo4j 5.26.30 fails it with
// "Invalid input '<': expected an expression". NornicDB's MERGE stored the
// text as the value and DDL took it as a label (#504, #217).
func validatePlaceholderOperands(query string) error {
	if strings.IndexByte(query, '<') < 0 {
		return nil
	}
	for i := 0; i+1 < len(query); i++ {
		switch c := query[i]; cypherLexicalByte(c) {
		case cypherQuoteClass:
			i = skipCypherQuotedText(query, i, c) - 1
		case '/':
			if end := queryCommentEnd(query, i); end >= 0 {
				i = end - 1
			}
		case '<':
			if !cyphertext.NameStartAt(query, i+1) {
				continue
			}
			before := i - 1
			for before >= 0 && cyphertext.IsWhitespaceAt(query, before) {
				before--
			}
			if before >= 0 && strings.IndexByte(":=,([{", query[before]) >= 0 {
				return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
					localization.CypherCoreInvalidInputExpectedExpression("<"))
			}
		}
	}
	return nil
}
