package cypher

import (
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
	return validateArrowOperatorsWithCancellation(query, nil)
}

func validateArrowOperatorsWithCancellation(query string, probe *pipelineCancellation) error {
	if probe != nil && !probe.check() {
		return probe.err
	}
	if !containsTextWithCancellation(query, "->", probe) {
		if probe != nil {
			return probe.err
		}
		return nil
	}
	for i := 0; i+1 < len(query); i++ {
		if probe != nil && !probe.check() {
			return probe.err
		}
		switch c := query[i]; c {
		case '\'', '"', '`':
			end, _ := scanCypherQuotedTextWithCancellation(query, i, c, probe)
			i = end - 1
		case '/':
			if end := queryCommentEndWithCancellation(query, i, probe); end >= 0 {
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
	if probe != nil {
		return probe.err
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
	return validatePlaceholderOperandsWithCancellation(query, nil)
}

func validatePlaceholderOperandsWithCancellation(query string, probe *pipelineCancellation) error {
	if probe != nil && !probe.check() {
		return probe.err
	}
	if indexByteWithCancellation(query, '<', probe) < 0 {
		if probe != nil {
			return probe.err
		}
		return nil
	}
	for i := 0; i+1 < len(query); i++ {
		if probe != nil && !probe.check() {
			return probe.err
		}
		switch c := query[i]; c {
		case '\'', '"', '`':
			end, _ := scanCypherQuotedTextWithCancellation(query, i, c, probe)
			i = end - 1
		case '/':
			if end := queryCommentEndWithCancellation(query, i, probe); end >= 0 {
				i = end - 1
			}
		case '<':
			if !isIdentStartByte(query[i+1]) {
				continue
			}
			before := i - 1
			for before >= 0 && isASCIIWhitespace(query[before]) {
				if probe != nil && !probe.check() {
					return probe.err
				}
				before--
			}
			if before >= 0 && strings.IndexByte(":=,([{", query[before]) >= 0 {
				return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
					localization.CypherCoreInvalidInputExpectedExpression("<"))
			}
		}
	}
	if probe != nil {
		return probe.err
	}
	return nil
}
