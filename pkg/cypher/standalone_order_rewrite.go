package cypher

import (
	"strings"
)

// desugarStandaloneOrderClauses rewrites ORDER BY, SKIP / OFFSET and LIMIT
// used as clauses of their own into the WITH * they stand for, and OFFSET
// into SKIP, once, for every route; the result's errors are mapped back.
//
// Neo4j 5.26.30 accepts them between any clauses ("MATCH (n) ORDER BY n.v
// LIMIT 2 RETURN n"), each run equivalent to WITH * ORDER BY … SKIP …
// LIMIT …, and OFFSET as a synonym of SKIP. A run belongs to the WITH or
// RETURN before it while it keeps that clause's ORDER BY → SKIP → LIMIT order;
// any other one starts its own WITH * (#907). Without this the text after a
// pattern was read as part of the MATCH and the statement matched nothing.
func desugarStandaloneOrderClauses(query string) (string, *queryRewrite) {
	if indexASCIIFold(query, "order") < 0 && indexASCIIFold(query, "skip") < 0 &&
		indexASCIIFold(query, "offset") < 0 && indexASCIIFold(query, "limit") < 0 {
		return query, nil
	}
	var edits []labelRewriteEdit
	scanStandaloneOrderClauses(query, 0, len(query), &edits)
	if len(edits) == 0 {
		return query, nil
	}
	rewrite := &queryRewrite{original: query, edits: make([]queryTextEdit, 0, len(edits)), verbatimColumns: true}
	var out strings.Builder
	out.Grow(len(query) + 8*len(edits))
	last := 0
	for _, edit := range edits {
		out.WriteString(query[last:edit.start])
		canonStart := out.Len()
		out.WriteString(edit.text)
		rewrite.edits = append(rewrite.edits, queryTextEdit{origStart: edit.start, origEnd: edit.end, canonStart: canonStart, canonEnd: out.Len()})
		last = edit.end
	}
	out.WriteString(query[last:])
	rewrite.canonical = out.String()
	return rewrite.canonical, rewrite
}

// Stages of a projection's tail, in the order its clauses may come.
const (
	standaloneStageItems = iota
	standaloneStageOrder
	standaloneStageSkip
	standaloneStageLimit
	standaloneStageWhere
)

// scanStandaloneOrderClauses scans the statement or subquery body
// query[start:end], appending its edits in order.
func scanStandaloneOrderClauses(query string, start, end int, edits *[]labelRewriteEdit) {
	inProjection, stage := false, standaloneStageItems
	// previousWord is the last word read, "" after any other token; last is
	// the last character of the last token, comments and spaces skipped (0
	// at the start of the block).
	previousWord, last := "", byte(0)
	for index := start; index < end; {
		character := query[index]
		switch {
		case character == '\'' || character == '"' || character == '`':
			index = skipCypherQuotedText(query, index, character)
			previousWord, last = "", character
			continue
		case character == '/':
			if commentEnd := queryCommentEnd(query, index); commentEnd > index {
				index = commentEnd
				continue
			}
		case character == '(' || character == '[':
			closer := byte(')')
			if character == '[' {
				closer = ']'
			}
			close := findMatchingDelimiter(query[:end], index, rune(character), rune(closer))
			if close < 0 {
				return
			}
			index = close + 1
			previousWord, last = "", closer
			continue
		case character == '{':
			close := findMatchingDelimiter(query[:end], index, '{', '}')
			if close < 0 {
				return
			}
			if isSubqueryBrace(query, index) {
				scanStandaloneOrderClauses(query, index+1, close, edits)
			}
			index = close + 1
			previousWord, last = "", '}'
			continue
		}
		name, next, ok := scanIdentifierToken(query, index)
		if !ok {
			if character == '%' && last != 0 && strings.IndexByte(":|&!", last) >= 0 {
				// The label wildcard (a:%) is a complete operand, not the
				// modulo operator.
				previousWord, last = "", ')'
			} else if character > ' ' {
				previousWord, last = "", character
			}
			index++
			continue
		}
		if index > start && (query[index-1] == '.' || query[index-1] == '$') {
			previousWord, last = "", query[next-1]
			index = next
			continue
		}
		upper := upperASCII(name)
		kind := standaloneClauseKind(query, upper, next, end)
		if kind == standaloneStageItems && standaloneOperandPosition(previousWord, last) {
			// A keyword where an operand is expected is a variable:
			// RETURN finish, n.id AS optional, WITH a, match.
			previousWord, last = "", query[next-1]
			index = next
			continue
		}
		if kind == standaloneStageItems {
			switch upper {
			case "WITH", "RETURN", "YIELD":
				// YIELD (SHOW … YIELD x ORDER BY x) has its own tail, as WITH.
				inProjection, stage = true, standaloneStageItems
			case "WHERE":
				if inProjection {
					stage = standaloneStageWhere
				}
			case "MATCH", "OPTIONAL", "UNWIND", "FOR", "LET", "FILTER", "CREATE", "MERGE", "SET", "REMOVE",
				"DELETE", "DETACH", "FOREACH", "CALL", "UNION", "LOAD", "USE", "FINISH":
				inProjection = false
			}
			previousWord, last = upper, query[next-1]
			index = next
			continue
		}
		if !standaloneClausePosition(previousWord, last) {
			// A variable: x > limit, AS skip.
			previousWord, last = "", query[next-1]
			index = next
			continue
		}
		if !inProjection || stage >= kind {
			*edits = append(*edits, labelRewriteEdit{start: index, end: index, text: "WITH * "})
			inProjection = true
		}
		stage = kind
		if upper == "OFFSET" {
			*edits = append(*edits, labelRewriteEdit{start: index, end: next, text: "SKIP"})
		}
		if upper == "ORDER" {
			next = skipSpaces(query, next) + len("BY")
		}
		previousWord, last = upper, query[next-1]
		index = next
	}
}

// standaloneOperandWord reports whether word (upper case) expects an
// operand after it: an operator word, AS, BY, or a clause's opening keyword.
func standaloneOperandWord(word string) bool {
	switch word {
	case "AND", "OR", "XOR", "NOT", "IN", "IS", "AS", "BY", "THEN", "ELSE", "WHEN", "CASE", "DISTINCT",
		"STARTS", "ENDS", "CONTAINS", "WITH", "RETURN", "WHERE", "UNWIND", "SET", "SKIP", "OFFSET", "LIMIT":
		return true
	}
	return false
}

// standaloneOperandPosition reports whether an operand is expected after
// the token previousWord / last: after a standaloneOperandWord, a comma or an
// operator character (not *, which after WITH or RETURN is the wildcard).
func standaloneOperandPosition(previousWord string, last byte) bool {
	return standaloneOperandWord(previousWord) || (last != 0 && strings.IndexByte(",:=<>+-/%^|", last) >= 0)
}

// standaloneClauseKind is the projection stage the word upper at
// query[:next] starts (ORDER BY, SKIP / OFFSET, LIMIT), or
// standaloneStageItems for any other word.
func standaloneClauseKind(query, upper string, next, end int) int {
	switch upper {
	case "ORDER":
		after := skipSpaces(query, next)
		if after+len("BY") <= end && matchKeywordAt(query, after, "BY") {
			return standaloneStageOrder
		}
	case "SKIP", "OFFSET":
		return standaloneStageSkip
	case "LIMIT":
		return standaloneStageLimit
	}
	return standaloneStageItems
}

// standaloneClausePosition reports whether a clause can start after the
// token previousWord / last: at the start of the block, or after a complete
// expression or pattern, not after an operator or a word that expects an
// operand (a variable named skip or limit is read as one: x > limit, AS skip).
func standaloneClausePosition(previousWord string, last byte) bool {
	switch {
	case last == 0:
		return true
	case standaloneOperandWord(previousWord):
		return false
	}
	return isIdentByte(last) || strings.IndexByte(")]}'\"`", last) >= 0
}
