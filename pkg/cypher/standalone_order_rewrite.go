package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
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
	rewrite := &queryRewrite{Source: cyphertext.Source{Original: query, Edits: make([]cyphertext.SourceEdit, 0, len(edits))}, verbatimColumns: true}
	var out strings.Builder
	out.Grow(len(query) + 8*len(edits))
	last := 0
	for _, edit := range edits {
		out.WriteString(query[last:edit.start])
		canonStart := out.Len()
		out.WriteString(edit.text)
		rewrite.Edits = append(rewrite.Edits, cyphertext.SourceEdit{Original: cyphertext.Span{Start: edit.start, End: edit.end}, Accepted: cyphertext.Span{Start: canonStart, End: out.Len()}})
		last = edit.end
	}
	out.WriteString(query[last:])
	rewrite.Text = out.String()
	return rewrite.Text, rewrite
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
	braces := newSubqueryBraceScanner(query, start)
	inProjection, stage := false, standaloneStageItems
	// previousWord is the last word read, "" after any other token; last is
	// the last character of the last token, comments and spaces skipped (0
	// at the start of the block).
	previousWord, last := "", rune(0)
	for index := start; index < end; {
		character := query[index]
		switch {
		case isCypherQuoteByte(character):
			index = skipCypherQuotedText(query, index, character)
			previousWord, last = "", rune(character)
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
			close := cyphertext.FindMatchingDelimiter(query[:end], index, rune(character), rune(closer), cyphertext.OpeningDelimiter)
			if close < 0 {
				return
			}
			scanStandaloneOrderContainers(query, index+1, close, edits)
			index = close + 1
			previousWord, last = "", rune(closer)
			continue
		case character == '{':
			close := cyphertext.FindMatchingDelimiter(query[:end], index, '{', '}', cyphertext.OpeningDelimiter)
			if close < 0 {
				return
			}
			if braces.at(index) {
				scanStandaloneOrderClauses(query, index+1, close, edits)
			} else {
				scanStandaloneOrderContainers(query, index+1, close, edits)
			}
			index = close + 1
			previousWord, last = "", '}'
			continue
		}
		name, ok := cyphertext.ScanIdentifier(query, index)
		if !ok {
			if character == '%' && last != 0 && strings.ContainsRune(":|&!", last) {
				// The label wildcard (a:%) is a complete operand, not the
				// modulo operator.
				previousWord, last = "", ')'
			} else if (character == '+' || character == '*') && last == ')' && quantifiedGroupQuantifierAt(query, index) {
				// A quantified path pattern's quantifier ends the pattern:
				// ((a)-->(b))+ RETURN ….
				previousWord = ""
			} else if !cyphertext.IsWhitespaceAt(query, index) {
				previousWord, last = "", rune(character)
			}
			index++
			continue
		}
		if index > start && (query[index-1] == '.' || query[index-1] == '$') {
			previousWord, last = "", cyphertext.RuneAt(query, name.End-1)
			index = name.End
			continue
		}
		upper := standaloneKeyword(name.Value)
		kind := standaloneClauseKind(query, upper, name.End, end)
		if kind == standaloneStageItems && standaloneOperandPosition(previousWord, last) {
			// A keyword where an operand is expected is a variable:
			// RETURN finish, n.id AS optional, WITH a, match.
			previousWord, last = "", cyphertext.RuneAt(query, name.End-1)
			index = name.End
			continue
		}
		if upper == "USE" {
			// The USE grammar owns its graph reference, which may be spelled
			// like a clause keyword: USE GRAPH limit CALL { … }.
			if referenceEnd := useClauseReferenceEnd(query[:end], index, start > 0); referenceEnd > name.End {
				reference := strings.TrimRightFunc(query[index:referenceEnd], cyphertext.IsWhitespace)
				inProjection = false
				previousWord, last = "", cyphertext.RuneAt(reference, len(reference)-1)
				index = referenceEnd
				continue
			}
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
			previousWord, last = upper, cyphertext.RuneAt(query, name.End-1)
			index = name.End
			continue
		}
		if !standaloneClausePosition(previousWord, last) {
			// A variable: x > limit, AS skip.
			previousWord, last = "", cyphertext.RuneAt(query, name.End-1)
			index = name.End
			continue
		}
		if !inProjection || stage >= kind {
			*edits = append(*edits, labelRewriteEdit{start: index, end: index, text: "WITH * "})
			inProjection = true
		}
		stage = kind
		if upper == "OFFSET" {
			*edits = append(*edits, labelRewriteEdit{start: index, end: name.End, text: "SKIP"})
		}
		if upper == "ORDER" {
			name.End = skipSpaces(query, name.End) + len("BY")
		}
		previousWord, last = upper, cyphertext.RuneAt(query, name.End-1)
		index = name.End
	}
}

// standaloneKeyword is word's upper-case form when it is a keyword the scan
// reads, else "". The result is a constant, so scanning allocates nothing.
func standaloneKeyword(word string) string {
	if len(word) > len("CONTAINS") {
		return ""
	}
	var buffer [len("CONTAINS")]byte
	for index := 0; index < len(word); index++ {
		character := word[index]
		if 'a' <= character && character <= 'z' {
			character -= 'a' - 'A'
		}
		buffer[index] = character
	}
	// Indexing a map by a converted byte slice doesn't allocate.
	return standaloneKeywords[string(buffer[:len(word)])]
}

// standaloneKeywords maps each keyword standaloneKeyword reads to itself, so
// the name it returns is the map's string, not the scan buffer.
var standaloneKeywords = func() map[string]string {
	keywords := make(map[string]string)
	for _, keyword := range []string{"AND", "OR", "XOR", "NOT", "IN", "IS", "AS", "BY", "THEN", "ELSE", "WHEN", "CASE", "DISTINCT",
		"STARTS", "ENDS", "CONTAINS", "WITH", "RETURN", "WHERE", "UNWIND", "SET", "SKIP", "OFFSET", "LIMIT",
		"ORDER", "YIELD", "MATCH", "OPTIONAL", "FOR", "LET", "FILTER", "CREATE", "MERGE", "REMOVE",
		"DELETE", "DETACH", "FOREACH", "CALL", "UNION", "LOAD", "USE", "FINISH"} {
		keywords[keyword] = keyword
	}
	return keywords
}()

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
func standaloneOperandPosition(previousWord string, last rune) bool {
	return standaloneOperandWord(previousWord) || (last != 0 && strings.ContainsRune(",:=<>+-/%^|", last))
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
func standaloneClausePosition(previousWord string, last rune) bool {
	switch {
	case last == 0:
		return true
	case standaloneOperandWord(previousWord):
		return false
	}
	return cyphertext.NamePart(last) || strings.ContainsRune(")]}'\"`", last)
}

// scanStandaloneOrderContainers visits expression containers without treating
// their identifiers as statement clauses. Only classified subquery bodies have
// an independent projection/order state; offsets stay in the original query.
func scanStandaloneOrderContainers(query string, start, end int, edits *[]labelRewriteEdit) {
	braces := newSubqueryBraceScanner(query, start)
	for index := start; index < end; index++ {
		switch cypherLexicalByte(query[index]) {
		case cypherQuoteClass:
			index = skipCypherQuotedText(query, index, query[index]) - 1
		case '/':
			if next := queryCommentEnd(query, index); next > index {
				index = next - 1
			}
		case '{':
			if !braces.at(index) {
				continue
			}
			close := cyphertext.FindMatchingDelimiter(query[:end], index, '{', '}', cyphertext.OpeningDelimiter)
			if close < 0 {
				return
			}
			scanStandaloneOrderClauses(query, index+1, close, edits)
			index = close
		}
	}
}
