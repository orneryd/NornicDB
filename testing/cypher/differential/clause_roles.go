package differential

import (
	"strings"

	"github.com/orneryd/nornicdb/internal/cyphertext"
)

type clauseFacts struct {
	arbitrary     bool
	writes        bool
	calls         bool
	hiddenNames   []cyphertext.Span
	nestedClauses []cyphertext.Span
}

// clause admits a grammatical marker, rather than a name with its spelling.
func (facts *clauseFacts) clause(span cyphertext.Span, root bool) {
	if last := len(facts.hiddenNames) - 1; last >= 0 && facts.hiddenNames[last].Start == span.Start {
		facts.hiddenNames = facts.hiddenNames[:last]
	}
	if !root {
		facts.nestedClauses = append(facts.nestedClauses, span)
	}
}

// roleMask preserves offsets in the lexical view. Root consumers exclude child
// query clauses; collection-order consumers retain those clauses' own markers.
func (facts clauseFacts) roleMask(mask string, root bool) string {
	if len(facts.hiddenNames) == 0 && (!root || len(facts.nestedClauses) == 0) {
		return mask
	}
	hidden := []byte(mask)
	spans := facts.hiddenNames
	for pass := 0; pass < 2; pass++ {
		for _, span := range spans {
			for index := span.Start; index < span.End; index++ {
				hidden[index] = '_'
			}
		}
		if !root {
			break
		}
		spans = facts.nestedClauses
	}
	return string(hidden)
}

// inspectClauseRoles distinguishes query clauses from expression names before
// granting the oracle's arbitrary-row exemption. It uses lexical primitives,
// not the implementation's parser or execution normalization. Each nested query
// owns its ordering state; ordinary maps, patterns and arguments are expressions.
func inspectClauseRoles(query string) clauseFacts {
	text, _ := maskQuery(query, maskOpaqueSyntax)
	var facts clauseFacts
	inspectClauseRegion(text, 0, len(text), true, &facts)
	return facts
}

func inspectClauseRegion(text string, start, end int, query bool, facts *clauseFacts) {
	wantOperand, named, ordered := false, false, false
	header := ""
	for index := start; index < end; {
		if width := cyphertext.WhitespaceAt(text, index); width > 0 {
			index += width
			continue
		}
		character := text[index]
		if cyphertext.IsQuote(character) {
			index, _ = cyphertext.ScanQuotedText(text, index)
			wantOperand, named, header = false, false, ""
			continue
		}
		if character == '(' || character == '[' || character == '{' {
			closer := ')'
			if character == '[' {
				closer = ']'
			} else if character == '{' {
				closer = '}'
			}
			close := cyphertext.FindMatchingDelimiter(text[:end], index, rune(character), closer, cyphertext.OpeningDelimiter)
			if close < 0 {
				return
			}
			childQuery := character == '{' && header != "" && oracleQueryBody(text, index+1, close)
			inspectClauseRegion(text, index+1, close, childQuery, facts)
			// CALL (imports) { query } retains its admitted query header.
			if character != '(' || header != "CALL" {
				header = ""
			}
			wantOperand, named = false, false
			index = close + 1
			continue
		}
		if cyphertext.NumberStartsAt(text, index) {
			number, _ := cyphertext.ScanNumber(text, index)
			index = number.TailEnd
			wantOperand, named, header = false, false, ""
			continue
		}
		if cyphertext.NameStartAt(text, index) {
			wordStart := index
			after := cyphertext.NamePartEnd(text, index)
			word := strings.ToUpper(text[index:after])
			if cyphertext.KeywordEnd(text, index, word, cyphertext.CypherWord) != after {
				word = ""
			}
			index = after
			switch word {
			case "WITH", "RETURN", "LIMIT", "SKIP", "OFFSET", "ORDER", "BY", "UNION", "AS":
				facts.hiddenNames = append(facts.hiddenNames, cyphertext.Span{Start: wordStart, End: after})
			}
			if named {
				wantOperand, named, header = false, false, ""
				continue
			}
			header = ""
			if word == "EXISTS" || word == "COUNT" || word == "COLLECT" {
				header = word
			}
			if query {
				switch word {
				case "WITH", "RETURN":
					if !wantOperand {
						facts.clause(cyphertext.Span{Start: wordStart, End: after}, start == 0)
						ordered, wantOperand = false, true
						continue
					}
				case "LIMIT", "SKIP", "OFFSET":
					if !wantOperand {
						facts.clause(cyphertext.Span{Start: wordStart, End: after}, start == 0)
						facts.arbitrary = facts.arbitrary || !ordered
						wantOperand = true
						continue
					}
				case "ORDER":
					if !wantOperand {
						by, _ := cyphertext.GapEnd(text, index)
						if afterBy := cyphertext.KeywordEnd(text, by, "BY", cyphertext.CypherWord); afterBy >= 0 {
							facts.clause(cyphertext.Span{Start: wordStart, End: afterBy}, start == 0)
							ordered, wantOperand, index = true, true, afterBy
							continue
						}
					}
				case "CREATE", "MERGE", "SET", "DELETE", "REMOVE", "DROP", "FOREACH":
					if !wantOperand {
						facts.writes = true
						wantOperand = word == "SET" || word == "DELETE" || word == "REMOVE" || word == "DROP"
						continue
					}
				case "CALL":
					if !wantOperand {
						facts.calls = true
						header, wantOperand = "CALL", true
						continue
					}
				case "MATCH", "OPTIONAL", "DETACH", "ON", "UNION":
					if !wantOperand {
						if word == "UNION" {
							facts.clause(cyphertext.Span{Start: wordStart, End: after}, start == 0)
						}
						ordered = ordered && word != "UNION"
						continue
					}
				case "WHERE", "UNWIND", "YIELD", "USE":
					if !wantOperand {
						wantOperand = true
						continue
					}
				case "AS":
					if !wantOperand {
						facts.clause(cyphertext.Span{Start: wordStart, End: after}, start == 0)
						named = true
						continue
					}
				}
			}
			switch word {
			case "AND", "OR", "XOR", "NOT", "IN", "IS", "CONTAINS", "STARTS", "ENDS", "CASE", "WHEN", "THEN", "ELSE", "DISTINCT":
				wantOperand = true
			default:
				wantOperand = false
			}
			continue
		}
		header = ""
		switch character {
		case '.', ':', '$':
			named = true
		case ';':
			wantOperand, named, ordered = false, false, false
		case '*':
			wantOperand = !wantOperand
		case ',', '+', '-', '/', '%', '^', '=', '<', '>', '|':
			wantOperand = true
		}
		index++
	}
}

func oracleQueryBody(text string, start, end int) bool {
	start, _ = cyphertext.GapEnd(text[:end], start)
	for _, head := range []string{"MATCH", "OPTIONAL", "RETURN", "WITH", "UNWIND", "CALL", "USE", "CREATE", "MERGE", "DELETE", "SET", "REMOVE", "FOREACH", "LOAD", "SHOW", "FINISH"} {
		if after := cyphertext.KeywordEnd(text[:end], start, head, cyphertext.CypherWord); after >= 0 {
			next, _ := cyphertext.GapEnd(text[:end], after)
			return next == end || text[next] != ':' && text[next] != '.'
		}
	}
	return false
}
