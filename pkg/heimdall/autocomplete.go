package heimdall

import (
	"regexp"
	"strings"

	"github.com/orneryd/nornicdb/internal/cyphertext"
)

const autocompleteActionName = "heimdall_watcher_autocomplete_suggest"

var autocompleteQueryStart = regexp.MustCompile(`^(MATCH|CREATE|MERGE|DELETE|SET|RETURN|WITH|UNWIND|CALL|LOAD|START|UNION|OPTIONAL)`)
var autocompleteInstruction = regexp.MustCompile(`(?i)^(IMPORTANT|Rules?:|Available|Complete)`)
var autocompleteProse = regexp.MustCompile(`(?i)^(The user|They are|I need to|I don't know|So they|Probably not|correct syntax|asking for help)`)

// extractAutocompleteCypher removes model framing without rewriting Cypher
// tokens. Keep the existing first-query and prose boundaries, but inspect them
// only outside the shared lexer's opaque quoted tokens and comments.
func extractAutocompleteCypher(text string) string {
	text = strings.TrimSpace(text)
	// Only an opening model fence authorizes stripping a closing fence.
	// Retain the existing tolerance for an opening fence without a closer.
	if body, fenced := strings.CutPrefix(text, "```"); fenced {
		body = strings.TrimPrefix(body, "cypher")
		text = strings.TrimSpace(strings.TrimSuffix(body, "```"))
	}

	start := -1
	lineStart := true
	for index := 0; index < len(text); {
		if width := cyphertext.WhitespaceAt(text, index); width > 0 {
			if text[index] == '\n' {
				lineStart = true
			}
			index += width
			continue
		}
		tail := text[index:]
		if lineStart {
			if autocompleteInstruction.MatchString(tail) {
				if start < 0 {
					return ""
				}
				return strings.TrimSpace(text[start:index])
			}
			if autocompleteQueryStart.MatchString(tail) {
				if start >= 0 {
					return strings.TrimSpace(text[start:index])
				}
				start = index
			}
			lineStart = false
		}
		if start < 0 {
			// Preamble lines are framing; quotation is meaningful only once
			// the existing query-start contract admits a Cypher line.
			index++
			continue
		}
		if end, opaque := cyphertext.SkipToken(text, index); opaque {
			index = end
			continue
		}
		// Preserve the original regexp \s framing boundary (ASCII whitespace),
		// rather than treating additional Unicode separators as model prose.
		if index > start && strings.IndexByte("\t\n\f\r ", text[index-1]) >= 0 &&
			(autocompleteInstruction.MatchString(tail) || autocompleteProse.MatchString(tail)) {
			return strings.TrimSpace(text[start:index])
		}
		// Retain the existing instruction rejection outside opaque tokens.
		if len(tail) >= 9 && strings.EqualFold(tail[:9], "IMPORTANT") || len(tail) >= 5 && strings.EqualFold(tail[:5], "RULES") {
			return ""
		}
		index++
	}
	if start < 0 {
		return ""
	}
	return strings.TrimSpace(text[start:])
}
