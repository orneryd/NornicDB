package cypher

import (
	"strings"

	"github.com/orneryd/nornicdb/pkg/observability"
)

// Admin handlers can embed an unparsed suffix in a name or a manager error.
// Retain literal and URL credential spans before alias whitespace compaction
// or OPTIONS splitting destroys that context.
func redactAdminCommandError(query string, retErr *error) {
	if *retErr == nil {
		return
	}
	values := observability.ContextCredentials(query)
	for offset := 0; offset < len(query); {
		// Use a raw, boundary-checked search here. A malformed quoted prefix can
		// make the normal Cypher keyword scanner treat the rest of the command as
		// string contents, even though an error may still include a later
		// PASSWORD value verbatim.
		index := rawPasswordKeywordIndex(query, offset)
		if index < 0 {
			break
		}
		i := queryGapEnd(query, index+len("PASSWORD"))
		if i < len(query) && (query[i] == '=' || query[i] == ':') {
			i = queryGapEnd(query, i+1)
		}
		if i >= len(query) {
			break
		}
		// An invalid value can itself be PASSWORD. Continue scanning there so
		// PASSWORD PASSWORD 'value' still supplies its literal for redaction.
		offset = i
		from := startsWithKeywordFold(query[i:], "FROM")
		if from {
			i = queryGapEnd(query, i+len("FROM"))
		}
		for slot := 0; slot < 2 && i < len(query); slot++ {
			// A malformed duplicate PASSWORD value can be followed by the
			// actual quoted value. Skip each duplicate keyword so the following
			// value is included in the redaction set too.
			if !from && startsWithKeywordFold(query[i:], "PASSWORD") {
				i = queryGapEnd(query, i+len("PASSWORD"))
				slot--
				continue
			}
			if query[i] == '\'' || query[i] == '"' {
				start := i + 1
				end, closed := scanCypherQuotedText(query, i, query[i])
				bodyEnd := end
				if closed {
					bodyEnd--
				}
				redactionValues := []string{query[start:bodyEnd]}
				combinedEnd := bodyEnd
				groupEnd := end
				for closed {
					next := queryGapEnd(query, groupEnd)
					if next >= len(query) || !isCypherStringQuoteByte(query[next]) {
						break
					}
					nextEnd, nextClosed := scanCypherQuotedText(query, next, query[next])
					if !nextClosed {
						break
					}
					redactionValues = append(redactionValues, query[next+1:nextEnd-1])
					combinedEnd = nextEnd - 1
					groupEnd = nextEnd
				}
				if groupEnd != end {
					// Invalid adjacent tokens are redacted both as individual
					// pieces and as the original joined span.
					redactionValues = append(redactionValues, query[start:combinedEnd])
				}
				values = append(values, redactionValues...)
				for _, value := range redactionValues {
					compacted := strings.NewReplacer(" ", "", "\t", "", "\n", "", "\r", "").Replace(value)
					if compacted != value {
						values = append(values, compacted)
					}
				}
				i = groupEnd
			} else {
				start := i
				for i < len(query) && !isWhitespace(query[i]) && query[i] != ';' {
					i++
				}
				value := query[start:i]
				values = append(values, value)
				compacted := strings.NewReplacer(" ", "", "\t", "", "\n", "", "\r", "").Replace(value)
				if compacted != value {
					values = append(values, compacted)
				}
			}
			if !from {
				break
			}
			i = queryGapEnd(query, i)
			if !startsWithKeywordFold(query[i:], "TO") {
				break
			}
			i = queryGapEnd(query, i+len("TO"))
		}
		offset = i
	}
	*retErr = observability.RedactError(*retErr, values...)
}

func rawPasswordKeywordIndex(query string, from int) int {
	const keyword = "PASSWORD"
	for index := from; index+len(keyword) <= len(query); index++ {
		if !strings.EqualFold(query[index:index+len(keyword)], keyword) {
			continue
		}
		end := index + len(keyword)
		if index > 0 && isIdentByte(query[index-1]) {
			continue
		}
		if end < len(query) && isIdentByte(query[end]) {
			continue
		}
		return index
	}
	return -1
}
