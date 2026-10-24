package cypher

import (
	"strings"

	"github.com/orneryd/nornicdb/pkg/observability"
)

// Admin handlers can embed an unparsed suffix in a name or a manager error.
// Retain the original command's PASSWORD literal before alias whitespace
// compaction or OPTIONS splitting destroys that context. Other literals retain
// their ordinary diagnostics. Only RedactError projects the diagnostic,
// including URI credentials, localized data and encoded forms.
func redactAdminCommandError(query string, retErr *error) {
	if *retErr == nil {
		return
	}
	var values []string
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
		// An invalid value can itself be PASSWORD. Keep that next keyword
		// visible so PASSWORD PASSWORD 'value' still supplies its literal.
		offset = i
		quote := query[i]
		if quote != '\'' && quote != '"' {
			continue
		}
		end, closed := scanCypherQuotedText(query, i, quote)
		valueStart := i + 1
		valueEnd := end
		if closed {
			valueEnd--
		}
		redactionValues := []string{query[valueStart:valueEnd]}
		combinedEnd := valueEnd
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
			// Invalid adjacent literals must be redacted as one original span
			// as well as by their individual pieces.
			redactionValues = append(redactionValues, query[valueStart:combinedEnd])
		}
		values = append(values, redactionValues...)
		// CREATE/DROP ALIAS historically remove these four whitespace bytes
		// from their whole suffix, including any supplied literal password.
		for _, value := range redactionValues {
			compacted := strings.NewReplacer(" ", "", "\t", "", "\n", "", "\r", "").Replace(value)
			if compacted != value {
				values = append(values, compacted)
			}
		}
		offset = groupEnd
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
