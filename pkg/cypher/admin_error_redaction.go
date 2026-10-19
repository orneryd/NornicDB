package cypher

import (
	"strings"

	"github.com/orneryd/nornicdb/pkg/observability"
)

// Admin handlers can embed an unparsed suffix in a name or a manager error.
// Retain the original command's PASSWORD value before alias whitespace
// compaction or OPTIONS splitting destroys that context. Other literals retain
// their ordinary diagnostics. Only RedactError projects the diagnostic,
// including URI credentials, localized data and encoded forms.
func redactAdminCommandError(query string, retErr *error) {
	if *retErr == nil {
		return
	}
	var values []string
	for offset := 0; offset < len(query); {
		index := keywordIndexFrom(query, "PASSWORD", offset, defaultKeywordScanOpts())
		if index < 0 {
			break
		}
		i := queryGapEnd(query, index+len("PASSWORD"))
		if i >= len(query) {
			break
		}
		// An invalid value can itself be PASSWORD. Keep that next keyword
		// visible so PASSWORD PASSWORD 'value' still supplies its literal.
		offset = i
		from := startsWithKeywordFold(query[i:], "FROM")
		if from {
			i = queryGapEnd(query, i+len("FROM"))
		}
		for slot := 0; slot < 2 && i < len(query); slot++ {
			start, end := i, i
			quote := query[i]
			if quote == '\'' || quote == '"' {
				var closed bool
				end, closed = scanCypherQuotedText(query, i, quote)
				start++
				i = end
				if closed {
					end--
				}
			} else {
				for i < len(query) && !isWhitespace(query[i]) && query[i] != ';' {
					i++
				}
				end = i
			}
			value := query[start:end]
			values = append(values, value)
			// Alias handlers can compact whitespace in the entire suffix.
			compacted := strings.NewReplacer(" ", "", "\t", "", "\n", "", "\r", "").Replace(value)
			if compacted != value {
				values = append(values, compacted)
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
	}
	*retErr = observability.RedactError(*retErr, values...)
}
