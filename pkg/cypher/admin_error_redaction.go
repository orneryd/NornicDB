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
		quote := query[i]
		if quote != '\'' && quote != '"' {
			continue
		}
		end, closed := scanCypherQuotedText(query, i, quote)
		valueEnd := end
		if closed {
			valueEnd--
		}
		value := query[i+1 : valueEnd]
		values = append(values, value)
		// CREATE/DROP ALIAS historically remove these four whitespace bytes
		// from their whole suffix, including any supplied literal password.
		compacted := strings.NewReplacer(" ", "", "\t", "", "\n", "", "\r", "").Replace(value)
		if compacted != value {
			values = append(values, compacted)
		}
		offset = end
	}
	*retErr = observability.RedactError(*retErr, values...)
}
