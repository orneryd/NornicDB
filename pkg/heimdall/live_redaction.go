package heimdall

import (
	"strings"
	"unicode"

	"github.com/orneryd/nornicdb/pkg/observability"
)

// liveModelRedactor releases complete lexical spans so generated credential
// URLs split across callbacks cannot leak. Structured JSON and responses with
// known opaque credentials retain whole-value redaction semantics.
type liveModelRedactor struct {
	pending        string
	decided, whole bool
	credentials    []string
	emit           func(string) error
}

func (s *liveModelRedactor) write(text string, final bool) error {
	s.pending += text
	if !s.decided {
		trimmed := strings.TrimSpace(s.pending)
		if trimmed != "" {
			s.decided = true
			s.whole = strings.ContainsRune("{[\"", rune(trimmed[0]))
		}
	}
	end := 0
	if final {
		end = len(s.pending)
	} else if !s.whole && s.decided {
		for index, r := range s.pending {
			if unicode.IsSpace(r) {
				end = index + len(string(r))
			}
		}
	}
	if end == 0 {
		return nil
	}
	if !final {
		end = observability.SafeRedactionStreamBoundary(s.pending, end, s.credentials)
	}
	if end == 0 {
		return nil
	}
	value := s.pending[:end]
	s.pending = s.pending[end:]
	return s.emit(observability.RedactText(value, s.credentials...))
}
