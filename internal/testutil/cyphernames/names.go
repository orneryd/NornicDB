// Package cyphernames supplies the same generated literal-name population to
// tests of the independent Cypher source emitters. It is only imported by tests.
package cyphernames

import (
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/internal/cyphertext"
)

// Names includes the review witness and a deterministic cross product, so each
// emitter sees plain names, delimiters, escaped backticks, keywords and Unicode.
func Names() []string {
	names := []string{" ", "  ", "\t", "a.b", "a,b", "a(b)", "space ", "A|B", "tick`name", "A:B", "two words", "MATCH", "RETURN", "名", "Ω", "R_2"}
	seen := map[string]bool{}
	for _, name := range names {
		seen[name] = true
	}
	for _, prefix := range []string{"R", "A|B", "tick`", "space ", "名", "MATCH"} {
		for _, suffix := range []string{"", "|T", ":T", "`end", " Ω"} {
			name := prefix + suffix
			if !seen[name] {
				names = append(names, name)
				seen[name] = true
			}
		}
	}
	return names
}

// ReadType re-reads one literal type from an emitted relationship pattern. The
// complete token boundary matters: accepting just A from A|B is a failed round trip.
func ReadType(text string) (string, error) {
	open := strings.IndexByte(text, '[')
	if open < 0 {
		return "", fmt.Errorf("missing relationship pattern: %s", text)
	}
	colon := strings.IndexByte(text[open:], ':')
	if colon < 0 {
		return "", fmt.Errorf("missing type: %s", text)
	}
	start := open + colon + 1
	token, ok := cyphertext.ScanIdentifier(text, start)
	name, end := token.Value, token.End
	if !ok || end == len(text) || !strings.ContainsRune("] *{", rune(text[end])) {
		return "", fmt.Errorf("not one complete literal type: %s", text)
	}
	return name, nil
}
