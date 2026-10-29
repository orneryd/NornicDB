package cypher

import (
	"regexp"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/testutil/compositeddl"
)

// Administration-only words must retain identifier treatment in the lexer
// shared by native logging and ANTLR validation, including malformed queries.
func TestCompositeDDLKeywordsPreserveIdentifierRedaction(t *testing.T) {
	for _, word := range []string{"AT", "PASSWORD", "OIDC", "CREDENTIAL", "FORWARDING", "SECRET", "REF", "ACCESS", "CONSTITUENTS"} {
		t.Run(word, func(t *testing.T) {
			out := RedactLiterals("MATCH (n {name: John, " + word + "}) RETURN n")
			if regexp.MustCompile(`\b`+word+`\b`).MatchString(out) || strings.Contains(out, "John") {
				t.Error("malformed query retained a composite contextual identifier")
			}
		})
	}
}

func TestCompositeDDLNativePasswordRedaction(t *testing.T) {
	query := "CREATE COMPOSITE DATABASE ddl ALIAS r FOR DATABASE nornic AT 'http://127.0.0.1:1' USER admin PASSWORD " + compositeddl.BarePassword
	if strings.Contains(RedactLiterals(query), compositeddl.BarePassword) {
		t.Error("grammar change exposed a native bare password through canonical logging")
	}
}
