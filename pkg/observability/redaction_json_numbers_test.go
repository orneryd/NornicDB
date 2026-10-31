package observability

import (
	"strings"
	"testing"
)

// A JSON text RedactText rewrites can be a statement's input (Heimdall tool
// arguments): its numbers stay exactly as written.
func TestRedactTextKeepsJSONNumbersExact(t *testing.T) {
	redacted := RedactText(`{"params":{"id":9007199254740993,"ratio":1.0,"small":1e-7,"password":"x"}}`)
	for _, want := range []string{`"id":9007199254740993`, `"ratio":1.0`, `"small":1e-7`} {
		if !strings.Contains(redacted, want) {
			t.Fatalf("RedactText changed a number: want %s in %s", want, redacted)
		}
	}
	if strings.Contains(redacted, `"password":"x"`) {
		t.Fatalf("RedactText kept the password: %s", redacted)
	}
	if got := RedactText(`{"a":1}`); got != `{"a":1}` {
		t.Fatalf("a text without credentials changed: %s", got)
	}
}
