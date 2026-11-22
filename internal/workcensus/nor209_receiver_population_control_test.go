package workcensus

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// nor209ReceiverSource holds four known calls of (*counter).Bump: a direct
// selection, a bound method value, a method-expression value, and a method
// expression call.
const nor209ReceiverSource = `package nor209fixture

type counter struct{ n int }

func (c *counter) Bump() { c.n++ }

func drive(c *counter) {
	c.Bump()
	boundMethod := c.Bump
	boundMethod()
	methodExpression := (*counter).Bump
	methodExpression(c)
	(*counter).Bump(c)
}
`

// nor209CensusConfig builds a typed, single-package fixture module and loads it
// with the caller's selectors. It mirrors the narrow typed fixture used by
// declarationIdentityConfig, without the repository's own sources.
func nor209CensusConfig(t *testing.T, source string, selectors []Selector) Config {
	t.Helper()
	dir := t.TempDir()
	for name, raw := range map[string]string{"go.mod": "module nor209fixture\n\ngo 1.26\n", "source.go": source} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(raw), 0644); err != nil {
			t.Fatal(err)
		}
	}
	return Config{
		Dir:             dir,
		Patterns:        []string{"./..."},
		IncludeInactive: true,
		SourceRoots:     []SourceRoot{{Path: ".", Languages: map[string]string{".go": "go"}}},
		Selectors:       selectors,
	}
}

func nor209Codes(members []Member) []string {
	codes := make([]string, 0, len(members))
	for _, member := range members {
		codes = append(codes, fmt.Sprintf("%s:%d node=%s code=%q", member.File, member.Line, member.Node, member.Code))
	}
	return codes
}

func TestNor209ReceiverTypesSelectBoundMethodValueCalls(t *testing.T) {
	cfg := nor209CensusConfig(t, nor209ReceiverSource, []Selector{{
		Kind:          "bump-call",
		Node:          "call",
		Reachability:  "all",
		ReceiverTypes: []string{"*nor209fixture.counter"},
	}})
	inventory := mustExtract(t, cfg)
	codes := nor209Codes(inventory.Members)
	if len(inventory.Members) != 4 {
		t.Fatalf("receiver_types population is %d of 4 known (*counter).Bump calls; a filtered population built this way comes back falsely small: %v",
			len(inventory.Members), codes)
	}
	for _, want := range []string{"c.Bump()", "boundMethod()", "methodExpression(c)", "(*counter).Bump(c)"} {
		found := false
		for _, member := range inventory.Members {
			found = found || strings.Contains(member.Code, want)
		}
		if !found {
			t.Fatalf("receiver_types population omits %q: %v", want, codes)
		}
	}
}

// TestNor209ReceiverTypesRejectAPlainFunctionValueControl keeps the receiver
// bound honest: the filter must still exclude calls whose target is not a
// method of the requested receiver type.
func TestNor209ReceiverTypesRejectAPlainFunctionValueControl(t *testing.T) {
	const source = `package nor209fixture

type counter struct{ n int }

func (c *counter) Bump() { c.n++ }

func plain() {}

func drive(c *counter) {
	c.Bump()
	alias := plain
	alias()
}
`
	cfg := nor209CensusConfig(t, source, []Selector{{
		Kind:          "bump-call",
		Node:          "call",
		Reachability:  "all",
		ReceiverTypes: []string{"*nor209fixture.counter"},
	}})
	inventory := mustExtract(t, cfg)
	if len(inventory.Members) != 1 || inventory.Members[0].Code != "c.Bump()" {
		t.Fatalf("receiver_types must select the method and omit the plain function: %v", nor209Codes(inventory.Members))
	}
}
