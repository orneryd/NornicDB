package workcensus

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// NOR-209 mixed-scope comment (2026-11-01) records one shared engine defect in
// this package: receiver filtering takes the receiver from TypeOf(call.Fun),
// where go/types omits Recv for bound method values, and "field" selectors emit
// declarations instead of uses. These are the narrow control cases for that
// report. They exercise one small fixture module per case; no repository-wide
// census is run. Author: NOR-209 unit-test lane (DeepSeek), 2026-11-18.

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

// nor209FieldSource declares one named field and performs two uses of it.
const nor209FieldSource = `package nor209fixture

type record struct{ name string }

func readName(r *record) string { return r.name }

func writeName(r *record, value string) { r.name = value }
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

// TestNor209ReceiverTypesSelectBoundMethodValueCalls is the receiver control.
// The approved engine takes the receiver from the typed method selection's
// receiver operand and falls back to the resolved target method for bound
// aliases; the original engine found 1 of these 4 calls.
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
	for _, member := range inventory.Members {
		if strings.Contains(member.Code, "alias()") {
			t.Fatalf("receiver_types admitted a non-method call: %s", member.Code)
		}
	}
}

// TestNor209FieldKindSelectsUsesNotOnlyDeclarations is the field control. A
// "field" population must hold the field uses; declaration-only membership
// makes any population built with this filter falsely empty or small. Parameter
// declarations are not field uses, so the assertion is on the selector text.
func TestNor209FieldKindSelectsUsesNotOnlyDeclarations(t *testing.T) {
	cfg := nor209CensusConfig(t, nor209FieldSource, []Selector{{
		Kind:         "field",
		Node:         "field",
		Reachability: "all",
	}})
	inventory := mustExtract(t, cfg)
	t.Logf("field population: %v", nor209Codes(inventory.Members))
	uses := map[string]bool{}
	for _, member := range inventory.Members {
		if !strings.Contains(member.Code, "r.name") {
			continue
		}
		if strings.Contains(member.Owner, "readName") {
			uses["readName"] = true
		}
		if strings.Contains(member.Owner, "writeName") {
			uses["writeName"] = true
		}
	}
	if !uses["readName"] || !uses["writeName"] {
		t.Fatalf(`field population holds declarations, not uses (readNameUse=%v writeNameUse=%v): %v`,
			uses["readName"], uses["writeName"], nor209Codes(inventory.Members))
	}
}

// TestNor209MinimumMembersConfig is the undersized-population control. The
// approved engine fails loudly (POPULATION ALARM) when a configuration declares
// a minimum_members floor and the extracted population is smaller; the current
// engine has no such key, so an undersized population is reported as a normal
// small result instead.
func TestNor209MinimumMembersConfig(t *testing.T) {
	field, ok := reflect.TypeOf(Config{}).FieldByName("MinimumMembers")
	if !ok {
		t.Fatalf("workcensus.Config has no MinimumMembers/minimum_members floor, so an undersized population cannot fail loudly")
	}
	if tag := field.Tag.Get("json"); !strings.Contains(tag, "minimum_members") {
		t.Fatalf("MinimumMembers is not serialized as minimum_members: %q", tag)
	}
}
