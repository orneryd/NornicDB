package workcensus

// Author: #1667 implementation lane (Codex).

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const compatibilityContextClass = "exact public compatibility relay"

func TestStorageCompatibilitySourceProofControls(t *testing.T) {
	dir := t.TempDir()
	pkg := "compat.fixture"
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module "+pkg+"\n\ngo 1.26\n"), 0600); err != nil {
		t.Fatal(err)
	}
	source := `package fixture
import "context"
type Store struct{}
func (s *Store) Execute(k int)(int,error){return s.ExecuteWithContext(context.Background(),k)}
func (s *Store) ExecuteWithContext(ctx context.Context,k int)(int,error){return k,ctx.Err()}
`
	extract := func(raw string) Inventory {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, "entry.go"), []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
		return mustExtract(t, Config{Dir: dir, Patterns: []string{"./..."}, IncludeSyntax: true, Roots: []Root{{Package: pkg, Receiver: "Store", Name: "Execute", Required: true}}, Selectors: []Selector{{Kind: "owner-call", Node: "call"}, {Kind: "context-boundary", Node: "call", Packages: []string{"context"}, Names: []string{"Background", "TODO"}}}})
	}
	find := func(inv Inventory) Member {
		t.Helper()
		for _, m := range inv.Members {
			if m.Kind == "context-boundary" {
				return m
			}
		}
		t.Fatal("adapter context source missing")
		return Member{}
	}
	base := extract(source)
	m := find(base)
	proof := storageContextClassFacts(base)[storagePhysicalSpan(m)]
	if len(proof) < 3 || proof[0] != compatibilityContextClass {
		t.Fatal("exact exported compatibility relay was not proved", proof)
	}
	row := m
	row.Class = compatibilityContextClass
	row.Reason = "Exact public compatibility relay only; callers retain independent context obligations."
	if failures := validateStorageContextClasses(base, Inventory{Roots: base.Roots, Members: []Member{row}}); len(failures) > 0 {
		t.Fatal(failures)
	}
	for _, tc := range []struct{ name, old, replacement string }{
		{"context available", "Execute(k int)", "Execute(ctx context.Context,k int)"},
		{"discarded context available", "Execute(k int)", "Execute(_ context.Context,k int)"},
		{"receiver substituted", "return s.ExecuteWithContext(", "return (&Store{}).ExecuteWithContext("},
		{"input changed", "Background(),k)", "Background(),k+1)"},
		{"error discarded", "return s.ExecuteWithContext(context.Background(),k)", "value,_:=s.ExecuteWithContext(context.Background(),k);return value,nil"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual := extract(strings.Replace(source, tc.old, tc.replacement, 1))
			m := find(actual)
			if proof := storageContextClassFacts(actual)[storagePhysicalSpan(m)]; len(proof) > 0 && proof[0] == compatibilityContextClass {
				t.Fatal("false relay supplied proof", proof)
			}
			m.Class = compatibilityContextClass
			m.Reason = "plant: fresh ordinary pins, false relay classification"
			failures := validateStorageContextClasses(actual, Inventory{Roots: actual.Roots, Members: []Member{m}})
			if len(failures) != 1 || !strings.HasPrefix(failures[0], "CLASS FACT CONTRADICTION ") {
				t.Fatal("same class gate missed contradiction", failures)
			}
		})
	}
}
