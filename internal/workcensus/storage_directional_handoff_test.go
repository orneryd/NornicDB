package workcensus

// Author: #1667 implementation lane (Codex).

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStorageDirectionalHandoffProofControls(t *testing.T) {
	dir := t.TempDir()
	pkg := module + "pkg/storage"
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module "+pkg+"\n\ngo 1.26\n"), 0600); err != nil {
		t.Fatal(err)
	}
	source := `package storage
import "context"
type Edge struct{}
func readAdjacencyWithContext(ctx context.Context)([]*Edge,[]*Edge,error){return nil,nil,ctx.Err()}
func Execute(ctx context.Context)([]*Edge,[]*Edge,error){return readAdjacencyWithContext(ctx)}`
	extract := func(raw string) Inventory {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, "entry.go"), []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
		return mustExtract(t, Config{Dir: dir, Patterns: []string{"./..."}, IncludeSyntax: true, Roots: []Root{{Package: pkg, Name: "Execute", Required: true}}, Selectors: []Selector{{Kind: "owner-call", Node: "call"}}})
	}
	find := func(inv Inventory) Member {
		t.Helper()
		for _, m := range inv.Members {
			if strings.HasSuffix(m.OwnerSymbol, ".Execute") && contains(m.Callees, pkg+".readAdjacencyWithContext") {
				return m
			}
		}
		t.Fatal("directional owner call missing")
		return Member{}
	}
	inv := extract(source)
	m := find(inv)
	proof := storageContextClassFacts(inv)[storagePhysicalSpan(m)]
	if len(proof) < 4 || proof[0] != directionalHandoffClass {
		t.Fatal("whole directional result/error tuple not proved", proof)
	}
	m.Class = directionalHandoffClass
	m.Reason = "Same caller and complete directional/error tuple to the retained read owner."
	if failures := validateStorageContextClasses(inv, Inventory{Roots: inv.Roots, Members: []Member{m}}); len(failures) != 0 {
		t.Fatal(failures)
	}
	for _, tc := range []struct{ name, old, replacement string }{
		{"substituted context", "return readAdjacencyWithContext(ctx)", "return readAdjacencyWithContext(context.Background())"},
		{"discarded error", "return readAdjacencyWithContext(ctx)", "out,in,_:=readAdjacencyWithContext(ctx);return out,in,nil"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual := extract(strings.Replace(source, tc.old, tc.replacement, 1))
			row := find(actual)
			row.Class = directionalHandoffClass
			row.Reason = "plant: ordinary pins refreshed, directional handoff contradicted"
			failures := validateStorageContextClasses(actual, Inventory{Roots: actual.Roots, Members: []Member{row}})
			if len(failures) != 1 || !strings.HasPrefix(failures[0], "CLASS FACT CONTRADICTION ") {
				t.Fatal(failures)
			}
		})
	}
}
