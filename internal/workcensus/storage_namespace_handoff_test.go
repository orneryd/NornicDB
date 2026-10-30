package workcensus

// Author: #1667 implementation lane (Codex).

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStorageNamespaceHandoffProofControls(t *testing.T) {
	dir := t.TempDir()
	pkg := module + "pkg/storage"
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module "+pkg+"\n\ngo 1.26\n"), 0600); err != nil {
		t.Fatal(err)
	}
	source := `package storage
import "context"
type SchemaManager struct{}
func (*SchemaManager)RegisterTokensWithContext(ctx context.Context)error{return ctx.Err()}
func sortIndexCandidatesWithContext[T any](ctx context.Context, values []T)error{return ctx.Err()}
func Execute(ctx context.Context)error{
 schema:=&SchemaManager{}
 if err:=sortIndexCandidatesWithContext(ctx, []int{2,1});err!=nil{return err}
 if err:=schema.RegisterTokensWithContext(ctx);err!=nil{return err}
 return nil
}
`
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
			if strings.HasSuffix(m.OwnerSymbol, ".Execute") && contains(m.Callees, "(*"+pkg+".SchemaManager).RegisterTokensWithContext") {
				return m
			}
		}
		t.Fatal("typed token preparation handoff missing")
		return Member{}
	}
	actual := extract(source)
	m := find(actual)
	proof := storageContextClassFacts(actual)[storagePhysicalSpan(m)]
	if len(proof) < 4 || proof[0] != namespaceHandoffClass {
		t.Fatal("error-only preparation handoff not proved", proof)
	}
	// Instantiated generic owner calls use the same scope/error proof as methods.
	foundSort := false
	for _, candidate := range actual.Members {
		if !contains(candidate.Callees, pkg+".sortIndexCandidatesWithContext") {
			continue
		}
		foundSort = true
		facts := storageContextClassFacts(actual)[storagePhysicalSpan(candidate)]
		if len(facts) < 4 || facts[0] != namespaceHandoffClass {
			t.Fatal("generic sort handoff not proved", facts)
		}
	}
	if !foundSort {
		t.Fatal("generic preparation owner missing")
	}
	m.Class = namespaceHandoffClass
	m.Reason = "Same caller context and exact guarded error; canonical target body pinned."
	if failures := validateStorageContextClasses(actual, Inventory{Roots: actual.Roots, Members: []Member{m}}); len(failures) != 0 {
		t.Fatal(failures)
	}
	for _, tc := range []struct{ name, old, replacement string }{
		{"substitute scope", "schema.RegisterTokensWithContext(ctx)", "schema.RegisterTokensWithContext(context.Background())"},
		{"swallow error", "if err:=schema.RegisterTokensWithContext(ctx);err!=nil{return err}", "if err:=schema.RegisterTokensWithContext(ctx);err!=nil{return nil}"},
		{"discard error", "if err:=schema.RegisterTokensWithContext(ctx);err!=nil{return err}", "_ = schema.RegisterTokensWithContext(ctx)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inv := extract(strings.Replace(source, tc.old, tc.replacement, 1))
			row := find(inv)
			row.Class = namespaceHandoffClass
			row.Reason = "plant: refreshed source pins, contradicted preparation handoff"
			failures := validateStorageContextClasses(inv, Inventory{Roots: inv.Roots, Members: []Member{row}})
			if len(failures) != 1 || !strings.HasPrefix(failures[0], "CLASS FACT CONTRADICTION ") {
				t.Fatal("same production gate missed contradiction", failures)
			}
		})
	}
}
