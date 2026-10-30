package workcensus

// Author: #1667 implementation lane (Codex). Exercise the production class
// query and Validate with typed methods/function-value handoffs, not a parser.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStorageSchemaHandoffProofControls(t *testing.T) {
	dir := t.TempDir()
	pkg := module + "pkg/storage"
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module "+pkg+"\n\ngo 1.26\n"), 0600); err != nil {
		t.Fatal(err)
	}
	source := `package storage
import "context"
type SchemaManager struct{}
func (*SchemaManager) GetConstraintsForLabelsWithContext(ctx context.Context,labels []string)([]int,error){
 var rows []int
 for range labels { if err:=ctx.Err();err!=nil{return nil,err}; rows=append(rows,1) }
 return rows,nil
}
func Execute(ctx context.Context)([]int,error){
 schema:=&SchemaManager{}
 read:=schema.GetConstraintsForLabelsWithContext
 rows,err:=read(ctx,[]string{"P"})
 if err!=nil{return nil,err}
 return rows,nil
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
			if strings.HasSuffix(m.OwnerSymbol, ".Execute") && contains(m.Callees, "(*"+pkg+".SchemaManager).GetConstraintsForLabelsWithContext") {
				return m
			}
		}
		t.Fatal("typed bound-method handoff missing")
		return Member{}
	}
	base := extract(source)
	member := find(base)
	proof := storageContextClassFacts(base)[storagePhysicalSpan(member)]
	if len(proof) < 4 || proof[0] != schemaHandoffClass {
		t.Fatal("schema handoff or callee body not proved", proof)
	}
	approved := member
	approved.Class = schemaHandoffClass
	approved.Reason = "Exact bound-method caller scope and returned error; callee body pinned in fresh facts."
	if failures := validateStorageContextClasses(base, Inventory{Roots: base.Roots, Members: []Member{approved}}); len(failures) != 0 {
		t.Fatal(failures)
	}
	for _, tc := range []struct{ name, old, replacement string }{
		{"substitute scope", "read(ctx,", "read(context.Background(),"},
		{"discard error", "rows,err:=read(ctx,[]string{\"P\"})\n if err!=nil{return nil,err}", "rows,_:=read(ctx,[]string{\"P\"})"},
		{"swallow error", "if err!=nil{return nil,err}", "if err!=nil{return nil,nil}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual := extract(strings.Replace(source, tc.old, tc.replacement, 1))
			row := find(actual)
			row.Class = schemaHandoffClass
			row.Reason = "plant: refreshed ordinary digest, retained false scope/error classification"
			failures := validateStorageContextClasses(actual, Inventory{Roots: actual.Roots, Members: []Member{row}})
			if len(failures) != 1 || !strings.HasPrefix(failures[0], "CLASS FACT CONTRADICTION ") {
				t.Fatal("production class gate accepted planted handoff", failures)
			}
		})
	}
}
