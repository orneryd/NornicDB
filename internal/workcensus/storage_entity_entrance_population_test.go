package workcensus

// Author: #1667 implementation lane (Codex).

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStorageEntityEntrancePopulation(t *testing.T) {
	dir := t.TempDir()
	for file, body := range map[string]string{
		"go.mod": "module " + module[:len(module)-1] + "\n\ngo 1.26\n",
		"pkg/storage/types.go": `package storage
import "context"
type Node struct{}
type NodeID string
type Edge struct{}
type EdgeID string
func selectNodes(context.Context)([]*Node,error){return nil,nil}
func selectEdges(context.Context)([]*Edge,error){return nil,nil}
func selectEdgeIDs(context.Context)([]EdgeID,error){return nil,nil}
func Execute(ctx context.Context){
 _,_=selectNodes(ctx)
 alias:=selectEdges
 _,_=alias(ctx)
 _,_=selectEdgeIDs(ctx)
}`,
	} {
		path := filepath.Join(dir, file)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	inv := mustExtract(t, Config{Dir: dir, Patterns: []string{"./..."}, Roots: []Root{{Package: module + "pkg/storage", Name: "Execute", Required: true}}, Selectors: storageSelectors()})
	seen := map[string]bool{}
	for _, m := range inv.Members {
		if m.Kind == "entrance" {
			for _, callee := range m.Callees {
				seen[callee] = true
			}
		}
	}
	for _, name := range []string{"selectNodes", "selectEdges", "selectEdgeIDs"} {
		if !seen[module+"pkg/storage."+name] {
			t.Errorf("production selectors omit typed preparation entrance %s (including function-value dispatch)", name)
		}
	}
}
