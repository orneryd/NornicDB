package storage

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

func relationshipCacheOwnerViolations(name string, source []byte) []string {
	fs := token.NewFileSet()
	file, err := parser.ParseFile(fs, name, source, 0)
	if err != nil {
		return []string{err.Error()}
	}
	var problems []string
	reject := func(node ast.Node, why string) {
		problems = append(problems, fmt.Sprintf("%s: %s", fs.Position(node.Pos()), why))
	}
	cacheField := func(expr ast.Expr) bool {
		sel, ok := expr.(*ast.SelectorExpr)
		if !ok {
			return false
		}
		switch sel.Sel.Name {
		case "edgeCache", "outgoingAdjCache", "incomingAdjCache", "edgeTypeCache":
			return true
		}
		return false
	}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		owned := name == "badger_relationship_cache_owner.go"
		aliases := map[string]bool{}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			if assign, ok := node.(*ast.AssignStmt); ok {
				for i, rhs := range assign.Rhs {
					if i < len(assign.Lhs) && cacheField(rhs) {
						if id, ok := assign.Lhs[i].(*ast.Ident); ok {
							aliases[id.Name] = true
						}
					}
				}
			}
			return true
		})
		cache := func(expr ast.Expr) bool {
			if cacheField(expr) {
				return true
			}
			id, ok := expr.(*ast.Ident)
			return ok && aliases[id.Name]
		}
		prepare, write := token.NoPos, token.NoPos
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			switch node := node.(type) {
			case *ast.RangeStmt:
				if cache(node.X) {
					reject(node, "cache population scan; invalidation must use writer keys")
				}
			case *ast.CallExpr:
				if id, ok := node.Fun.(*ast.Ident); ok && id.Name == "delete" && len(node.Args) > 0 && cache(node.Args[0]) {
					reject(node, "removing a key erases its monotonic revocation record")
				}
				if sel, ok := node.Fun.(*ast.SelectorExpr); ok {
					if sel.Sel.Name == "prepareRelationshipCacheWrites" {
						prepare = node.Pos()
					}
					if sel.Sel.Name == "CommitAt" {
						write = node.Pos()
					}
					if domain, ok := sel.X.(*ast.SelectorExpr); ok {
						switch domain.Sel.Name {
						case "edgeCoherence", "adjCoherence", "edgeTypeCoherence":
							if !owned {
								reject(node, "coherence domain mutation outside owner")
							}
						}
					}
				}
			case *ast.AssignStmt:
				for _, lhs := range node.Lhs {
					if sel, ok := lhs.(*ast.SelectorExpr); ok {
						if (sel.Sel.Name == "writeTs" || sel.Sel.Name == "coherence" || sel.Sel.Name == "edgeCoherence" || sel.Sel.Name == "adjCoherence" || sel.Sel.Name == "edgeTypeCoherence") && !owned {
							reject(node, "revocation mutation outside owner")
						}
						if cacheField(sel) && !owned && !(name == "badger.go" && fn.Name.Name == "NewBadgerEngineWithOptions") && !(name == "badger_stats.go" && fn.Name.Name == "releaseClosedStateLocked") {
							reject(node, "cache reset outside coherence owner")
						}
					}
					if index, ok := lhs.(*ast.IndexExpr); ok && cache(index.X) && !owned {
						reject(node, "cache publication outside coherence owner")
					}
				}
			case *ast.CompositeLit:
				if typ, ok := node.Type.(*ast.Ident); ok {
					switch typ.Name {
					case "edgeCacheEntry", "adjacencyCacheEntry", "edgeListCacheEntry":
						reject(node, "fresh entry discards the existing coherence record; update the owned record")
					}
				}
			}
			return true
		})
		if name == "badger_cache_commit.go" && fn.Name.Name == "commitDurableWrite" && (prepare == token.NoPos || write == token.NoPos || prepare >= write) {
			reject(fn, "scope preparation must precede physical durability")
		}
	}
	return problems
}

func TestRelationshipCacheOwnerGuard(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		source, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		for _, problem := range relationshipCacheOwnerViolations(name, source) {
			t.Error(problem)
		}
	}
}

func TestRelationshipCacheOwnerGuardMutations(t *testing.T) {
	// Initialization is an exact constructor exemption, not a filename-wide
	// permission to erase live key records.
	reset := []byte("package storage;func(b *BadgerEngine) rogue(){b.edgeCache=make(map[EdgeID]edgeCacheEntry)}")
	if len(relationshipCacheOwnerViolations("badger.go", reset)) == 0 {
		t.Fatal("reset in initializer file escaped coherence owner")
	}
	for name, body := range map[string]string{
		"delete record":              "delete(b.edgeCache,id)",
		"delete aliased record":      "cache:=b.edgeCache;delete(cache,id)",
		"drop adjacency stamp":       "delete(b.outgoingAdjCache,nodeID)",
		"clear type stamp":           "delete(b.edgeTypeCache,kind)",
		"lower stamp":                "entry.coherence=cacheKeyCoherence{}",
		"fresh publication":          "b.edgeCache[id]=edgeCacheEntry{Edge:edge}",
		"population scan":            "for _,entry:=range b.edgeTypeCache{_ = entry}",
		"aliased population scan":    "cache:=b.edgeTypeCache;for _,entry:=range cache{_ = entry}",
		"domain reset outside owner": "b.edgeCoherence.reset()",
	} {
		t.Run(name, func(t *testing.T) {
			src := []byte("package storage;func(b *BadgerEngine) rogue(){" + body + "}")
			if len(relationshipCacheOwnerViolations("rogue.go", src)) == 0 {
				t.Fatal("mutation escaped coherence owner")
			}
		})
	}
	source, err := os.ReadFile("badger_cache_commit.go")
	if err != nil {
		t.Fatal(err)
	}
	call := "scope, err = engine.prepareRelationshipCacheWrites(scope, txn, false)"
	if !strings.Contains(string(source), call) {
		t.Fatal("preparation mutation anchor missing")
	}
	mutated := strings.Replace(string(source), call, "_ = txn", 1)
	if len(relationshipCacheOwnerViolations("badger_cache_commit.go", []byte(mutated))) == 0 {
		t.Fatal("deleted preparation escaped guard")
	}
}
