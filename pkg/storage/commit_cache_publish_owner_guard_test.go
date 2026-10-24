package storage

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"strings"
	"testing"
)

func commitCachePublisherViolations(name string, source []byte) []string {
	fs := token.NewFileSet()
	file, err := parser.ParseFile(fs, name, source, 0)
	if err != nil {
		return []string{err.Error()}
	}
	var problems []string
	text := func(expr ast.Expr) string {
		var out bytes.Buffer
		_ = printer.Fprint(&out, fs, expr)
		return out.String()
	}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		owner := name == "badger_commit_writer.go" && receiverType(fn) == "commitWriter" && fn.Name.Name == "finish"
		publisher := name == "badger_commit_cache_images.go" && fn.Name.Name == "publishCommitCacheImages"
		callback := strings.HasPrefix(fn.Name.Name, "cacheOn") || fn.Name.Name == "publishCommitOperations"
		if callback {
			for _, field := range fn.Type.Params.List {
				if _, variadic := field.Type.(*ast.Ellipsis); variadic {
					problems = append(problems, "operation callbacks cannot accept publication tokens")
				}
			}
		}
		calls := map[ast.Expr]*ast.CallExpr{}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				calls[call.Fun] = call
			}
			return true
		})
		publishCalls := 0
		var publish, committed token.Pos
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			reject := func(why string) { problems = append(problems, fs.Position(sel.Pos()).String()+": "+why) }
			switch sel.Sel.Name {
			case "cacheStoreEdge", "cacheStoreWrittenNode", "publishNodeCacheLocked":
				reject("operation-level payload publisher")
			case "publishCommitCacheImages":
				call := calls[sel]
				if !owner || call == nil {
					reject("publication outside logical commit completion")
					break
				}
				publishCalls++
				publish = call.Pos()
				if len(call.Args) != 4 || text(call.Args[0]) != "cw.cacheScope" || text(call.Args[1]) != "cw.cacheImages" || text(call.Args[2]) != "cw.commitTs" || text(call.Args[3]) != "cacheToken" {
					reject("publication must pair coalesced final images with their own commit timestamp")
				}
			case "putNodeLocked":
				if !publisher && !(name == "badger_cache.go" && fn.Name.Name == "cacheFillNode") {
					reject("node payload bypasses commit publisher/read fill")
				}
			case "putEdgeLocked":
				if !publisher && !(name == "badger_relationship_cache_owner.go" && fn.Name.Name == "cacheFillEdgeAtVersion") {
					reject("relationship payload bypasses commit publisher/read fill")
				}
			case "commitOutcome":
				if owner && sel.Pos() > committed {
					committed = sel.Pos()
				}
			case "finish":
				if owner && text(sel.X) == "cw.large" && sel.Pos() > committed {
					committed = sel.Pos()
				}
			case "cacheFillNode", "cacheFillQueryNode", "cacheFillEdge", "cacheFillEdgeAtVersion", "cacheStoreNodeBody", "storeAdjacencyCache", "edgeTypeCacheFill":
				if callback {
					reject("operation callback cannot install a read payload")
				}
			}
			return true
		})
		if owner && (publishCalls != 1 || committed == token.NoPos || publish <= committed) {
			problems = append(problems, "logical commit must publish final images exactly once after completion")
		}
	}
	return problems
}

func TestCommitCachePublisherSingleOwner(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		source, err := os.ReadFile(entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		for _, problem := range commitCachePublisherViolations(entry.Name(), source) {
			t.Error(problem)
		}
	}
}

func TestCommitCachePublisherGuardMutations(t *testing.T) {
	for _, code := range []string{
		"b.putEdgeLocked(op.Edge,0,ts)",
		"f:=b.putNodeLocked;f(op.Node,ts)",
		"b.cacheStoreEdge(op.Edge,token...)",
		"b.cacheFillEdge(gen,op.Edge,ts)",
		"b.cacheFillQueryNode(gen,op.Node,ts)",
		"b.cacheStoreNodeBody(op.NodeID,ts,op.Node)",
		"b.edgeTypeCacheFill(gen,kind,edges,ts)",
		"b.storeAdjacencyCache(gen,id,ids,ts,true)",
		"f:=b.publishCommitCacheImages;_=f",
	} {
		if len(commitCachePublisherViolations("badger_write_behind.go", []byte("package storage;func(b *BadgerEngine)publishCommitOperations(){"+code+"}"))) == 0 {
			t.Fatalf("callback publisher escaped guard: %s", code)
		}
	}
	if len(commitCachePublisherViolations("badger_cache.go", []byte("package storage;func(b *BadgerEngine)cacheOnNodeUpdated(n *Node,before ...uint64){}"))) == 0 {
		t.Fatal("operation callback token escaped guard")
	}
	source, err := os.ReadFile("badger_commit_writer.go")
	if err != nil {
		t.Fatal(err)
	}
	call := "cw.engine.publishCommitCacheImages(cw.cacheScope, cw.cacheImages, cw.commitTs, cacheToken)"
	if !strings.Contains(string(source), call) {
		t.Fatal("commit image publisher anchor missing")
	}
	for name, mutated := range map[string]string{
		"deleted":           strings.Replace(string(source), call, "", 1),
		"method value":      strings.Replace(string(source), call, "_ = cw.engine.publishCommitCacheImages", 1),
		"sampled timestamp": strings.Replace(string(source), call, "cw.engine.publishCommitCacheImages(cw.cacheScope, cw.cacheImages, cw.engine.nodeCacheCommitTs.Load(), cacheToken)", 1),
		"reordered":         strings.Replace(strings.Replace(string(source), call, "", 1), "cw.collectPendingEmbeddingAdds(cw.txn)", call+"\n cw.collectPendingEmbeddingAdds(cw.txn)", 1),
	} {
		t.Run(name, func(t *testing.T) {
			if len(commitCachePublisherViolations("badger_commit_writer.go", []byte(mutated))) == 0 {
				t.Fatal("commit publisher mutation survived")
			}
		})
	}
}
