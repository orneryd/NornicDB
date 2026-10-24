package storage

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"strings"
	"testing"
)

// This inventory is deliberately closed: a new acquisition or call inside an
// adjacency critical section requires auditing its waits, including callees.
// Cache capture may already hold writeBehind.mu.RLock; issuance never takes
// that mutex. The only nested locks are adjacency -> body -> type.
// Their holders do map/copy/atomic work and never acquire adjacency in
// reverse order. No critical section calls storage, scoring, replay or waits
// for publication. In particular, the fill hook and visibility filtering run
// outside adjacency/body locks.
func TestAdjacencyCacheLockInventory(t *testing.T) {
	inventory := map[string]string{
		"captureCachedAdjacency":           "body RLock; existing overlay maps, copies and versions only",
		"adjCacheCapture":                  "no nested lock; map and version reads only",
		"adjCacheLoadOutgoing":             "no nested lock; map and version reads only",
		"adjCacheLoadIncoming":             "no nested lock; map and version reads only",
		"storeAdjacencyCache":              "no nested lock; maps, copies and atomics only; hook precedes lock",
		"adjCacheInvalidateForEdge":        "no nested lock; payload discard retains revocation; atomic generation only",
		"adjCacheInvalidateAll":            "no nested lock; map reset and atomic generation only",
		"invalidateCachesAfterRestore":     "no nested lock; all other cache locks released before adjacency",
		"invalidateRelationshipCacheScope": "node already held; body -> type; prepared key updates only; no dictionary lookup or I/O",
	}
	allowed := map[string]bool{}
	for _, name := range strings.Fields(`len make max copy append string []byte EdgeID cacheVisibleSince copyEdge read visit stampBody stampAdj stampType operations raw rawKey
 b.adjCacheMu.Lock b.adjCacheMu.RLock b.adjCacheMu.Unlock b.adjCacheMu.RUnlock
 b.edgeCacheMu.RLock b.edgeCacheMu.RUnlock b.edgeCacheMu.Lock b.edgeCacheMu.Unlock
 b.edgeTypeCacheMu.Lock b.edgeTypeCacheMu.Unlock
 b.adjCacheGen.current b.adjCacheGen.advance b.edgeCacheGen.advance b.edgeTypeCacheGen.advance
 b.nodeCacheCommitTs.Load w.entityStateLocked w.liveAdjacencyLocked
 delete
 entry.coherence.admits ids.coherence.admits
 b.revokeEdgeLocked b.revokeAdjacencyLocked b.revokeEdgeTypeLocked
 b.resetEdgeCacheLocked b.resetAdjacencyCacheLocked b.resetEdgeTypeCacheLocked
 b.revokeRelationshipCachesLocked`) {
		allowed[name] = true
	}
	files, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	fs := token.NewFileSet()
	for _, entry := range files {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fs, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			var first, last token.Pos
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				sel, ok := n.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "adjCacheMu" {
					return true
				}
				// Every reference must be a direct lock operation; aliases/method values
				// would invalidate the lexical critical-section audit below.
				references, direct := 0, 0
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					if s, ok := n.(*ast.SelectorExpr); ok && s == sel {
						references++
					}
					if c, ok := n.(*ast.CallExpr); ok {
						if method, ok := c.Fun.(*ast.SelectorExpr); ok && method.X == sel {
							switch method.Sel.Name {
							case "Lock", "RLock", "Unlock", "RUnlock":
								direct++
							}
						}
					}
					return true
				})
				if references != 1 || direct != 1 {
					t.Errorf("%s: escaped adjacency mutex", fs.Position(sel.Pos()))
				}
				return true
			})
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				method, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				mutex, ok := method.X.(*ast.SelectorExpr)
				if !ok || mutex.Sel.Name != "adjCacheMu" {
					return true
				}
				switch method.Sel.Name {
				case "Lock", "RLock":
					seen[fn.Name.Name]++
					if first == 0 || call.Pos() < first {
						first = call.Pos()
					}
					note, known := inventory[fn.Name.Name]
					if !known {
						t.Errorf("%s: unreviewed adjacency lock in %s", fs.Position(call.Pos()), fn.Name.Name)
					}
					t.Logf("ADJ_LOCK %s %s %s waits=%s", fs.Position(call.Pos()), fn.Name.Name, method.Sel.Name, note)
				case "Unlock", "RUnlock":
					if call.End() > last {
						last = call.End()
					}
				}
				return true
			})
			if first == 0 {
				continue
			}
			// A deferred unlock covers the whole function, including local closures.
			for _, stmt := range fn.Body.List {
				if d, ok := stmt.(*ast.DeferStmt); ok {
					if s, ok := d.Call.Fun.(*ast.SelectorExpr); ok {
						if m, ok := s.X.(*ast.SelectorExpr); ok && m.Sel.Name == "adjCacheMu" {
							last = fn.End()
						}
					}
				}
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if n == nil || n.Pos() < first || n.Pos() > last {
					return true
				}
				switch node := n.(type) {
				case *ast.CallExpr:
					var text bytes.Buffer
					_ = printer.Fprint(&text, fs, node.Fun)
					if !allowed[text.String()] {
						t.Errorf("%s: unaudited call %s while adjacency may be held", fs.Position(node.Pos()), text.String())
					}
				case *ast.SendStmt:
					t.Errorf("%s: channel send while adjacency held", fs.Position(node.Pos()))
				case *ast.UnaryExpr:
					if node.Op == token.ARROW {
						t.Errorf("%s: channel receive while adjacency held", fs.Position(node.Pos()))
					}
				}
				return true
			})
		}
	}
	for fn := range inventory {
		if seen[fn] != 1 {
			t.Error(fmt.Sprintf("%s: expected one audited acquisition, got %d", fn, seen[fn]))
		}
	}
}
