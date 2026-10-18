package search

// Every exported operation that changes what the search indexes hold, or what
// a write applies to, enters the population sequence (node_population_writes.go), so a
// build overlapping it cannot reorder it. The check is mechanical: each
// exported Service method's in-package call closure is computed from the
// source, and one that writes index state must reach the sequence's entry
// points, or be listed below with the reason it needs no ordering.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// buildSequenceEntries are the entry points into the build sequence.
var buildSequenceEntries = map[string]bool{
	"lockNodeMutation": true, "queueOnPopulation": true, "orderWithPopulation": true, "orderWithPopulationAs": true, "orderDimensionChange": true,
	// A population is the sequence's owner: what it writes is the sequence.
	"runBasePopulationOperation": true,
}

// buildSequenceExempt are writers that need no ordering with the sequence.
var buildSequenceExempt = map[string]string{
	"BuildIndexes":             "the population itself, the sequence's owner",
	"ClearVectorIndex":         "pauses and joins the population, which applies its sequence on exit",
	"Close":                    "cancels and joins the population, which applies its sequence on exit",
	"SetPersistenceEnabled":    "pauses and joins the population before it changes the vector source",
	"PersistIndexesToDisk":     "writes files, not index contents",
	"MarkReadyDisabled":        "service construction, before any build",
	"EnableRelationshipSearch": "service construction, before any build",
	"IndexRelationship":        "the relationship index is not part of the population; updated at the call, in call order",
	"RemoveRelationship":       "the relationship index is not part of the population; updated at the call, in call order",
}

// buildSequenceState are the Service fields whose change alters what the
// indexes hold or what a write applies to.
var buildSequenceState = map[string]bool{
	"fulltextRef": true, "vectorIndex": true, "vectorFileStore": true,
	"nodeLabels": true, "nodeTypes": true, "nodeNamedVector": true, "nodePropVector": true,
	"nodeChunkVectors": true, "nodeChunkSpans": true, "edgeTypes": true, "edgePropVector": true,
	"relationships": true, "fulltextProperties": true, "relationshipFulltextProperties": true,
	"fulltextLabels": true, "embedTextOptions": true, "embeddingSpace": true, "bm25Enabled": true,
	"vectorEnabled": true, "vectorDimensionsFixed": true, "indexCapacityByNode": true, "indexCapacityByEdge": true,
}

var buildSequenceAtomic = map[string]bool{
	"bm25Enabled": true, "vectorEnabled": true, "vectorDimensionsFixed": true, "embeddingSpace": true,
	"relationships": true, "fulltextRef": true,
}

var buildSequenceMutators = map[string]bool{
	"Add": true, "Remove": true, "Index": true, "IndexBatch": true, "Clear": true, "Store": true, "Swap": true,
	"Update": true, "Load": true, "Delete": true, "Close": true, "Reset": true, "CompareAndSwap": true, "loadSnapshot": true,
}

type buildSequenceFunc struct {
	calls  map[string]bool
	writes bool
}

func TestPopulationSequenceOrdersEveryIndexWriter(t *testing.T) {
	funcs := parseBuildSequenceFuncs(t)
	var violations []string
	names := make([]string, 0, len(funcs))
	for name := range funcs {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if name[0] < 'A' || name[0] > 'Z' {
			continue
		}
		seen := map[string]bool{}
		writes, entered := false, false
		var writers []string
		var visit func(string)
		visit = func(n string) {
			if seen[n] {
				return
			}
			seen[n] = true
			if buildSequenceEntries[n] {
				entered = true
				return // what the sequence installs is ordered by it
			}
			fn := funcs[n]
			if fn == nil {
				return
			}
			if fn.writes {
				writes = true
				writers = append(writers, n)
			}
			for c := range fn.calls {
				visit(c)
			}
		}
		visit(name)
		if !writes || entered {
			continue
		}
		if _, ok := buildSequenceExempt[name]; ok {
			continue
		}
		sort.Strings(writers)
		violations = append(violations, name+" (writes in "+strings.Join(writers, ", ")+")")
	}
	if len(violations) > 0 {
		t.Fatalf("exported index writers outside the build sequence (route them through lockNodeMutation/orderWithPopulation, or list the reason in buildSequenceExempt): %s", strings.Join(violations, ", "))
	}
	for name := range buildSequenceExempt {
		if funcs[name] == nil {
			t.Errorf("exempt writer %s no longer exists; remove it from the list", name)
		}
	}
}

func parseBuildSequenceFuncs(t *testing.T) map[string]*buildSequenceFunc {
	t.Helper()
	fset := token.NewFileSet()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	funcs := map[string]*buildSequenceFunc{}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(fset, f, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Recv == nil || fd.Body == nil || len(fd.Recv.List) != 1 || len(fd.Recv.List[0].Names) != 1 {
				continue
			}
			if star, ok := fd.Recv.List[0].Type.(*ast.StarExpr); !ok {
				continue
			} else if id, ok := star.X.(*ast.Ident); !ok || id.Name != "Service" {
				continue
			}
			recv := fd.Recv.List[0].Names[0].Name
			fn := &buildSequenceFunc{calls: map[string]bool{}}
			funcs[fd.Name.Name] = fn
			recvField := func(e ast.Expr) (string, bool) {
				for {
					switch x := e.(type) {
					case *ast.SelectorExpr:
						if id, ok := x.X.(*ast.Ident); ok && id.Name == recv {
							return x.Sel.Name, true
						}
						e = x.X
					case *ast.IndexExpr:
						e = x.X
					case *ast.StarExpr:
						e = x.X
					case *ast.CallExpr:
						e = x.Fun
					default:
						return "", false
					}
				}
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.AssignStmt:
					for _, l := range x.Lhs {
						if name, ok := recvField(l); ok && buildSequenceState[name] {
							fn.writes = true
						}
					}
				case *ast.IncDecStmt:
					if name, ok := recvField(x.X); ok && buildSequenceState[name] {
						fn.writes = true
					}
				case *ast.CallExpr:
					if id, ok := x.Fun.(*ast.Ident); ok && (id.Name == "delete" || id.Name == "clear") && len(x.Args) > 0 {
						if name, ok := recvField(x.Args[0]); ok && buildSequenceState[name] {
							fn.writes = true
						}
					}
					sel, ok := x.Fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					if id, ok := sel.X.(*ast.Ident); ok && id.Name == recv {
						fn.calls[sel.Sel.Name] = true
					}
					if buildSequenceMutators[sel.Sel.Name] || strings.HasPrefix(sel.Sel.Name, "Set") || strings.HasPrefix(sel.Sel.Name, "Remove") {
						if name, ok := recvField(sel.X); ok && buildSequenceState[name] && !(sel.Sel.Name == "Load" && buildSequenceAtomic[name]) {
							fn.writes = true
						}
						if inner, ok := sel.X.(*ast.CallExpr); ok {
							if m, ok := inner.Fun.(*ast.SelectorExpr); ok {
								if id, ok := m.X.(*ast.Ident); ok && id.Name == recv && (m.Sel.Name == "fulltext" || m.Sel.Name == "relationshipSearch") {
									fn.writes = true
								}
							}
						}
					}
				}
				return true
			})
		}
	}
	return funcs
}
