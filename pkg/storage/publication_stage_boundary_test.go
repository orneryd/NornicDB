package storage

import (
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The publication boundary (F-PUBLISH), extracted from this package's
// source so it stays complete as the code changes:
//   - the publishing commit paths: every function that hands the ordered
//     stage a publication (badger_publication_stage.go);
//   - every function a publication can reach, and every lock it takes there;
//   - the operation types a commit carries.
//
// TestPublicationStageBoundary fails when the extraction finds a publishing
// path, a lock (a consumer) or an operation type that the tables below don't
// classify, or a commit a publication could reach: the stage must never wait
// on itself.

// publishingPaths maps each function that hands the stage a publication to
// the test that runs it under a held older publication.
var publishingPaths = map[string]string{
	"CreateNode":                        "TestPostCommitPublicationOrder (create)",
	"UpdateNode":                        "TestPostCommitPublicationOrder (update, restore, label add/remove)",
	"DeleteNode":                        "TestPostCommitPublicationOrder (delete)",
	"CreateEdge":                        "TestPostCommitEdgePublicationOrder (create)",
	"UpdateEdge":                        "TestPostCommitEdgePublicationOrder (update, restore, type change)",
	"DeleteEdge":                        "TestPostCommitEdgePublicationOrder (delete)",
	"Commit":                            "TestPostCommitPublicationOrder, TestPostCommitEdgePublicationOrder (older-tx/newer-tx)",
	"publication":                       "the transaction's and the commit writer's publication",
	"applyBufferedCommitsWithCountPublication": "TestPostCommitPublicationOrderWriteBehindReplay",
	"EnqueueDeindexIfSuppressed":        "TestPostCommitPublicationOrderSuppression",
	"UpdateNodeEmbeddingSidecar":        "TestPostCommitPublicationOrderEmbeddingSidecar",
}

// stageLocks classifies every lock a publication takes: the consumer it
// guards. A publishing committer must not hold any of them across its
// commit (the deadlock analysis in the PR body); a new one fails the test.
var stageLocks = map[string]string{
	"b.nodeCacheMu":                  "node and query-node caches",
	"b.edgeCacheMu":                  "edge cache",
	"b.labelFirstCacheMu":            "label index cache",
	"b.edgeTypeCacheMu":              "edge-type index cache",
	"b.adjCacheMu":                   "adjacency cache",
	"b.namespaceCountsMu":            "node and edge counts",
	"b.schemasMu":                    "schema lookup (read lock)",
	"sm.mu":                          "schema: index and constraint lookup (read lock)",
	"idx.mu":                         "property and composite indexes",
	"constraint.mu":                  "unique-value registry",
	"set.mu":                         "relationship-uniqueness index",
	"p.mu":                           "pending-embedding candidates",
	"o.mu":                           "the stage's pending set (oracle)",
	"publicationStageHookMu":         "test seam",
	"commitTailHookMu":               "test seam",
}

// commitFunctions must never be reachable from a publication.
var commitFunctions = []string{
	"commitEngineWrite", "commitEngineWriteWithRead", "commitEngineWriteHeld", "withUpdate", "withNotifyingUpdate",
	"withPublishingUpdate", "withUpdateUnits", "commitPublishing", "commitShared", "commitBatch", "beginLargeCommit",
	"applyThrough", "waitPublished", "CommitAt", "Commit", "CommitContext",
}

// operationCoverage maps every operation type to the matrix rows that cover
// it, or why it publishes nothing.
var operationCoverage = map[string]string{
	"OpCreateNode":      "create, delete then recreate",
	"OpUpdateNode":      "update, restore, label add, label remove, two updates in one transaction",
	"OpDeleteNode":      "delete, delete without a snapshot (TestPostCommitPublicationWithinOneCommit)",
	"OpCreateEdge":      "edge create (delete then recreate)",
	"OpUpdateEdge":      "edge update, restore, type change",
	"OpDeleteEdge":      "edge delete",
	"OpUpdateEmbedding": "no derived state: embeddings are regenerable; the sidecar write path is TestPostCommitPublicationOrderEmbeddingSidecar",
}

type packageSource struct {
	fset  *token.FileSet
	funcs map[string][]*ast.FuncDecl // by function or method name
	files []*ast.File
}

func parsePackageSource(t *testing.T) *packageSource {
	t.Helper()
	src := &packageSource{fset: token.NewFileSet(), funcs: map[string][]*ast.FuncDecl{}}
	paths, err := filepath.Glob("*.go")
	require.NoError(t, err)
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		file, err := parser.ParseFile(src.fset, path, data, 0)
		require.NoError(t, err)
		src.files = append(src.files, file)
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Body != nil {
				src.funcs[fn.Name.Name] = append(src.funcs[fn.Name.Name], fn)
			}
		}
	}
	return src
}

func (src *packageSource) text(node ast.Node) string {
	var b strings.Builder
	_ = printer.Fprint(&b, src.fset, node)
	return b.String()
}

// stageRoots returns the publications in the source, with the function each
// is handed over in: closures assigned to a commit writer's publish field or
// to a local named publish, closures passed to withPublishingUpdate, and the
// publication methods.
func (src *packageSource) stageRoots() map[string][]ast.Node {
	roots := map[string][]ast.Node{}
	for _, file := range src.files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			if fn.Name.Name == "publication" {
				roots[fn.Name.Name] = append(roots[fn.Name.Name], fn.Body)
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.AssignStmt:
					for i, lhs := range n.Lhs {
						if i >= len(n.Rhs) {
							break
						}
						lit, isLit := n.Rhs[i].(*ast.FuncLit)
						if !isLit {
							continue
						}
						switch target := lhs.(type) {
						case *ast.SelectorExpr:
							if target.Sel.Name == "publish" {
								roots[fn.Name.Name] = append(roots[fn.Name.Name], lit.Body)
							}
						case *ast.Ident:
							if target.Name == "publish" {
								roots[fn.Name.Name] = append(roots[fn.Name.Name], lit.Body)
							}
						}
					}
				case *ast.CallExpr:
					if sel, ok := n.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "withPublishingUpdate" && len(n.Args) >= 2 {
						if lit, ok := n.Args[1].(*ast.FuncLit); ok {
							roots[fn.Name.Name] = append(roots[fn.Name.Name], lit.Body)
						}
					}
				}
				return true
			})
		}
	}
	return roots
}

func calledName(call *ast.CallExpr) string {
	switch fun := call.Fun.(type) {
	case *ast.Ident:
		return fun.Name
	case *ast.SelectorExpr:
		return fun.Sel.Name
	}
	return ""
}

// reach walks the package call graph (by function name, an
// over-approximation) from roots, returning the functions reached and the
// locks taken, each lock with the functions taking it.
func (src *packageSource) reach(roots []ast.Node) (reached map[string]bool, locks map[string][]string) {
	reached, locks = map[string]bool{}, map[string][]string{}
	var visit func(owner string, node ast.Node)
	visit = func(owner string, node ast.Node) {
		ast.Inspect(node, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			name := calledName(call)
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok && (name == "Lock" || name == "RLock" || name == "TryLock" || name == "TryRLock") {
				lock := src.text(sel.X)
				if !slices.Contains(locks[lock], owner) {
					locks[lock] = append(locks[lock], owner)
				}
				return true
			}
			decls, declared := src.funcs[name]
			if !declared || reached[name] {
				return true
			}
			reached[name] = true
			for _, decl := range decls {
				visit(name, decl.Body)
			}
			return true
		})
	}
	for _, root := range roots {
		visit("publication", root)
	}
	return reached, locks
}

func TestPublicationStageBoundary(t *testing.T) {
	src := parsePackageSource(t)
	roots := src.stageRoots()

	var paths []string
	var all []ast.Node
	for path, nodes := range roots {
		paths = append(paths, path)
		all = append(all, nodes...)
	}
	sort.Strings(paths)
	t.Logf("publishing paths: %s", strings.Join(paths, ", "))
	for _, path := range paths {
		_, known := publishingPaths[path]
		require.True(t, known, "publishing path %s has no case in the publication matrix (publishingPaths)", path)
	}
	for path := range publishingPaths {
		if _, found := roots[path]; !found && path != "Commit" {
			t.Errorf("publishingPaths lists %s, which no longer hands the stage a publication", path)
		}
	}

	reached, locks := src.reach(all)
	var names []string
	for name := range reached {
		names = append(names, name)
	}
	sort.Strings(names)
	t.Logf("functions a publication reaches (%d): %s", len(names), strings.Join(names, ", "))
	for _, name := range commitFunctions {
		require.False(t, reached[name], "a publication reaches %s: the stage could wait on itself", name)
	}

	var lockNames []string
	for lock := range locks {
		lockNames = append(lockNames, lock)
	}
	sort.Strings(lockNames)
	for _, lock := range lockNames {
		t.Logf("stage lock %-28s taken in %s", lock, strings.Join(locks[lock], ", "))
		_, known := stageLocks[lock]
		require.True(t, known, "a publication takes %s, which stageLocks doesn't classify: add its consumer, a matrix case and the deadlock analysis", lock)
	}

	// Every operation type a commit carries is covered.
	var ops []string
	for _, file := range src.files {
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}
			for _, spec := range gen.Specs {
				value := spec.(*ast.ValueSpec)
				if ident, ok := value.Type.(*ast.Ident); !ok || ident.Name != "OperationType" {
					continue
				}
				for _, name := range value.Names {
					if src.fset.Position(name.Pos()).Filename == "transaction.go" {
						ops = append(ops, name.Name)
					}
				}
			}
		}
	}
	sort.Strings(ops)
	t.Logf("operation types: %s", strings.Join(ops, ", "))
	require.NotEmpty(t, ops)
	for _, op := range ops {
		_, known := operationCoverage[op]
		require.True(t, known, "operation type %s has no matrix coverage (operationCoverage)", op)
	}
	require.NotEmpty(t, publicationMatrixNodeConsumers)
	require.NotEmpty(t, publicationMatrixEdgeConsumers)
}
