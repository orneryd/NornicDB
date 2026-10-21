package storage

import (
	"go/ast"
	"go/build/constraint"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// entityStateKeys build the keys of what a node or relationship read returns:
// the body, its embeddings and its manual suppression.
var entityStateKeys = map[string]bool{
	"nodeKey": true, "edgeKey": true, "embeddingKey": true, "embeddingChunkPartKey": true,
	"embeddingMetaKey": true, "manualSuppressionKey": true,
	// every key family a namespace owns (a namespace-wide deletion)
	"namespaceOwnsBadgerKey": true,
}

// entityStateWriters are the calls that persist or drop keys.
var entityStateWriters = map[string]bool{"Set": true, "SetEntry": true, "Delete": true, "DropPrefix": true}

// storageMutatorPopulation computes, from the package's production source,
// every exported method of the Badger engine and its transaction that reaches
// a function both building an entity-state key and writing or deleting keys.
// It over-approximates (a function may build the key only to read it), which
// only adds methods that must be classified.
func storageMutatorPopulation(t *testing.T) []string {
	t.Helper()
	fset := token.NewFileSet()
	paths, err := filepath.Glob("*.go")
	require.NoError(t, err)
	var files []*ast.File
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
		require.NoError(t, err)
		if !buildsOnLinux(file) {
			continue
		}
		files = append(files, file)
	}
	// Calls are resolved by type: a method call reaches the method of the
	// receiver's static type, not every method of that name.
	info := &types.Info{Uses: make(map[*ast.Ident]types.Object), Selections: make(map[*ast.SelectorExpr]*types.Selection)}
	conf := types.Config{Importer: importer.ForCompiler(fset, "source", nil), Error: func(error) {}}
	pkg, _ := conf.Check("github.com/orneryd/nornicdb/pkg/storage", fset, files, info)
	require.NotNil(t, pkg)
	keyOf := func(fn *types.Func) string {
		if fn == nil || fn.Pkg() != pkg {
			return ""
		}
		sig, _ := fn.Type().(*types.Signature)
		if sig == nil || sig.Recv() == nil {
			return fn.Name()
		}
		recv := sig.Recv().Type()
		if ptr, ok := recv.(*types.Pointer); ok {
			recv = ptr.Elem()
		}
		named, ok := recv.(*types.Named)
		if !ok {
			return "" // an interface method: dispatched at run time
		}
		return named.Obj().Name() + "." + fn.Name()
	}
	type fn struct {
		calls map[string]bool
		seed  bool
	}
	funcs := make(map[string]*fn)
	for _, file := range files {
		for _, decl := range file.Decls {
			d, ok := decl.(*ast.FuncDecl)
			if !ok || d.Body == nil {
				continue
			}
			name := d.Name.Name
			if d.Recv != nil && len(d.Recv.List) == 1 {
				recv := d.Recv.List[0].Type
				if star, ok := recv.(*ast.StarExpr); ok {
					recv = star.X
				}
				if id, ok := recv.(*ast.Ident); ok {
					name = id.Name + "." + name
				}
			}
			f := &fn{calls: make(map[string]bool)}
			builds, writes := false, false
			ast.Inspect(d.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				var callee *types.Func
				switch fun := call.Fun.(type) {
				case *ast.Ident:
					if entityStateKeys[fun.Name] {
						builds = true
					}
					callee, _ = info.Uses[fun].(*types.Func)
				case *ast.SelectorExpr:
					if entityStateWriters[fun.Sel.Name] {
						writes = true
					}
					if sel := info.Selections[fun]; sel != nil {
						callee, _ = sel.Obj().(*types.Func)
					} else {
						callee, _ = info.Uses[fun.Sel].(*types.Func)
					}
				}
				if key := keyOf(callee); key != "" {
					f.calls[key] = true
				}
				return true
			})
			f.seed = builds && writes
			funcs[name] = f
		}
	}
	reaches := make(map[string]bool)
	var visit func(name string, seen map[string]bool) bool
	visit = func(name string, seen map[string]bool) bool {
		if r, ok := reaches[name]; ok {
			return r
		}
		if seen[name] {
			return false
		}
		seen[name] = true
		f := funcs[name]
		if f == nil {
			return false
		}
		result := f.seed
		for callee := range f.calls {
			if result {
				break
			}
			result = visit(callee, seen)
		}
		reaches[name] = result
		return result
	}
	var population []string
	for name := range funcs {
		receiver, method, ok := strings.Cut(name, ".")
		if !ok || (receiver != "BadgerEngine" && receiver != "BadgerTransaction") || !ast.IsExported(method) {
			continue
		}
		if visit(name, make(map[string]bool)) {
			population = append(population, name)
		}
	}
	sort.Strings(population)
	return population
}

// buildsOnLinux reports whether a file's build constraint admits the
// production build the CI runs (linux/amd64, noui, nolocalllm).
func buildsOnLinux(file *ast.File) bool {
	for _, group := range file.Comments {
		for _, comment := range group.List {
			if !strings.HasPrefix(comment.Text, "//go:build ") {
				continue
			}
			expr, err := constraint.Parse(comment.Text)
			if err != nil {
				return true
			}
			return expr.Eval(func(tag string) bool {
				switch tag {
				case "linux", "amd64", "noui", "nolocalllm", "cgo", "unix":
					return true
				}
				return strings.HasPrefix(tag, "go1.")
			})
		}
	}
	return true
}

// storageMutatorClassification assigns every member of the population a
// publication scenario of TestStorageMutatorsPublishTheirEvents (or
// TestDecayFlipsPublishUpdates), or the reason it publishes nothing a search
// derives from. Adding a mutator without a row fails
// TestStorageMutatorPopulationIsClassified.
var storageMutatorClassification = map[string]string{
	"BadgerEngine.CreateNode":                           scenarioPublishes,
	"BadgerEngine.UpdateNode":                           scenarioPublishes,
	"BadgerEngine.DeleteNode":                           scenarioPublishes,
	"BadgerEngine.CreateEdge":                           scenarioPublishes,
	"BadgerEngine.UpdateEdge":                           scenarioPublishes,
	"BadgerEngine.DeleteEdge":                           scenarioPublishes,
	"BadgerEngine.BulkCreateNodes":                      scenarioPublishes,
	"BadgerEngine.BulkCreateEdges":                      scenarioPublishes,
	"BadgerEngine.BulkDeleteNodes":                      scenarioPublishes,
	"BadgerEngine.BulkDeleteEdges":                      scenarioPublishes,
	"BadgerEngine.SetNodeManuallySuppressed":            scenarioPublishes,
	"BadgerEngine.SetEdgeManuallySuppressed":            scenarioPublishes,
	"BadgerEngine.UpdateNodeEmbeddingSidecar":           scenarioPublishes,
	"BadgerEngine.ClearAllEmbeddings":                   scenarioPublishes,
	"BadgerEngine.ClearAllEmbeddingsForPrefix":          scenarioPublishes,
	"BadgerEngine.DeleteByPrefix":                       scenarioPublishes,
	"BadgerEngine.EnqueueDeindexIfSuppressed":           scenarioDecay,
	"BadgerEngine.ReconcileDecaySuppression":            scenarioDecay,
	"BadgerEngine.ReconcileDecaySuppressionWithChanges": scenarioDecay,
	"BadgerTransaction.Commit":                          scenarioPublishes + " (a transaction commit)",
	"BadgerTransaction.CommitContext":                   "Commit's implementation; " + scenarioPublishes,
	"BadgerTransaction.CommitContextWithPublication":    "Commit's implementation; " + scenarioPublishes,
	"BadgerTransaction.CreateNode":                      "buffers in the transaction; published by its Commit",
	"BadgerTransaction.CreateEdge":                      "buffers in the transaction; published by its Commit",
	"BadgerTransaction.UpdateNode":                      "buffers in the transaction; published by its Commit",
	"BadgerTransaction.BulkCreateEdges":                 "buffers in the transaction; published by its Commit",
	"BadgerTransaction.CreateMintedNode":                "CreateNode for a minted ID; published by its Commit",
	"BadgerTransaction.CreateMintedEdge":                "CreateEdge for a minted ID; published by its Commit",
	"BadgerTransaction.BulkCreateMintedEdges":           "BulkCreateEdges for minted IDs; published by its Commit",
	"BadgerTransaction.PrepareMergeKey":                 "reserves a MERGE key in the transaction; no entity state",
	"BadgerTransaction.PrepareMergeNode":                "admits MERGE constraint tuples and may advance the snapshot; entity writes are published by CreateNode/UpdateNode at Commit",
	"BadgerTransaction.PrepareMergeNodeForWrite":        "admits MERGE write keys and may land previously published buffered commits; new entity writes are published by its Commit",
	"BadgerEngine.Restore":                              "replaces the whole store offline; DB.Restore owns the reset of search (restartSearchAfterRestore)",
	"BadgerEngine.ConsumeCleanShutdownMarker":           "a startup marker; no entity state",
	"BadgerEngine.FindNodeNeedingEmbedding":             "maintains the pending-embedding work index; no entity state",
	"BadgerEngine.RefreshPendingEmbeddingsIndex":        "maintains the pending-embedding work index; no entity state",
	"BadgerEngine.GetSchema":                            "persists schema metadata; reads entities only",
	"BadgerEngine.GetSchemaForNamespace":                "persists schema metadata; reads entities only",
	"BadgerEngine.GetTemporalNodeAsOfInNamespace":       "temporal history index; below the current version",
	"BadgerEngine.IsCurrentTemporalNode":                "temporal history index; below the current version",
	"BadgerEngine.IsCurrentTemporalNodeInNamespace":     "temporal history index; below the current version",
	"BadgerEngine.PruneTemporalHistory":                 "temporal history index; below the current version",
	"BadgerEngine.RebuildTemporalIndexes":               "temporal history index; below the current version",
}

const (
	scenarioPublishes = "TestStorageMutatorsPublishTheirEvents"
	scenarioDecay     = "TestDecayFlipsPublishUpdates"
)

// The population of storage mutators is derived from the source, so a new one
// cannot be added without its publication being tested or its exclusion
// reasoned.
func TestStorageMutatorPopulationIsClassified(t *testing.T) {
	population := storageMutatorPopulation(t)
	require.NotEmpty(t, population)
	var missing []string
	for _, name := range population {
		if storageMutatorClassification[name] == "" {
			missing = append(missing, name)
		}
	}
	require.Empty(t, missing, "storage mutators without a publication scenario or a reasoned exclusion:\n%s", strings.Join(missing, "\n"))
	var stale []string
	inPopulation := make(map[string]bool, len(population))
	for _, name := range population {
		inPopulation[name] = true
	}
	for name := range storageMutatorClassification {
		if !inPopulation[name] {
			stale = append(stale, name)
		}
	}
	sort.Strings(stale)
	require.Empty(t, stale, "classified methods no longer in the population")
}
