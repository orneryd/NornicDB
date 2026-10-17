package search

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// populationBoundaryOps are the operations a population runs on a structure
// live writes also reach: snapshot or persist it, reload it, replace (clear)
// it or retire (close) it. Method values count as well as calls: a save
// handed to a publisher is still a save.
var populationBoundaryOps = map[string]bool{
	"Save": true, "saveSnapshot": true, "Load": true, "loadSnapshot": true,
	"Clear": true, "Close": true,
}

// populationBoundaryPersistFuncs persist a structure through a package
// function rather than a method.
var populationBoundaryPersistFuncs = map[string]bool{
	"saveIVFHNSWDir": true, "saveIVFPQBundleSnapshotIf": true,
}

// populationBoundaryIgnored are receivers that are not write-reachable
// structures: files, read leases and registries.
var populationBoundaryIgnored = map[string]bool{
	"file": true, "lease": true, "s.continuationRegistry": true,
}

type populationBoundarySite struct {
	key  string
	body *ast.BlockStmt
}

// populationBoundarySites lists every population operation a Service method
// in file runs on a write-reachable structure, keyed by file, method and
// target.
func populationBoundarySites(file *ast.File, name string) []populationBoundarySite {
	var out []populationBoundarySite
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil || fn.Recv == nil || len(fn.Recv.List) == 0 || types.ExprString(fn.Recv.List[0].Type) != "*Service" {
			continue
		}
		add := func(target string) {
			out = append(out, populationBoundarySite{key: name + ": " + fn.Name.Name + ": " + target, body: fn.Body})
		}
		// Selectors already seen as the function of a call; any other
		// selector naming an operation is a method value.
		called := map[*ast.SelectorExpr]bool{}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.CallExpr:
				switch fun := node.Fun.(type) {
				case *ast.Ident:
					// The builtin clear on a service map replaces that map.
					if fun.Name == "clear" && len(node.Args) == 1 && len(selectorPath(node.Args[0])) == 1 {
						add("clear(" + types.ExprString(node.Args[0]) + ")")
					}
					if populationBoundaryPersistFuncs[fun.Name] {
						add(fun.Name)
					}
				case *ast.SelectorExpr:
					called[fun] = true
					// Atomic and sync.Map loads take no path.
					if fun.Sel.Name == "Load" && len(node.Args) == 0 {
						return true
					}
					if populationBoundaryOps[fun.Sel.Name] && !populationBoundaryIgnored[types.ExprString(fun.X)] {
						add(types.ExprString(fun))
					}
				}
			case *ast.SelectorExpr:
				if !called[node] && populationBoundaryOps[node.Sel.Name] && node.Sel.Name != "Load" {
					add(types.ExprString(node) + " (method value)")
				}
			}
			return true
		})
	}
	return out
}

// populationBoundaryClass is how a site keeps the live writes admitted
// during a population. Each class names the owner primitive its method must
// use, and populationBoundaryClassHolds checks that it does.
type populationBoundaryClass string

const (
	// The structure encodes a copy taken under its own lock; no encoder
	// reads the live maps (sol r3 finding 1).
	boundaryOwnedCopy populationBoundaryClass = "owned-copy"
	// The artifact is staged and published only while its generation is
	// current; the save is acknowledged only after publication (finding 3).
	boundaryStagedPublish populationBoundaryClass = "staged-publish"
	// A source step: the structure is replaced and the recorded live writes
	// re-applied under indexMu before any reader sees it (finding 2).
	boundarySourceStep populationBoundaryClass = "source-step"
	// Explicit retirement under indexMu: the operation drops the structure,
	// live content included, or moves it intact.
	boundaryRetireLocked populationBoundaryClass = "retire-locked"
	// The vector file store serialises its own save against appends.
	boundarySelfSerialised populationBoundaryClass = "self-serialised"
	// A staging-private structure no live write reaches until a source step
	// adopts it.
	boundaryStaging populationBoundaryClass = "staging"
)

// populationBoundaryRegistry is the complete boundary: every snapshot,
// persist, reload, replace or retire of a write-reachable structure, with the
// primitive that keeps live writes. A new site fails until it is classified
// here, and a classification fails when its method stops using the primitive.
var populationBoundaryRegistry = map[string]populationBoundaryClass{
	// Persist (snapshot) sites.
	"search.go: persistBaseIndexes: s.fulltext().Save":                    boundaryOwnedCopy,
	"search.go: persistBaseIndexes: vfs.Save":                             boundarySelfSerialised,
	"search.go: persistBM25Background: fulltext.saveSnapshot":             boundaryStagedPublish,
	"search.go: persistVectorStoreBackground: vfs.Save":                   boundarySelfSerialised,
	"search.go: persistHNSWBackground: idx.Save (method value)":           boundaryStagedPublish,
	"search.go: persistIVFHNSWBackground: saveIVFHNSWDir":                 boundaryStagedPublish,
	"ivfpq_persist.go: persistIVFPQBackground: saveIVFPQBundleSnapshotIf": boundaryStagedPublish,

	// Reload sites. Only staging reads a saved image; a source step installs it.
	"vector_lifecycle_write.go: stagePersistedSources: index.loadSnapshot": boundaryStaging,
	"vector_lifecycle_write.go: stageVectorFileStore: store.Close":         boundaryStaging,
	"search.go: admitStagedVectorFileStore: vfs.Close":                     boundaryStaging,
	"search.go: admitStagedVectorFileStore: clear(s.nodeChunkSpans)":       boundaryStaging,

	// Replace sites, each re-applying the record in the same step.
	"node_population.go: resetBaseVectorSource: s.vectorFileStore.Close":                    boundarySourceStep,
	"node_population.go: resetBaseVectorSource: s.vectorIndex.Clear":                        boundarySourceStep,
	"search.go: runBasePopulationOperation: s.fulltext().Clear":                             boundarySourceStep,
	"search.go: runBasePopulationOperation: s.hnswIndex.Clear":                              boundarySourceStep,
	"node_population.go: clearNodeVectorMetadataLocked: clear(s.nodeLabels)":                boundaryRetireLocked,
	"node_population.go: clearNodeVectorMetadataLocked: clear(s.nodeTypes)":                 boundaryRetireLocked,
	"node_population.go: clearNodeVectorMetadataLocked: clear(s.nodeNamedVector)":           boundaryRetireLocked,
	"node_population.go: clearNodeVectorMetadataLocked: clear(s.nodePropVector)":            boundaryRetireLocked,
	"node_population.go: clearNodeVectorMetadataLocked: clear(s.nodeChunkVectors)":          boundaryRetireLocked,
	"node_population.go: clearNodeVectorMetadataLocked: clear(s.nodeChunkSpans)":            boundaryRetireLocked,
	"vector_lifecycle_write.go: clearPopulationWritesLocked: clear(s.nodePopulationWrites)": boundaryRetireLocked,
	"vector_lifecycle_write.go: clearPopulationWritesLocked: clear(s.edgePopulationWrites)": boundaryRetireLocked,

	// Retire sites: explicit clears, strategy switches, dimension changes and
	// persistence-off, all under indexMu.
	"search.go: ClearVectorIndex: s.vectorFileStore.Close":                     boundaryRetireLocked,
	"search.go: ClearVectorIndex: s.vectorIndex.Clear":                         boundaryRetireLocked,
	"search.go: ClearVectorIndex: s.gpuEmbeddingIndex.Clear":                   boundaryRetireLocked,
	"search.go: ClearVectorIndex: s.clusterIndex.Clear":                        boundaryRetireLocked,
	"search.go: ClearVectorIndex: s.hnswIndex.Clear":                           boundaryRetireLocked,
	"search.go: ClearVectorIndex: clear(s.edgeTypes)":                          boundaryRetireLocked,
	"search.go: ClearVectorIndex: clear(s.edgePropVector)":                     boundaryRetireLocked,
	"search.go: replaceEmptyVectorDimensionsLocked: s.vectorFileStore.Close":   boundaryRetireLocked,
	"search.go: replaceEmptyVectorDimensionsLocked: s.gpuEmbeddingIndex.Clear": boundaryRetireLocked,
	"search.go: replaceEmptyVectorDimensionsLocked: s.clusterIndex.Clear":      boundaryRetireLocked,
	"search.go: replaceEmptyVectorDimensionsLocked: s.hnswIndex.Clear":         boundaryRetireLocked,
	"search.go: applyTransitionSwapLocked: old.Clear":                          boundaryRetireLocked,
	"search.go: switchBruteStrategy: s.hnswIndex.Clear":                        boundaryRetireLocked,
	"search.go: closeVectorFileStore: s.vectorFileStore.Close":                 boundaryRetireLocked,
}

func populationBoundaryClassHolds(class populationBoundaryClass, key string, body *ast.BlockStmt) bool {
	calls := map[string]bool{}
	ast.Inspect(body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			switch fun := call.Fun.(type) {
			case *ast.SelectorExpr:
				calls[types.ExprString(fun)] = true
				calls[fun.Sel.Name] = true
			case *ast.Ident:
				calls[fun.Name] = true
			}
		}
		return true
	})
	method := strings.Split(key, ": ")[1]
	switch class {
	case boundaryOwnedCopy:
		// Save copies, then encodes; no other encoder exists.
		return strings.HasSuffix(key, ".Save") || strings.HasSuffix(key, ".saveSnapshot")
	case boundaryStagedPublish:
		return calls["publishStagedArtifact"] || calls["publishPersistedArtifact"]
	case boundarySourceStep:
		return calls["beginSourceStepLocked"] && calls["replayPopulationWritesLocked"]
	case boundaryRetireLocked:
		return strings.HasSuffix(method, "Locked") || calls["s.indexMu.Lock"]
	case boundarySelfSerialised:
		return strings.HasSuffix(key, "vfs.Save")
	case boundaryStaging:
		return strings.HasPrefix(method, "stage") || calls["takeVectors"]
	}
	return false
}

// F-I86 sol r3: the population boundary is enumerated from the code, not by
// hand. The r2 enumeration listed artifact publications and missed the base
// BM25 checkpoint, because it treated the population's own persistence as
// exclusive; with live writes admitted during a population, every operation
// on a write-reachable structure is on the boundary.
func TestPopulationBoundaryIsEnumerated(t *testing.T) {
	probe := "package search\nfunc (s *Service) leak() { s.fulltext().Save(p); clear(s.nodeLabels); file.Close(); publish(idx.Save) }\n"
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, "probe.go", probe, 0)
	require.NoError(t, err)
	require.Len(t, populationBoundarySites(parsed, "probe.go"), 3, "the enumeration must see persist calls, method values and clears, and skip files")

	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	found := map[string]bool{}
	var problems []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		require.NoError(t, err)
		for _, site := range populationBoundarySites(file, name) {
			found[site.key] = true
			class, ok := populationBoundaryRegistry[site.key]
			if !ok {
				problems = append(problems, fmt.Sprintf("unclassified: %q", site.key))
				continue
			}
			if !populationBoundaryClassHolds(class, site.key, site.body) {
				problems = append(problems, fmt.Sprintf("%s does not hold for %q", class, site.key))
			}
		}
	}
	for key := range populationBoundaryRegistry {
		if !found[key] {
			problems = append(problems, fmt.Sprintf("stale: %q", key))
		}
	}
	sort.Strings(problems)
	problems = slices.Compact(problems)
	require.Empty(t, problems, strings.Join(problems, "\n"))
}
