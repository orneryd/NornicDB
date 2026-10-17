package search

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// F-1372-CLUSTER enumeration: the cluster generation has one owner. These
// source checks fail when a new code path writes generation state, mutates
// the cluster index or a staged clustering, takes the owner's locks out of
// order, or hand-parses vector-entry keys, outside the owner.

// clusterOwnerFuncs may change the cluster generation; methods of
// clusterGeneration are part of the owner too.
var clusterOwnerFuncs = map[string]bool{
	"applyClusterAdmission":     true,
	"publishClusterGeneration":  true,
	"retireClusterGeneration":   true,
	"admitClusterVectorLocked":  true,
	"admitClusterVectorsLocked": true,
	"retireClusterVectorLocked": true,
	"openClusterStaging":        true,
	"closeClusterStaging":       true,
	// Owner-internal steps, called only from the entries above.
	"admitLocked":                      true,
	"beginClusterNodeChangeLocked":     true,
	"endClusterNodeChangeLocked":       true,
	"applyClusterPolicyLocked":         true,
	"reconcileClusterPopulationLocked": true,
	"publishClusterServing":            true,
}

// Fields that hold cluster generation state; written only by the owner.
var clusterGenerationFields = map[string]bool{
	"clusterIndex": true, "clusterGen": true, "clusterStaging": true,
	"clusterHNSW": true, "clusterLexicalProfiles": true,
}

// Fields the owner replaced: they must not exist.
var retiredClusterFields = map[string]bool{"clusterHNSW": true, "clusterLexicalProfiles": true}

// ClusterIndex / Clustering methods that change published or staged state.
var clusterMutators = map[string]bool{
	"Add": true, "AddBatch": true, "Remove": true, "PublishClustering": true,
	"Clear": true, "RestoreClusteringState": true, "ClusterWithContext": true,
	"Cluster": true, "Release": true, "OnNodeUpdate": true,
	"AssignToClustering": true, "AssignLive": true, "Unassign": true,
}

type clusterSourceFunc struct {
	file string
	name string
	recv string
	decl *ast.FuncDecl
}

func clusterSourceFuncs(t *testing.T) []clusterSourceFunc {
	t.Helper()
	fset := token.NewFileSet()
	matches, err := filepath.Glob("*.go")
	require.NoError(t, err)
	var funcs []clusterSourceFunc
	for _, path := range matches {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		src, err := os.ReadFile(path)
		require.NoError(t, err)
		file, err := parser.ParseFile(fset, path, src, 0)
		require.NoError(t, err)
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			recv := ""
			if fn.Recv != nil && len(fn.Recv.List) == 1 {
				switch rt := fn.Recv.List[0].Type.(type) {
				case *ast.StarExpr:
					if id, ok := rt.X.(*ast.Ident); ok {
						recv = id.Name
					}
				case *ast.Ident:
					recv = rt.Name
				}
			}
			funcs = append(funcs, clusterSourceFunc{file: path, name: fn.Name.Name, recv: recv, decl: fn})
		}
	}
	return funcs
}

func (f clusterSourceFunc) owner() bool {
	return clusterOwnerFuncs[f.name] || f.recv == "clusterGeneration"
}

// clusterIndexHandles are the identifiers bound to the cluster index or a
// staged clustering within fn: parameters typed *gpu.ClusterIndex or
// *gpu.Clustering, and locals assigned from <x>.clusterIndex.
func clusterIndexHandles(fn *ast.FuncDecl) map[string]bool {
	handles := map[string]bool{}
	if fn.Type.Params != nil {
		for _, field := range fn.Type.Params.List {
			star, ok := field.Type.(*ast.StarExpr)
			if !ok {
				continue
			}
			if sel, ok := star.X.(*ast.SelectorExpr); ok && (sel.Sel.Name == "ClusterIndex" || sel.Sel.Name == "Clustering") {
				for _, name := range field.Names {
					handles[name.Name] = true
				}
			}
		}
	}
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		assign, ok := n.(*ast.AssignStmt)
		if !ok {
			return true
		}
		for i, rhs := range assign.Rhs {
			if sel, ok := rhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "clusterIndex" && i < len(assign.Lhs) {
				if id, ok := assign.Lhs[i].(*ast.Ident); ok {
					handles[id.Name] = true
				}
			}
		}
		return true
	})
	return handles
}

func TestClusterGenerationOwnerAllowList(t *testing.T) {
	var violations []string
	for _, fn := range clusterSourceFuncs(t) {
		handles := clusterIndexHandles(fn.decl)
		ast.Inspect(fn.decl.Body, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.SelectorExpr:
				if retiredClusterFields[node.Sel.Name] {
					violations = append(violations, fmt.Sprintf("%s uses retired field %s", fn.file+":"+fn.name, node.Sel.Name))
				}
			case *ast.AssignStmt:
				for _, lhs := range node.Lhs {
					target := lhs
					if idx, ok := target.(*ast.IndexExpr); ok {
						target = idx.X
					}
					if sel, ok := target.(*ast.SelectorExpr); ok && clusterGenerationFields[sel.Sel.Name] && !fn.owner() {
						violations = append(violations, fmt.Sprintf("%s writes %s outside the owner", fn.file+":"+fn.name, sel.Sel.Name))
					}
				}
			case *ast.CallExpr:
				sel, ok := node.Fun.(*ast.SelectorExpr)
				if !ok || !clusterMutators[sel.Sel.Name] || fn.owner() {
					return true
				}
				onIndex := false
				switch x := sel.X.(type) {
				case *ast.SelectorExpr:
					onIndex = x.Sel.Name == "clusterIndex"
				case *ast.Ident:
					onIndex = handles[x.Name]
				}
				if onIndex || sel.Sel.Name == "AssignToClustering" {
					violations = append(violations, fmt.Sprintf("%s calls %s on the cluster generation outside the owner", fn.file+":"+fn.name, sel.Sel.Name))
				}
			}
			return true
		})
	}
	sort.Strings(violations)
	require.Empty(t, violations)
}

var clusterLockRank = map[string]int{"pipelineMu": 1, "indexMu": 2, "clusterStateMu": 3, "mu": 4}

// Owner entries take pipelineMu -> indexMu -> clusterStateMu -> mu in order
// (statement order; deferred unlocks hold to the end). The one known
// exception is ConfigureHNSWQuantization's indexMu -> pipelineMu
// (setHNSWQuantizationOverride; sol r5).
func TestClusterGenerationOwnerLockOrder(t *testing.T) {
	var violations []string
	for _, fn := range clusterSourceFuncs(t) {
		takesState := false
		ast.Inspect(fn.decl.Body, func(n ast.Node) bool {
			if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "clusterStateMu" {
				takesState = true
			}
			return true
		})
		if !fn.owner() && !takesState {
			continue
		}
		held := map[string]int{}
		ast.Inspect(fn.decl.Body, func(n ast.Node) bool {
			if _, ok := n.(*ast.FuncLit); ok {
				return false
			}
			if _, ok := n.(*ast.DeferStmt); ok {
				return false
			}
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			lock, ok := sel.X.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if lock.Sel.Name == "mu" {
				if recv, ok := lock.X.(*ast.Ident); !ok || recv.Name != "s" {
					return true
				}
			}
			rank, ok := clusterLockRank[lock.Sel.Name]
			if !ok {
				return true
			}
			switch sel.Sel.Name {
			case "Lock", "RLock":
				for name, r := range held {
					if r > rank && !(fn.name == "setHNSWQuantizationOverride" && name == "indexMu" && lock.Sel.Name == "pipelineMu") {
						violations = append(violations, fmt.Sprintf("%s takes %s while holding %s", fn.file+":"+fn.name, lock.Sel.Name, name))
					}
				}
				held[lock.Sel.Name] = rank
			case "Unlock", "RUnlock":
				delete(held, lock.Sel.Name)
			}
			return true
		})
	}
	sort.Strings(violations)
	require.Empty(t, violations)
}

// The files #1372 changes decode vector-entry keys through one decoder
// (audit W12) instead of hand-parsing the ambiguous key format.
func TestClusterGenerationKeyDecodingOwner(t *testing.T) {
	var violations []string
	for _, fn := range clusterSourceFuncs(t) {
		switch fn.file {
		case "hybrid_cluster_routing.go", "vector_query_spec.go", "cluster_generation.go":
		default:
			continue
		}
		ast.Inspect(fn.decl.Body, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.BasicLit:
				if node.Kind == token.STRING && (strings.Contains(node.Value, "-chunk-") || strings.Contains(node.Value, "-named-") || strings.Contains(node.Value, "-prop-")) {
					violations = append(violations, fmt.Sprintf("%s parses a vector key literal %s", fn.file+":"+fn.name, node.Value))
				}
			case *ast.CallExpr:
				if id, ok := node.Fun.(*ast.Ident); ok && id.Name == "normalizeVectorResultIDToNodeID" {
					violations = append(violations, fmt.Sprintf("%s hand-decodes a vector key (use decodeVectorKey)", fn.file+":"+fn.name))
				}
			}
			return true
		})
	}
	sort.Strings(violations)
	require.Empty(t, violations)
}

// The Service fields holding cluster generation state are exactly the
// owner's: the population index, the published generation, the staging log
// and their lock (plus configuration and the run flag).
func TestClusterGenerationOwnerFieldSet(t *testing.T) {
	want := []string{"clusterEnabled", "clusterGen", "clusterIndex", "clusterStaging", "clusterStateMu", "clusterUsesGPU", "minEmbeddingsForClustering"}
	var got []string
	typ := reflect.TypeOf(Service{})
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		name := strings.ToLower(field.Name)
		if strings.Contains(name, "cluster") {
			got = append(got, field.Name)
		}
	}
	sort.Strings(got)
	require.Equal(t, want, got)
}
