package storagecensus

import (
	"encoding/json"
	"fmt"
	"go/token"
	"go/types"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
)

// The cache/admission population queries use the census's canonical typed
// program and VTA graph. The root worklist is discovery, not an acquisition,
// approval table or lifetime proof. No optional future compiler package or
// composition build tag is required.
type residualCompiledMember struct {
	Kind, Caller, Target, Position, Type string
}

type residualCompiledCaller struct {
	Symbol, Start, End string
}

func TestResidualCompiledPopulation(t *testing.T) {
	p, err := load(root, nil)
	require.NoError(t, err)
	runtimePackage := func(pkg string) bool {
		return strings.HasPrefix(pkg, Module) && !strings.HasPrefix(pkg, Module+"testing/")
	}
	// Exported runtime APIs, init and main remain the entry roots. Traverse
	// the existing graph through dependency/synthetic bridges; only runtime
	// module functions contribute population records below.
	var roots []string
	reached := map[*ssa.Function]bool{}
	var queue []*ssa.Function
	for _, fn := range p.funcs {
		if fn.Parent() != nil || fn.Pkg == nil || !runtimePackage(fn.Pkg.Pkg.Path()) {
			continue
		}
		if fn.Name() != "init" && fn.Name() != "main" && (fn.Object() == nil || !fn.Object().Exported()) {
			continue
		}
		roots = append(roots, p.funcName(fn))
		reached[fn] = true
		queue = append(queue, fn)
	}
	require.NotEmpty(t, roots)
	sort.Strings(roots)
	for len(queue) > 0 {
		fn := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		if node := p.cg.Nodes[fn]; node != nil {
			for _, edge := range node.Out {
				callee := edge.Callee.Func
				if callee != nil && !reached[callee] {
					reached[callee] = true
					queue = append(queue, callee)
				}
			}
		}
	}
	position := func(pos token.Pos) string {
		x := p.fset.Position(pos)
		rel, err := filepath.Rel(p.root, x.Filename)
		if err != nil {
			rel = x.Filename
		}
		return fmt.Sprintf("%s:%d:%d", filepath.ToSlash(rel), x.Line, x.Column)
	}
	cacheNames := map[string]bool{}
	for _, name := range []string{"cacheFillNode", "cacheStoreNode", "cacheStoreNodeBody", "cacheStoreWrittenNode", "cacheStoreEdge", "cacheFillEdge", "cacheFillEdgeAtVersion", "adjCacheStore", "labelCacheSetFirst", "edgeTypeCacheFill"} {
		cacheNames[name] = true
	}
	propertyNames := map[string]bool{}
	for _, name := range []string{"copyNode", "copyNodeWithoutEmbeddings", "copyNodeForCaller", "copyEdge", "CopyNode", "CopyEdge", "codecImageProperties", "canonicalPropertyValue", "ownedProperties", "ownPropertyValue", "ownRetainedValue", "ownRetainedMap", "ownedValueCopy", "ownedEntries", "canonicalPropertyList", "StoragePropertyValue", "ReadNumber", "NumericFloat64", "ReadList"} {
		propertyNames[name] = true
	}
	admissionNames := map[string]bool{}
	for _, name := range []string{"Lock", "RLock", "LockContext", "RLockContext", "acquireUniqueConstraintCommitLocks", "beginBarrierWrite", "beginLifecycleWrite", "beginMaintenanceWrite", "beginEntityWrite", "acquireEntityWriteKeys", "orderAfterBufferedCommits", "orderCovering", "beginLargeCommitContext", "waitPublished", "waitForReadersBefore", "flushContext", "FlushWriteBehind", "RetainAcceptedAuthority", "DecidePublication", "SubmitAcceptedCommand", "SubmitEntityCommit", "AppendBufferedCommit"} {
		admissionNames[name] = true
	}
	for _, name := range []string{"Acquire", "AcquireContext", "Reserve", "Admit", "Wait", "WaitApplied", "WaitSettled", "BeginTransaction", "CommitContext", "PrepareEntityCommit"} {
		admissionNames[name] = true
	}
	var members []residualCompiledMember
	seen := map[residualCompiledMember]bool{}
	add := func(row residualCompiledMember) {
		if !seen[row] {
			seen[row] = true
			members = append(members, row)
		}
	}
	for fn := range reached {
		synthetic := fn.Pkg == nil && fn.Synthetic != ""
		if !synthetic && (fn.Pkg == nil || !runtimePackage(fn.Pkg.Pkg.Path())) {
			continue
		}
		if node := p.cg.Nodes[fn]; node != nil {
			for _, edge := range node.Out {
				callee := edge.Callee.Func
				if edge.Site == nil {
					continue
				}
				pkg, name := "", callee.Name()
				if callee.Pkg != nil {
					pkg = callee.Pkg.Pkg.Path()
				} else if obj := callee.Object(); obj != nil && obj.Pkg() != nil {
					pkg = obj.Pkg().Path()
				}
				if pkg == "github.com/dgraph-io/badger/v4" {
					switch name {
					case "Set", "SetEntry", "Delete", "CommitAt", "DropAll", "DropPrefix", "Load":
						add(residualCompiledMember{"physical-call", fn.String(), callee.String(), position(edge.Site.Pos()), callee.Signature.String()})
					}
				}
				if pkg == Module+"pkg/storage" && cacheNames[name] {
					add(residualCompiledMember{"cache-call", fn.String(), callee.String(), position(edge.Site.Pos()), callee.Signature.String()})
				}
				if (pkg == Module+"pkg/storage" || pkg == Module+"pkg/cypher" || pkg == Module+"pkg/convert") && propertyNames[name] {
					add(residualCompiledMember{"property-owner-call", fn.String(), callee.String(), position(edge.Site.Pos()), callee.Signature.String()})
				}
				callerPackage := ""
				if fn.Pkg != nil {
					callerPackage = fn.Pkg.Pkg.Path()
				}
				physicalOwner := synthetic || callerPackage == Module+"pkg/storage" || callerPackage == Module+"pkg/replication"
				if admissionNames[name] && (pkg == Module+"pkg/storage" || pkg == Module+"pkg/replication" || physicalOwner && (pkg == "sync" || runtimePackage(pkg))) {
					add(residualCompiledMember{"admission-call", fn.String(), callee.String(), position(edge.Site.Pos()), callee.Signature.String()})
				}
			}
		}
		if synthetic {
			continue
		}
		for _, block := range fn.Blocks {
			for _, instruction := range block.Instrs {
				switch ins := instruction.(type) {
				case *ssa.UnOp:
					if ins.Op == token.ARROW && (fn.Pkg.Pkg.Path() == Module+"pkg/storage" || fn.Pkg.Pkg.Path() == Module+"pkg/replication") {
						add(residualCompiledMember{"channel-wait", fn.String(), "receive", position(ins.Pos()), ins.X.Type().String()})
					}
				case *ssa.Select:
					if fn.Pkg.Pkg.Path() != Module+"pkg/storage" && fn.Pkg.Pkg.Path() != Module+"pkg/replication" {
						continue
					}
					for _, state := range ins.States {
						if state.Dir == types.RecvOnly {
							add(residualCompiledMember{"channel-wait", fn.String(), "select-receive", position(state.Pos), state.Chan.Type().String()})
						}
					}
				case *ssa.MapUpdate:
					// Include typed retained Node/Edge/ID maps, even when the
					// containing helper has no familiar cache method name.
					typ := ins.Map.Type().String()
					if strings.Contains(typ, Module+"pkg/storage.Node") || strings.Contains(typ, Module+"pkg/storage.Edge") || strings.Contains(typ, Module+"pkg/storage.nodeBodyCacheEntry") {
						add(residualCompiledMember{"typed-image-map-write", fn.String(), "map-update", position(ins.Pos()), typ})
					}
				case *ssa.Store:
					if field, ok := ins.Addr.(*ssa.FieldAddr); ok {
						ptr, ok := field.X.Type().Underlying().(*types.Pointer)
						if !ok {
							continue
						}
						named, ok := types.Unalias(ptr.Elem()).(*types.Named)
						if !ok || named.Obj().Pkg() == nil || named.Obj().Pkg().Path() != Module+"pkg/storage" {
							continue
						}
						st, ok := ptr.Elem().Underlying().(*types.Struct)
						if !ok || field.Field >= st.NumFields() {
							continue
						}
						name := st.Field(field.Field).Name()
						if strings.Contains(strings.ToLower(name), "cache") {
							add(residualCompiledMember{"cache-field-store", fn.String(), name, position(ins.Pos()), field.X.Type().String()})
						}
					}
				}
			}
		}
	}
	sort.Slice(members, func(i, j int) bool {
		a, b := members[i], members[j]
		if a.Position != b.Position {
			return a.Position < b.Position
		}
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		return a.Target < b.Target
	})
	wanted := map[string]bool{}
	for _, row := range members {
		wanted[row.Caller] = true
	}
	var callers []residualCompiledCaller
	for _, fn := range p.funcs {
		if syntax := fn.Syntax(); wanted[fn.String()] && syntax != nil {
			callers = append(callers, residualCompiledCaller{fn.String(), position(syntax.Pos()), position(syntax.End())})
		}
	}
	sort.Slice(callers, func(i, j int) bool { return callers[i].Symbol < callers[j].Symbol })
	result := struct {
		Author, Status string
		Roots          []string
		Members        []residualCompiledMember
		Callers        []residualCompiledCaller
	}{"#1548 implementation lane (Codex)", "Compiled domain-query discovery only; every member needs a semantic disposition; no approval or lifetime proof", roots, members, callers}
	encoded, err := json.MarshalIndent(result, "", "  ")
	require.NoError(t, err)
	fmt.Printf("RESIDUAL-COMPILED-POPULATION-BEGIN\n%s\nRESIDUAL-COMPILED-POPULATION-END\n", encoded)
}
