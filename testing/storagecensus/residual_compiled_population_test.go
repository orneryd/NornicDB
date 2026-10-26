//go:build r73_composition

package storagecensus

import (
	"encoding/json"
	"fmt"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
)

// Author: #1548 implementation lane (Codex). Domain queries over #1667's
// shared compiled Program. This client has no loader, source traversal, call
// graph builder, reachability walker, approval table or callback proof.
// The composition tag is required until the shared engine is in our parent.
type residualCompiledMember struct {
	Kind, Caller, Target, Position, Type string
}

type residualCompiledCaller struct {
	Symbol, Start, End string
}

func TestResidualCompiledPopulation(t *testing.T) {
	p, err := workcensus.LoadProgram(workcensus.Config{
		Dir: root, Patterns: []string{"./..."}, BuildFlags: []string{"-tags=noui,nolocalllm"},
		Env: append(os.Environ(), "GOOS=linux", "GOARCH=amd64", "CGO_ENABLED=0", "GOFLAGS=-mod=mod"),
	})
	require.NoError(t, err)
	runtimePackage := func(pkg string) bool {
		return strings.HasPrefix(pkg, Module) && !strings.HasPrefix(pkg, Module+"testing/") && pkg != Module+"internal/workcensus"
	}
	// Public runtime APIs, init and main are entry roots, including maintenance
	// and recovery. The engine expands their real VTA/closure reach. Tooling is
	// loaded for type completeness but is not a database execution entrypoint.
	var roots []workcensus.Root
	for _, fn := range p.Functions {
		if fn.Parent() != nil || fn.Pkg == nil {
			continue
		}
		pkg := fn.Pkg.Pkg.Path()
		if !runtimePackage(pkg) || (fn.Name() != "init" && fn.Name() != "main" && (fn.Object() == nil || !fn.Object().Exported())) {
			continue
		}
		name, receiver := fn.Name(), ""
		if fn.Object() != nil {
			name = fn.Object().Name()
		}
		if recv := fn.Signature.Recv(); recv != nil {
			r := types.Unalias(recv.Type())
			if ptr, ok := r.(*types.Pointer); ok {
				r = types.Unalias(ptr.Elem())
			}
			if named, ok := r.(*types.Named); ok {
				receiver = named.Obj().Name()
			}
		}
		roots = append(roots, workcensus.Root{Package: pkg, Name: name, Receiver: receiver, Required: true})
	}
	reach, err := p.ReachableFrom(roots)
	require.NoError(t, err)
	position := func(pos token.Pos) string {
		x := p.FileSet.Position(pos)
		rel, err := filepath.Rel(p.Root, x.Filename)
		if err != nil {
			rel = x.Filename
		}
		return fmt.Sprintf("%s:%d:%d", filepath.ToSlash(rel), x.Line, x.Column)
	}
	cacheNames := map[string]bool{}
	for _, name := range []string{"cacheFillNode", "cacheStoreNode", "cacheStoreNodeBody", "cacheStoreWrittenNode", "cacheStoreEdge", "cacheFillEdge", "cacheFillEdgeAtVersion", "adjCacheStoreOutgoing", "adjCacheStoreIncoming", "labelCacheSetFirst", "edgeTypeCacheFill"} {
		cacheNames[name] = true
	}
	propertyNames := map[string]bool{}
	for _, name := range []string{"copyNode", "copyNodeWithoutEmbeddings", "copyNodeForCaller", "copyEdge", "CopyNode", "CopyEdge", "codecImageProperties", "canonicalPropertyValue", "ownedProperties", "ownPropertyValue", "ownRetainedValue", "ownRetainedMap", "ownedMetadataValue", "canonicalPropertyList", "StoragePropertyValue", "ReadNumber", "NumericFloat64", "ReadList"} {
		propertyNames[name] = true
	}
	admissionNames := map[string]bool{}
	for _, name := range []string{"Lock", "RLock", "LockContext", "RLockContext", "acquireUniqueConstraintCommitLocks", "beginBarrierWrite", "beginBarrierWriteContext", "beginLifecycleWrite", "beginMaintenanceWrite", "beginOrderedWrite", "beginEntityWrite", "acquireEntityWriteKeys", "orderAfterBufferedCommits", "orderAfterBufferedCommitsContext", "orderCovering", "beginLargeCommit", "beginLargeCommitContext", "waitPublished", "waitPublishedContext", "waitForReadersBefore", "flushContext", "FlushWriteBehind", "FlushWriteBehindContext", "RetainAcceptedAuthority", "RetainAcceptedAuthorityWithContext", "DecidePublication", "SubmitAcceptedCommand", "SubmitEntityCommit", "AppendBufferedCommit"} {
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
	for fn := range reach.Functions {
		synthetic := fn.Pkg == nil && fn.Synthetic != ""
		if !synthetic && (fn.Pkg == nil || !runtimePackage(fn.Pkg.Pkg.Path())) {
			continue
		}
		if node := p.CallGraph.Nodes[fn]; node != nil {
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
	for _, fn := range p.Functions {
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
	}{"#1548 implementation lane (Codex)", "Compiled domain-query discovery only; every member needs a semantic disposition; no approval or lifetime proof", reach.Roots, members, callers}
	encoded, err := json.MarshalIndent(result, "", "  ")
	require.NoError(t, err)
	fmt.Printf("RESIDUAL-COMPILED-POPULATION-BEGIN\n%s\nRESIDUAL-COMPILED-POPULATION-END\n", encoded)
}
