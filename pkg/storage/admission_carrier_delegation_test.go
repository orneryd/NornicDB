package storage

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// carrierOwnMethods are the methods the admission carrier declares beyond
// Engine: admission transport, the canonical unwrap accessor, event
// subscription through the shared helper, and (streaming variant only) reads.
var carrierOwnMethods = map[string]bool{
	"GetInnerEngine": true, "SubscribeEvents": true,
	"StreamNodes": true, "StreamEdges": true, "StreamNodeChunks": true,
}

// TestAdmissionCarrierExposesOnlyDelegatingMethods checks the compiled method
// sets of both carrier variants: every method is an Engine method or one of the
// declared transport methods, and the plain carrier exposes no streaming.
func TestAdmissionCarrierExposesOnlyDelegatingMethods(t *testing.T) {
	engineMethods := map[string]bool{}
	engineType := reflect.TypeOf((*Engine)(nil)).Elem()
	for i := 0; i < engineType.NumMethod(); i++ {
		engineMethods[engineType.Method(i).Name] = true
	}
	for _, carrier := range []reflect.Type{reflect.TypeOf(&vectorWorkOriginEngine{}), reflect.TypeOf(&streamingVectorWorkOriginEngine{})} {
		for i := 0; i < carrier.NumMethod(); i++ {
			name := carrier.Method(i).Name
			require.True(t, engineMethods[name] || carrierOwnMethods[name], "%s exposes %s, which is neither an Engine method nor declared transport", carrier, name)
		}
	}
	_, streams := Engine(&vectorWorkOriginEngine{}).(StreamingEngine)
	require.False(t, streams, "the plain carrier must not expose streaming it would have to implement")

	plain := &nonStreamingEngine{Engine: NewMemoryEngine()}
	t.Cleanup(func() { _ = plain.Close() })
	carrier, err := EngineWithVectorWorkOrigin(plain, CaptureVectorWorkOrigin())
	require.NoError(t, err)
	_, streams = carrier.(StreamingEngine)
	require.False(t, streams, "a carrier of a non-streaming engine exposes no streaming")
	_, logs := carrier.(StructuredLogger)
	require.False(t, logs, "the carrier exposes no logger of its own")

	base := newTestEngine(t)
	_, err = base.CreateNode(&Node{ID: "test:carrier-stream", Labels: []string{"Doc"}})
	require.NoError(t, err)
	carrier, err = EngineWithVectorWorkOrigin(base, CaptureVectorWorkOrigin())
	require.NoError(t, err)
	streamer, ok := carrier.(StreamingEngine)
	require.True(t, ok, "a carrier of a streaming engine forwards streaming")
	var direct, forwarded [][]NodeID
	collect := func(into *[][]NodeID) func([]*Node) error {
		return func(nodes []*Node) error {
			ids := make([]NodeID, len(nodes))
			for i, node := range nodes {
				ids[i] = node.ID
			}
			*into = append(*into, ids)
			return nil
		}
	}
	require.NoError(t, base.StreamNodeChunks(context.Background(), 2, collect(&direct)))
	require.NoError(t, streamer.StreamNodeChunks(context.Background(), 2, collect(&forwarded)))
	require.Equal(t, direct, forwarded, "chunking is the wrapped engine's own")
}

type nonStreamingEngine struct{ Engine }

// TestAdmissionCarrierMethodsOnlyDelegate checks every carrier method body (and
// the admission dispatch helpers): no loop, goroutine, select or closure; every
// call is the wrapped engine's method of the same operation, the carrier's own
// admitted form of it, argument binding, or an admission dispatch helper.
func TestAdmissionCarrierMethodsOnlyDelegate(t *testing.T) {
	fset := token.NewFileSet()
	allowedFuncs := map[string]bool{
		"admittedNodeArgument": true, "admittedEdgeArgument": true, "admittedNodeArguments": true, "admittedEdgeArguments": true,
		"SubscribeEvents": true, "len": true,
	}
	checked := 0
	for _, file := range []string{"entity_work_origin_engine.go"} {
		parsed, err := parser.ParseFile(fset, file, nil, 0)
		require.NoError(t, err)
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			receiver := ""
			if fn.Recv != nil && len(fn.Recv.List) == 1 {
				if star, ok := fn.Recv.List[0].Type.(*ast.StarExpr); ok {
					if ident, ok := star.X.(*ast.Ident); ok {
						receiver = ident.Name
					}
				}
			}
			carrierMethod := receiver == "vectorWorkOriginEngine" || receiver == "streamingVectorWorkOriginEngine"
			if !carrierMethod {
				continue
			}
			checked++
			operation := fn.Name.Name
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				switch n := node.(type) {
				case *ast.ForStmt, *ast.RangeStmt, *ast.GoStmt, *ast.SelectStmt, *ast.FuncLit:
					t.Errorf("%s: %s performs work of its own (%T)", fset.Position(n.Pos()), fn.Name.Name, n)
				case *ast.CallExpr:
					switch callee := n.Fun.(type) {
					case *ast.Ident:
						require.True(t, allowedFuncs[callee.Name], "%s: %s calls %s", fset.Position(n.Pos()), fn.Name.Name, callee.Name)
					case *ast.SelectorExpr:
						name := callee.Sel.Name
						switch holder := callee.X.(type) {
						case *ast.SelectorExpr: // e.Engine.X / e.streamer.X
							require.Contains(t, []string{"Engine", "streamer"}, holder.Sel.Name, "%s: %s", fset.Position(n.Pos()), fn.Name.Name)
							require.Equal(t, operation, name, "%s: %s delegates to a different operation %s", fset.Position(n.Pos()), fn.Name.Name, name)
						case *ast.Ident: // e.admission: the origin a deletion passes on
							require.Equal(t, "admission", name, "%s: %s calls %s.%s", fset.Position(n.Pos()), fn.Name.Name, holder.Name, name)
						default:
							t.Errorf("%s: %s calls %s through an expression", fset.Position(n.Pos()), fn.Name.Name, name)
						}
					}
				}
				return true
			})
		}
	}
	require.Greater(t, checked, 15, "the check found the carrier's methods")
}

// TestAdmissionCarrierRewrapKeepsCapabilitySetAndOrigin re-admits an already
// admitted engine through both public constructors, in every order and
// repeated: the result is the same carrier, with exactly the wrapped engine's
// capability set and the first admission's origin.
func TestAdmissionCarrierRewrapKeepsCapabilitySetAndOrigin(t *testing.T) {
	plain := &nonStreamingEngine{Engine: NewMemoryEngine()}
	t.Cleanup(func() { _ = plain.Close() })
	inners := map[string]Engine{"streaming": newTestEngine(t), "plain": plain}
	origin := CaptureVectorWorkOrigin() + 7
	constructors := map[string]func(Engine) Engine{
		"SystemStoreEngine": SystemStoreEngine,
		"EngineWithVectorWorkOrigin": func(engine Engine) Engine {
			carrier, err := EngineWithVectorWorkOrigin(engine, origin)
			require.NoError(t, err)
			return carrier
		},
	}
	for innerName, inner := range inners {
		_, innerStreams := inner.(StreamingEngine)
		for firstName, first := range constructors {
			for secondName, second := range constructors {
				t.Run(innerName+"/"+firstName+"/"+secondName, func(t *testing.T) {
					admitted := first(inner)
					carrier, ok := asVectorWorkOriginCarrier(admitted)
					require.True(t, ok)
					want := carrier.origin
					rewrapped := second(second(admitted))
					require.Same(t, admitted, rewrapped, "an admitted engine is returned as it is, never rewrapped")
					_, streams := rewrapped.(StreamingEngine)
					require.Equal(t, innerStreams, streams, "the capability set is the wrapped engine's")
					again, ok := asVectorWorkOriginCarrier(rewrapped)
					require.True(t, ok)
					require.Equal(t, want, again.origin, "the first admission's origin is kept")
				})
			}
		}
	}
}

// overridingDeleteEngine overrides every deletion and inherits the rest from its
// embedded engine; it records each call and the origin it received.
type overridingDeleteEngine struct {
	Engine
	calls   []string
	origins []uint64
}

func (e *overridingDeleteEngine) record(call string, origins []uint64) {
	e.calls = append(e.calls, call)
	e.origins = append(e.origins, origins...)
}

func (e *overridingDeleteEngine) DeleteNode(id NodeID, origins ...uint64) error {
	e.record("DeleteNode", origins)
	return e.Engine.DeleteNode(id, origins...)
}

func (e *overridingDeleteEngine) DeleteEdge(id EdgeID, origins ...uint64) error {
	e.record("DeleteEdge", origins)
	return e.Engine.DeleteEdge(id, origins...)
}

func (e *overridingDeleteEngine) BulkDeleteNodes(ids []NodeID, origins ...uint64) error {
	e.record("BulkDeleteNodes", origins)
	return e.Engine.BulkDeleteNodes(ids, origins...)
}

func (e *overridingDeleteEngine) BulkDeleteEdges(ids []EdgeID, origins ...uint64) error {
	e.record("BulkDeleteEdges", origins)
	return e.Engine.BulkDeleteEdges(ids, origins...)
}

// The engine shapes a deletion passes through: pointer and value, embedded
// struct and embedded interface, more than one level, generic.
type (
	pointerPassThroughEngine        struct{ *BadgerEngine }
	interfacePassThroughEngine      struct{ Engine }
	valuePassThroughEngine          struct{ Engine }
	twoLevelPassThroughEngine       struct{ pointerPassThroughEngine }
	twoLevelOverrideEngine          struct{ *overridingDeleteEngine }
	genericPassThroughEngine[T any] struct{ *BadgerEngine }
)

// genericOverrideEngine is a generic instantiation of an override.
type genericOverrideEngine[T any] struct {
	*overridingDeleteEngine
}

func (e *genericOverrideEngine[T]) DeleteNode(id NodeID, origins ...uint64) error {
	return e.overridingDeleteEngine.DeleteNode(id, origins...)
}

func vectorWorkFallbackTotal() int64 {
	var total int64
	for _, fallback := range VectorWorkFallbacks() {
		total += fallback.Count
	}
	return total
}

// TestDeletionThroughCarrierReachesOverrideAndForwardsOrigin: a deletion through
// the carrier calls the method the engine exposes, whatever its shape, so every
// override runs; and it reaches the storage owner under the admitted origin, with
// no counted self-admission. Each operation is checked on a value of the same
// outer type with and without an override behind it.
func TestDeletionThroughCarrierReachesOverrideAndForwardsOrigin(t *testing.T) {
	allOperations := []string{"DeleteEdge", "BulkDeleteEdges", "DeleteNode", "BulkDeleteNodes"}
	run := func(t *testing.T, base *BadgerEngine, engine Engine) {
		t.Helper()
		nodes := bulkTestNodes(4, "shape")
		require.NoError(t, base.BulkCreateNodes(nodes))
		edges := bulkTestEdges(3, "shape-e", nodes)
		require.NoError(t, base.BulkCreateEdges(edges))
		before := vectorWorkFallbackTotal()
		carrier, err := EngineWithVectorWorkOrigin(engine, CaptureVectorWorkOrigin())
		require.NoError(t, err)
		require.NoError(t, carrier.DeleteEdge(edges[0].ID))
		require.NoError(t, carrier.BulkDeleteEdges([]EdgeID{edges[1].ID}))
		require.NoError(t, carrier.DeleteNode(nodes[0].ID))
		require.NoError(t, carrier.BulkDeleteNodes([]NodeID{nodes[1].ID}))
		require.Equal(t, before, vectorWorkFallbackTotal(), "the admitted origin reached the owner: no counted self-admission")
		requireBulkCounts(t, base, 2, 1)
	}
	for name, shape := range map[string]func(*BadgerEngine) Engine{
		"pointer embedding":             func(b *BadgerEngine) Engine { return &pointerPassThroughEngine{b} },
		"value with embedded struct":    func(b *BadgerEngine) Engine { return pointerPassThroughEngine{b} },
		"embedded interface":            func(b *BadgerEngine) Engine { return &interfacePassThroughEngine{b} },
		"value with embedded interface": func(b *BadgerEngine) Engine { return valuePassThroughEngine{b} },
		"two levels":                    func(b *BadgerEngine) Engine { return &twoLevelPassThroughEngine{pointerPassThroughEngine{b}} },
		"generic":                       func(b *BadgerEngine) Engine { return &genericPassThroughEngine[int]{b} },
		"adjacencyOverrideDecorator":    func(b *BadgerEngine) Engine { return &adjacencyOverrideDecorator{BadgerEngine: b} },
		"traced":                        func(b *BadgerEngine) Engine { return NewTracedEngine(b) },
	} {
		t.Run("pass-through/"+name, func(t *testing.T) {
			base := newTestEngine(t)
			run(t, base, shape(base))
		})
	}
	for name, shape := range map[string]func(*overridingDeleteEngine) Engine{
		"direct":                        func(o *overridingDeleteEngine) Engine { return o },
		"embedded interface":            func(o *overridingDeleteEngine) Engine { return &interfacePassThroughEngine{o} },
		"value with embedded interface": func(o *overridingDeleteEngine) Engine { return valuePassThroughEngine{o} },
		"two levels":                    func(o *overridingDeleteEngine) Engine { return &twoLevelOverrideEngine{o} },
		"generic":                       func(o *overridingDeleteEngine) Engine { return &genericOverrideEngine[int]{o} },
		"traced":                        func(o *overridingDeleteEngine) Engine { return NewTracedEngine(o) },
	} {
		t.Run("override/"+name, func(t *testing.T) {
			base := newTestEngine(t)
			override := &overridingDeleteEngine{Engine: base}
			run(t, base, shape(override))
			require.Equal(t, allOperations, override.calls, "every override is reached")
			require.Len(t, override.origins, len(allOperations), "every override received the origin")
		})
	}
}

// TestNamespacedAndWALDeletionForwardOriginToAnOverrideBehindThem: the wrappers
// that do work of their own keep the override and the origin behind them.
func TestNamespacedAndWALDeletionForwardOriginToAnOverrideBehindThem(t *testing.T) {
	base := newTestEngine(t)
	override := &overridingDeleteEngine{Engine: base}
	nodes := bulkTestNodes(4, "wrapped")
	require.NoError(t, base.BulkCreateNodes(nodes))
	edges := bulkTestEdges(3, "wrapped-e", nodes)
	require.NoError(t, base.BulkCreateEdges(edges))
	origin := CaptureVectorWorkOrigin()
	before := vectorWorkFallbackTotal()
	carrier, err := EngineWithVectorWorkOrigin(NewNamespacedEngine(override, "test"), origin)
	require.NoError(t, err)
	local := func(id string) string { return strings.TrimPrefix(id, "test:") }
	require.NoError(t, carrier.DeleteEdge(EdgeID(local(string(edges[0].ID)))))
	require.NoError(t, carrier.BulkDeleteEdges([]EdgeID{EdgeID(local(string(edges[1].ID)))}))
	require.NoError(t, carrier.DeleteNode(NodeID(local(string(nodes[0].ID)))))
	require.NoError(t, carrier.BulkDeleteNodes([]NodeID{NodeID(local(string(nodes[1].ID)))}))
	require.Equal(t, []string{"DeleteEdge", "BulkDeleteEdges", "DeleteNode", "BulkDeleteNodes"}, override.calls)
	require.Equal(t, []uint64{origin, origin, origin, origin}, override.origins)
	require.Equal(t, before, vectorWorkFallbackTotal(), "no counted self-admission")
	requireBulkCounts(t, base, 2, 1)
}

// TestDeletionWithoutAdmissionStillSelfAdmits: a caller outside a request supplies
// no origin, and the storage owner counts its own admission, as before.
func TestDeletionWithoutAdmissionStillSelfAdmits(t *testing.T) {
	base := newTestEngine(t)
	nodes := bulkTestNodes(1, "plain")
	require.NoError(t, base.BulkCreateNodes(nodes))
	before := vectorWorkFallbackTotal()
	require.NoError(t, (&interfacePassThroughEngine{base}).DeleteNode(nodes[0].ID))
	require.Greater(t, vectorWorkFallbackTotal(), before)
}

// deletionOriginNotForwarded lists the production deletions that do not pass
// origins on, each for a reason the origin has no use there.
var deletionOriginNotForwarded = map[string]string{
	"RemoteEngine":              "the deletion runs on another server, which admits its own work",
	"storeTransaction":          "the transaction it stages in carries its own admission",
	"transactionStorageWrapper": "the transaction it stages in carries its own admission",
}

// TestEveryDeletionForwardsItsOrigin reads every deletion method in the module. A
// method that passes a deletion on must pass origins with it; a production method
// must use origins at all, or be listed above. The compiler makes every
// implementation take origins; this makes every one in the module forward it.
func TestEveryDeletionForwardsItsOrigin(t *testing.T) {
	operations := map[string]bool{"DeleteNode": true, "DeleteEdge": true, "BulkDeleteNodes": true, "BulkDeleteEdges": true}
	root := filepath.Join("..", "..")
	fset := token.NewFileSet()
	checked := 0
	require.NoError(t, filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() && (entry.Name() == ".git" || entry.Name() == "node_modules" || entry.Name() == "vendor" || entry.Name() == "_l6census") {
			return filepath.SkipDir
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		parsed, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return nil // generated or build-tagged sources outside this contract
		}
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || fn.Body == nil || !operations[fn.Name.Name] || !takesDeletionOrigins(fn) {
				continue
			}
			checked++
			position := fset.Position(fn.Pos())
			usesOrigins := false
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				switch n := node.(type) {
				case *ast.Ident:
					usesOrigins = usesOrigins || n.Name == "origins"
				case *ast.CallExpr:
					selector, ok := n.Fun.(*ast.SelectorExpr)
					if !ok || !operations[selector.Sel.Name] {
						return true
					}
					if deletionHolderName(selector.X) == "tx" {
						return true // a transaction carries its own admission
					}
					forwards := n.Ellipsis.IsValid() && len(n.Args) == 2
					if forwards {
						switch last := n.Args[1].(type) {
						case *ast.Ident:
							forwards = last.Name == "origins"
						case *ast.CallExpr: // the carrier: e.admission(origins)
							callee, ok := last.Fun.(*ast.SelectorExpr)
							forwards = ok && callee.Sel.Name == "admission"
						default:
							forwards = false
						}
					}
					if !forwards {
						t.Errorf("%s: %s passes a deletion on without its origin", position, fn.Name.Name)
					}
				}
				return true
			})
			if !strings.HasSuffix(path, "_test.go") && !usesOrigins {
				if _, listed := deletionOriginNotForwarded[deletionReceiver(fn)]; !listed {
					t.Errorf("%s: %s.%s drops its origin", position, deletionReceiver(fn), fn.Name.Name)
				}
			}
		}
		return nil
	}))
	require.Greater(t, checked, 60, "the check found the module's deletion methods")
}

func deletionHolderName(holder ast.Expr) string {
	switch h := holder.(type) {
	case *ast.Ident:
		return h.Name
	case *ast.SelectorExpr:
		return h.Sel.Name
	}
	return ""
}

func takesDeletionOrigins(fn *ast.FuncDecl) bool {
	params := fn.Type.Params.List
	if len(params) != 2 {
		return false
	}
	variadic, ok := params[1].Type.(*ast.Ellipsis)
	if !ok {
		return false
	}
	element, ok := variadic.Elt.(*ast.Ident)
	return ok && element.Name == "uint64"
}

func deletionReceiver(fn *ast.FuncDecl) string {
	receiver := fn.Recv.List[0].Type
	if star, ok := receiver.(*ast.StarExpr); ok {
		receiver = star.X
	}
	if ident, ok := receiver.(*ast.Ident); ok {
		return ident.Name
	}
	return ""
}
