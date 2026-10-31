package storage

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
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
	"DeleteNodeWithVectorWorkOrigin": true, "DeleteEdgeWithVectorWorkOrigin": true,
	"BulkDeleteNodesWithVectorWorkOrigin": true, "BulkDeleteEdgesWithVectorWorkOrigin": true,
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
		"deleteNodeAdmitted": true, "deleteEdgeAdmitted": true, "bulkDeleteNodesAdmitted": true, "bulkDeleteEdgesAdmitted": true,
		"SubscribeEvents": true, "admittedDeleter": true,
	}
	checked := 0
	for _, file := range []string{"entity_work_origin_engine.go", "vector_work_admission.go"} {
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
			helper := receiver == "" && strings.HasSuffix(fn.Name.Name, "Admitted") && strings.Contains(fn.Name.Name, "elete")
			if !carrierMethod && !helper {
				continue
			}
			checked++
			// The operation a body may delegate to: its own name without the
			// admitted suffix; for a dispatch helper (deleteNodeAdmitted) the
			// operation it dispatches (DeleteNode).
			operation := strings.TrimSuffix(fn.Name.Name, "WithVectorWorkOrigin")
			if helper {
				operation = strings.ToUpper(operation[:1]) + strings.TrimSuffix(operation[1:], "Admitted")
			}
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
						case *ast.Ident: // e.X (its own admitted form) or deleter.X (dispatch)
							require.Equal(t, operation+"WithVectorWorkOrigin", name, "%s: %s calls %s.%s", fset.Position(n.Pos()), fn.Name.Name, holder.Name, name)
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

// overridingDeleteEngine embeds a concrete engine and overrides every deletion
// the carrier offers in an admitted form, without declaring that form: it only
// inherits the admitted deletions (and their owner) from the embedded engine.
type overridingDeleteEngine struct {
	*BadgerEngine
	calls []string
}

func (e *overridingDeleteEngine) DeleteNode(id NodeID) error {
	e.calls = append(e.calls, "DeleteNode")
	return e.BadgerEngine.DeleteNode(id)
}

func (e *overridingDeleteEngine) DeleteEdge(id EdgeID) error {
	e.calls = append(e.calls, "DeleteEdge")
	return e.BadgerEngine.DeleteEdge(id)
}

func (e *overridingDeleteEngine) BulkDeleteNodes(ids []NodeID) error {
	e.calls = append(e.calls, "BulkDeleteNodes")
	return e.BadgerEngine.BulkDeleteNodes(ids)
}

func (e *overridingDeleteEngine) BulkDeleteEdges(ids []EdgeID) error {
	e.calls = append(e.calls, "BulkDeleteEdges")
	return e.BadgerEngine.BulkDeleteEdges(ids)
}

// TestAdmittedDeletionHonoursOuterOverride: through the admission carrier,
// every deletion reaches the behaviour the wrapped engine itself exposes. An
// override on the outer engine is not skipped by an admitted deletion promoted
// from its embedded engine; an engine that declares the admitted form gets it.
func TestAdmittedDeletionHonoursOuterOverride(t *testing.T) {
	outer := &overridingDeleteEngine{BadgerEngine: newTestEngine(t)}
	_, inherited := admittedDeleter(outer, "DeleteNode")
	require.False(t, inherited, "the admitted form sits below the outer engine's own DeleteNode")
	_, declared := admittedDeleter(outer.BadgerEngine, "DeleteNode")
	require.True(t, declared, "BadgerEngine declares both forms at the same depth")

	nodes := bulkTestNodes(4, "override")
	require.NoError(t, outer.BadgerEngine.BulkCreateNodes(nodes))
	edges := bulkTestEdges(3, "override-e", nodes)
	require.NoError(t, outer.BadgerEngine.BulkCreateEdges(edges))
	carrier, err := EngineWithVectorWorkOrigin(outer, CaptureVectorWorkOrigin())
	require.NoError(t, err)
	require.NoError(t, carrier.DeleteEdge(edges[0].ID))
	require.NoError(t, carrier.BulkDeleteEdges([]EdgeID{edges[1].ID}))
	require.NoError(t, carrier.DeleteNode(nodes[0].ID))
	require.NoError(t, carrier.BulkDeleteNodes([]NodeID{nodes[1].ID}))
	require.Equal(t, []string{"DeleteEdge", "BulkDeleteEdges", "DeleteNode", "BulkDeleteNodes"}, outer.calls, "every override is reached through the carrier")
	requireBulkCounts(t, outer.BadgerEngine, 2, 1)

	// Through the namespaced wrapper too, which forwards admitted deletions.
	outer.calls = nil
	namespaced := NewNamespacedEngine(outer, "test")
	carrier, err = EngineWithVectorWorkOrigin(namespaced, CaptureVectorWorkOrigin())
	require.NoError(t, err)
	require.NoError(t, carrier.DeleteNode(NodeID(strings.TrimPrefix(string(nodes[2].ID), "test:"))))
	require.Equal(t, []string{"DeleteNode"}, outer.calls, "an inner override is reached through an outer wrapper")
}

// passThroughDeleteEngine overrides no deletion: both forms come from Badger.
type passThroughDeleteEngine struct{ *BadgerEngine }

// admittedOnlyDeleteEngine declares the admitted deletions and inherits the
// plain ones; it records the origins it receives.
type admittedOnlyDeleteEngine struct {
	*BadgerEngine
	origins []uint64
}

func (e *admittedOnlyDeleteEngine) DeleteNodeWithVectorWorkOrigin(id NodeID, origin uint64) error {
	e.origins = append(e.origins, origin)
	return e.BadgerEngine.DeleteNodeWithVectorWorkOrigin(id, origin)
}

func (e *admittedOnlyDeleteEngine) DeleteEdgeWithVectorWorkOrigin(id EdgeID, origin uint64) error {
	e.origins = append(e.origins, origin)
	return e.BadgerEngine.DeleteEdgeWithVectorWorkOrigin(id, origin)
}

func (e *admittedOnlyDeleteEngine) BulkDeleteNodesWithVectorWorkOrigin(ids []NodeID, origin uint64) error {
	e.origins = append(e.origins, origin)
	return e.BadgerEngine.BulkDeleteNodesWithVectorWorkOrigin(ids, origin)
}

func (e *admittedOnlyDeleteEngine) BulkDeleteEdgesWithVectorWorkOrigin(ids []EdgeID, origin uint64) error {
	e.origins = append(e.origins, origin)
	return e.BadgerEngine.BulkDeleteEdgesWithVectorWorkOrigin(ids, origin)
}

// valueAdmittedDeleteEngine is a value engine: it embeds an interface and
// declares the admitted deletions with value receivers.
type valueAdmittedDeleteEngine struct {
	Engine
	origins *[]uint64
}

func (e valueAdmittedDeleteEngine) DeleteNodeWithVectorWorkOrigin(id NodeID, origin uint64) error {
	*e.origins = append(*e.origins, origin)
	return e.Engine.(VectorWorkOriginDeleter).DeleteNodeWithVectorWorkOrigin(id, origin)
}

func (e valueAdmittedDeleteEngine) DeleteEdgeWithVectorWorkOrigin(id EdgeID, origin uint64) error {
	*e.origins = append(*e.origins, origin)
	return e.Engine.(VectorWorkOriginDeleter).DeleteEdgeWithVectorWorkOrigin(id, origin)
}

func (e valueAdmittedDeleteEngine) BulkDeleteNodesWithVectorWorkOrigin(ids []NodeID, origin uint64) error {
	*e.origins = append(*e.origins, origin)
	return e.Engine.(VectorWorkOriginDeleter).BulkDeleteNodesWithVectorWorkOrigin(ids, origin)
}

func (e valueAdmittedDeleteEngine) BulkDeleteEdgesWithVectorWorkOrigin(ids []EdgeID, origin uint64) error {
	*e.origins = append(*e.origins, origin)
	return e.Engine.(VectorWorkOriginDeleter).BulkDeleteEdgesWithVectorWorkOrigin(ids, origin)
}

// leafAdmittedDeleteEngine declares leaf deletions the compiler can inline into
// the promotion wrappers of an engine that embeds it; leafOverrideEngine
// overrides the plain deletions above them.
type leafAdmittedDeleteEngine struct {
	*BadgerEngine
	admitted int
}

func (e *leafAdmittedDeleteEngine) DeleteNodeWithVectorWorkOrigin(NodeID, uint64) error {
	e.admitted++
	return nil
}

func (e *leafAdmittedDeleteEngine) DeleteEdgeWithVectorWorkOrigin(EdgeID, uint64) error {
	e.admitted++
	return nil
}

func (e *leafAdmittedDeleteEngine) BulkDeleteNodesWithVectorWorkOrigin([]NodeID, uint64) error {
	e.admitted++
	return nil
}

func (e *leafAdmittedDeleteEngine) BulkDeleteEdgesWithVectorWorkOrigin([]EdgeID, uint64) error {
	e.admitted++
	return nil
}

type leafOverrideEngine struct {
	*leafAdmittedDeleteEngine
	plain int
}

func (e *leafOverrideEngine) DeleteNode(NodeID) error {
	e.plain++
	return nil
}

func (e *leafOverrideEngine) DeleteEdge(EdgeID) error {
	e.plain++
	return nil
}

func (e *leafOverrideEngine) BulkDeleteNodes([]NodeID) error {
	e.plain++
	return nil
}

func (e *leafOverrideEngine) BulkDeleteEdges([]EdgeID) error {
	e.plain++
	return nil
}

// genericOverrideEngine is a generic instantiation of the plain-only override.
type genericOverrideEngine[T any] struct {
	*BadgerEngine
	calls []string
}

func (e *genericOverrideEngine[T]) DeleteNode(id NodeID) error {
	e.calls = append(e.calls, "DeleteNode")
	return e.BadgerEngine.DeleteNode(id)
}

// deeperOverrideEngine and deeperPassThroughEngine put the shapes one level down.
type deeperOverrideEngine struct{ *overridingDeleteEngine }
type deeperPassThroughEngine struct{ passThroughDeleteEngine }

func vectorWorkFallbackTotal() int64 {
	var total int64
	for _, fallback := range VectorWorkFallbacks() {
		total += fallback.Count
	}
	return total
}

// TestAdmittedDeletionReachesTheRightOwnerForEveryShape: through the carrier,
// an override of a deletion on the wrapped engine is always reached, and when
// there is none the admitted origin is still forwarded (no counted
// self-admission), for every engine shape: both forms inherited, plain-only
// override, admitted-only declaration, a value engine, and two embedding levels.
func TestAdmittedDeletionReachesTheRightOwnerForEveryShape(t *testing.T) {
	origin := CaptureVectorWorkOrigin()
	seed := func(t *testing.T, base *BadgerEngine) ([]*Node, []*Edge) {
		nodes := bulkTestNodes(4, "shape")
		require.NoError(t, base.BulkCreateNodes(nodes))
		edges := bulkTestEdges(3, "shape-e", nodes)
		require.NoError(t, base.BulkCreateEdges(edges))
		return nodes, edges
	}
	deleteAll := func(t *testing.T, engine Engine, nodes []*Node, edges []*Edge) {
		carrier, err := EngineWithVectorWorkOrigin(engine, origin)
		require.NoError(t, err)
		require.NoError(t, carrier.DeleteEdge(edges[0].ID))
		require.NoError(t, carrier.BulkDeleteEdges([]EdgeID{edges[1].ID}))
		require.NoError(t, carrier.DeleteNode(nodes[0].ID))
		require.NoError(t, carrier.BulkDeleteNodes([]NodeID{nodes[1].ID}))
	}
	forwarded := func(t *testing.T, engine Engine, base *BadgerEngine) {
		nodes, edges := seed(t, base)
		before := vectorWorkFallbackTotal()
		deleteAll(t, engine, nodes, edges)
		require.Equal(t, before, vectorWorkFallbackTotal(), "the admitted origin reached the owner: no counted self-admission")
		requireBulkCounts(t, base, 2, 1)
	}
	t.Run("both forms inherited (pass-through)", func(t *testing.T) {
		base := newTestEngine(t)
		forwarded(t, &passThroughDeleteEngine{BadgerEngine: base}, base)
	})
	t.Run("both forms inherited (adjacencyOverrideDecorator)", func(t *testing.T) {
		base := newTestEngine(t)
		forwarded(t, &adjacencyOverrideDecorator{BadgerEngine: base}, base)
	})
	t.Run("two levels, both forms inherited", func(t *testing.T) {
		base := newTestEngine(t)
		forwarded(t, &deeperPassThroughEngine{passThroughDeleteEngine{BadgerEngine: base}}, base)
	})
	t.Run("admitted-only declaration", func(t *testing.T) {
		base := newTestEngine(t)
		engine := &admittedOnlyDeleteEngine{BadgerEngine: base}
		forwarded(t, engine, base)
		require.Equal(t, []uint64{origin, origin, origin, origin}, engine.origins)
	})
	t.Run("value engine", func(t *testing.T) {
		base := newTestEngine(t)
		var origins []uint64
		forwarded(t, valueAdmittedDeleteEngine{Engine: base, origins: &origins}, base)
		require.Equal(t, []uint64{origin, origin, origin, origin}, origins)
	})
	t.Run("plain-only override", func(t *testing.T) {
		base := newTestEngine(t)
		engine := &overridingDeleteEngine{BadgerEngine: base}
		nodes, edges := seed(t, base)
		deleteAll(t, engine, nodes, edges)
		require.Equal(t, []string{"DeleteEdge", "BulkDeleteEdges", "DeleteNode", "BulkDeleteNodes"}, engine.calls)
	})
	t.Run("two levels, plain-only override", func(t *testing.T) {
		base := newTestEngine(t)
		inner := &overridingDeleteEngine{BadgerEngine: base}
		nodes, edges := seed(t, base)
		deleteAll(t, &deeperOverrideEngine{inner}, nodes, edges)
		require.Equal(t, []string{"DeleteEdge", "BulkDeleteEdges", "DeleteNode", "BulkDeleteNodes"}, inner.calls)
	})
}

// TestAdmittedDeletionShapesWithInlinedOrGenericMethods: the shapes whose
// runtime position can mislead the dispatch still reach the override.
func TestAdmittedDeletionShapesWithInlinedOrGenericMethods(t *testing.T) {
	require.True(t, promotionDetectionWorks(), "this toolchain reports promotion wrappers as declaresMethod expects")
	t.Run("plain override above an inlinable admitted form", func(t *testing.T) {
		leaf := &leafAdmittedDeleteEngine{BadgerEngine: newTestEngine(t)}
		engine := &leafOverrideEngine{leafAdmittedDeleteEngine: leaf}
		carrier, err := EngineWithVectorWorkOrigin(engine, CaptureVectorWorkOrigin())
		require.NoError(t, err)
		require.NoError(t, carrier.DeleteNode("n"))
		require.NoError(t, carrier.DeleteEdge("e"))
		require.NoError(t, carrier.BulkDeleteNodes([]NodeID{"n"}))
		require.NoError(t, carrier.BulkDeleteEdges([]EdgeID{"e"}))
		require.Equal(t, 4, engine.plain, "every override is reached")
		require.Zero(t, leaf.admitted)
	})
	t.Run("generic plain-only override", func(t *testing.T) {
		engine := &genericOverrideEngine[int]{BadgerEngine: newTestEngine(t)}
		_, usable := admittedDeleter(engine, "DeleteNode")
		require.False(t, usable, "the admitted form sits below the override")
	})
}

// TestAdmittedDeletionFailsToThePlainMethodWithoutPromotionDetection: on a
// toolchain whose wrappers can no longer be recognized, the carrier uses every
// engine's plain deletions. Overrides are reached, and the lost origin is a
// counted self-admission.
func TestAdmittedDeletionFailsToThePlainMethodWithoutPromotionDetection(t *testing.T) {
	restore := promotionDetectionWorks
	promotionDetectionWorks = func() bool { return false }
	admittedDeletionDecisions.Clear()
	t.Cleanup(func() {
		promotionDetectionWorks = restore
		admittedDeletionDecisions.Clear()
	})

	base := newTestEngine(t)
	override := &overridingDeleteEngine{BadgerEngine: base}
	nodes := bulkTestNodes(4, "nodetect")
	require.NoError(t, base.BulkCreateNodes(nodes))
	carrier, err := EngineWithVectorWorkOrigin(override, CaptureVectorWorkOrigin())
	require.NoError(t, err)
	require.NoError(t, carrier.DeleteNode(nodes[0].ID))
	require.Equal(t, []string{"DeleteNode"}, override.calls, "the override is reached")

	before := vectorWorkFallbackTotal()
	carrier, err = EngineWithVectorWorkOrigin(base, CaptureVectorWorkOrigin())
	require.NoError(t, err)
	require.NoError(t, carrier.DeleteNode(nodes[1].ID))
	require.Greater(t, vectorWorkFallbackTotal(), before, "the lost origin is counted")
	_, usable := admittedDeleter(base, "DeleteNode")
	require.False(t, usable)
}
