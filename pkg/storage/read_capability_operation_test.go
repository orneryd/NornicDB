package storage_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type operationProjectedRead struct {
	storage.Engine
	available     bool
	beforeRead    func()
	optionalValue bool
}

func (e *operationProjectedRead) ReadCapabilityAvailable(reflect.Type) bool { return e.available }
func (e *operationProjectedRead) StreamNodesByLabelProjected(label string, properties []string, visit func(*storage.Node) error) error {
	if e.beforeRead != nil {
		e.beforeRead()
	}
	if !e.available {
		return storage.ErrNotImplemented
	}
	return e.Engine.StreamNodesWithOptions(context.Background(), storage.StreamNodesOptions{Label: label, Projection: properties}, func(node *storage.Node) error {
		if e.optionalValue {
			node = storage.CopyNode(node)
			node.Properties["v"] = int64(1000)
		}
		return visit(node)
	})
}

// Either constituent can run first. Whichever does disables the other during
// its read, after composite discovery has approved both. No external visitor
// may observe the partial optional result before the whole read can fall back.
func TestReadCapabilityCompositeRefusalInsideRead(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	a := &operationProjectedRead{Engine: storage.NewNamespacedEngine(base, "a"), available: true, optionalValue: true}
	b := &operationProjectedRead{Engine: storage.NewNamespacedEngine(base, "b"), available: true, optionalValue: true}
	a.beforeRead = func() { b.available = false }
	b.beforeRead = func() { a.available = false }
	for _, view := range []*operationProjectedRead{a, b} {
		_, err := view.CreateNode(&storage.Node{ID: "n", Labels: []string{"Operation"}, Properties: map[string]interface{}{"v": int64(1)}})
		require.NoError(t, err)
	}
	composite := storage.NewCompositeEngine(map[string]storage.Engine{"a": a, "b": b}, map[string]string{"a": "a", "b": "b"}, map[string]string{"a": "read", "b": "read"})
	var databases []string
	values := map[int64]int{}
	err := storage.StreamNodesByLabelProjected(composite, "Operation", []string{"v"}, func(node *storage.Node) error {
		require.False(t, a.available && b.available, "availability flip must occur inside the optional read")
		value := node.Properties["v"].(int64)
		values[value]++
		databases = append(databases, node.Database)
		return nil
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"a", "b"}, databases, "each child's chosen operation delivers exactly once")
	require.Equal(t, map[int64]int{1000: 1, 1: 1}, values, "only the refusing child takes its ordinary fallback")
}

func TestReadCapabilityOperationOwnsRefusalAndDelivery(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	tenant := storage.NewNamespacedEngine(base, "tenant")
	conditional := &operationProjectedRead{Engine: tenant, available: true}
	for _, id := range []storage.NodeID{"a", "b"} {
		_, err := tenant.CreateNode(&storage.Node{ID: id, Labels: []string{"Operation"}})
		require.NoError(t, err)
	}
	for _, outcome := range []string{"refusal", "after_row", "visitor_refusal", "storage_error", "success", "absent"} {
		t.Run(outcome, func(t *testing.T) {
			visits, fallbacks := 0, 0
			failure := errors.New("read failed")
			var engine storage.Engine = conditional
			if outcome == "absent" {
				engine = struct{ storage.Engine }{base}
			}
			err := storage.VisitWithCapability(engine, func(reader storage.ProjectedLabelNodeReader, deliver func(*storage.Node) error) error {
				switch outcome {
				case "refusal":
					return storage.ErrNotImplemented
				case "storage_error":
					return failure
				case "after_row":
					require.NoError(t, deliver(&storage.Node{}))
					return storage.ErrNotImplemented
				default:
					return reader.StreamNodesByLabelProjected("Operation", nil, deliver)
				}
			}, func(deliver func(*storage.Node) error) error {
				fallbacks++
				return deliver(&storage.Node{})
			}, func(*storage.Node) error {
				visits++
				if outcome == "visitor_refusal" {
					return storage.ErrNotImplemented
				}
				return nil
			})
			switch outcome {
			case "storage_error":
				require.ErrorIs(t, err, failure)
				require.Zero(t, visits)
				require.Zero(t, fallbacks)
			case "after_row", "visitor_refusal":
				require.ErrorIs(t, err, storage.ErrNotImplemented)
				require.Equal(t, 1, visits)
				require.Zero(t, fallbacks)
			case "refusal", "absent":
				require.NoError(t, err)
				require.Equal(t, 1, visits)
				require.Equal(t, 1, fallbacks)
			case "success":
				require.NoError(t, err)
				require.Equal(t, 2, visits)
				require.Zero(t, fallbacks)
			}
		})
	}
	_, err := storage.ReadWithCapability(conditional, func(storage.ProjectedLabelNodeReader) (int, error) { return 99, storage.ErrNotImplemented }, nil)
	require.ErrorIs(t, err, storage.ErrNotImplemented)
	err = storage.VisitWithCapability(conditional, func(storage.ProjectedLabelNodeReader, func(*storage.Node) error) error {
		t.Fatal("nil visitor must not start a read")
		return nil
	}, nil, (func(*storage.Node) error)(nil))
	require.ErrorIs(t, err, storage.ErrInvalidData)
}

type operationHistoryRead struct {
	*storage.MemoryEngine
	available bool
}

type reservationRead struct {
	storage.Engine
	database        string
	started         bool
	finished        chan struct{}
	afterFirstError error
}

func (e *reservationRead) StreamNodesByLabelProjected(label string, properties []string, visit func(*storage.Node) error) error {
	e.started = true
	defer close(e.finished)
	node := &storage.Node{Database: e.database, Labels: []string{label}, Properties: map[string]any{}}
	for i := 0; i < 3; i++ {
		node.ID = storage.NodeID(fmt.Sprintf("row%d", i))
		node.Properties["v"] = i
		if err := visit(node); err != nil {
			return err
		}
		if i == 0 && e.afterFirstError != nil {
			return e.afterFirstError
		}
	}
	return nil
}

func (e *reservationRead) StreamParkedEmbeddingFailures(ctx context.Context, visit func(storage.NodeID, map[string]any) error) (int, error) {
	e.started = true
	defer close(e.finished)
	meta := map[string]any{}
	for i := 0; i < 3; i++ {
		if err := ctx.Err(); err != nil {
			return i, err
		}
		meta["v"] = i
		if err := visit(storage.NodeID(fmt.Sprintf("row%d", i)), meta); err != nil {
			return i, err
		}
	}
	return 3, nil
}

func reservationComposite(t *testing.T) (*storage.CompositeEngine, []*reservationRead) {
	t.Helper()
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	views, names, modes := map[string]storage.Engine{}, map[string]string{}, map[string]string{}
	var sources []*reservationRead
	for _, name := range []string{"a", "b"} {
		source := &reservationRead{Engine: storage.NewNamespacedEngine(base, name), database: name, finished: make(chan struct{})}
		views[name], names[name], modes[name] = source, name, "read"
		sources = append(sources, source)
	}
	return storage.NewCompositeEngine(views, names, modes), sources
}

func requireReservationsReleased(t *testing.T, sources []*reservationRead) {
	t.Helper()
	for _, source := range sources {
		if !source.started {
			continue
		}
		select {
		case <-source.finished:
		default:
			t.Fatalf("read for %s is still held after the operation returned", source.database)
		}
	}
}

func TestReadCapabilityCompositeReservationRows(t *testing.T) {
	composite, sources := reservationComposite(t)
	var seen []string
	err := storage.StreamNodesByLabelProjected(composite, "Reservation", []string{"v"}, func(node *storage.Node) error {
		if len(seen) == 0 {
			for _, source := range sources {
				select {
				case <-source.finished:
					t.Fatal("a source was buffered to completion before delivery")
				default:
				}
			}
		}
		seen = append(seen, fmt.Sprintf("%s/%s/%v", node.Database, node.ID, node.Properties["v"]))
		return nil
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"a/row0/0", "a/row1/1", "a/row2/2", "b/row0/0", "b/row1/1", "b/row2/2"}, seen)
	requireReservationsReleased(t, sources)
}

func TestReadCapabilityCompositeReservationStops(t *testing.T) {
	composite, sources := reservationComposite(t)
	visits := 0
	err := storage.StreamNodesByLabelProjected(composite, "Reservation", nil, func(*storage.Node) error {
		visits++
		return storage.ErrNotImplemented
	})
	require.ErrorIs(t, err, storage.ErrNotImplemented, "a visitor refusal must never become ordinary-read replay")
	require.Equal(t, 1, visits)
	requireReservationsReleased(t, sources)
}

func TestReadCapabilityCompositeReservationProviderError(t *testing.T) {
	composite, sources := reservationComposite(t)
	failure := errors.New("provider failed after admission")
	for _, source := range sources {
		source.afterFirstError = failure
	}
	visits := 0
	err := storage.StreamNodesByLabelProjected(composite, "Reservation", nil, func(*storage.Node) error {
		visits++
		return nil
	})
	require.ErrorIs(t, err, failure)
	require.Equal(t, 1, visits)
	requireReservationsReleased(t, sources)
	require.ErrorIs(t, composite.StreamNodesByLabelProjected("Reservation", nil, nil), storage.ErrInvalidData)
}

func TestReadCapabilityCompositeReservationFailureRows(t *testing.T) {
	composite, sources := reservationComposite(t)
	var seen []string
	count, err := composite.StreamParkedEmbeddingFailures(context.Background(), func(id storage.NodeID, meta map[string]any) error {
		seen = append(seen, fmt.Sprintf("%s/%v", id, meta["v"]))
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 6, count)
	require.ElementsMatch(t, []string{"4:a:row0/0", "4:a:row1/1", "4:a:row2/2", "4:b:row0/0", "4:b:row1/1", "4:b:row2/2"}, seen)
	requireReservationsReleased(t, sources)
}

type refusingCoreReads struct{ storage.Engine }

func (e refusingCoreReads) NodeCountByLabel(string) (int64, error) {
	return 0, storage.ErrNotImplemented
}

func (e refusingCoreReads) GetAdjacentEdges(storage.NodeID) ([]*storage.Edge, []*storage.Edge, error) {
	return nil, nil, storage.ErrNotImplemented
}

func TestReadCapabilityCoreRefusalsUseOrdinaryView(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	tenant := storage.NewNamespacedEngine(base, "tenant")
	for _, namespace := range []*storage.NamespacedEngine{tenant, storage.NewNamespacedEngine(base, "foreign")} {
		for _, id := range []storage.NodeID{"a", "b"} {
			_, err := namespace.CreateNode(&storage.Node{ID: id, Labels: []string{"Operation"}})
			require.NoError(t, err)
		}
		require.NoError(t, namespace.CreateEdge(&storage.Edge{ID: "out", StartNode: "a", EndNode: "b", Type: "LINK"}))
		require.NoError(t, namespace.CreateEdge(&storage.Edge{ID: "in", StartNode: "b", EndNode: "a", Type: "LINK"}))
	}
	view := storage.NewTracedEngine(storage.NewWALEngine(refusingCoreReads{Engine: tenant}, nil))
	count, err := storage.CountNodesWithLabel(context.Background(), view, "Operation")
	require.NoError(t, err)
	require.Equal(t, int64(2), count)
	outgoing, incoming, err := storage.GetAdjacentEdges(view, "a")
	require.NoError(t, err)
	require.Len(t, outgoing, 1)
	require.Len(t, incoming, 1)
	require.Equal(t, storage.EdgeID("out"), outgoing[0].ID)
	require.Equal(t, storage.EdgeID("in"), incoming[0].ID)
}

func (e *operationHistoryRead) ReadCapabilityAvailable(reflect.Type) bool { return e.available }

func TestReadCapabilityCompositeHistoricalVersionNeverBecomesCurrent(t *testing.T) {
	base := storage.NewMemoryEngineWithMVCCHistory()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	conditional := &operationHistoryRead{MemoryEngine: base, available: true}
	engines, names, modes := map[string]storage.Engine{}, map[string]string{}, map[string]string{}
	var tenants []*storage.NamespacedEngine
	var version storage.MVCCVersion
	for _, name := range []string{"a", "b", "c"} {
		tenant := storage.NewNamespacedEngine(conditional, name)
		tenants = append(tenants, tenant)
		engines[name] = storage.NewTracedEngine(storage.NewWALEngine(tenant, nil))
		names[name], modes[name] = name, "read"
		for _, id := range []storage.NodeID{"start", "end"} {
			_, err := tenant.CreateNode(&storage.Node{ID: id, Labels: []string{"Past"}, Properties: map[string]interface{}{"v": "past"}})
			require.NoError(t, err)
		}
		require.NoError(t, tenant.CreateEdge(&storage.Edge{ID: "past", StartNode: "start", EndNode: "end", Type: "PAST"}))
		head, err := tenant.GetEdgeCurrentHead("past")
		require.NoError(t, err)
		if head.Version.Compare(version) > 0 {
			version = head.Version
		}
	}
	require.False(t, version.IsZero())
	for _, tenant := range tenants {
		require.NoError(t, tenant.UpdateNode(&storage.Node{ID: "start", Labels: []string{"Past"}, Properties: map[string]interface{}{"v": "current"}}))
		require.NoError(t, tenant.UpdateEdge(&storage.Edge{ID: "past", StartNode: "start", EndNode: "end", Type: "CURRENT"}))
		require.NoError(t, tenant.CreateEdge(&storage.Edge{ID: "later", StartNode: "start", EndNode: "end", Type: "PAST"}))
	}
	composite := storage.NewCompositeEngine(engines, names, modes)
	for _, available := range []bool{true, false} {
		conditional.available = available
		for _, qualified := range []bool{false, true} {
			prefix, count := "", 3
			if qualified {
				prefix, count = "4:a:", 1
			}
			t.Run(string(rune('0'+count))+"/"+map[bool]string{true: "available", false: "refused"}[available], func(t *testing.T) {
				checkEdges := func(t *testing.T, read func() ([]*storage.Edge, error)) {
					edges, err := read()
					if !available {
						require.ErrorIs(t, err, storage.ErrNotImplemented)
						require.Empty(t, edges)
						return
					}
					require.NoError(t, err)
					require.Len(t, edges, count)
					for _, edge := range edges {
						require.Equal(t, storage.EdgeID("past"), edge.ID)
						require.Equal(t, "PAST", edge.Type)
					}
				}
				for name, read := range map[string]func() ([]*storage.Edge, error){
					"outgoing": func() ([]*storage.Edge, error) {
						return composite.GetOutgoingEdgesVisibleAt(storage.NodeID(prefix+"start"), version)
					},
					"incoming": func() ([]*storage.Edge, error) {
						return composite.GetIncomingEdgesVisibleAt(storage.NodeID(prefix+"end"), version)
					},
					"pair": func() ([]*storage.Edge, error) {
						return composite.GetEdgesBetweenVisibleAt(storage.NodeID(prefix+"start"), storage.NodeID(prefix+"end"), version)
					},
				} {
					t.Run(name, func(t *testing.T) { checkEdges(t, read) })
				}
				node, err := composite.GetNodeVisibleAt(storage.NodeID(prefix+"start"), version)
				if available {
					require.NoError(t, err)
					require.Equal(t, "past", node.Properties["v"])
				} else {
					require.ErrorIs(t, err, storage.ErrNotImplemented)
					require.Nil(t, node)
				}
				edgeID := storage.EdgeID("past")
				if qualified {
					edgeID = "5:a:past"
				}
				edge, err := composite.GetEdgeVisibleAt(edgeID, version)
				if available {
					require.NoError(t, err)
					require.Equal(t, "PAST", edge.Type)
				} else {
					require.ErrorIs(t, err, storage.ErrNotImplemented)
					require.Nil(t, edge)
				}
			})
		}
		t.Run(map[bool]string{true: "available", false: "refused"}[available]+"/merged", func(t *testing.T) {
			nodes, err := composite.GetNodesByLabelVisibleAt("Past", version)
			if available {
				require.NoError(t, err)
				require.Len(t, nodes, 6)
				for _, node := range nodes {
					require.Equal(t, "past", node.Properties["v"])
				}
			} else {
				require.ErrorIs(t, err, storage.ErrNotImplemented)
				require.Empty(t, nodes)
			}
			edges, err := composite.GetEdgesByTypeVisibleAt("PAST", version)
			if available {
				require.NoError(t, err)
				require.Len(t, edges, 3)
				for _, edge := range edges {
					require.Equal(t, storage.EdgeID("past"), edge.ID)
				}
			} else {
				require.ErrorIs(t, err, storage.ErrNotImplemented)
				require.Empty(t, edges)
			}
		})
	}
}
