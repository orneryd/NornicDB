package cypher

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The oracle reads the physical owner (or the pinned transaction), then applies
// the requested scope independently. It never calls a tested view to obtain
// expected rows. Duplicate local IDs retain their source property in each row.
type boundaryOracle struct {
	engine       storage.Engine
	prefixes     []string
	empty        bool
	adjacentType string
}

func (o boundaryOracle) contains(id string) bool {
	if o.empty {
		return false
	}
	for _, prefix := range o.prefixes {
		if strings.HasPrefix(id, prefix) {
			return true
		}
	}
	return false
}

func boundaryLocal(id string) string {
	id = storage.EntityIdentityFromKey(id).LocalID
	if at := strings.LastIndexByte(id, ':'); at >= 0 {
		return id[at+1:]
	}
	return id
}

func boundaryNodeRow(n *storage.Node) string {
	return fmt.Sprintf("%s|%v|%v|%v", boundaryLocal(string(n.ID)), n.Labels, n.Properties["db"], n.Properties["v"])
}

func boundaryEdgeRow(e *storage.Edge) string {
	return fmt.Sprintf("%s|%s|%s|%s|%v|%v", boundaryLocal(string(e.ID)), boundaryLocal(string(e.StartNode)), boundaryLocal(string(e.EndNode)), e.Type, e.Properties["db"], e.Properties["v"])
}

type boundaryMatrixRead struct {
	name   string
	read   func(storage.Engine) ([]string, error)
	oracle func(boundaryOracle) ([]string, error)
}

func boundaryMatrixReads(nodeID func(string) storage.NodeID) []boundaryMatrixRead {
	ctx := context.Background()
	nodeRows := func(nodes []*storage.Node) []string {
		rows := []string{}
		for _, n := range nodes {
			if n != nil {
				rows = append(rows, boundaryNodeRow(n))
			}
		}
		return rows
	}
	edgeRows := func(edges []*storage.Edge) []string {
		rows := []string{}
		for _, e := range edges {
			if e != nil {
				rows = append(rows, boundaryEdgeRow(e))
			}
		}
		return rows
	}
	nodesOracle := func(o boundaryOracle) ([]string, error) {
		nodes, err := o.engine.GetNodesByLabel("P")
		if err != nil {
			return nil, err
		}
		rows := []string{}
		for _, n := range nodes {
			if n != nil && o.contains(string(n.ID)) {
				rows = append(rows, boundaryNodeRow(n))
			}
		}
		return rows, nil
	}
	reads := []boundaryMatrixRead{
		{"node-stream", func(e storage.Engine) ([]string, error) {
			var nodes []*storage.Node
			err := storage.VisitWithCapability(e, func(r storage.StreamingEngine, deliver func(*storage.Node) error) error {
				return r.StreamNodes(ctx, deliver)
			}, func(deliver func(*storage.Node) error) error {
				all, err := e.AllNodes()
				if err != nil {
					return err
				}
				for _, n := range all {
					if err := deliver(n); err != nil {
						return err
					}
				}
				return nil
			}, func(n *storage.Node) error {
				for _, label := range n.Labels {
					if label == "P" {
						nodes = append(nodes, n)
						break
					}
				}
				return nil
			})
			return nodeRows(nodes), err
		}, nodesOracle},
		{"projected-label-stream", func(e storage.Engine) ([]string, error) {
			var nodes []*storage.Node
			err := storage.VisitWithCapability(e, func(r storage.ProjectedLabelNodeReader, deliver func(*storage.Node) error) error {
				return r.StreamNodesByLabelProjected("P", []string{"db", "v"}, deliver)
			}, func(deliver func(*storage.Node) error) error {
				all, err := e.GetNodesByLabel("P")
				if err != nil {
					return err
				}
				for _, n := range all {
					if err := deliver(n); err != nil {
						return err
					}
				}
				return nil
			}, func(n *storage.Node) error { nodes = append(nodes, n); return nil })
			return nodeRows(nodes), err
		}, nodesOracle},
		{"adjacency", func(e storage.Engine) ([]string, error) {
			out, in, err := storage.GetAdjacentEdges(e, nodeID("same"))
			return edgeRows(append(out, in...)), err
		}, func(o boundaryOracle) ([]string, error) {
			rows := []string{}
			if o.empty {
				return rows, nil
			}
			for _, prefix := range o.prefixes {
				out, err := o.engine.GetOutgoingEdges(storage.NodeID(prefix + "same"))
				if err != nil {
					return nil, err
				}
				in, err := o.engine.GetIncomingEdges(storage.NodeID(prefix + "same"))
				if err != nil {
					return nil, err
				}
				for _, edge := range append(out, in...) {
					if o.contains(string(edge.ID)) && (o.adjacentType == "" || edge.Type == o.adjacentType) {
						rows = append(rows, boundaryEdgeRow(edge))
					}
				}
			}
			return rows, nil
		}},
		{"predicate", func(e storage.Engine) ([]string, error) {
			edges, err := storage.MatchEdgesBetween(e, nodeID("same"), nodeID("target"), "R", nil, func(edge *storage.Edge) bool { return edge.Type == "R" })
			return edgeRows(edges), err
		}, func(o boundaryOracle) ([]string, error) {
			rows := []string{}
			if o.empty {
				return rows, nil
			}
			for _, prefix := range o.prefixes {
				edges, err := o.engine.GetEdgesBetween(storage.NodeID(prefix+"same"), storage.NodeID(prefix+"target"))
				if err != nil {
					return nil, err
				}
				for _, edge := range edges {
					if edge.Type == "R" && o.contains(string(edge.ID)) {
						rows = append(rows, boundaryEdgeRow(edge))
					}
				}
			}
			return rows, nil
		}},
		{"temporal-predicate", func(e storage.Engine) ([]string, error) {
			nodes, err := e.GetNodesByLabel("P")
			if err != nil {
				return nil, err
			}
			rows := []string{}
			for _, node := range nodes {
				ok, err := storage.IsCurrentTemporalNode(e, node, time.Unix(1800000000, 0))
				if err != nil {
					return nil, err
				}
				if ok {
					rows = append(rows, boundaryNodeRow(node))
				}
			}
			return rows, nil
		}, func(o boundaryOracle) ([]string, error) {
			nodes, err := o.engine.GetNodesByLabel("P")
			if err != nil {
				return nil, err
			}
			rows := []string{}
			for _, node := range nodes {
				if !o.contains(string(node.ID)) {
					continue
				}
				ok, err := storage.IsCurrentTemporalNode(o.engine, node, time.Unix(1800000000, 0))
				if err != nil {
					return nil, err
				}
				if ok {
					rows = append(rows, boundaryNodeRow(node))
				}
			}
			return rows, nil
		}},
	}
	for _, kind := range []string{"", "R", "OTHER", "missing"} {
		kind := kind
		reads = append(reads, boundaryMatrixRead{"edge-stream/" + kind, func(e storage.Engine) ([]string, error) {
			var edges []*storage.Edge
			err := storage.StreamEdgesByType(ctx, e, kind, func(edge *storage.Edge) error { edges = append(edges, edge); return nil })
			return edgeRows(edges), err
		}, func(o boundaryOracle) ([]string, error) {
			edges, err := o.engine.GetEdgesByType(kind)
			if err != nil {
				return nil, err
			}
			rows := []string{}
			for _, edge := range edges {
				if edge != nil && o.contains(string(edge.ID)) && o.contains(string(edge.StartNode)) && o.contains(string(edge.EndNode)) {
					rows = append(rows, boundaryEdgeRow(edge))
				}
			}
			return rows, nil
		}})
	}
	for _, key := range []string{"db", "v", "missing"} {
		key := key
		fallback := func(e storage.Engine, accept func(*storage.Node) bool) ([]string, error) {
			nodes, err := e.GetNodesByLabel("P")
			if err != nil {
				return nil, err
			}
			known := false
			for _, node := range nodes {
				if node != nil && accept(node) {
					if _, ok := node.Properties[key]; ok {
						known = true
					}
				}
			}
			return []string{fmt.Sprint(known)}, nil
		}
		reads = append(reads, boundaryMatrixRead{"property-metadata/" + key, func(e storage.Engine) ([]string, error) {
			return storage.ReadWithCapability(e, func(r storage.PropertyKeyLookup) ([]string, error) {
				return []string{fmt.Sprint(r.PropertyKeyKnown(key))}, nil
			}, func() ([]string, error) { return fallback(e, func(*storage.Node) bool { return true }) })
		}, func(o boundaryOracle) ([]string, error) {
			return fallback(o.engine, func(node *storage.Node) bool { return o.contains(string(node.ID)) })
		}})
	}
	return reads
}

// Hide optional reads on the oracle while retaining the exact transaction's
// ordinary reads. This also differentially exercises the empty-type fallback.
type boundaryTransactionOracle struct {
	storage.Engine
	tx *storage.BadgerTransaction
}

type boundaryCoreReadMask struct{ storage.Engine }

func (e boundaryTransactionOracle) AllNodes() ([]*storage.Node, error) { return e.tx.AllNodes() }
func (e boundaryTransactionOracle) AllEdges() ([]*storage.Edge, error) { return e.tx.AllEdges() }
func (e boundaryTransactionOracle) GetNodesByLabel(label string) ([]*storage.Node, error) {
	return e.tx.GetNodesByLabel(label)
}
func (e boundaryTransactionOracle) GetEdgesByType(kind string) ([]*storage.Edge, error) {
	return e.tx.GetEdgesByType(kind)
}
func (e boundaryTransactionOracle) GetOutgoingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	return e.tx.GetOutgoingEdges(id)
}
func (e boundaryTransactionOracle) GetIncomingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	return e.tx.GetIncomingEdges(id)
}
func (e boundaryTransactionOracle) GetEdgesBetween(a, b storage.NodeID) ([]*storage.Edge, error) {
	return e.tx.GetEdgesBetween(a, b)
}

func TestReadBoundaryDifferentialMatrix(t *testing.T) {
	_, manager := newCompositeIdentityExecutor(t, false)
	one, err := manager.GetStorage("one")
	require.NoError(t, err)
	base := unwrapBadgerEngine(one)
	require.NotNil(t, base)
	for _, db := range []string{"one", "two", "foreign"} {
		view := storage.NewNamespacedEngine(base, db)
		if db == "foreign" {
			for _, id := range []storage.NodeID{"same", "target"} {
				_, err := view.CreateNode(&storage.Node{ID: id, Labels: []string{"P"}, Properties: map[string]interface{}{"db": db, "v": int64(99)}})
				require.NoError(t, err)
			}
		}
		if db != "foreign" {
			edge, err := view.GetEdge("same-rel")
			require.NoError(t, err)
			edge.Properties = map[string]interface{}{"db": db, "v": int64(1)}
			require.NoError(t, view.UpdateEdge(edge))
		}
		require.NoError(t, view.CreateEdge(&storage.Edge{ID: "other-rel", StartNode: "same", EndNode: "target", Type: "OTHER", Properties: map[string]interface{}{"db": db, "v": int64(2)}}))
	}
	composite, err := manager.GetStorage("comp")
	require.NoError(t, err)
	tx, err := base.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetNamespace("one"))
	_, err = tx.CreateNode(&storage.Node{ID: "one:pending", Labels: []string{"P"}, Properties: map[string]interface{}{"db": "one", "v": int64(9)}})
	require.NoError(t, err)
	require.NoError(t, tx.CreateEdge(&storage.Edge{ID: "one:pending-rel", StartNode: "one:same", EndNode: "one:pending", Type: "R", Properties: map[string]interface{}{"db": "one", "v": int64(9)}}))
	transaction := &transactionStorageWrapper{tx: tx, underlying: one, namespace: "one", separator: ":"}
	require.True(t, storage.HasReadCapability[storage.EdgeTypeStreamer](transaction), "the pending stream is owned and admitted")
	require.False(t, storage.HasReadCapability[storage.EdgesBetweenMatcher](composite), "the unscoped matcher must use its declared ordinary fallback")
	_, ok := storage.ViewMetadataForEngine[interface {
		ConstituentStorage(string) (storage.Engine, error)
	}](composite)
	require.True(t, ok, "constituent metadata selection is owned and admitted")
	namespace := storage.NewNamespacedEngine(base, "one")
	views := []struct {
		name   string
		engine storage.Engine
		oracle boundaryOracle
	}{
		{"namespace", namespace, boundaryOracle{engine: base, prefixes: []string{"one:"}}},
		{"size-tracking", one, boundaryOracle{engine: base, prefixes: []string{"one:"}}},
		{"composite-constituents", composite, boundaryOracle{engine: base, prefixes: []string{"one:", "two:"}}},
		{"transaction-pending", transaction, boundaryOracle{engine: boundaryTransactionOracle{Engine: base, tx: tx}, prefixes: []string{"one:"}}},
		{"transaction-core-fallback", boundaryCoreReadMask{Engine: transaction}, boundaryOracle{engine: boundaryTransactionOracle{Engine: base, tx: tx}, prefixes: []string{"one:"}}},
		{"algorithm-filtered", &algorithmRelationshipReader{Engine: namespace, types: map[string]struct{}{"R": {}}}, boundaryOracle{engine: base, prefixes: []string{"one:"}, adjacentType: "R"}},
		{"identity-filtered", graphIdentityView(namespace), boundaryOracle{engine: base, prefixes: []string{"one:"}}},
		{"system-empty", systemGraphView{Engine: namespace}, boundaryOracle{engine: base, prefixes: []string{"one:"}, empty: true}},
	}
	for _, view := range views {
		for _, order := range []string{"", "TW", "WT"} {
			engine := view.engine
			for i := len(order) - 1; i >= 0; i-- {
				if order[i] == 'T' {
					engine = storage.NewTracedEngine(engine)
				} else {
					engine = storage.NewWALEngine(engine, nil)
				}
			}
			nodeID := func(id string) storage.NodeID { return storage.NodeID(id) }
			if view.name == "identity-filtered" {
				nodeID = func(id string) storage.NodeID { return storage.NodeID(storage.EntityIdentityKey("one", id)) }
			}
			for _, read := range boundaryMatrixReads(nodeID) {
				t.Run(view.name+"/"+order+"/"+read.name, func(t *testing.T) {
					want, err := read.oracle(view.oracle)
					require.NoError(t, err)
					got, err := read.read(engine)
					require.NoError(t, err)
					sort.Strings(want)
					sort.Strings(got)
					require.Equal(t, want, got)
				})
			}
		}
	}
}
