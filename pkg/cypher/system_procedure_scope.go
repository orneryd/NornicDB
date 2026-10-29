package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// systemProcedureScope is where a procedure call runs: on the system
// database, a view of it that holds no graph (systemGraphView), so a system
// procedure that reads the graph (db.labels, db.propertyKeys, db.schema.*,
// the full-text queries, graph counts) sees none of the records the system
// database keeps. Neo4j's system database lists no labels or property keys
// to these procedures either. The statement's transaction storage is left
// out of the context too, so the procedure reads the view, not the
// transaction. On any other database the call runs as it is.
func (e *StorageExecutor) systemProcedureScope(ctx context.Context) (context.Context, *StorageExecutor) {
	if !strings.EqualFold(e.currentDatabaseName(), "system") {
		return ctx, e
	}
	return withoutTransactionStorage{ctx}, e.cloneWithStorage(systemGraphView{Engine: e.storage})
}

// withoutTransactionStorage is ctx without the transaction storage wrapper
// a statement carries (ctxKeyTxStorage).
type withoutTransactionStorage struct{ context.Context }

func (c withoutTransactionStorage) Value(key any) any {
	if key == ctxKeyTxStorage {
		return nil
	}
	return c.Context.Value(key)
}

// systemGraphView is the system database as its procedures read it: its
// name, and no nodes, relationships or schema. Writes are refused before a
// procedure runs there (systemDatabaseStatementError).
type systemGraphView struct{ storage.Engine }

// systemGraphView declares every optional method; admission is derived from this policy.
var systemGraphViewReadPolicy = storage.BoundaryReadPolicy{
	{Methods: "ReadStored StoredReadPolicy", Disposition: storage.CapabilityOwned, Scope: "Explicit stored modes preserve this view; mandatory facts never use ordinary/full-body fallback."},
	{Methods: "Namespace",
		Disposition: storage.CapabilityOwned, Scope: "System namespace identity; core Engine reads expose an empty graph."},
}

func (systemGraphView) ReadCapabilityPolicy() storage.BoundaryReadPolicy {
	return systemGraphViewReadPolicy.Copy()
}

func (systemGraphView) Namespace() string { return "system" }

func (systemGraphView) GetNode(storage.NodeID) (*storage.Node, error) {
	return nil, storage.ErrNotFound
}

func (systemGraphView) GetEdge(storage.EdgeID) (*storage.Edge, error) {
	return nil, storage.ErrNotFound
}

func (systemGraphView) GetNodesByLabel(string) ([]*storage.Node, error) { return nil, nil }

func (systemGraphView) GetFirstNodeByLabel(string) (*storage.Node, error) {
	return nil, storage.ErrNotFound
}

func (systemGraphView) GetOutgoingEdges(storage.NodeID) ([]*storage.Edge, error) { return nil, nil }

func (systemGraphView) GetIncomingEdges(storage.NodeID) ([]*storage.Edge, error) { return nil, nil }

func (systemGraphView) GetEdgesBetween(storage.NodeID, storage.NodeID) ([]*storage.Edge, error) {
	return nil, nil
}

// Deprecated: Use GetEdgesBetween and filter by type to receive errors. Failed lookups may appear as nil.
func (systemGraphView) GetEdgeBetween(storage.NodeID, storage.NodeID, string) *storage.Edge {
	return nil
}

func (systemGraphView) GetEdgesByType(string) ([]*storage.Edge, error) { return nil, nil }

func (systemGraphView) AllNodes() ([]*storage.Node, error) { return nil, nil }

func (systemGraphView) AllEdges() ([]*storage.Edge, error) { return nil, nil }

// Deprecated: Use AllNodes to receive errors. Failed reads are omitted and may appear as an empty collection.
func (systemGraphView) GetAllNodes() []*storage.Node { return nil }

func (systemGraphView) StreamNodesWithOptions(context.Context, storage.StreamNodesOptions, func(*storage.Node) error) error {
	return nil
}

// Deprecated: Use GetIncomingEdges and count the returned edges to receive errors. Failed reads may appear as zero.
func (systemGraphView) GetInDegree(storage.NodeID) int { return 0 }

// Deprecated: Use GetOutgoingEdges and count the returned edges to receive errors. Failed reads may appear as zero.
func (systemGraphView) GetOutDegree(storage.NodeID) int { return 0 }

func (systemGraphView) GetSchema() *storage.SchemaManager { return storage.NewSchemaManager() }

func (systemGraphView) BatchGetNodes([]storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	return map[storage.NodeID]*storage.Node{}, nil
}

func (systemGraphView) NodeCount() (int64, error) { return 0, nil }

func (systemGraphView) EdgeCount() (int64, error) { return 0, nil }

func (systemGraphView) EdgeCountByType(string) (int64, error) { return 0, nil }

func (systemGraphView) EdgeCountByStartLabel(string, string) (int64, error) { return 0, nil }

func (systemGraphView) EdgeCountByEndLabel(string, string) (int64, error) { return 0, nil }

func (systemGraphView) EngineCapabilityPolicy() storage.EngineBoundaryPolicy {
	return storage.EngineBoundaryPolicy{Kind: storage.EngineView, ReadState: storage.BindingOwned, Membership: storage.BindingCarried, Controller: storage.BindingRefused, PendingActions: storage.BindingCarried, SchemaPublication: storage.BindingCarried, ReadLease: storage.BindingCarried, StatementMode: storage.BindingCarried, Identity: storage.BindingOwned, Enforcement: storage.BindingCarried, CompoundDependencies: storage.BindingCarried, SameVersionFallback: storage.BindingCarried, Delivery: storage.BindingCarried, ExactEffects: storage.BindingCarried, FullRead: storage.BindingOwned, ObservedNode: storage.BindingRefused, ObservedEdge: storage.BindingRefused}
}

func (systemGraphView) StoredReadPolicy() storage.StoredReadPolicy {
	return storage.NativeStoredReadPolicy()
}
func (v systemGraphView) ReadStored(ctx context.Context, r storage.StoredReadRequest) (storage.StoredReadResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := storage.MembershipForEngine(v).ReadError(); err != nil {
		return nil, err
	}
	identity := storage.StoredReadIdentity{Namespace: "system"}
	facts := storage.StoredMetadata{Views: []storage.StoredReadIdentity{identity}}
	for _, id := range r.Nodes {
		f := storage.StoredNodeFact{ID: id, View: identity}
		facts.Nodes = append(facts.Nodes, f)
		if r.Physical {
			facts.PhysicalNodes = append(facts.PhysicalNodes, f)
		}
	}
	for _, id := range r.Edges {
		f := storage.StoredEdgeFact{ID: id, View: identity}
		facts.Edges = append(facts.Edges, f)
		if r.Physical {
			facts.PhysicalEdges = append(facts.PhysicalEdges, f)
		}
	}
	switch r.Mode {
	case storage.StoredMetadataOnly:
		return facts, nil
	case storage.StoredSelectedProperties:
		return storage.StoredProjection{Facts: facts}, nil
	case storage.StoredFullMaterialization:
		return storage.StoredFullRead{Facts: facts}, nil
	}
	return nil, storage.ErrInvalidData
}
