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
	if !strings.EqualFold(e.databaseName(), "system") {
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

func (systemGraphView) GetEdgeBetween(storage.NodeID, storage.NodeID, string) *storage.Edge {
	return nil
}

func (systemGraphView) GetEdgesByType(string) ([]*storage.Edge, error) { return nil, nil }

func (systemGraphView) AllNodes() ([]*storage.Node, error) { return nil, nil }

func (systemGraphView) AllEdges() ([]*storage.Edge, error) { return nil, nil }

func (systemGraphView) GetAllNodes() []*storage.Node { return nil }

func (systemGraphView) StreamNodesWithOptions(context.Context, storage.StreamNodesOptions, func(*storage.Node) error) error {
	return nil
}

func (systemGraphView) GetInDegree(storage.NodeID) int { return 0 }

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
