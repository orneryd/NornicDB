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

func (systemGraphView) GetNode(ctx context.Context, _ storage.NodeID) (*storage.Node, error) {
	return nil, storage.ErrNotFound
}

func (systemGraphView) GetEdge(ctx context.Context, _ storage.EdgeID) (*storage.Edge, error) {
	return nil, storage.ErrNotFound
}

func (systemGraphView) GetNodesByLabel(ctx context.Context, _ string) ([]*storage.Node, error) {
	return nil, nil
}

func (systemGraphView) GetFirstNodeByLabel(ctx context.Context, _ string) (*storage.Node, error) {
	return nil, storage.ErrNotFound
}

func (systemGraphView) GetOutgoingEdges(ctx context.Context, _ storage.NodeID) ([]*storage.Edge, error) {
	return nil, nil
}

func (systemGraphView) GetIncomingEdges(ctx context.Context, _ storage.NodeID) ([]*storage.Edge, error) {
	return nil, nil
}

func (systemGraphView) GetEdgesBetween(ctx context.Context, _, _ storage.NodeID) ([]*storage.Edge, error) {
	return nil, nil
}

func (systemGraphView) GetEdgeBetween(ctx context.Context, _ storage.NodeID, _ storage.NodeID, _ string) (*storage.Edge, error) {
	return nil, nil
}

func (systemGraphView) GetEdgesByType(ctx context.Context, _ string) ([]*storage.Edge, error) {
	return nil, nil
}

func (systemGraphView) AllNodes(ctx context.Context) ([]*storage.Node, error) { return nil, nil }

func (systemGraphView) AllEdges(ctx context.Context) ([]*storage.Edge, error) { return nil, nil }

func (systemGraphView) StreamNodesWithOptions(context.Context, storage.StreamNodesOptions, func(*storage.Node) error) error {
	return nil
}

func (systemGraphView) GetInDegree(ctx context.Context, _ storage.NodeID) (int, error) { return 0, nil }

func (systemGraphView) GetOutDegree(ctx context.Context, _ storage.NodeID) (int, error) {
	return 0, nil
}

func (systemGraphView) GetSchema() *storage.SchemaManager { return storage.NewSchemaManager() }

func (systemGraphView) BatchGetNodes(ctx context.Context, _ []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	return map[storage.NodeID]*storage.Node{}, nil
}

func (systemGraphView) NodeCount(ctx context.Context) (int64, error) { return 0, nil }

func (systemGraphView) EdgeCount(ctx context.Context) (int64, error) { return 0, nil }

func (systemGraphView) EdgeCountByType(ctx context.Context, _ string) (int64, error) { return 0, nil }

func (systemGraphView) EdgeCountByStartLabel(ctx context.Context, _ string, _ string) (int64, error) {
	return 0, nil
}

func (systemGraphView) EdgeCountByEndLabel(ctx context.Context, _ string, _ string) (int64, error) {
	return 0, nil
}
