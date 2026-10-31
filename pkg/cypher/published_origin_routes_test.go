package cypher

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// publishedOrigins records the origin of every operation a store publishes.
type publishedOrigins struct {
	mu   sync.Mutex
	seen map[storage.OperationType][]uint64
}

func subscribePublishedOrigins(t *testing.T, store storage.Engine) *publishedOrigins {
	t.Helper()
	published := &publishedOrigins{seen: map[storage.OperationType][]uint64{}}
	require.NoError(t, storage.SubscribeEvents(store, storage.EventHandlers{EntityCommitted: func(event storage.EntityCommitEvent) {
		published.mu.Lock()
		defer published.mu.Unlock()
		published.seen[event.Operation.Type] = append(published.seen[event.Operation.Type], event.VectorWorkOrigin)
	}}))
	return published
}

// requireOnly waits for at least one published operation of each type and
// requires every operation published so far to carry origin.
func (p *publishedOrigins) requireOnly(t *testing.T, origin uint64, types ...storage.OperationType) {
	t.Helper()
	require.Eventually(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, kind := range types {
			if len(p.seen[kind]) == 0 {
				return false
			}
		}
		return true
	}, 5*time.Second, 5*time.Millisecond, "published: %v", types)
	p.mu.Lock()
	defer p.mu.Unlock()
	for kind, origins := range p.seen {
		for _, published := range origins {
			require.Equal(t, origin, published, "operation %v", kind)
		}
	}
}

func (p *publishedOrigins) reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.seen = map[storage.OperationType][]uint64{}
}

// directEngine offers only Engine: no transaction capability and no unwrapping,
// so a statement takes the direct route.
type directEngine struct {
	storage.Engine
	failCreateEdge bool
}

func (e *directEngine) CreateEdge(edge *storage.Edge) error {
	if e.failCreateEdge {
		return errors.New("rejected relationship")
	}
	return e.Engine.CreateEdge(edge)
}

func (e *directEngine) BulkCreateEdges(edges []*storage.Edge) error {
	if e.failCreateEdge {
		return errors.New("rejected relationship")
	}
	return e.Engine.BulkCreateEdges(edges)
}

// TestDirectRouteWritesPublishTheRequestOrigin drives each write consumer through
// public Execute on an engine without transactions, and reads the origin of the
// published operation.
func TestDirectRouteWritesPublishTheRequestOrigin(t *testing.T) {
	t.Setenv("NORNICDB_APOC_SECURITY_ALLOW_IMPORT_FILE_ACCESS", "true")
	importFile := filepath.Join(t.TempDir(), "import.json")
	require.NoError(t, os.WriteFile(importFile, []byte(`{"nodes":[{"id":"imp1","labels":["Imported"],"properties":{"name":"i"}},{"id":"imp2","labels":["Imported"],"properties":{"name":"j"}}],"relationships":[{"id":"imr","type":"R","startNode":"imp1","endNode":"imp2"}]}`), 0o600))

	namespaced := storage.NewNamespacedEngine(newTestMemoryEngine(t), "direct")
	engine := &directEngine{Engine: namespaced}
	exec := NewStorageExecutor(engine)
	published := subscribePublishedOrigins(t, namespaced)
	ctx, origin := storage.AdmitVectorWork(context.Background())

	for _, route := range []struct {
		name, query string
		params      map[string]interface{}
		types       []storage.OperationType
	}{
		{"create one node", "CREATE (:One {name: 'a'})", nil, []storage.OperationType{storage.OpCreateNode}},
		{"create nodes in bulk", "CREATE (:Many {name: 'b'}), (:Many {name: 'c'})", nil, []storage.OperationType{storage.OpCreateNode}},
		{"create one relationship", "CREATE (:Rel {name: 'r1'})-[:R {w: 1}]->(:Rel {name: 'r2'})", nil, []storage.OperationType{storage.OpCreateEdge}},
		{"create relationships in bulk", "CREATE (:Rel {name: 'r3'})-[:R]->(:Rel {name: 'r4'})-[:R]->(:Rel {name: 'r5'})", nil, []storage.OperationType{storage.OpCreateEdge}},
		{"set a node", "MATCH (n:One {name: 'a'}) SET n.title = 'x'", nil, []storage.OperationType{storage.OpUpdateNode}},
		{"set a relationship", "MATCH (:Rel {name: 'r1'})-[r:R]->() SET r.w = 2", nil, []storage.OperationType{storage.OpUpdateEdge}},
		{"remove from a node", "MATCH (n:One {name: 'a'}) REMOVE n.title", nil, []storage.OperationType{storage.OpUpdateNode}},
		{"remove from a relationship", "MATCH (:Rel {name: 'r1'})-[r:R]->() REMOVE r.w", nil, []storage.OperationType{storage.OpUpdateEdge}},
		{"merge a node", "MERGE (:Merged {name: 'm'})", nil, []storage.OperationType{storage.OpCreateNode}},
		{"merge a relationship", "MATCH (a:Merged {name: 'm'}), (b:One {name: 'a'}) MERGE (a)-[:M]->(b)", nil, []storage.OperationType{storage.OpCreateEdge}},
		{"delete through a pipeline", "MATCH ()-[r:M]->() WITH r DELETE r", nil, []storage.OperationType{storage.OpDeleteEdge}},
		{"delete a node through a pipeline", "MATCH (n:Merged) WITH n DELETE n", nil, []storage.OperationType{storage.OpDeleteNode}},
		{"delete a node", "MATCH (n:Many {name: 'b'}) DELETE n", nil, []storage.OperationType{storage.OpDeleteNode}},
		{"detach delete", "MATCH (n:Rel {name: 'r5'}) DETACH DELETE n", nil, []storage.OperationType{storage.OpDeleteNode}},
		{"delete a relationship", "MATCH (:Rel {name: 'r3'})-[r:R]->() DELETE r", nil, []storage.OperationType{storage.OpDeleteEdge}},
		{"apoc import", "CALL apoc.import.json($file) YIELD source, nodes, relationships", map[string]interface{}{"file": importFile}, []storage.OperationType{storage.OpCreateNode, storage.OpCreateEdge}},
	} {
		t.Run(route.name, func(t *testing.T) {
			published.reset()
			_, err := exec.Execute(ctx, route.query, route.params)
			require.NoError(t, err)
			published.requireOnly(t, origin, route.types...)
		})
	}

	t.Run("cleanup of a create whose relationship fails", func(t *testing.T) {
		published.reset()
		engine.failCreateEdge = true
		defer func() { engine.failCreateEdge = false }()
		_, err := exec.Execute(ctx, "CREATE (:Cleanup {name: 'x'})-[:R]->(:Cleanup {name: 'y'})", nil)
		require.Error(t, err)
		published.requireOnly(t, origin, storage.OpCreateNode, storage.OpDeleteNode)
		count, err := exec.Execute(ctx, "MATCH (n:Cleanup) RETURN count(n) AS c", nil)
		require.NoError(t, err)
		require.EqualValues(t, 0, count.Rows[0][0])
	})
}

// TestTransactionalRouteWritesPublishTheRequestOrigin is the same oracle on the
// route an engine with transactions takes, implicit and explicit.
func TestTransactionalRouteWritesPublishTheRequestOrigin(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "tx")
	exec := NewStorageExecutor(store)
	published := subscribePublishedOrigins(t, store)
	ctx, origin := storage.AdmitVectorWork(context.Background())
	for _, statement := range []string{
		"CREATE (:T {name: 'a'})-[:R]->(:T {name: 'b'})",
		"MATCH (n:T {name: 'a'}) SET n.title = 'x'",
		"MATCH (n:T {name: 'b'}) DETACH DELETE n",
		"BEGIN",
		"CREATE (:InTx {v: 1})",
		"COMMIT",
	} {
		_, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}
	published.requireOnly(t, origin, storage.OpCreateNode, storage.OpCreateEdge, storage.OpUpdateNode, storage.OpDeleteNode)
}

// TestSystemWorkJoinedToATransactionPublishesTheSystemStoreAdmission: system-store
// work joined to a request's explicit transaction commits with it and keeps its
// own admission on every operation, while the request's own writes keep theirs.
func TestSystemWorkJoinedToATransactionPublishesTheSystemStoreAdmission(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "system")
	exec := NewStorageExecutor(store)
	published := subscribePublishedOrigins(t, store)
	ctx, origin := storage.AdmitVectorWork(context.Background())

	seed := []string{
		"CREATE (:Seed {name: 'n1'})-[:R]->(:Seed {name: 'n2'})",
		"CREATE (:Seed {name: 'n3'})-[:R]->(:Seed {name: 'n4'})",
	}
	for _, statement := range seed {
		_, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err)
	}
	published.requireOnly(t, origin, storage.OpCreateNode, storage.OpCreateEdge)

	_, err := exec.handleBegin(ctx)
	require.NoError(t, err)
	view := storage.Engine(exec.txContext.storageWrapper)
	published.reset()
	var joinErr error
	require.NoError(t, auth.WithStoreTransaction(ctx, view, func(system storage.Engine) error {
		node := &storage.Node{ID: "joined-a", Labels: []string{"Joined"}, Properties: map[string]any{"v": int64(1)}}
		if _, joinErr = system.CreateNode(node); joinErr != nil {
			return joinErr
		}
		other := &storage.Node{ID: "joined-b", Labels: []string{"Joined"}}
		if _, joinErr = system.CreateNode(other); joinErr != nil {
			return joinErr
		}
		edge := &storage.Edge{ID: "joined-e", Type: "J", StartNode: "joined-a", EndNode: "joined-b"}
		if joinErr = system.CreateEdge(edge); joinErr != nil {
			return joinErr
		}
		stored, err := system.GetNode("joined-a")
		if err != nil {
			return err
		}
		after := *stored
		after.Properties = map[string]any{"v": int64(2)}
		if joinErr = system.UpdateNode(storage.NodeUpdateRequest{Before: stored, After: &after}); joinErr != nil {
			return joinErr
		}
		storedEdge, err := system.GetEdge("joined-e")
		if err != nil {
			return err
		}
		afterEdge := *storedEdge
		afterEdge.Properties = map[string]any{"w": int64(2)}
		if joinErr = system.UpdateEdge(storage.EdgeUpdateRequest{Before: storedEdge, After: &afterEdge}); joinErr != nil {
			return joinErr
		}
		if joinErr = system.DeleteEdge("joined-e"); joinErr != nil {
			return joinErr
		}
		if joinErr = system.DeleteNode("joined-b"); joinErr != nil {
			return joinErr
		}
		return nil
	}))
	require.NoError(t, joinErr)
	_, err = exec.handleCommit(ctx)
	require.NoError(t, err)
	published.requireOnly(t, storage.SystemStoreAdmission, storage.OpCreateNode, storage.OpCreateEdge, storage.OpUpdateNode, storage.OpUpdateEdge, storage.OpDeleteEdge, storage.OpDeleteNode)

	// A deletion of a still-connected node is checked at commit, and keeps its
	// origin until then.
	_, err = exec.handleBegin(ctx)
	require.NoError(t, err)
	wrapper := exec.txContext.storageWrapper
	_, err = wrapper.CreateNode(&storage.Node{ID: "connected-a", Labels: []string{"Joined"}})
	require.NoError(t, err)
	_, err = wrapper.CreateNode(&storage.Node{ID: "connected-b", Labels: []string{"Joined"}})
	require.NoError(t, err)
	require.NoError(t, wrapper.CreateEdge(&storage.Edge{ID: "connected-e", Type: "J", StartNode: "connected-a", EndNode: "connected-b"}))
	_, err = exec.handleCommit(ctx)
	require.NoError(t, err)
	published.requireOnly(t, origin, storage.OpCreateEdge)
	published.reset()
	_, err = exec.handleBegin(ctx)
	require.NoError(t, err)
	wrapper = exec.txContext.storageWrapper
	require.NoError(t, wrapper.DeleteGraphNode("connected-a", storage.RejectConnected, storage.SystemStoreAdmission))
	require.NoError(t, wrapper.DeleteEdge("connected-e", storage.SystemStoreAdmission))
	_, err = exec.handleCommit(ctx)
	require.NoError(t, err)
	published.requireOnly(t, storage.SystemStoreAdmission, storage.OpDeleteNode, storage.OpDeleteEdge)

	// A request's own write in the same transaction keeps the request's origin.
	published.reset()
	_, err = exec.handleBegin(ctx)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:OwnWrite {name: 'w'})", nil)
	require.NoError(t, err)
	view = storage.Engine(exec.txContext.storageWrapper)
	require.NoError(t, auth.WithStoreTransaction(ctx, view, func(system storage.Engine) error {
		_, err := system.CreateNode(&storage.Node{ID: "joined-c", Labels: []string{"Joined"}})
		return err
	}))
	_, err = exec.handleCommit(ctx)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		published.mu.Lock()
		defer published.mu.Unlock()
		return len(published.seen[storage.OpCreateNode]) == 2
	}, 5*time.Second, 5*time.Millisecond)
	published.mu.Lock()
	defer published.mu.Unlock()
	require.ElementsMatch(t, []uint64{origin, storage.SystemStoreAdmission}, published.seen[storage.OpCreateNode])
}
