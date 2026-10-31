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

// testRequestOrigin admits a request with an origin no owner can capture for
// itself. Origins are retirement-clock values, and the clock stands still in a
// test, so a request admitted the ordinary way has the same origin as a write
// that lost it and admitted itself: comparing them would prove nothing.
func testRequestOrigin(t *testing.T) (context.Context, uint64) {
	t.Helper()
	origin := storage.CaptureVectorWorkOrigin() + 1<<40
	require.NotEqual(t, storage.SystemStoreAdmission, origin)
	return storage.WithVectorWorkOrigin(context.Background(), origin), origin
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
	ctx, origin := testRequestOrigin(t)

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
	ctx, origin := testRequestOrigin(t)
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
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "joined")
	exec := NewStorageExecutor(store)
	published := subscribePublishedOrigins(t, store)
	ctx, origin := testRequestOrigin(t)

	seed := []string{
		"CREATE (:Seed {name: 'n1'})-[:R]->(:Seed {name: 'n2'})",
		"CREATE (:Seed {name: 'n3'})-[:R]->(:Seed {name: 'n4'})",
	}
	for _, statement := range seed {
		_, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err)
	}
	published.requireOnly(t, origin, storage.OpCreateNode, storage.OpCreateEdge)

	joined := func(mutate func(system storage.Engine) error, types ...storage.OperationType) {
		t.Helper()
		published.reset()
		_, err := exec.handleBegin(ctx)
		require.NoError(t, err)
		require.NoError(t, auth.WithStoreTransaction(ctx, storage.Engine(exec.txContext.storageWrapper), mutate))
		_, err = exec.handleCommit(ctx)
		require.NoError(t, err)
		published.requireOnly(t, storage.SystemStoreAdmission, types...)
	}
	// One transaction each: a transaction publishes only its net effect, so an
	// entity it creates and deletes publishes nothing.
	joined(func(system storage.Engine) error {
		if _, err := system.CreateNode(&storage.Node{ID: "joined-a", Labels: []string{"Joined"}, Properties: map[string]any{"v": int64(1)}}); err != nil {
			return err
		}
		if _, err := system.CreateNode(&storage.Node{ID: "joined-b", Labels: []string{"Joined"}}); err != nil {
			return err
		}
		return system.CreateEdge(&storage.Edge{ID: "joined-e", Type: "J", StartNode: "joined-a", EndNode: "joined-b"})
	}, storage.OpCreateNode, storage.OpCreateEdge)
	joined(func(system storage.Engine) error {
		stored, err := system.GetNode("joined-a")
		if err != nil {
			return err
		}
		after := *stored
		after.Properties = map[string]any{"v": int64(2)}
		if err := system.UpdateNode(storage.NodeUpdateRequest{Before: stored, After: &after}); err != nil {
			return err
		}
		storedEdge, err := system.GetEdge("joined-e")
		if err != nil {
			return err
		}
		afterEdge := *storedEdge
		afterEdge.Properties = map[string]any{"w": int64(2)}
		return system.UpdateEdge(storage.EdgeUpdateRequest{Before: storedEdge, After: &afterEdge})
	}, storage.OpUpdateNode, storage.OpUpdateEdge)
	joined(func(system storage.Engine) error {
		if err := system.DeleteEdge("joined-e"); err != nil {
			return err
		}
		return system.DeleteNode("joined-b")
	}, storage.OpDeleteEdge, storage.OpDeleteNode)

	// A deletion of a still-connected node is checked at commit, and keeps its
	// origin until then.
	published.reset()
	_, err := exec.handleBegin(ctx)
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
	require.NoError(t, auth.WithStoreTransaction(ctx, storage.Engine(exec.txContext.storageWrapper), func(system storage.Engine) error {
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

// TestAdministrationInATransactionPublishesTheSystemStoreAdmission is the same
// outcome through public statements: a grant and a revoke in an explicit
// transaction on the system database.
func TestAdministrationInATransactionPublishesTheSystemStoreAdmission(t *testing.T) {
	exec, system := administrationReviewExecutor(t, "system")
	published := subscribePublishedOrigins(t, system)
	ctx, _ := testRequestOrigin(t)
	for _, statement := range []struct {
		query string
		kind  storage.OperationType
	}{
		{"GRANT EXECUTE PROCEDURE db.ping ON DBMS TO viewer", storage.OpCreateNode},
		{"REVOKE EXECUTE PROCEDURE db.ping ON DBMS FROM viewer", storage.OpDeleteNode},
	} {
		published.reset()
		_, err := exec.handleBegin(ctx)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, statement.query, nil)
		require.NoError(t, err, statement.query)
		_, err = exec.handleCommit(ctx)
		require.NoError(t, err)
		published.requireOnly(t, storage.SystemStoreAdmission, statement.kind)
	}
}

// TestUserRecordJoinedToATransactionPublishesTheSystemStoreAdmission: an
// authenticator on an explicit transaction's view writes its user records
// through the same join, on create and on update.
func TestUserRecordJoinedToATransactionPublishesTheSystemStoreAdmission(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "joined_user")
	exec := NewStorageExecutor(store)
	published := subscribePublishedOrigins(t, store)
	ctx, _ := testRequestOrigin(t)
	config := auth.DefaultAuthConfig()
	config.JWTSecret = []byte("test-secret-that-is-long-enough-32chars")
	inTransaction := func(write func(*auth.Authenticator), kind storage.OperationType) {
		t.Helper()
		published.reset()
		_, err := exec.handleBegin(ctx)
		require.NoError(t, err)
		authenticator, err := auth.NewAuthenticator(config, exec.txContext.storageWrapper)
		require.NoError(t, err)
		write(authenticator)
		_, err = exec.handleCommit(ctx)
		require.NoError(t, err)
		published.requireOnly(t, storage.SystemStoreAdmission, kind)
	}
	inTransaction(func(authenticator *auth.Authenticator) {
		_, err := authenticator.CreateUser("joined-user", "a-long-enough-password", []auth.Role{auth.RoleViewer})
		require.NoError(t, err)
	}, storage.OpCreateNode)
	inTransaction(func(authenticator *auth.Authenticator) {
		_, _, err := authenticator.Authenticate("joined-user", "the-wrong-password", "127.0.0.1", "test")
		require.ErrorIs(t, err, auth.ErrInvalidCredentials)
	}, storage.OpUpdateNode)
}

// TestDirectWriteWithoutAdmissionIsDeclared: an internal caller with no admission
// does not reach the raw engine: the write is admitted at the executor's named,
// counted site.
func TestDirectWriteWithoutAdmissionIsDeclared(t *testing.T) {
	const site = "cypher.direct-write"
	count := func() int64 {
		for _, fallback := range storage.VectorWorkFallbacks() {
			if fallback.Site == site {
				return fallback.Count
			}
		}
		return 0
	}
	exec := NewStorageExecutor(&directEngine{Engine: storage.NewNamespacedEngine(newTestMemoryEngine(t), "declared")})
	before := count()
	_, err := exec.writeStorage(context.Background()).CreateNode(&storage.Node{ID: "declared-a", Labels: []string{"Declared"}})
	require.NoError(t, err)
	require.Equal(t, before+1, count())
}
