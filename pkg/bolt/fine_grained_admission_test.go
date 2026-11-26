package bolt

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func openBoltBearerTestConn(t *testing.T, port int) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.NoError(t, PerformHandshakeWithTesting(t, conn))
	require.NoError(t, SendHello(t, conn, map[string]string{"scheme": "bearer", "credentials": "review-token"}))
	require.NoError(t, ReadSuccess(t, conn))
	return conn
}

func TestFineGrainedBoltLazyPullRechecksPolicy(t *testing.T) {
	store := storage.NewNamespacedEngine(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "nornic")
	for i := range 1500 {
		_, err := store.CreateNode(t.Context(), &storage.Node{ID: storage.NodeID(fmt.Sprintf("node-%d", i)), Labels: []string{"Item"}, Properties: map[string]any{"k": i}})
		require.NoError(t, err)
	}
	policy, err := auth.CompileFineGrainedPolicy(auth.GraphFineGrainedActions(), auth.FineGrainedCatalog{
		Definitions: []auth.FineGrainedEntitlement{
			{ID: "db", Action: "database.access", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase, DatabaseID: "db-id"}},
			{ID: "traverse", Action: "graph.node.traverse", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Item"}}},
			{ID: "read", Action: "graph.node.property.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Item"}, Properties: []string{"k"}}},
		},
		Assignments: map[string][]string{"reader": {"db", "traverse", "read"}},
	})
	require.NoError(t, err)
	server := New(&Config{Port: 0, MaxConnections: 10, ReadBufferSize: 8192, WriteBufferSize: 8192, RequireAuth: true, Authenticator: review1477BearerAuthenticator{}}, &cypherQueryExecutor{executor: cypher.NewStorageExecutor(store)})
	var version atomic.Int64
	server.SetFineGrainedAdmission(func(context.Context, []string) (*auth.FineGrainedRequest, error) {
		return &auth.FineGrainedRequest{Access: policy.ForRoles([]string{"reader"}), Fingerprint: fmt.Sprint(version.Load()), HomeDatabaseName: "nornic", DatabaseID: func(string) string { return "db-id" }}, nil
	})
	// Inspect the live Bolt session after RUN has answered. A materialized
	// FGAC result can pass the policy-change check below while still keeping
	// all 1,500 rows in memory, so this separately proves the lazy path runs.
	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() { _ = serverConn.Close(); _ = clientConn.Close() })
	direct := newTestSession(serverConn, &cypherQueryExecutor{executor: cypher.NewStorageExecutor(store)})
	direct.server.SetFineGrainedAdmission(server.fineGrainedAdmission)
	direct.authResult = &BoltAuthResult{Authenticated: true, Username: "review", PrincipalID: "review", Roles: []string{"reader"}}
	direct.authenticated = true
	direct.messageQueue = make(chan *boltMessage, 2)
	direct.readErrQueue = make(chan error, 1)
	done := make(chan error, 1)
	go func() { done <- direct.handleRun(BuildRunMessage("MATCH (n:Item) RETURN n.k AS k", nil, nil)[2:]) }()
	_, err = AssertSuccess(t, clientConn)
	require.NoError(t, err)
	require.NotNil(t, direct.lastLazy, "FGAC RUN must use the bounded lazy result path")
	direct.messageQueue <- &boltMessage{msgType: MsgPull, data: BuildPullMessage(map[string]any{"n": int64(10)})[2:]}
	rows, _, code := readBatch(t, clientConn)
	require.Empty(t, code)
	require.Len(t, rows, 10)
	direct.messageQueue <- nil
	require.NoError(t, <-done)

	port := serveBoltIntegrationServer(t, server)
	conn := openBoltBearerTestConn(t, port)
	require.NoError(t, SendRun(t, conn, "MATCH (n:Item) RETURN n.k AS k", nil, nil))
	_, err = AssertSuccess(t, conn)
	require.NoError(t, err)
	first, metadata, failure := pullBatch(t, conn, map[string]any{"n": int64(10)})
	require.Empty(t, failure)
	require.Len(t, first, 10)
	require.Equal(t, true, metadata["has_more"])
	version.Add(1)
	second, _, failure := pullBatch(t, conn, map[string]any{"n": int64(10)})
	require.Empty(t, second)
	require.Equal(t, "Neo.ClientError.Security.Forbidden", failure)
}

func TestFineGrainedBoltBufferedResultRetainsItsPrincipal(t *testing.T) {
	ctx := t.Context()
	server := &Server{}
	server.SetFineGrainedAdmission(func(ctx context.Context, roles []string) (*auth.FineGrainedRequest, error) {
		identity := auth.RequestIdentityFromContext(ctx)
		return &auth.FineGrainedRequest{Fingerprint: identity.ID + ":" + roles[0]}, nil
	})
	session := &Session{server: server, authResult: &BoltAuthResult{
		Authenticated: true, Username: "first", PrincipalID: "sub:first", Roles: []string{"reader"},
	}}
	admitted, _, err := session.fineGrainedPrincipal(ctx, map[string]any{"imp_user": nil})
	require.NoError(t, err)
	scope := newFineGrainedResultScope(admitted, nil)

	current, _, err := session.validateFineGrainedResult(ctx, scope)
	require.NoError(t, err)
	require.Equal(t, admitted.Fingerprint, current.Fingerprint)

	session.authResult = &BoltAuthResult{
		Authenticated: true, Username: "second", PrincipalID: "sub:second", Roles: []string{"reader"},
	}
	current, target, err := session.validateFineGrainedResult(ctx, scope)
	require.ErrorIs(t, err, auth.ErrFineGrainedForbidden)
	require.Nil(t, current)
	require.Nil(t, target)
}

func TestFineGrainedBoltImpersonationUnavailableWithoutResolver(t *testing.T) {
	session := &Session{server: &Server{}}
	for _, metadata := range []map[string]any{nil, {"imp_user": nil}} {
		principal, user, err := session.fineGrainedPrincipal(t.Context(), metadata)
		require.NoError(t, err)
		require.Nil(t, principal)
		require.Nil(t, user)
	}
	principal, user, err := session.fineGrainedPrincipal(t.Context(), map[string]any{"imp_user": "other"})
	require.ErrorIs(t, err, auth.ErrFineGrainedForbidden)
	require.Nil(t, principal)
	require.Nil(t, user)
}
