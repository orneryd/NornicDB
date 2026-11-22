package bolt

import (
	"testing"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestNOR11BoltImpUserIsIgnoredForAuthorizationAndAttribution characterizes
// what the current Bolt transport does with Neo4j's imp_user (#936): nothing.
//
// The extra dictionary's keys are read explicitly (bookmarks, tx_timeout,
// tx_metadata, db, database, mode, locale, traceparent); imp_user is not among
// them, so it is neither refused nor honored. The statement keeps running as
// the authenticated principal: SHOW CURRENT USER reports that principal, the
// authenticated principal's permissions still govern a write, and no
// impersonated identity is recorded. This is the missing IMPERSONATE
// capability, not an accepted impersonation without checks.
func TestNOR11BoltImpUserIsIgnoredForAuthorizationAndAttribution(t *testing.T) {
	base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "test")
	conn := &mockConn{}
	session := newTestSession(conn, &transactionalBoltQueryExecutorAdapter{
		boltQueryExecutorAdapter: boltQueryExecutorAdapter{executor: cypher.NewStorageExecutor(store)},
	})
	session.authenticated = true
	session.authResult = &BoltAuthResult{
		Authenticated: true,
		Username:      "service",
		PrincipalID:   "service",
		Roles:         []string{"viewer"},
		Permissions:   []string{"read"},
	}

	// BEGIN carries imp_user: the transaction is accepted and the session's
	// principal is unchanged.
	require.NoError(t, session.dispatchInner(MsgBegin, encodePackStreamMap(map[string]any{"imp_user": "alice"}), "BEGIN", nil))
	require.True(t, session.inTransaction)
	require.Equal(t, "service", session.authResult.Username)

	require.NoError(t, session.handleRun(buildRunMessageData("SHOW CURRENT USER YIELD user, roles", nil, nil)))
	require.NotNil(t, session.lastResult)
	require.Equal(t, [][]any{{"service", []string{"viewer"}}}, session.lastResult.Rows,
		"the executing identity reported is the authenticated principal, not imp_user")
	require.NoError(t, session.handleCommit(nil))

	// An auto-commit RUN with imp_user does not gain the named user's
	// privileges: a viewer's write is still refused, and nothing is created.
	require.NoError(t, session.handleRun(buildRunMessageData("CREATE (:ImpUserProbe)", nil, map[string]any{"imp_user": "alice"})))
	require.Contains(t, string(conn.writeData), "Neo.ClientError.Security.Forbidden")
	require.Contains(t, string(conn.writeData), "Write operations require write permission")

	require.NoError(t, session.handleRun(buildRunMessageData("MATCH (n:ImpUserProbe) RETURN count(*) AS c", nil, map[string]any{"imp_user": "alice"})))
	require.NotNil(t, session.lastResult)
	require.Equal(t, [][]any{{int64(0)}}, session.lastResult.Rows)
}

// TestNOR1112CollectionViewerVisibilityViaBoltRun repeats the reported
// #935/#936 two-document example through the public Bolt RUN path (the same
// StorageExecutor the transport hands statements to). It matches the direct
// executor characterization in pkg/cypher: the filtered read reflects only
// the caller's predicate and the unfiltered read returns both documents,
// because neither path has native collection visibility to apply.
func TestNOR1112CollectionViewerVisibilityViaBoltRun(t *testing.T) {
	for _, parser := range []struct {
		name  string
		scope func() func()
	}{
		{name: "nornic", scope: config.WithNornicParser},
		{name: "antlr", scope: config.WithANTLRParser},
	} {
		t.Run(parser.name, func(t *testing.T) {
			cleanup := parser.scope()
			t.Cleanup(cleanup)

			store := storage.NewNamespacedEngine(runtimeFixtureOwner.Own(t, runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "test")
			session := newTestSession(&mockConn{}, &boltQueryExecutorAdapter{executor: cypher.NewStorageExecutor(store)})

			require.NoError(t, session.handleRun(buildRunMessageData(`CREATE (a:PDDocument {id:'allowed'}),(b:PDDocument {id:'secret'}),
(c:PDCollection {id:'A'}),(d:PDCollection {id:'B'}),
(a)-[:MEMBER_OF]->(c),(b)-[:MEMBER_OF]->(d)`, nil, nil)))

			require.NoError(t, session.handleRun(buildRunMessageData(`MATCH (d:PDDocument) WHERE EXISTS { MATCH (d)-[:MEMBER_OF]->(project:PDCollection) WHERE project.id IN $collection_ids } RETURN d.id AS id ORDER BY id`, map[string]any{"collection_ids": []any{"A"}}, nil)))
			require.NotNil(t, session.lastResult)
			require.Equal(t, [][]any{{"allowed"}}, session.lastResult.Rows)

			require.NoError(t, session.handleRun(buildRunMessageData(`MATCH (d:PDDocument) RETURN d.id AS id ORDER BY id`, map[string]any{}, nil)))
			require.NotNil(t, session.lastResult)
			require.Equal(t, [][]any{{"allowed"}, {"secret"}}, session.lastResult.Rows)
		})
	}
}
