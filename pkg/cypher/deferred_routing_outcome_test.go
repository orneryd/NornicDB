package cypher

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

// Cancel at a selected execution frame's deferred log, after the query has
// returned internally. A routed statement can log once for each dispatch frame.
type cancelDeferredFrameHandler struct {
	cancel context.CancelFunc
	frame  int
	seen   int
}

func (h *cancelDeferredFrameHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *cancelDeferredFrameHandler) Handle(_ context.Context, r slog.Record) error {
	if r.Message == "slow query" {
		h.seen++
		if h.seen == h.frame {
			h.cancel()
		}
	}
	return nil
}
func (h *cancelDeferredFrameHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *cancelDeferredFrameHandler) WithGroup(string) slog.Handler      { return h }

// A successful stored write remains successful through every outer dispatcher.
// A read, rollback or still-open explicit write has no committed outcome to
// preserve. The observer uses an uncancelled public Execute, not the write's
// result or transaction view, to check exactly what a retry would duplicate.
func TestExecuteRoutedDeferredOutcome(t *testing.T) {
	const create = "CREATE (:DeferredRouted)"
	const count = "MATCH (n:DeferredRouted) RETURN count(n)"
	cases := []struct {
		name, query, observe, fixture string
		prepare                       []string
		params                        map[string]interface{}
		frame, persisted              int
		committed, failed             bool
	}{
		{name: "direct", query: create, frame: 1, persisted: 1, committed: true},
		{name: "USE same", query: "USE nornic " + create, frame: 2, persisted: 1, committed: true},
		{name: "USE other", query: "USE other " + create, observe: "USE other " + count, frame: 1, persisted: 1, committed: true},
		{name: "shell USE", query: ":USE nornic\n" + create, frame: 2, persisted: 1, committed: true},
		{name: "PROFILE", query: "PROFILE " + create, frame: 2, persisted: 1, committed: true},
		{name: "PROFILE USE", query: "PROFILE USE nornic " + create, frame: 3, persisted: 1, committed: true},
		{name: "procedure re-entry", query: "CALL apoc.cypher.run('CREATE (:DeferredRouted) RETURN 1 AS one', {}) YIELD value RETURN value", frame: 1, persisted: 1, committed: true},
		{name: "procedure parameters", query: "CALL apoc.cypher.run($query, {}) YIELD value RETURN value", params: map[string]interface{}{"query": "CREATE (:DeferredRouted) RETURN 1 AS one"}, frame: 1, persisted: 1, committed: true},
		{name: "legacy write", fixture: "legacy", query: create, frame: 1, persisted: 1, committed: true},
		{name: "CALL USE", query: "CALL { USE nornic CREATE (:DeferredRouted) } RETURN 1 AS one", frame: 1, persisted: 1, committed: true},
		{name: "CALL in transactions", query: "UNWIND [1,2,3] AS x CALL (x) { CREATE (:DeferredRouted {x: x}) } IN TRANSACTIONS OF 1 ROWS", frame: 1, persisted: 3, committed: true},
		{name: "USE CALL in transactions", query: "USE nornic UNWIND [1,2,3] AS x CALL (x) { CREATE (:DeferredRouted {x: x}) } IN TRANSACTIONS OF 1 ROWS", frame: 2, persisted: 3, committed: true},
		{name: "transaction script", query: "BEGIN " + create + " COMMIT", frame: 1, persisted: 1, committed: true},
		{name: "transaction CASE script", query: "BEGIN CALL { CREATE (n:DeferredRouted) RETURN n } CASE WHEN false THEN ROLLBACK ELSE RETURN n COMMIT", frame: 1, persisted: 1, committed: true},
		{name: "explicit COMMIT", prepare: []string{"BEGIN", create}, query: "COMMIT", frame: 1, persisted: 1, committed: true},
		{name: "Fabric USE", fixture: "fabric", query: "USE pcomp.other " + create, observe: "USE pcomp.other " + count, frame: 1, persisted: 1, committed: true},
		{name: "Fabric CALL", fixture: "fabric", query: "CALL { USE pcomp.other CREATE (:DeferredRouted) RETURN 1 AS one } RETURN one", observe: "USE pcomp.other " + count, frame: 1, persisted: 1, committed: true},
		{name: "Fabric dynamic USE", fixture: "fabric", query: "USE graph.byName('pcomp.other') " + create, observe: "USE pcomp.other " + count, frame: 1, persisted: 1, committed: true},
		{name: "Fabric explicit COMMIT", fixture: "fabric", prepare: []string{"BEGIN", "USE pcomp.other " + create}, query: "COMMIT", observe: "USE pcomp.other " + count, frame: 1, persisted: 1, committed: true},
		{name: "administration forwarding", query: "GRANT EXECUTE PROCEDURE db.deferred ON DBMS TO viewer", observe: "SHOW PRIVILEGES", frame: 1, persisted: 1, committed: true},
		{name: "USE administration rejection", query: "USE system GRANT EXECUTE PROCEDURE db.deferred ON DBMS TO viewer", observe: "SHOW PRIVILEGES", frame: 1, failed: true},
		{name: "PROFILE administration", query: "PROFILE GRANT EXECUTE PROCEDURE db.deferred ON DBMS TO viewer", observe: "SHOW PRIVILEGES", frame: 1, persisted: 1, committed: true},
		{name: "catalog forwarding", fixture: "fabric", query: "CREATE DATABASE deferred_created", observe: "SHOW DATABASES YIELD name WHERE name = 'deferred_created' RETURN name", frame: 1, persisted: 1, committed: true},
		{name: "schema", query: "CREATE INDEX deferred_routed_idx FOR (n:DeferredRouted) ON (n.x)", observe: "SHOW INDEXES YIELD name WHERE name = 'deferred_routed_idx' RETURN name", frame: 1, persisted: 1, committed: true},
		{name: "USE schema", query: "USE nornic CREATE INDEX deferred_routed_idx FOR (n:DeferredRouted) ON (n.x)", observe: "SHOW INDEXES YIELD name WHERE name = 'deferred_routed_idx' RETURN name", frame: 2, persisted: 1, committed: true},
		{name: "USE read", query: "USE nornic RETURN 1 AS one", frame: 2},
		{name: "PROFILE read", query: "PROFILE RETURN 1 AS one", frame: 2},
		{name: "Fabric read", fixture: "fabric", query: "USE pcomp.other RETURN 1 AS one", observe: "USE pcomp.other " + count, frame: 1},
		{name: "CALL in transactions read", query: "UNWIND [1,2] AS x CALL (x) { RETURN x AS y } IN TRANSACTIONS OF 1 ROWS RETURN y", frame: 1},
		{name: "procedure parameters read", query: "CALL apoc.cypher.run($query, {}) YIELD value RETURN value", params: map[string]interface{}{"query": "RETURN 1 AS one"}, frame: 1},
		{name: "procedure literal read", query: "CALL apoc.cypher.run($query, {}) YIELD value RETURN value", params: map[string]interface{}{"query": "RETURN 'CREATE' AS word"}, frame: 1},
		{name: "legacy read", fixture: "legacy", query: "RETURN 1 AS one", frame: 1},
		{name: "legacy literal read", fixture: "legacy", query: "RETURN 'CREATE' AS word", frame: 1},
		{name: "USE explicit uncommitted", prepare: []string{"BEGIN"}, query: "USE nornic " + create, frame: 2},
		{name: "transaction rollback", query: "BEGIN " + create + " ROLLBACK", frame: 1},
		{name: "USE failed child", query: "USE nornic RETURN missingDeferredVariable", frame: 2, failed: true},
		{name: "CALL partial commit failure", prepare: []string{"CREATE CONSTRAINT deferred_routed_unique FOR (n:DeferredRouted) REQUIRE n.x IS UNIQUE"}, query: "UNWIND [1,1] AS x CALL (x) { CREATE (:DeferredRouted {x: x}) } IN TRANSACTIONS OF 1 ROWS", frame: 1, persisted: 1, failed: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			session, stores := nestedUseRouterExecutor(t, "nornic")
			observer := NewStorageExecutor(stores["nornic"])
			observer.SetDatabaseManager(session.dbManager)
			if tc.fixture == "legacy" {
				manager := session.dbManager
				session = NewStorageExecutor(nonTransactionalEngine{Engine: stores["nornic"]})
				session.SetDatabaseManager(manager)
			}
			if tc.fixture == "fabric" {
				session = newCompositeFixture(t)
				observer = NewStorageExecutor(session.storage)
				observer.SetDatabaseManager(session.dbManager)
			}
			for _, query := range tc.prepare {
				_, err := session.Execute(t.Context(), query, nil)
				require.NoError(t, err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			h := &cancelDeferredFrameHandler{cancel: cancel, frame: tc.frame}
			session.SetLogger(slog.New(h))
			forceSlowQueryLogging(t, session)
			result, err := session.Execute(ctx, tc.query, tc.params)
			t.Logf("frames=%d canceled=%t nilResult=%t err=%v", h.seen, ctx.Err() != nil, result == nil, err)
			require.Equal(t, tc.frame, h.seen, "attack must reach the outer log")
			require.ErrorIs(t, ctx.Err(), context.Canceled)
			if session.txContext != nil && session.txContext.active {
				// Disable the handler before cleanup; its cancellation belongs to
				// the completed request, not the following ROLLBACK.
				session.SetSlowQueryThreshold(0)
				_, rollbackErr := session.Execute(t.Context(), "ROLLBACK", nil)
				require.NoError(t, rollbackErr)
			}
			query := tc.observe
			if query == "" {
				query = count
			}
			stored, observeErr := observer.Execute(t.Context(), query, nil)
			require.NoError(t, observeErr)
			t.Logf("stored=%v", stored.Rows)
			if tc.observe != "" && (tc.observe == "SHOW PRIVILEGES" || tc.name == "catalog forwarding" || tc.name == "schema" || tc.name == "USE schema") {
				require.Len(t, stored.Rows, tc.persisted)
			} else {
				require.Equal(t, [][]interface{}{{int64(tc.persisted)}}, stored.Rows)
			}
			if tc.committed {
				require.NoError(t, err, "a stored write must not invite a retry")
				require.NotNil(t, result)
			} else if tc.failed {
				require.Error(t, err)
				require.NotErrorIs(t, err, context.Canceled, "preserve the execution failure")
			} else {
				require.ErrorIs(t, err, context.Canceled)
				require.Nil(t, result)
			}
		})
	}
}
