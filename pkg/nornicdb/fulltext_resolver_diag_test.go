// Author: #1683 lane (Claude), 2026-10-30 (NOR-148). Diagnostic only, scratch
// branch, never on the PR branch. Logs the inputs of the full-text owner's
// incremental decision on the real DB/DatabaseManager path for the default
// database: event-view proof, wrapper chains, settled report and revisions
// around one existing-ID update.
package nornicdb

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

var diagReaderLatest = regexp.MustCompile(`readerLatest:(\d+)`)

func diagChain(engine storage.Engine) string {
	out := ""
	for i := 0; engine != nil && i < 12; i++ {
		out += fmt.Sprintf("%T -> ", engine)
		if forwarder, ok := engine.(storage.EventViewForwarder); ok {
			engine = forwarder.EventViewInner()
			continue
		}
		unwrapper, ok := engine.(storage.EngineUnwrapper)
		if !ok {
			break
		}
		engine = unwrapper.GetInnerEngine()
	}
	return out
}

func TestNOR148DiagResolverDecision(t *testing.T) {
	db, manager := openFulltextResolverDB(t)
	name := db.defaultDatabaseName()
	store, err := manager.GetStorage(name)
	require.NoError(t, err)
	seedFulltextResolverCorpus(t, store)
	settleQueuedRefreshes()
	exec, view := resolverExecutor(t, db, store, name, true)
	engine := storage.Engine(fulltextForwardingView{view})
	t.Logf("query chain: %s", diagChain(engine))
	t.Logf("service chain (db.storage): %s", diagChain(db.storage))
	t.Logf("SameEventView(query, db.storage)=%v SameEventView(store, db.storage)=%v", storage.SameEventView(engine, db.storage), storage.SameEventView(store, db.storage))
	revision := func(label string) (uint64, uint64) {
		snapshot, err := storage.OpenReadSnapshot(storage.WithSnapshotReader(context.Background(), storage.SnapshotAllNodes), store)
		require.NoError(t, err)
		defer snapshot.Close()
		rev, ok := snapshot.(storage.QueryReadSnapshot).CapturedRevision()
		var ts uint64
		if m := diagReaderLatest.FindStringSubmatch(fmt.Sprintf("%+v", rev)); m != nil {
			ts, _ = strconv.ParseUint(m[1], 10, 64)
		}
		t.Logf("%s: settled(query)=%v settled(service)=%v capturedOK=%v readerLatest=%d graph=%d scans=%d", label, storage.EventNotificationsSettled(engine), storage.EventNotificationsSettled(db.storage), ok, ts, rev.GraphMutationVersion(), view.scans.Load())
		return ts, rev.GraphMutationVersion()
	}
	ctx := context.Background()
	_, err = exec.Execute(ctx, "CREATE FULLTEXT INDEX source_text FOR (n:Source) ON EACH [n.text]", nil)
	require.NoError(t, err)
	query := "CALL db.index.fulltext.queryNodes('source_text', $q) YIELD node RETURN node.id"
	waitEventsDelivered(t, store)
	_, err = exec.Execute(ctx, query, map[string]any{"q": "hotel"})
	require.NoError(t, err)
	beforeTs, beforeGraph := revision("after-population")
	node, err := store.GetNode("n004")
	require.NoError(t, err)
	node.Properties["text"] = "newunique hotel"
	require.NoError(t, store.UpdateNode(node))
	waitEventsDelivered(t, store)
	afterTs, afterGraph := revision("after-update")
	settleQueuedRefreshes()
	settledTs, settledGraph := revision("after-db-refresh-debounce")
	_, err = exec.Execute(ctx, query, map[string]any{"q": "newunique"})
	require.NoError(t, err)
	revision("after-second-query")
	t.Logf("DECISION update: readerCommits=%d graphDelta=%d; after DB debounce: readerCommits=%d graphDelta=%d", afterTs-beforeTs, afterGraph-beforeGraph, settledTs-beforeTs, settledGraph-beforeGraph)
}
