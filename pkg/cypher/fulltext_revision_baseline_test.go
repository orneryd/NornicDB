// Author: #1683 lane (Claude), 2026-10-31 (NOR-148).
package cypher

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const fulltextBaselineQuery = "CALL db.index.fulltext.queryNodes('source_text', $query) YIELD node, score RETURN node.id, score"

// fulltextRevisionFixture builds the maintained fixture on the physical Badger
// owner (in-memory mode), the engine that publishes the revision baseline and
// the notification journal these tests read. It adds no population of its own:
// the fixture below owns the corpus.
func fulltextRevisionFixture(tb testing.TB, count int) (*StorageExecutor, *fulltextCountingStore) {
	tb.Helper()
	base, err := fixtureowner.TrackResult[*storage.BadgerEngine](tb, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{InMemory: true}))
	require.NoError(tb, err)
	return fulltextMaintainedFixtureOn(tb, base, count)
}

func fulltextBaselineRun(t *testing.T, exec *StorageExecutor, text string) [][]any {
	t.Helper()
	result, err := exec.Execute(context.Background(), fulltextBaselineQuery, map[string]any{"query": text})
	require.NoError(t, err)
	return result.Rows
}

func fulltextWaitSettled(t *testing.T, engine storage.Engine) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !storage.EventNotificationsSettled(engine) {
		require.True(t, time.Now().Before(deadline), "storage notifications were not delivered")
		time.Sleep(time.Millisecond)
	}
}

// A population that begins with a commit's events undelivered includes that commit
// and is reused at its revision, but cannot refresh by ID: its event count may not
// cover the commit. The next write repopulates; the one after it refreshes.
func TestFulltextPopulationDuringDeliveryIsReusedNotRefreshed(t *testing.T) {
	exec, store := fulltextRevisionFixture(t, 80)
	ctx := context.Background()
	publisher := storage.UnwrapEngine(store).(*storage.BadgerEngine)
	entered, release := make(chan struct{}), make(chan struct{})
	var enter, unblock sync.Once
	t.Cleanup(func() { unblock.Do(func() { close(release) }) })
	publisher.OnNodeUpdated(func(node *storage.Node) {
		if strings.HasSuffix(string(node.ID), ":n000004") {
			enter.Do(func() { close(entered) })
			<-release
		}
	})
	node, err := store.GetNode(ctx, "n000004")
	require.NoError(t, err)
	node.Properties["text"] = "inflightwrite"
	finished := make(chan error, 1)
	go func() { finished <- store.UpdateNode(ctx, storage.NodeUpdateRequest{After: node}) }()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the commit's notification did not begin")
	}
	require.False(t, storage.EventNotificationsSettled(store))
	require.True(t, storage.ReadPublicationSettled(store), "listener delivery is pending, but this committed view is fully published")
	before := store.scans.Load()
	rows := fulltextBaselineRun(t, exec, "inflightwrite")
	require.Len(t, rows, 1, "the read serves what the release reader sees")
	require.Equal(t, before+1, store.scans.Load())
	require.Len(t, fulltextBaselineRun(t, exec, "inflightwrite"), 1)
	require.Equal(t, before+1, store.scans.Load(), "an equal revision must reuse the population")
	unblock.Do(func() { close(release) })
	require.NoError(t, <-finished)
	fulltextWaitSettled(t, store)
	update := func(text string) {
		t.Helper()
		current, err := store.GetNode(ctx, "n000004")
		require.NoError(t, err)
		current.Properties["text"] = text
		require.NoError(t, store.UpdateNode(ctx, storage.NodeUpdateRequest{After: current}))
		fulltextWaitSettled(t, store)
	}
	update("firstafterdelivery")
	require.Equal(t, fulltextReleaseOracle(t, store.NamespacedEngine, "firstafterdelivery", []string{"Source"}, []string{"text", "title"}), fulltextBaselineRun(t, exec, "firstafterdelivery"))
	require.Equal(t, before+2, store.scans.Load(), "an uncovered baseline must not refresh by ID")
	update("secondafterdelivery")
	require.Equal(t, fulltextReleaseOracle(t, store.NamespacedEngine, "secondafterdelivery", []string{"Source"}, []string{"text", "title"}), fulltextBaselineRun(t, exec, "secondafterdelivery"))
	require.Equal(t, before+2, store.scans.Load(), "a covered baseline must refresh by ID")
}

// A write that lands during a certified population is not in its rows, or is; the
// next read must equal a fresh rebuild either way, without another full read.
func TestFulltextWriteDuringPopulationConvergesAtNextRead(t *testing.T) {
	for _, deleted := range []bool{false, true} {
		t.Run(fmt.Sprintf("deleted=%t", deleted), func(t *testing.T) {
			_, counted := fulltextRevisionFixture(t, 80)
			ctx := context.Background()
			probe := &fulltextPopulationProbe{NamespacedEngine: counted.NamespacedEngine}
			exec := NewStorageExecutor(probe, QueryCachePolicy{MaxEntries: 0})
			var once sync.Once
			probe.crossing = func(context.Context) error {
				var err error
				once.Do(func() {
					if deleted {
						err = probe.DeleteNode(ctx, "n000000")
						return
					}
					var node *storage.Node
					if node, err = probe.GetNode(ctx, "n000004"); err == nil {
						node.Properties["text"] = "duringpopulation"
						err = probe.UpdateNode(ctx, storage.NodeUpdateRequest{After: node})
					}
				})
				return err
			}
			fulltextBaselineRun(t, exec, "hotel")
			fulltextWaitSettled(t, probe)
			for _, text := range []string{"hotel", "duringpopulation"} {
				require.Equal(t, fulltextReleaseOracle(t, probe.NamespacedEngine, text, []string{"Source"}, []string{"text", "title"}), fulltextBaselineRun(t, exec, text))
			}
		})
	}
}

// Once writers stop, whatever the interleaving was, a read equals a fresh rebuild.
func TestFulltextConcurrentWritersEndEqualToRebuild(t *testing.T) {
	exec, store := fulltextRevisionFixture(t, 160)
	ctx := context.Background()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var writes atomic.Int64
	writeErr := make(chan error, 2)
	for writer := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				id := storage.NodeID(fmt.Sprintf("n%06d", (writer*4+i*4)%160))
				node, err := store.GetNode(ctx, id)
				if err != nil {
					writeErr <- err
					return
				}
				node.Properties["text"] = fmt.Sprintf("writer%d value%d hotel", writer, i)
				if err := store.UpdateNode(ctx, storage.NodeUpdateRequest{After: node}); err != nil && !strings.Contains(err.Error(), "conflict") {
					writeErr <- err
					return
				}
				writes.Add(1)
			}
		}()
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		fulltextBaselineRun(t, exec, "hotel")
	}
	close(stop)
	wg.Wait()
	select {
	case err := <-writeErr:
		t.Fatal(err)
	default:
	}
	require.Positive(t, writes.Load())
	fulltextWaitSettled(t, store)
	require.Equal(t, fulltextReleaseOracle(t, store.NamespacedEngine, "hotel", []string{"Source"}, []string{"text", "title"}), fulltextBaselineRun(t, exec, "hotel"))
}

// A restore moves no event, only the revision: the next read rebuilds.
func TestFulltextRestoreIsNeverServedFromTheOldCorpus(t *testing.T) {
	exec, store := fulltextRevisionFixture(t, 40)
	ctx := context.Background()
	publisher := storage.UnwrapEngine(store).(*storage.BadgerEngine)
	backup := t.TempDir() + "/backup"
	require.NoError(t, publisher.Backup(backup))
	node, err := store.GetNode(ctx, "n000004")
	require.NoError(t, err)
	node.Properties["text"] = "afterbackup"
	require.NoError(t, store.UpdateNode(ctx, storage.NodeUpdateRequest{After: node}))
	fulltextWaitSettled(t, store)
	require.Len(t, fulltextBaselineRun(t, exec, "afterbackup"), 1)
	require.NoError(t, publisher.Restore(backup))
	fulltextWaitSettled(t, store)
	require.Empty(t, fulltextBaselineRun(t, exec, "afterbackup"), "a restored store was served from the replaced corpus")
}

// A retained corpus must not answer where a fresh read fails. On release, a prune
// after this sequence makes the relationship read fail; the read must report the
// same error.
func TestFulltextPruneErrorIsNotHiddenByTheCorpus(t *testing.T) {
	exec, store := fulltextRevisionFixture(t, 8)
	base := storage.UnwrapEngine(store).(*storage.BadgerEngine)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE FULLTEXT INDEX link_text FOR ()-[r:LINK]-() ON EACH [r.text]", nil)
	require.NoError(t, err)
	_, err = store.CreateNode(ctx, &storage.Node{ID: "start", Labels: []string{"Other"}})
	require.NoError(t, err)
	require.NoError(t, store.DeleteNode(ctx, "start"))
	_, err = store.CreateNode(ctx, &storage.Node{ID: "start", Labels: []string{"Other"}})
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(ctx, &storage.Edge{ID: "link", StartNode: "start", EndNode: "n000000", Type: "LINK", Properties: map[string]any{"text": "hotel"}}))
	fulltextWaitSettled(t, store)
	query := "CALL db.index.fulltext.queryRelationships('link_text', 'hotel') YIELD relationship, score RETURN score"
	first, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Len(t, first.Rows, 1)
	require.NoError(t, base.RebuildMVCCHeads(ctx))
	_, err = base.PruneMVCCVersions(ctx, storage.MVCCPruneOptions{})
	require.NoError(t, err)
	fresh := store.NamespacedEngine.StreamEdges(ctx, func(*storage.Edge) error { return nil })
	_, served := exec.Execute(ctx, query, nil)
	if fresh == nil {
		require.NoError(t, served)
		return
	}
	require.Error(t, served, "the corpus answered where a fresh read fails: %v", fresh)
	require.Contains(t, served.Error(), fresh.Error())
}

// A database named with ':' counts writes under the part before it: a retained
// corpus must still see them.
func TestFulltextColonNamedDatabaseSeesWrites(t *testing.T) {
	base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{InMemory: true}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	store := storage.NewNamespacedEngine(base, "a:b")
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
	ctx := context.Background()
	_, err = exec.Execute(ctx, "CREATE FULLTEXT INDEX source_text FOR (n:Source) ON EACH [n.text]", nil)
	require.NoError(t, err)
	create := func(id, text string) {
		t.Helper()
		_, err := store.CreateNode(ctx, &storage.Node{ID: storage.NodeID(id), Labels: []string{"Source"}, Properties: map[string]any{"id": id, "text": text}})
		require.NoError(t, err)
		fulltextWaitSettled(t, store)
	}
	create("one", "hotel")
	require.Len(t, fulltextBaselineRun(t, exec, "hotel"), 1)
	create("two", "hotel")
	require.Len(t, fulltextBaselineRun(t, exec, "hotel"), 2)
}
