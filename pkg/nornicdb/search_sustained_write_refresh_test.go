package nornicdb

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/orneryd/nornicdb/pkg/lifecycle"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/stretchr/testify/require"
)

// sustainedServedSimilarity is the vector similarity the ready search service's
// index holds for vector, read through the service's own search: "zulu" appears
// in no node, so only the vector channel can answer, and the reported similarity
// is the one the index holds, not one recomputed from the stored node. The index
// holds one property vector in this fixture, so the vector channel's best
// similarity is that node's, and 0 when the index does not serve it for vector.
func sustainedServedSimilarity(t *testing.T, svc *search.Service, vector []float32) float64 {
	t.Helper()
	opts := search.DefaultSearchOptions()
	opts.Limit, opts.MinRRFScore = 10, 0
	response, err := svc.Search(context.Background(), "zulu", vector, opts)
	require.NoError(t, err)
	served := 0.0
	for _, result := range response.Results {
		if result.VectorRank > 0 && result.Similarity > served {
			served = result.Similarity
		}
	}
	return served
}

// armedFlushWindow is the pending flush timer the search service has armed,
// sampled under its own lock. It is absent while a flush runs.
func armedFlushWindow(entry *dbSearchService) *lifecycle.OwnedTimer {
	entry.pendingFlushMu.Lock()
	defer entry.pendingFlushMu.Unlock()
	return entry.pendingFlushTimer
}

// axisVector is a 3-dimension unit vector: two of them differ by a right angle,
// so the index serves one at similarity 1 and the other at 0.
func axisVector(axis int) []float32 {
	vector := make([]float32, 3)
	vector[axis] = 1
	return vector
}

// The flush window bounds how long a committed write may stay out of search:
// later writes coalesce into the window that is already armed instead of
// restarting it. A vector update committed through storage (the route of
// replication apply, imports and APOC writes, which reach search only through
// its storage events) waits in the deferred refresh; an uninterrupted burst of
// ordinary committed writes then runs through the public Cypher path. The
// queued update must be served while those writes continue.
//
// Virtual time places each public commit one quarter-window apart and waits
// for background work to settle. Machine speed cannot create a quiet interval
// or turn an unexecuted refresh into a passing result.
func TestSustainedWritesDoNotPostponeAQueuedVectorRefresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		db, entry := openIncarnationDB(t)
		// The fixture parks queued refreshes for manual application; this test needs
		// the documented production window instead of an hour.
		entry.pendingFlushDelay = searchMutationDebounceDelay
		window := searchMutationDebounceDelay
		svc := entry.svc
		earlier, later := axisVector(0), axisVector(1)

		incarnationRun(t, db, incarnationCreateDocs)
		incarnationRun(t, db, "CREATE (:Doc {id: 'a', emb: [1.0, 0.0, 0.0]}), (:Writer {seq: 0})")
		incarnationQueued(t, entry)
		applyQueued(t, entry)
		require.Greater(t, sustainedServedSimilarity(t, svc, earlier), 0.99,
			"the index serves a's first vector")
		require.Less(t, sustainedServedSimilarity(t, svc, later), 0.99,
			"the index serves a's first vector, not the one this test sets next")
		docs, err := db.storage.GetNodesByLabel(t.Context(), "Doc")
		require.NoError(t, err)
		require.Len(t, docs, 1)

		// The committed update whose search refresh must not be postponed.
		incarnationStore(t, db, entry, docs[0].ID, []float64{0, 1, 0})
		require.Less(t, sustainedServedSimilarity(t, svc, later), 0.99,
			"a committed update waits for the deferred refresh")
		armed := armedFlushWindow(entry)
		require.NotNil(t, armed, "the committed update armed the flush window")
		fresh := func() bool { return sustainedServedSimilarity(t, svc, later) > 0.99 }

		// Public commits arrive inside every armed window under virtual time.
		burstStart := time.Now()
		lastCommit := burstStart
		var commits int
		var maxGap time.Duration
		var servedAfter time.Duration
		var replaced int
		sample := armed
		for iteration := 0; iteration < 12; iteration++ {
			time.Sleep(window / 4)
			synctest.Wait()
			_, err := db.ExecuteCypher(t.Context(), "MATCH (w:Writer) SET w.seq = $seq", map[string]interface{}{"seq": commits})
			require.NoError(t, err)
			now := time.Now()
			if gap := now.Sub(lastCommit); gap > maxGap {
				maxGap = gap
			}
			lastCommit = now
			commits++
			if current := armedFlushWindow(entry); current != sample {
				replaced++
				sample = current
			}
			if servedAfter == 0 && fresh() {
				servedAfter = time.Since(burstStart)
			}
		}
		elapsed := time.Since(burstStart)

		if servedAfter > 0 {
			// Commits arrived after the queued refresh landed, and it stayed served:
			// the update was applied with the workload still running.
			_, err := db.ExecuteCypher(t.Context(), "MATCH (w:Writer) SET w.seq = $seq", map[string]interface{}{"seq": commits})
			require.NoError(t, err)
			require.Greater(t, sustainedServedSimilarity(t, svc, later), 0.99,
				"a commit after the refresh landed reverted it")
		}
		quiet := time.Duration(0)
		if servedAfter == 0 {
			quietStart := time.Now()
			require.Eventually(t, fresh, 10*time.Second, time.Millisecond,
				"the queued vector update was never served, so it was lost rather than postponed")
			quiet = time.Since(quietStart)
		}
		t.Logf("%d commits over %s, largest commit gap %s, armed flush window replaced %d times; queued refresh served after %s of them, %s after they stopped",
			commits, elapsed, maxGap, replaced, servedAfter, quiet)

		require.Less(t, maxGap, window,
			"a commit gap reached the %s flush window, so the burst did not keep writes inside every armed window and the observation is inconclusive",
			window)
		require.NotEqual(t, time.Duration(0), servedAfter,
			"the vector update committed before %d further commits over %s was not served while they continued, although it was served %s after they stopped: each commit restarted the %s flush window (%d replacements), so the queued refresh was postponed for as long as the writes kept arriving",
			commits, elapsed, quiet, window, replaced)
	})
}
