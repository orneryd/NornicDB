package server

import (
	"fmt"
	"net/http"
	"os"
	"runtime"
	"runtime/metrics"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
)

// TestMemoryLimitAcceptanceLargeStatement is the transaction memory limits'
// acceptance test (#1343): one large statement with every shape whose
// working set used to escape the limit, an independent CALL, a correlated
// CALL with UNION, lists built per row by comprehensions over range(),
// collect() of maps, reduce, and a large OPTIONAL MATCH output. Run over
// HTTP with no limit, it completes (its peak heap is the size to beat);
// under a limit far below that peak, it fails with TransactionMemoryLimit
// with the heap near the limit, and the server answers the next statement.
//
// It is heavy, so it runs only with NORNICDB_MEMORY_ACCEPTANCE=1; the
// NORNICDB_MEMORY_ACCEPTANCE_{DOCS,THINGS,PER_DOC,WIDTH,LIMIT_MB} variables
// scale it.
func TestMemoryLimitAcceptanceLargeStatement(t *testing.T) {
	if os.Getenv("NORNICDB_MEMORY_ACCEPTANCE") != "1" {
		t.Skip("set NORNICDB_MEMORY_ACCEPTANCE=1 to run the large memory-limit acceptance statement")
	}
	setting := func(name string, fallback int) int {
		if value, err := strconv.Atoi(os.Getenv("NORNICDB_MEMORY_ACCEPTANCE_" + name)); err == nil && value > 0 {
			return value
		}
		return fallback
	}
	docs, things, perDoc, width := setting("DOCS", 2000), setting("THINGS", 400), setting("PER_DOC", 6), setting("WIDTH", 8)
	limitBytes := int64(setting("LIMIT_MB", 64)) << 20

	server, client, token := startMemoryHTTPServer(t)
	client.client = &http.Client{Timeout: 60 * time.Minute}
	status, body, _ := client.post("/db/nornic/tx/commit",
		fmt.Sprintf("UNWIND range(0, %d) AS i CREATE (:AccThing {id: i, text: 'thing text number ' + toString(i)})", things-1))
	require.Equal(t, http.StatusOK, status, "%+v", body)
	require.Empty(t, body.Errors)
	status, body, _ = client.post("/db/nornic/tx/commit", "CREATE INDEX acc_thing_id IF NOT EXISTS FOR (t:AccThing) ON (t.id)")
	require.Equal(t, http.StatusOK, status, "%+v", body)
	status, body, _ = client.post("/db/nornic/tx/commit",
		fmt.Sprintf(`UNWIND range(0, %d) AS d
CREATE (doc:AccDoc {id: d})
WITH doc, d
UNWIND range(0, %d) AS k
MATCH (t:AccThing {id: (d * 7919 + k * 104729) %% %d})
MERGE (doc)-[:HAS]->(t)`, docs-1, perDoc-1, things))
	require.Equal(t, http.StatusOK, status, "%+v", body)
	require.Empty(t, body.Errors)

	statement := fmt.Sprintf(`CALL () { MATCH (d:AccDoc) RETURN collect(d) AS docs }
CALL (docs) {
  UNWIND docs AS doc
  CALL (doc) {
    MATCH (doc)-[:HAS]->(t:AccThing) RETURN t AS thing, 'direct' AS via
    UNION
    MATCH (doc)-[:HAS]->(:AccThing)<-[:HAS]-(:AccDoc)-[:HAS]->(t:AccThing) RETURN t AS thing, 'shared' AS via
  }
  WITH doc, thing, via, [i IN range(1, %d) | thing.text + ' word ' + toString(i)] AS words
  RETURN collect({doc: doc.id, thing: thing.id, via: via, words: words}) AS incidence
}
WITH incidence, reduce(n = 0, e IN incidence | n + size(e.words)) AS words
UNWIND incidence AS e
OPTIONAL MATCH (t:AccThing {id: e.thing})
WITH e, words, t.text AS text
RETURN count(*) AS rows, count(text) AS matched, max(words) AS words`, width)

	run := func() (time.Duration, uint64, int, TransactionResponse) {
		var elapsed time.Duration
		var status int
		var response TransactionResponse
		peak := acceptancePeakHeap(func() {
			start := time.Now()
			status, response, _ = client.post("/db/nornic/tx/commit", statement)
			elapsed = time.Since(start)
		})
		return elapsed, peak, status, response
	}

	// Limited first: VmHWM is the process's high-water mark, so the limited
	// run's reading comes before the unlimited run raises it.
	response := makeRequest(t, server, http.MethodPut, "/admin/databases/nornic/config",
		map[string]any{"overrides": map[string]string{"db.memory.transaction.max": strconv.FormatInt(limitBytes, 10)}}, token)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	limitedTime, limitedPeak, status, body := run()
	limitedHWM := processHighWaterMark()
	requireHTTPMemoryError(t, status, body, "Neo.TransientError.General.TransactionMemoryLimit", "db.memory.transaction.max")
	status, body, _ = client.post("/db/nornic/tx/commit", "MATCH (d:AccDoc) RETURN count(d) AS n")
	require.Equal(t, http.StatusOK, status)
	require.Empty(t, body.Errors, "the server answers the next statement")
	global, database := cypher.TransactionMemoryPoolUsage("nornic")
	require.Zero(t, global)
	require.Zero(t, database)

	response = makeRequest(t, server, http.MethodPut, "/admin/databases/nornic/config",
		map[string]any{"overrides": map[string]string{"db.memory.transaction.max": "0"}}, token)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	unlimitedTime, unlimitedPeak, status, body := run()
	unlimitedHWM := processHighWaterMark()
	require.Equal(t, http.StatusOK, status)
	require.Empty(t, body.Errors, "%+v", body.Errors)
	require.Len(t, body.Results, 1)

	t.Logf("ACCEPTANCE docs=%d things=%d per_doc=%d width=%d limit=%d MiB", docs, things, perDoc, width, limitBytes>>20)
	t.Logf("ACCEPTANCE unlimited: %v, peak heap growth %d MiB, VmHWM %s, result %v", unlimitedTime, unlimitedPeak>>20, unlimitedHWM, body.Results[0].Data[0].Row)
	t.Logf("ACCEPTANCE limited:   %v, peak heap growth %d MiB, VmHWM %s", limitedTime, limitedPeak>>20, limitedHWM)
	t.Logf("ACCEPTANCE cgroup memory.peak %s", cgroupMemoryPeak())
	require.Greater(t, unlimitedPeak, 4*uint64(limitBytes), "the statement's working set is far above the limit")
	require.Less(t, limitedPeak, unlimitedPeak/4, "the limited run stopped well before the unlimited run's peak")
}

// acceptancePeakHeap is the most the live heap grew over its level before
// fn, sampled every millisecond while fn runs.
func acceptancePeakHeap(fn func()) uint64 {
	read := func() uint64 {
		sample := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
		metrics.Read(sample)
		return sample[0].Value.Uint64()
	}
	runtime.GC()
	base := read()
	var peak atomic.Uint64
	peak.Store(base)
	done := make(chan struct{})
	var sampler sync.WaitGroup
	sampler.Add(1)
	go func() {
		defer sampler.Done()
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			if current := read(); current > peak.Load() {
				peak.Store(current)
			}
			select {
			case <-done:
				return
			case <-ticker.C:
			}
		}
	}()
	fn()
	close(done)
	sampler.Wait()
	return peak.Load() - base
}

// processHighWaterMark is the process's peak resident set (VmHWM), or
// "unavailable" off Linux.
func processHighWaterMark() string {
	status, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return "unavailable"
	}
	for _, line := range strings.Split(string(status), "\n") {
		if strings.HasPrefix(line, "VmHWM:") {
			return strings.TrimSpace(strings.TrimPrefix(line, "VmHWM:"))
		}
	}
	return "unavailable"
}

// cgroupMemoryPeak is the process cgroup's memory.peak (v2), or
// "unavailable".
func cgroupMemoryPeak() string {
	peak, err := os.ReadFile("/sys/fs/cgroup/memory.peak")
	if err != nil {
		return "unavailable"
	}
	bytes, err := strconv.ParseInt(strings.TrimSpace(string(peak)), 10, 64)
	if err != nil {
		return strings.TrimSpace(string(peak))
	}
	return fmt.Sprintf("%d MiB", bytes>>20)
}
