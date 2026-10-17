package bolt

import (
	"fmt"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Concurrent explicit transactions that write first and then MERGE one key
// under a UNIQUE constraint, from a CALL subquery of a statement that also
// deletes: as in Neo4j, one creates the key and the others end up matching
// it. A loser may see a transient error, which the driver retries; it must
// never see ConstraintValidationFailed, which the driver gives up on. Each
// round races several sessions on a fresh key, with other writes before the
// MERGE to widen the window.
func TestBoltConcurrentMergeOfUniqueKeyAfterOtherWrites(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	_, port := startBoltIntegrationServerWithExplicitTx(t, store)

	setup := openBoltTestConn(t, port)
	runBoltQueryAndCollectRecords(t, setup, "CREATE CONSTRAINT race_key_id IF NOT EXISTS FOR (k:RaceKey) REQUIRE k.id IS UNIQUE")
	runBoltQueryAndCollectRecords(t, setup, "CREATE CONSTRAINT race_source_id IF NOT EXISTS FOR (s:RaceSource) REQUIRE s.id IS UNIQUE")
	if err := setup.Close(); err != nil {
		t.Fatalf("close setup: %v", err)
	}

	const rounds, concurrency = 10, 6
	for round := 0; round < rounds; round++ {
		key := fmt.Sprintf("k%d", round)
		var wg sync.WaitGroup
		failures := make([]string, concurrency)
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				source := fmt.Sprintf("s%d-%d", round, idx)
				query := fmt.Sprintf("MERGE (s:RaceSource {id: %q}) ON CREATE SET s.n = %d SET s.keys = [%q] "+
					"WITH s "+
					"CALL (s) { UNWIND range(1, 20) AS i MERGE (f:RaceFiller {id: s.id + '-' + toString(i)}) MERGE (s)-[:FILLS]->(f) RETURN count(*) AS filled } "+
					"CALL (s) { OPTIONAL MATCH (s)-[old:OWNS]->(k:RaceKey) WHERE NOT k.id IN coalesce(s.keys, []) DELETE old RETURN count(*) AS retired } "+
					"CALL (s) { UNWIND coalesce(s.keys, []) AS key MERGE (k:RaceKey {id: key}) MERGE (s)-[:OWNS]->(k) RETURN count(*) AS owned } "+
					"RETURN s.id", source, idx, key)
				if err := runBoltExplicitTxWithRetry(t, port, query); err != nil {
					failures[idx] = err.Error()
				}
			}(i)
		}
		wg.Wait()
		for idx, msg := range failures {
			if msg != "" {
				t.Errorf("round %d session %d failed: %s", round, idx, msg)
			}
		}

		check := openBoltTestConn(t, port)
		records := runBoltQueryAndCollectRecords(t, check,
			fmt.Sprintf("MATCH (k:RaceKey {id: %q}) OPTIONAL MATCH (s:RaceSource)-[:OWNS]->(k) RETURN count(DISTINCT k), count(s)", key))
		_ = check.Close()
		if len(records) != 1 || len(records[0]) != 2 {
			t.Fatalf("round %d: expected one count row, got %v", round, records)
		}
		if records[0][0] != int64(1) || records[0][1] != int64(concurrency) {
			t.Errorf("round %d: expected one key owned by %d sources, got %v", round, concurrency, records[0])
		}
	}
}
