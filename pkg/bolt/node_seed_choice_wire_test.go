package bolt

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

// NOR-414 over Bolt, auto-commit and in an explicit transaction: a lookup by
// an indexed id whose WHERE also filters on an indexed status reads the id
// index, not every node holding the status. Reading the 20,000 pending nodes
// once per id allocated over a gigabyte; the id lookups allocate a few
// megabytes at most.
func TestBoltNodeSeedChoiceReadsSelectiveIndex(t *testing.T) {
	const database = "seed_choice_wire"
	store := storage.NewNamespacedEngine(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), database)
	defer store.Close()
	_, port := startBoltIntegrationServer(t, store, boltIntegrationExplicitTx)
	ctx := context.Background()
	driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4j.NoAuth())
	require.NoError(t, err)
	defer driver.Close(ctx)

	setup := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: database})
	for _, statement := range []string{
		"CREATE INDEX wire_case_id FOR (n:Case) ON (n.id)",
		"CREATE INDEX wire_case_status FOR (n:Case) ON (n.status)",
		"UNWIND range(0, 29999) AS i CREATE (:Case {id: 'c-' + toString(i), status: CASE WHEN i % 3 = 0 THEN 'done' ELSE 'pending' END})",
	} {
		result, err := setup.Run(ctx, statement, nil)
		require.NoError(t, err, statement)
		_, err = result.Consume(ctx)
		require.NoError(t, err, statement)
	}
	require.NoError(t, setup.Close(ctx))

	ids := make([]interface{}, 0, 20)
	var want []string
	for i := 0; i < 20; i++ {
		n := i*50 + 1
		ids = append(ids, fmt.Sprintf("c-%d", n))
		if n%3 != 0 {
			want = append(want, fmt.Sprintf("c-%d", n))
		}
	}
	sort.Strings(want)

	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%v", explicit), func(t *testing.T) {
			session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: database})
			defer session.Close(ctx)
			run := session.Run
			if explicit {
				tx, err := session.BeginTransaction(ctx)
				require.NoError(t, err)
				defer tx.Close(ctx)
				run = func(ctx context.Context, query string, params map[string]interface{}, _ ...func(*neo4j.TransactionConfig)) (neo4j.ResultWithContext, error) {
					return tx.Run(ctx, query, params)
				}
			}
			for _, statement := range []string{
				"UNWIND $ids AS id MATCH (c:Case {id: id}) WHERE c.status = 'pending' RETURN c.id",
				"UNWIND $ids AS id MATCH (c:Case) WHERE c.status = 'pending' AND c.id = id RETURN c.id",
			} {
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				result, err := run(ctx, statement, map[string]interface{}{"ids": ids})
				require.NoError(t, err, statement)
				records, err := result.Collect(ctx)
				require.NoError(t, err, statement)
				runtime.ReadMemStats(&after)
				got := make([]string, 0, len(records))
				for _, record := range records {
					got = append(got, fmt.Sprint(record.Values[0]))
				}
				sort.Strings(got)
				require.Equal(t, want, got, statement)
				require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(64<<20), "%s: allocated %d bytes; the id index must seed the MATCH", statement, after.TotalAlloc-before.TotalAlloc)
			}
		})
	}
}
