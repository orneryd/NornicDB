package cypher

// Review lane R2 of PR #1826: does the visibility filter of a decay profile
// decide the same way for a narrow (property-only) read as for a whole-node
// read of the same node? Test-only file; runs unchanged on head and base.

import (
	"testing"
	"time"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPR1826R2DecayVisibilityOnNarrowReads(t *testing.T) {
	be, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = be.Close() })
	be.SetDecayEnabled(true)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(be, "r2decay"), QueryCachePolicy{MaxEntries: 0})
	ctx := t.Context()
	run := func(statement string) error {
		_, err := exec.Execute(ctx, statement, nil)
		if err != nil {
			t.Logf("R2DECAY statement failed: %s: %v", statement, err)
		}
		return err
	}
	require.NoError(t, run("CREATE DECAY PROFILE r2decay OPTIONS {halfLifeSeconds: 1, function: 'exponential', scope: 'NODE', scoreFrom: 'CREATED', visibilityThreshold: 0.3}"))
	require.NoError(t, run("CREATE PROMOTION PROFILE r2promo OPTIONS {multiplier: 1.25, scope: 'NODE', scoreFloor: 0.9, scoreCap: 0.95}"))
	type population struct {
		label, rule string
		indexed     bool
	}
	populations := []population{
		{"R2KeepIdx", "WHEN n.keep = true APPLY PROFILE r2promo", true},
		{"R2KeepScan", "WHEN n.keep = true APPLY PROFILE r2promo", false},
		{"R2ArchIdx", "WHEN n.archived IS NULL APPLY PROFILE r2promo", true},
		{"R2ArchScan", "WHEN n.archived IS NULL APPLY PROFILE r2promo", false},
	}
	var ready []population
	for _, p := range populations {
		if p.indexed {
			require.NoError(t, run("CREATE CONSTRAINT r2c_"+p.label+" IF NOT EXISTS FOR (n:"+p.label+") REQUIRE n.id IS UNIQUE"))
		}
		if run("CREATE DECAY PROFILE r2bind_"+p.label+" FOR (n:"+p.label+") APPLY {DECAY PROFILE r2decay}") != nil {
			continue
		}
		if run("CREATE PROMOTION POLICY r2pol_"+p.label+" FOR (n:"+p.label+") APPLY {"+p.rule+"}") != nil {
			continue
		}
		if run("CREATE (:"+p.label+" {id: 1, keep: true, archived: true, name: 'one'}), (:"+p.label+" {id: 2, keep: false, name: 'two'})") != nil {
			continue
		}
		ready = append(ready, p)
	}
	require.NotEmpty(t, ready, "no population could be set up")
	time.Sleep(5 * time.Second)
	rows := func(query string, id int64) int {
		result, err := exec.Execute(ctx, query, map[string]any{"id": id})
		if !assert.NoError(t, err, query) {
			return -1
		}
		return len(result.Rows)
	}
	for _, p := range ready {
		for _, id := range []int64{1, 2} {
			whole := rows("MATCH (n:"+p.label+" {id: $id}) RETURN n", id)
			narrow := rows("MATCH (n:"+p.label+" {id: $id}) RETURN n.name AS name", id)
			narrowAgain := rows("MATCH (n:"+p.label+" {id: $id}) RETURN n.name AS name", id)
			wholeAgain := rows("MATCH (n:"+p.label+" {id: $id}) RETURN n", id)
			t.Logf("R2DECAY label=%s rule=%q id=%d whole=%d narrow=%d narrowAgain=%d wholeAgain=%d", p.label, p.rule, id, whole, narrow, narrowAgain, wholeAgain)
			assert.Equal(t, whole, narrow, "label %s id %d: a property-only read and a whole-node read must agree on whether the node is visible", p.label, id)
			assert.Equal(t, whole, narrowAgain, "label %s id %d (repeated narrow read)", p.label, id)
			assert.Equal(t, whole, wholeAgain, "label %s id %d (repeated whole read)", p.label, id)
		}
	}
}
