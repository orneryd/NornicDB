package bolt

import (
	"context"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type pdW161Run func(context.Context, string, map[string]any) (neo4j.ResultWithContext, error)

func pdW161Collect(t *testing.T, run pdW161Run, query string, params map[string]any) ([]*neo4j.Record, neo4j.ResultSummary) {
	t.Helper()
	ctx := t.Context()
	result, err := run(ctx, query, params)
	require.NoError(t, err, query)
	rows, err := result.Collect(ctx)
	require.NoError(t, err, query)
	summary, err := result.Consume(ctx)
	require.NoError(t, err, query)
	return rows, summary
}

func pdW161Profile(t *testing.T, plan neo4j.ProfiledPlan) {
	t.Helper()
	t.Logf("PDW161_PROFILE operator=%s hits=%d rows=%d", plan.Operator(), plan.DbHits(), plan.Records())
	for _, child := range plan.Children() {
		pdW161Profile(t, child)
	}
}

func TestPDW161PassedRelationshipSnapshot(t *testing.T) {
	t.Run("nornic", func(t *testing.T) {
		for _, size := range []int{1000, 8000} {
			t.Run(fmt.Sprint(size), func(t *testing.T) {
				base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
				require.NoError(t, err)
				t.Cleanup(func() { _ = base.Close() })
				store := storage.NewNamespacedEngine(base, "pd_w161")
				_, port := startBoltIntegrationServer(t, store, boltIntegrationExplicitTx)
				driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4j.NoAuth())
				require.NoError(t, err)
				t.Cleanup(func() { _ = driver.Close(context.Background()) })
				pdW161PassedSnapshot(t, driver, "pd_w161", size, false)
			})
		}
	})
	t.Run("neo4j", func(t *testing.T) {
		uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")
		if uri == "" {
			t.Skip("requires the lab's Neo4j reference lease")
		}
		driver, err := neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
		require.NoError(t, err)
		t.Cleanup(func() { _ = driver.Close(context.Background()) })
		for _, size := range []int{1000, 8000} {
			t.Run(fmt.Sprint(size), func(t *testing.T) {
				pdW161PassedSnapshot(t, driver, "neo4j", size, true)
			})
		}
	})
}

func pdW161PassedSnapshot(t *testing.T, driver neo4j.DriverWithContext, database string, size int, reference bool) {
	t.Helper()
	ctx := t.Context()
	peer := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: database, AccessMode: neo4j.AccessModeWrite})
	defer peer.Close(ctx)
	if reference {
		const clean = "MATCH (n:PDW161Fixture) DETACH DELETE n"
		pdW161Collect(t, peer.Run, clean, nil)
		defer pdW161Collect(t, peer.Run, clean, nil)
		defer pdW161Collect(t, peer.Run, "DROP CONSTRAINT pd_w161_link IF EXISTS", nil)
	}
	pdW161Collect(t, peer.Run, "CREATE CONSTRAINT pd_w161_link IF NOT EXISTS FOR ()-[r:PDW161_LINK]-() REQUIRE r.id IS UNIQUE", nil)
	if reference {
		_, info := pdW161Collect(t, peer.Run, "CALL db.awaitIndexes(300)", nil)
		t.Logf("PDW161_REFERENCE agent=%s", info.Server().Agent())
	}
	pdW161Collect(t, peer.Run, "CREATE (:PDW161Fixture:PDW161Hub {id:'hub'})", nil)
	const seed = "MATCH (hub:PDW161Hub {id:'hub'}) " +
		"UNWIND range($from,$to) AS i " +
		"CREATE (hub)-[:PDW161_LINK {id:'h'+toString(i)}]->(:PDW161Fixture) " +
		"CREATE (:PDW161Fixture)-[:PDW161_LINK {id:'n'+toString(i)}]->(:PDW161Fixture)"
	for from := 0; from < size; from += 500 {
		pdW161Collect(t, peer.Run, seed, map[string]any{"from": from, "to": min(from+500, size)-1})
	}
	const query = "MATCH ()-[r:PDW161_LINK {id:$x}]->() RETURN r.id AS id"
	const markerCount = "MATCH (n:PDW161Fixture:PDW161Marker {mode:$mode}) RETURN count(n)"
	for _, mode := range []struct {
		name string
		mode neo4j.AccessMode
	}{{"read", neo4j.AccessModeRead}, {"write", neo4j.AccessModeWrite}} {
		t.Run(mode.name, func(t *testing.T) {
			ctx := t.Context()
			session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: database, AccessMode: mode.mode})
			defer session.Close(ctx)
			tx, err := session.BeginTransaction(ctx)
			require.NoError(t, err)
			defer tx.Close(ctx)

			params := map[string]any{"mode": mode.name}
			checkMarker := func(run pdW161Run, want int64) {
				rows, _ := pdW161Collect(t, run, markerCount, params)
				require.Len(t, rows, 1)
				require.Equal(t, []any{want}, rows[0].Values)
			}
			checkMarker(tx.Run, 0)
			measure := func(phase string, run pdW161Run) {
				t.Helper()
				var samples []time.Duration
				for i := 0; i < 5; i++ {
					id := fmt.Sprintf("h%d", i)
					started := time.Now()
					rows, _ := pdW161Collect(t, run, query, map[string]any{"x": id})
					samples = append(samples, time.Since(started))
					require.Len(t, rows, 1)
					require.Equal(t, []any{id}, rows[0].Values)
				}
				sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
				t.Logf("PDW161_TIMING degree=%d type=%d mode=%s phase=%s median=%s samples=%v", size, 2*size, mode.name, phase, samples[2], samples)
				t.Logf("PDW161_PROFILE_PHASE degree=%d mode=%s phase=%s", size, mode.name, phase)
				rows, summary := pdW161Collect(t, run, "PROFILE "+query, map[string]any{"x": "h10"})
				require.Len(t, rows, 1)
				require.Equal(t, []any{"h10"}, rows[0].Values)
				if profile := summary.Profile(); profile != nil {
					pdW161Profile(t, profile)
				}
			}
			measure("current", tx.Run)

			writer, err := peer.BeginTransaction(ctx)
			require.NoError(t, err)
			defer writer.Close(ctx)
			pdW161Collect(t, writer.Run, "CREATE (:PDW161Fixture:PDW161Marker {mode:$mode})", params)
			require.NoError(t, writer.Commit(ctx))
			checkMarker(peer.Run, 1)
			t.Logf("PDW161_PEER_COMMITTED degree=%d mode=%s marker_visible_to_peer=1", size, mode.name)

			measure("passed", tx.Run)
			if !reference {
				checkMarker(tx.Run, 0)
				t.Logf("PDW161_PINNED degree=%d mode=%s marker_visible_to_reader=0", size, mode.name)
			}
			require.NoError(t, tx.Rollback(ctx))

			fresh, err := session.BeginTransaction(ctx)
			require.NoError(t, err)
			defer fresh.Close(ctx)
			checkMarker(fresh.Run, 1)
			measure("fresh", fresh.Run)
		})
	}
}
