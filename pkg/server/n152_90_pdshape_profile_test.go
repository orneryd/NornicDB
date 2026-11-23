package server

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/stretchr/testify/require"
)

// TestN15290DirectPageCPUProfile profiles one direct-filter first page on the
// tip build and prints go tool pprof's top frames for the campaign closeout.
func TestN15290DirectPageCPUProfile(t *testing.T) {
	ctx := context.Background()
	srv, _ := setupTestServer(t)
	database := srv.dbManager.DefaultDatabaseName()
	store, err := srv.dbManager.GetStorage(database)
	require.NoError(t, err)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	boltServer := bolt.NewWithDatabaseManager(bolt.DefaultConfig(), searchNodeWireExecutor{server: srv}, srv.GetDatabaseManager())
	serveDone := make(chan error, 1)
	go func() { serveDone <- boltServer.Serve(listener) }()
	t.Cleanup(func() { _ = boltServer.Close(); <-serveDone })
	driver, err := neo4j.NewDriverWithContext("bolt://"+listener.Addr().String(), neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, driver.Close(ctx)) })
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: database})
	t.Cleanup(func() { require.NoError(t, session.Close(ctx)) })

	n15290Library(t, ctx, store, 400, 300)
	searchService, err := srv.db.GetOrCreateSearchService(database, store)
	require.NoError(t, err)
	require.NoError(t, searchService.BuildIndexes(ctx))
	queryVector := make([]float64, 2048)
	queryVector[0] = 1
	require.Eventually(t, func() bool {
		result, runErr := session.Run(ctx, "CALL db.retrieve({query:'quokka',embedding:$vector,types:['PDSearchSource'],minSimilarity:-1,minRRFScore:0,limit:100000}) YIELD node,vector_rank,bm25_rank RETURN count(CASE WHEN vector_rank > 0 AND bm25_rank > 0 THEN 1 END) AS both", map[string]any{"vector": queryVector})
		if runErr != nil {
			return false
		}
		record, recordErr := result.Single(ctx)
		if recordErr != nil {
			return false
		}
		both, _ := record.Get("both")
		return both == int64(400)
	}, 15*time.Minute, 500*time.Millisecond, "source indexing must complete before profiling")

	profileFile, err := os.CreateTemp("", "n15290-direct-page-*.pprof")
	require.NoError(t, err)
	profilePath := profileFile.Name()
	t.Cleanup(func() { _ = os.Remove(profilePath) })
	require.NoError(t, pprof.StartCPUProfile(profileFile))
	profileStarted := true
	defer func() {
		if profileStarted {
			pprof.StopCPUProfile()
		}
	}()
	started := time.Now()
	result, err := session.Run(ctx, n15290PageQuery, map[string]any{"request": map[string]any{
		"query": "quokka", "embedding": queryVector, "types": []string{"PDSearchSource"},
		"where": n15290Direct, "failClosed": true, "minRRFScore": 0, "n": 20,
	}})
	require.NoError(t, err)
	record, err := result.Single(ctx)
	require.NoError(t, err)
	pageMS := float64(time.Since(started).Microseconds()) / 1000
	idsValue, ok := record.Get("ids")
	require.True(t, ok)
	ids, ok := idsValue.([]any)
	require.True(t, ok, "%T", idsValue)
	require.Len(t, ids, 20)
	pprof.StopCPUProfile()
	profileStarted = false
	require.NoError(t, profileFile.Close())

	top, err := exec.Command("go", "tool", "pprof", "-top", "-nodecount=20", profilePath).CombinedOutput()
	require.NoError(t, err, string(top))
	fmt.Printf("N15290_PROFILED_PAGE {\"fixture\":\"d400-degree300\",\"rule\":\"direct\",\"n\":20,\"rows\":%d,\"page_ms\":%.3f}\n", len(ids), pageMS)
	fmt.Printf("N15290_CPU_PROFILE_TOP\n%s\n", top)
	cumulative, err := exec.Command("go", "tool", "pprof", "-top", "-cum", "-nodecount=20", profilePath).CombinedOutput()
	require.NoError(t, err, string(cumulative))
	fmt.Printf("N15290_CPU_PROFILE_CUMULATIVE_TOP\n%s\n", cumulative)
}
