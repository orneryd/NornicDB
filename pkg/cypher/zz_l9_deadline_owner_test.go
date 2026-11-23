package cypher

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestL9DeadlineTimestampChild(t *testing.T) {
	mode := os.Getenv("L9_DEADLINE_MODE")
	if mode == "" {
		t.Skip("run by TestL9DeadlineTimestampOwner")
	}
	manager, err := multidb.NewDatabaseManager(t.Context(), fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "determinism_local"}))
	require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "determinism_composite", []multidb.ConstituentRef{
		{Alias: "local", DatabaseName: "determinism_local", Type: "local", AccessMode: "read_write"},
	}))
	store, err := manager.GetStorage(manager.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 64, TTL: time.Hour})
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	ctx := WithPermissionChecker(context.Background(), func(string) bool { return true })
	const query = "USE determinism_composite CALL { USE determinism_composite.local CALL apoc.cypher.run($statement,{}) YIELD value RETURN value.sampled AS sampled, value.fresh AS fresh } RETURN sampled, fresh"
	start := time.Now()
	deadline := start.Add(time.Second)
	if mode == "extended" {
		deadline = start.Add(15 * time.Second)
	}
	params := map[string]any{"statement": "WITH timestamp() AS sampled RETURN sampled, sampled < $deadline AS fresh", "deadline": deadline.UnixMilli()}
	result, err := exec.Execute(ctx, query, params)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	row := result.Rows[0]
	sampled, ok := row[0].(int64)
	require.True(t, ok, "%v", row)
	fresh, ok := row[1].(bool)
	require.True(t, ok, "%v", row)
	t.Logf("L9 timestamp %s: start=%d cutoff=%d observed=%d delta=%d ms fresh=%t total=%v", mode, start.UnixMilli(), deadline.UnixMilli(), sampled, sampled-start.UnixMilli(), fresh, time.Since(start))
	require.Equal(t, sampled < deadline.UnixMilli(), fresh)
	if mode == "extended" {
		require.True(t, fresh)
	}
}

func TestL9DeadlineTimestampOwner(t *testing.T) {
	for _, mode := range []string{"original", "extended"} {
		t.Run(mode, func(t *testing.T) {
			for round := range 2 {
				var children sync.WaitGroup
				for worker := range 8 {
					children.Add(1)
					go func() {
						defer children.Done()
						cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestL9DeadlineTimestampChild$", "-test.count=1", "-test.v")
						cmd.Env = append(os.Environ(), "GOMAXPROCS=2", "L9_DEADLINE_MODE="+mode)
						output, err := cmd.CombinedOutput()
						t.Logf("%s round=%d worker=%d: %s", mode, round, worker, output)
						if err != nil {
							t.Error(fmt.Errorf("child: %w", err))
						}
					}()
				}
				children.Wait()
			}
		})
	}
}
