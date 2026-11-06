// Author: L6 / #1665 lane (Codex gpt-6.1-sol).
package bolt

import (
	"context"
	"fmt"
	"math"
	"os"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNumericWireBoltSignedAndReference(t *testing.T) {
	store := storage.NewNamespacedEngine(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "test")
	t.Cleanup(func() { _ = store.Close() })
	_, port := startBoltIntegrationServer(t, store, boltIntegrationExplicitTx)
	for _, uri := range []string{fmt.Sprintf("bolt://127.0.0.1:%d", port), os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")} {
		if uri == "" {
			continue
		}
		driver, err := neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
		require.NoError(t, err)
		defer driver.Close(context.Background())
		for _, explicit := range []bool{false, true} {
			session := driver.NewSession(context.Background(), neo4j.SessionConfig{})
			run := func(ctx context.Context, q string, p map[string]interface{}) (neo4j.ResultWithContext, error) {
				return session.Run(ctx, q, p)
			}
			if explicit {
				tx, err := session.BeginTransaction(context.Background())
				require.NoError(t, err)
				defer tx.Close(context.Background())
				run = tx.Run
			}
			for _, v := range []int64{9007199254740991, 9007199254740992, 9007199254740993, -9007199254740991, -9007199254740992, -9007199254740993, math.MaxInt64, math.MinInt64} {
				r, err := run(context.Background(), "RETURN $v, $nested, $s, $u", map[string]interface{}{"v": v, "nested": map[string]interface{}{"list": []interface{}{v, float64(2), 1.25}}, "s": fmt.Sprint(v), "u": "18446744073709551615"})
				require.NoError(t, err)
				records, err := r.Collect(context.Background())
				require.NoError(t, err)
				require.Len(t, records, 1)
				require.Equal(t, []interface{}{v, map[string]interface{}{"list": []interface{}{v, float64(2), 1.25}}, fmt.Sprint(v), "18446744073709551615"}, records[0].Values)
			}
			session.Close(context.Background())
		}
	}
}
