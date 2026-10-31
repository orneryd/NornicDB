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
	_, port := startBoltIntegrationServerWithExplicitTx(t, store)
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

func TestNumericWireCDCUnsignedBolt(t *testing.T) {
	base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	wal, err := storage.NewWAL(t.TempDir(), nil)
	require.NoError(t, err)
	walEngine := storage.NewWALEngine(base, wal)
	store := storage.NewNamespacedEngine(walEngine, "test")
	t.Cleanup(func() { _ = walEngine.Close() })
	capture, ok := storage.ChangeCaptureOf(store)
	require.True(t, ok)
	capture.SetChangeCapture("test", storage.ChangeCaptureFull)
	_, port := startBoltIntegrationServer(t, store)
	driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4j.NoAuth())
	require.NoError(t, err)
	defer driver.Close(context.Background())
	session := driver.NewSession(context.Background(), neo4j.SessionConfig{DatabaseName: "test"})
	defer session.Close(context.Background())
	r, err := session.Run(context.Background(), "CALL db.cdc.current() YIELD id RETURN id", nil)
	require.NoError(t, err)
	records, err := r.Collect(context.Background())
	require.NoError(t, err)
	require.Len(t, records, 1)
	from := records[0].Values[0]
	_, err = store.CreateNode(&storage.Node{ID: "unsigned", Labels: []string{"ReviewCDC"}, Properties: map[string]interface{}{"v": uint64(math.MaxUint64)}})
	require.NoError(t, err)
	r, err = session.Run(context.Background(), "CALL db.cdc.query($from) YIELD event RETURN event", map[string]interface{}{"from": from})
	require.NoError(t, err)
	records, err = r.Collect(context.Background())
	require.NoError(t, err)
	require.Len(t, records, 1)
	event := records[0].Values[0].(map[string]interface{})
	got := event["state"].(map[string]interface{})["after"].(map[string]interface{})["properties"].(map[string]interface{})["v"]
	t.Logf("CDC Bolt uint64 max -> %T(%v)", got, got)
	require.Equal(t, fmt.Sprint(uint64(math.MaxUint64)), got, "Bolt exposes unsupported unsigned integers as exact decimal strings")
}
