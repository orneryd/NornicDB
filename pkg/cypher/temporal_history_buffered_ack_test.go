package cypher

import (
	"context"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The same cutoff must answer the acknowledged commit before and after its
// physical drain. The write's eventual metadata proves the pending state.
func TestTemporalAsOfIncludesActuallyBufferedAcknowledgedCreate(t *testing.T) {
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
				DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour,
				EngineOptions: storage.EngineOptions{RetentionPolicy: storage.RetentionPolicy{MaxVersionsPerKey: 100}},
			}))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			ctx := context.Background()
			store := storage.NewNamespacedEngine(base, "buffered_history")
			fresh := func() *StorageExecutor {
				return NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
			}
			ack, err := fresh().Execute(ctx, "CREATE (n:AckProbe {v:'created'}) RETURN id(n) AS id", nil)
			require.NoError(t, err)
			at := time.Now().UTC()
			require.Equal(t, true, ack.Metadata["eventual"], "the write must actually acknowledge a buffered commit")
			require.Len(t, ack.Rows, 1)
			id, ok := ack.Rows[0][0].(string)
			require.True(t, ok)
			_, err = base.GetNodeCurrentHead(ctx, storage.NodeID("buffered_history:"+id))
			require.ErrorIs(t, err, storage.ErrNotFound, "the persisted head must still be absent before drain")
			live, err := fresh().Execute(ctx, "MATCH (n:AckProbe) RETURN n.v AS v", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"created"}}, live.Rows)
			read := func(t *testing.T) {
				t.Helper()
				result, err := fresh().Execute(ctx, "CALL db.temporal.asOf($at,'MATCH (n:AckProbe) RETURN n.v AS v') YIELD row RETURN row.v AS v", map[string]interface{}{"at": at})
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{"created"}}, result.Rows)
			}
			t.Run("before_flush", read)
			require.NoError(t, base.FlushWriteBehind(ctx))
			t.Run("after_flush", read)
		})
	}
}
