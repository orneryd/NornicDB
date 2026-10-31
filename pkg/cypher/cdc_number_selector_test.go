package cypher

// Author: #1665 implementation lane (Claude Opus 5.5).

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// CDC selectors compare numbers as Cypher does: a FLOAT selector selects the
// equal INTEGER, and an integer beyond 2^53 selects only itself, not its
// neighbour that the same FLOAT would round to.
func TestChangeDataCaptureNumberSelectors(t *testing.T) {
	exec, _ := newCDCExecutor(t, storage.ChangeCaptureFull)
	ctx := context.Background()
	start := cdcID(t, exec, "db.cdc.current")
	_, err := exec.Execute(ctx, "CREATE (:N {k: 2}), (:N {k: 9007199254740993}), (:N {k: 9007199254740992})", nil)
	require.NoError(t, err)
	count := func(key interface{}) int {
		return len(cdcRows(t, exec, start, map[string]interface{}{"select": "n", "operation": "c", "key": map[string]interface{}{"k": key}}))
	}
	require.Equal(t, 1, count(int64(2)))
	require.Equal(t, 1, count(float64(2)), "FLOAT 2.0 selects the INTEGER 2")
	require.Equal(t, 1, count(int64(9007199254740993)))
	require.Equal(t, 1, count(int64(9007199254740992)), "an adjacent integer selects only itself")
	require.Equal(t, 0, count(int64(9007199254740994)))
}
