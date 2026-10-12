package nornicdb

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A node suppressed with kp.suppress is no keyword hit, with decay
// disabled, and is one again after kp.unsuppress, which indexes it
// again (#1064).
func TestVisibilitySuppressHidesFromSearch(t *testing.T) {
	ctx := context.Background()
	db, err := Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.CreateNode(ctx, []string{"Doc"}, map[string]interface{}{"name": "zephyrine report"})
	require.NoError(t, err)
	hits := func() int {
		results, err := db.Search(ctx, "zephyrine", nil, 10)
		require.NoError(t, err)
		return len(results)
	}
	require.Eventually(t, func() bool { return hits() == 1 }, 5*time.Second, 20*time.Millisecond)

	_, err = db.ExecuteCypher(ctx, "MATCH (n:Doc {name: 'zephyrine report'}) CALL kp.suppress(n)", nil)
	require.NoError(t, err)
	require.Equal(t, 0, hits())

	_, err = db.ExecuteCypher(ctx, "MATCH (n:Doc) WHERE reveal(n).name = 'zephyrine report' CALL kp.unsuppress(n)", nil)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return hits() == 1 }, 5*time.Second, 20*time.Millisecond)
}
