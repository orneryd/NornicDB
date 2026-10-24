package storage_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
)

// A statement that deletes its own node and then writes it is not a
// concurrent conflict on any route: a retry cannot repair its own deletion.
func TestReview1655OwnDeletePublic(t *testing.T) {
	for _, async := range []bool{false, true} {
		for _, route := range r2Routes {
			t.Run(fmt.Sprintf("async=%t/%s/explicit=%t", async, route.name, route.explicit), func(t *testing.T) {
				env := r2Open(t, t.TempDir(), async, time.Hour)
				outcome := env.run(context.Background(), cypher.NewStorageExecutor(env.store), route, "CREATE (n:Doc {id:'own'}) WITH n DELETE n SET n.note='after-delete' RETURN n.id", nil)
				t.Logf("code=%s err=%v", outcome.code, outcome.err)
				require.NotEqual(t, "Neo.TransientError.Transaction.Outdated", outcome.code, "a retry cannot repair this transaction's own deletion")
				remaining := r2MustRun(t, cypher.NewStorageExecutor(env.store), "MATCH (n:Doc {id:'own'}) RETURN count(n)", nil)
				require.EqualValues(t, 0, remaining.Rows[0][0])
			})
		}
	}
}
