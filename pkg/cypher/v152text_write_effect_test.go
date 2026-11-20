package cypher

// v1.5.2 verification, built from the text of Plane item N15-101 (NOR-300):
// "MATCH (n:Doc) CALL kp.suppress(n) RETURN n" was classified read-only
// although the registered procedure writes. Public consequence checked here:
// in a read-only statement context (the one read-access sessions use) the
// statement must be refused and must not hide the node.

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestV152TextSuppressCallIsAWrite(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		base, err := storage.NewBadgerEngine(t.TempDir())
		require.NoError(t, err)
		t.Cleanup(func() { _ = base.Close() })
		exec := NewStorageExecutor(storage.NewNamespacedEngine(base, "nornic"))
		_, err = exec.Execute(context.Background(), "CREATE (:Doc {id:'d1'})", nil)
		require.NoError(t, err)
		for _, query := range []string{
			"MATCH (n:Doc {id:'d1'}) CALL kp.suppress(n) RETURN n",
			"MATCH (n:Doc {id:'d1'}) CALL kp.suppress(n)",
		} {
			result, err := exec.Execute(WithReadOnlyStatements(context.Background()), query, nil)
			visible, verr := exec.Execute(context.Background(), "MATCH (n:Doc {id:'d1'}) RETURN count(n) AS c", nil)
			require.NoError(t, verr)
			var rows any
			if result != nil {
				rows = result.Rows
			}
			t.Logf("V152TEXT NOR-300 read-only %q -> rows=%v err=%v; visible count after=%v", query, rows, err, visible.Rows)
			require.Error(t, err, "a statement calling the writing procedure kp.suppress ran in a read-only context")
			require.Equal(t, [][]interface{}{{int64(1)}}, visible.Rows, "the read-only context hid the node")
		}
	})
}
