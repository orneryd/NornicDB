package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReviewR7ConstraintQuotedProperty(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "r7_contract"))
	for _, q := range []string{
		"CREATE CONSTRAINT r7control FOR (n:R7Control) REQUIRE { n.`owner-name` IS NOT NULL }",
		"CREATE CONSTRAINT r7quoted FOR (n:R7Quoted) REQUIRE { n.`owner's` IS NOT NULL }",
	} {
		_, err := e.Execute(context.Background(), q, nil)
		require.NoError(t, err, q)
	}
}
