package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReviewR8ConstraintListQuotation(t *testing.T) {
	for _, tc := range []struct{ name, literal, value string }{
		{"control", "'Acme, Sons'", "Acme, Sons"},
		{"escaped", `'O\'Reilly, Sons'`, "O'Reilly, Sons"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "r8_list_"+tc.name))
			ctx := context.Background()
			q := "CREATE CONSTRAINT r8_list FOR (n:R8List) REQUIRE { n.name IN [" + tc.literal + ", 'Other'] }"
			_, err := e.Execute(ctx, q, nil)
			require.NoError(t, err, q)
			_, err = e.Execute(ctx, "CREATE (:R8List {name:$name})", map[string]interface{}{"name": tc.value})
			require.NoError(t, err, "allowed name must satisfy contract")
		})
	}
}
