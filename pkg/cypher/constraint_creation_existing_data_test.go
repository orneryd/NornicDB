package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Creating a constraint checks the data already stored: a node without the
// property satisfies a domain constraint (null is allowed), and node-key
// values that share some properties but not all are distinct keys.
func TestConstraintCreationOverExistingData(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "constraint_creation_existing_data"))
	ctx := context.Background()
	for _, statement := range []string{
		"CREATE (:DomainExisting {s: 'a'}), (:DomainExisting)",
		"CREATE CONSTRAINT domain_existing FOR (n:DomainExisting) REQUIRE n.s IN ['a', 'b']",
		"CREATE (:KeyExisting {a: 1, b: 1}), (:KeyExisting {a: 1, b: 2})",
		"CREATE CONSTRAINT key_existing FOR (n:KeyExisting) REQUIRE (n.a, n.b) IS NODE KEY",
	} {
		_, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}
	_, err := exec.Execute(ctx, "CREATE (:DomainRejected {s: 'z'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE CONSTRAINT domain_rejected FOR (n:DomainRejected) REQUIRE n.s IN ['a', 'b']", nil)
	require.Error(t, err)
	_, err = exec.Execute(ctx, "CREATE (:KeyRejected {a: 1, b: 1}), (:KeyRejected {a: 1, b: 1})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE CONSTRAINT key_rejected FOR (n:KeyRejected) REQUIRE (n.a, n.b) IS NODE KEY", nil)
	require.Error(t, err)
}
