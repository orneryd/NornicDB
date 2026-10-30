package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestConstraintContractEntrySplitAndNestedChecks(t *testing.T) {
	entries, err := splitConstraintContractEntries("n.id IS UNIQUE; n.email IS NOT NULL\n(n.a, n.b) IS NODE KEY")
	require.NoError(t, err)
	require.Equal(t, []string{"n.id IS UNIQUE", "n.email IS NOT NULL", "(n.a, n.b) IS NODE KEY"}, entries)

	_, err = splitConstraintContractEntries("n.id IS UNIQUE; {bad")
	require.EqualError(t, err, "malformed REQUIRE block")

	require.True(t, isNestedConstraintContractEntry("FOR (n:Person) REQUIRE { n.id IS UNIQUE }"))
	require.False(t, isNestedConstraintContractEntry("n.id IS UNIQUE"))
	require.ErrorContains(t, nestedConstraintContractEntryError("FOR (n) REQUIRE { n.id IS UNIQUE }"), "nested FOR ... REQUIRE entries are not supported")
}

func TestExecuteCreateConstraintContract_Branches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "contract_cov"))
	ctx := context.Background()

	res, handled, err := exec.executeCreateConstraintContract(ctx, `
		CREATE CONSTRAINT person_contract IF NOT EXISTS
		FOR (n:Person)
		REQUIRE {
			n.id IS UNIQUE;
			n.email IS NOT NULL;
			n.age IS :: INTEGER;
			(n.id, n.email) IS NODE KEY
		}
	`, true)
	require.True(t, handled)
	require.NoError(t, err)
	require.NotNil(t, res)

	res, handled, err = exec.executeCreateConstraintContract(ctx, `
		CREATE CONSTRAINT rel_contract IF NOT EXISTS
		FOR ()-[r:KNOWS]-()
		REQUIRE {
			r.since IS UNIQUE;
			r.reason IS NOT NULL;
			r.since IS :: INTEGER;
			(r.since, r.reason) IS RELATIONSHIP KEY
		}
	`, true)
	require.True(t, handled)
	require.NoError(t, err)
	require.NotNil(t, res)

	// Nested contract entries are rejected with deterministic guidance.
	_, handled, err = exec.executeCreateConstraintContract(ctx, `
		CREATE CONSTRAINT bad_nested FOR (n:Person) REQUIRE {
			FOR (m:Other) REQUIRE { m.id IS UNIQUE }
		}
	`, false)
	require.True(t, handled)
	require.ErrorContains(t, err, "nested FOR ... REQUIRE entries are not supported")

	// Non-contract statements should not be handled here.
	res, handled, err = exec.executeCreateConstraintContract(ctx, "CREATE CONSTRAINT x FOR (n:Person) REQUIRE n.id IS UNIQUE", false)
	require.NoError(t, err)
	require.False(t, handled)
	require.Nil(t, res)
}
