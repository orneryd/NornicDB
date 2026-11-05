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

	personContract := `
		CREATE CONSTRAINT person_contract IF NOT EXISTS
		FOR (n:Person)
		REQUIRE {
			n.id IS UNIQUE;
			n.email IS NOT NULL;
			n.age IS :: INTEGER;
			(n.id, n.email) IS NODE KEY
		}
	`
	person, err := exec.parseCreateConstraintContractDDL(ctx, personContract)
	require.NoError(t, err)
	res, err := exec.executeCreateConstraintContract(ctx, personContract, person, true)
	require.NoError(t, err)
	require.NotNil(t, res)

	relContract := `
		CREATE CONSTRAINT rel_contract IF NOT EXISTS
		FOR ()-[r:KNOWS]-()
		REQUIRE {
			r.since IS UNIQUE;
			r.reason IS NOT NULL;
			r.since IS :: INTEGER;
			(r.since, r.reason) IS RELATIONSHIP KEY
		}
	`
	rel, err := exec.parseCreateConstraintContractDDL(ctx, relContract)
	require.NoError(t, err)
	res, err = exec.executeCreateConstraintContract(ctx, relContract, rel, true)
	require.NoError(t, err)
	require.NotNil(t, res)

	// Nested contract entries are rejected with deterministic guidance.
	nested := `
		CREATE CONSTRAINT bad_nested FOR (n:Person) REQUIRE {
			FOR (m:Other) REQUIRE { m.id IS UNIQUE }
		}
	`
	nestedContract, err := exec.parseCreateConstraintContractDDL(ctx, nested)
	require.NoError(t, err)
	_, err = exec.executeCreateConstraintContract(ctx, nested, nestedContract, false)
	require.ErrorContains(t, err, "nested FOR ... REQUIRE entries are not supported")

	// Non-contract statements are not admitted by the contract owner.
	_, err = exec.parseCreateConstraintContractDDL(ctx, "CREATE CONSTRAINT x FOR (n:Person) REQUIRE n.id IS UNIQUE")
	require.Error(t, err)
}
