package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// BUG: node TEMPORAL NO OVERLAP constraints required exactly 3 properties,
// while relationship temporal constraints accept a composite grouping key
// (every property before the trailing valid_from, valid_to pair).
// `REQUIRE (n.k1, n.k2, n.vf, n.vt) IS TEMPORAL NO OVERLAP` failed with
// "TEMPORAL constraint requires 3 properties (key, valid_from, valid_to)".

func assertCompositeNodeTemporalEnforced(t *testing.T, exec *StorageExecutor) {
	t.Helper()
	ctx := context.Background()
	run := func(q string) error {
		_, err := exec.Execute(ctx, q, nil)
		return err
	}

	require.NoError(t, run(`CREATE (:E {k1: 'a', k2: 'x', vf: datetime('2024-01-01T00:00:00Z'), vt: datetime('2024-02-01T00:00:00Z')})`))
	require.Error(t, run(`CREATE (:E {k1: 'a', k2: 'x', vf: datetime('2024-01-15T00:00:00Z'), vt: datetime('2024-03-01T00:00:00Z')})`),
		"same composite key with overlapping window must be rejected")
	require.NoError(t, run(`CREATE (:E {k1: 'a', k2: 'y', vf: datetime('2024-01-15T00:00:00Z'), vt: datetime('2024-03-01T00:00:00Z')})`),
		"same k1 but different k2 is a different group")
	require.NoError(t, run(`CREATE (:E {k1: 'b', k2: 'x', vf: datetime('2024-01-15T00:00:00Z'), vt: datetime('2024-03-01T00:00:00Z')})`),
		"different k1 but same k2 is a different group")
	require.NoError(t, run(`CREATE (:E {k1: 'a', k2: 'x', vf: datetime('2024-02-01T00:00:00Z'), vt: datetime('2024-03-01T00:00:00Z')})`),
		"adjacent window (vt == next vf) is allowed")
	require.NoError(t, run(`CREATE (:E {k1: 'a', k2: 'x', vf: datetime('2024-04-01T00:00:00Z')})`),
		"open-ended window after existing versions is allowed")
	require.Error(t, run(`CREATE (:E {k1: 'a', k2: 'x', vf: datetime('2024-05-01T00:00:00Z'), vt: datetime('2024-06-01T00:00:00Z')})`),
		"window inside an open-ended version must be rejected")
	require.Error(t, run(`CREATE (:E {k1: 'a', vf: datetime('2030-01-01T00:00:00Z'), vt: datetime('2030-02-01T00:00:00Z')})`),
		"null key property must be rejected")
	require.Error(t, run(`CREATE (:E {k1: 'c', k2: 'z', vf: datetime('2024-01-01T00:00:00Z'), vt: datetime('2024-03-01T00:00:00Z')}), (:E {k1: 'c', k2: 'z', vf: datetime('2024-02-01T00:00:00Z'), vt: datetime('2024-04-01T00:00:00Z')})`),
		"two overlapping nodes in one statement must be rejected")
}

func TestBug_NodeTemporalCompositeKey(t *testing.T) {
	ctx := context.Background()

	t.Run("primitive DDL", func(t *testing.T) {
		exec, store := newConstraintGapExecutor(t)
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT e_temporal FOR (n:E) REQUIRE (n.k1, n.k2, n.vf, n.vt) IS TEMPORAL NO OVERLAP`, nil)
		require.NoError(t, err)

		var found bool
		for _, c := range store.GetSchema().GetAllConstraints() {
			if c.Name == "e_temporal" {
				found = true
				require.Equal(t, storage.ConstraintTemporal, c.Type)
				require.Equal(t, []string{"k1", "k2", "vf", "vt"}, c.Properties)
			}
		}
		require.True(t, found)
		assertCompositeNodeTemporalEnforced(t, exec)
	})

	t.Run("inside a contract", func(t *testing.T) {
		exec, _ := newConstraintGapExecutor(t)
		_, err := exec.Execute(ctx, `
			CREATE CONSTRAINT e_contract FOR (n:E) REQUIRE {
			  (n.k1, n.k2, n.vf, n.vt) IS TEMPORAL NO OVERLAP
			}`, nil)
		require.NoError(t, err)
		assertCompositeNodeTemporalEnforced(t, exec)
	})

	t.Run("creation validates existing data", func(t *testing.T) {
		exec, _ := newConstraintGapExecutor(t)
		_, err := exec.Execute(ctx, `CREATE (:E {k1: 'a', k2: 'x', vf: datetime('2024-01-01T00:00:00Z'), vt: datetime('2024-02-01T00:00:00Z')}), (:E {k1: 'a', k2: 'y', vf: datetime('2024-01-15T00:00:00Z'), vt: datetime('2024-03-01T00:00:00Z')})`, nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, `CREATE CONSTRAINT e_temporal FOR (n:E) REQUIRE (n.k1, n.k2, n.vf, n.vt) IS TEMPORAL NO OVERLAP`, nil)
		require.NoError(t, err, "different k2 values do not overlap")
		_, err = exec.Execute(ctx, `DROP CONSTRAINT e_temporal`, nil)
		require.NoError(t, err)

		_, err = exec.Execute(ctx, `CREATE (:E {k1: 'a', k2: 'x', vf: datetime('2024-01-20T00:00:00Z'), vt: datetime('2024-01-25T00:00:00Z')})`, nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, `CREATE CONSTRAINT e_temporal FOR (n:E) REQUIRE (n.k1, n.k2, n.vf, n.vt) IS TEMPORAL NO OVERLAP`, nil)
		require.Error(t, err, "pre-existing overlap within one composite key must block creation")
	})

	t.Run("fewer than 3 properties still rejected", func(t *testing.T) {
		exec, _ := newConstraintGapExecutor(t)
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT bad FOR (n:E) REQUIRE (n.vf, n.vt) IS TEMPORAL NO OVERLAP`, nil)
		require.ErrorContains(t, err, "at least 3 properties")
		_, err = exec.Execute(ctx, `CREATE CONSTRAINT bad FOR (n:E) REQUIRE { (n.vf, n.vt) IS TEMPORAL NO OVERLAP }`, nil)
		require.ErrorContains(t, err, "at least 3 properties")
	})
}
