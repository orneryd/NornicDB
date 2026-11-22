package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Review lane R1 of PR #1826, round 9: the write-back of finding 1 under a
// second unique property, under an existence constraint, and in an explicit
// transaction.
func TestR1PR1826Round9ConstrainedWriteBack(t *testing.T) {
	for _, test := range []struct {
		name       string
		statements []string
		query      string
		explicit   bool
		want       string
	}{
		{"two-unique-properties", []string{
			"CREATE CONSTRAINT r1_two_key IF NOT EXISTS FOR (n:Note) REQUIRE n.key IS UNIQUE",
			"CREATE CONSTRAINT r1_two_serial IF NOT EXISTS FOR (n:Note) REQUIRE n.serial IS UNIQUE",
			"CREATE (:Note {key:'one', serial:'s1', body:'body one'})",
		}, "CALL () { MATCH p=(n:Note {key:'one'}) RETURN nodes(p)[0] AS x } SET x.flag = 1", false, "{body:'body one',flag:1,key:'one',serial:'s1'}"},
		{"existence-constraint", []string{
			"CREATE CONSTRAINT r1_exists_key IF NOT EXISTS FOR (n:Note) REQUIRE n.key IS UNIQUE",
			"CREATE CONSTRAINT r1_exists_body IF NOT EXISTS FOR (n:Note) REQUIRE n.body IS NOT NULL",
			"CREATE (:Note {key:'one', serial:'s1', body:'body one'})",
		}, "CALL () { MATCH p=(n:Note {key:'one'}) RETURN nodes(p)[0] AS x } SET x.flag = 1", false, "{body:'body one',flag:1,key:'one',serial:'s1'}"},
		{"explicit-transaction", []string{
			"CREATE CONSTRAINT r1_tx_key IF NOT EXISTS FOR (n:Note) REQUIRE n.key IS UNIQUE",
			"CREATE (:Note {key:'one', serial:'s1', body:'body one'})",
		}, "CALL () { MATCH p=(n:Note {key:'one'}) RETURN nodes(p)[0] AS x } SET x.flag = 1", true, "{body:'body one',flag:1,key:'one',serial:'s1'}"},
		{"explicit-transaction-pair", []string{
			"CREATE CONSTRAINT r1_pair_key IF NOT EXISTS FOR (n:Note) REQUIRE n.key IS UNIQUE",
			"CREATE (:Note {key:'zero', body:'body zero'}), (:Note {key:'one', serial:'s1', body:'body one'})",
		}, "CALL () { MATCH (start:Note {key:'zero'}), (end:Note {key:'one'}) UNWIND [start, end] AS x RETURN x } SET x.flag = 1", true, "{body:'body one',flag:1,key:'one',serial:'s1'}"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "r1")
			exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
			for _, statement := range test.statements {
				_, err := exec.Execute(ctx, statement, nil)
				require.NoError(t, err, statement)
			}
			for try := 1; try <= 3; try++ {
				if test.explicit {
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
				}
				result, err := exec.Execute(ctx, test.query, nil)
				t.Logf("R1CONSTRAINED %s try %d: %q", test.name, try, r1Rows(result, err))
				if test.explicit {
					_, err := exec.Execute(ctx, "COMMIT", nil)
					t.Logf("R1CONSTRAINED %s try %d commit: %v", test.name, try, err)
				}
			}
			got := r1StoredProperties(t, store, "r1", "MATCH (n:Note) WHERE n.key = 'one' RETURN properties(n) AS p")
			require.Equal(t, []string{test.want}, got, test.query)
		})
	}
}
