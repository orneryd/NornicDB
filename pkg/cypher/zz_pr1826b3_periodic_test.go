package cypher

// Author: pr1826-base3 test lane (Claude Opus 5.5), 23 Nov 2026.
// Reproduction for: apoc.periodic.iterate with an action statement that
// starts with SET (or REMOVE) is refused by the default parser.

import (
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func pr1826b3Executor(t *testing.T) *StorageExecutor {
	t.Helper()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic"))
	_, err := exec.Execute(t.Context(), "CREATE (:Doc {key:'one', body:'b1'}), (:Doc {key:'two', body:'b2'})", nil)
	require.NoError(t, err)
	return exec
}

func pr1826b3One(t *testing.T, exec *StorageExecutor) string {
	t.Helper()
	result, err := exec.Execute(t.Context(), "MATCH (d:Doc {key:'one'}) RETURN d.flag, d:Actor, d.body", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	return fmt.Sprintf("flag=%v actor=%v body=%v", result.Rows[0][0], result.Rows[0][1], result.Rows[0][2])
}

// The exact R1 call forms, the APOC manual's own example shape ("SET p:Actor")
// and a REMOVE action. Neo4j with APOC runs each action once per batch,
// prefixed with UNWIND $_batch AS _batch WITH _batch.<column> AS <column>.
func TestPR1826B3PeriodicIterateSetAction(t *testing.T) {
	t.Logf("PR1826B3 parser=%s", config.GetParserType())
	for _, test := range []struct {
		name, query, want string
	}{
		{"r1-keyword", `CALL apoc.periodic.iterate("MATCH (end:Doc {key:'one'}) RETURN coalesce(end, null) AS x", "SET x.flag = 1", {batchSize: 10})`, "flag=1 actor=false body=b1"},
		{"r1-path", `CALL apoc.periodic.iterate("MATCH p=(n:Doc {key:'one'}) RETURN nodes(p)[0] AS x", "SET x.flag = 1", {batchSize: 10})`, "flag=1 actor=false body=b1"},
		{"plain-node", `CALL apoc.periodic.iterate("MATCH (x:Doc {key:'one'}) RETURN x", "SET x.flag = 1", {batchSize: 10})`, "flag=1 actor=false body=b1"},
		{"manual-label", `CALL apoc.periodic.iterate("MATCH (p:Doc) WHERE p.key = 'one' RETURN p", "SET p:Actor", {batchSize: 10000})`, "flag=<nil> actor=true body=b1"},
		{"remove", `CALL apoc.periodic.iterate("MATCH (x:Doc {key:'one'}) RETURN x", "REMOVE x.body", {batchSize: 10})`, "flag=<nil> actor=false body=<nil>"},
	} {
		t.Run(test.name, func(t *testing.T) {
			exec := pr1826b3Executor(t)
			result, err := exec.Execute(t.Context(), test.query, nil)
			got := pr1826b3One(t, exec)
			if err != nil {
				t.Logf("PR1826B3 case=%s parser=%s error=%v stored=%s", test.name, config.GetParserType(), err, got)
			} else {
				require.Len(t, result.Rows, 1)
				row := result.Rows[0]
				t.Logf("PR1826B3 case=%s parser=%s batches=%v total=%v committed=%v failed=%v errorMessages=%v stored=%s",
					test.name, config.GetParserType(), row[0], row[1], row[3], row[4], row[7], got)
				require.Equal(t, int64(0), row[4], "failedOperations for %s", test.query)
			}
			require.NoError(t, err, test.query)
			require.Equal(t, test.want, got, test.query)
		})
	}
}

// The deciding value at the owner: the statement admission the action runs
// through (validateSyntax, mode syntaxStatement) and its start-clause check.
func TestPR1826B3StartKeywordDecidingValue(t *testing.T) {
	listed := map[string]bool{}
	for _, start := range validSyntaxStarts {
		listed[start] = true
	}
	t.Logf("PR1826B3START parser=%s validSyntaxStarts has SET=%t REMOVE=%t (%d entries)", config.GetParserType(), listed["SET"], listed["REMOVE"], len(validSyntaxStarts))
	for _, statement := range []string{"SET x.flag = 1", "SET p:Actor", "REMOVE x.body", "WITH $x AS x SET x.flag = 1"} {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic"))
		err := exec.validateSyntax(t.Context(), statement, false, syntaxStatement)
		t.Logf("PR1826B3START parser=%s statement=%q hasValidStartKeyword=%t validateSyntax=%v",
			config.GetParserType(), statement, hasValidStartKeyword(t.Context(), statement), err)
	}
}
