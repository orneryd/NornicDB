package cypher

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/internal/callinputtrace"
	"github.com/orneryd/nornicdb/internal/statementfactstest"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Observe actual consumer inputs, including unused names, instead of inferring
// import agreement from the value of the one column these queries return.
func TestCallInputConsumerConsistency(t *testing.T) {
	exec := fabricDispatchRemoteExecutor(t)
	exec.semanticValidationCache, exec.matchSemanticValidationCache, exec.mergeSemanticValidationCache = nil, nil, nil
	seen := map[string]int{}
	var mu sync.Mutex
	ctx := callinputtrace.WithObserver(context.Background(), func(consumer string, branch resultmetadata.Statement, actual []string) {
		mu.Lock()
		defer mu.Unlock()
		branch.Incoming.Visit(func(expected []string) {
			assert.ElementsMatch(t, expected, actual, "%s input at %d:%d", consumer, branch.Start, branch.End)
		}, func() { t.Fatal("unknown canonical input") })
		seen[consumer]++
	})
	for _, sample := range statementfactstest.CallInputCases() {
		t.Run(sample.Name, func(t *testing.T) {
			before := map[string]int{}
			for consumer, count := range seen {
				before[consumer] = count
			}
			query := fabricSingleGraphStatement(ctx, sample.Query)
			program := exec.statementProjectionProgram(ctx, query, resultmetadata.KnownScope(nil), false, nil)
			require.True(t, program.complete, "%v", program.err)
			var inspect func(resultmetadata.Statement)
			inspect = func(statement resultmetadata.Statement) {
				for _, branch := range statement.Branches {
					inspect(branch)
				}
				for _, clause := range statement.Clauses {
					if clause.Call == nil {
						continue
					}
					body := clause.Call.Statement
					branches := body.Branches
					if len(branches) == 0 {
						branches = []resultmetadata.Statement{*body}
					}
					for _, branch := range branches {
						branch.Incoming.Visit(func(names []string) {
							require.NotEmpty(t, branch.Clauses)
							branch.Clauses[0].Before.Visit(func(actual []string) { require.ElementsMatch(t, names, actual, "compiled branch entry") }, func() { t.Fatal("unknown compiled branch entry") })
							if strings.Contains(sample.Name, "/none/") {
								require.Empty(t, names)
							}
							if (strings.Contains(sample.Name, "/depth=") || strings.Contains(sample.Name, "/review/")) && !strings.Contains(sample.Name, "/none/") {
								require.Contains(t, names, "value")
							}
							if strings.Contains(sample.Name, "/implicit/") || strings.Contains(sample.Name, "/legacy/") || strings.Contains(sample.Name, "/listed/") {
								require.NotContains(t, names, "unused")
							}
							seen["compiled-facts"]++
						}, func() { t.Fatal("unknown compiled input") })
					}
					inspect(*body)
				}
			}
			inspect(*program.statement)
			// The local program, admission and type walk consume the same corpus.
			local, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err)
			require.Equal(t, sample.Columns, local.Columns)
			// Its Fabric counterpart exercises real remote serialization, not a
			// helper that merely returns the expected canonical names.
			remote, err := exec.Execute(ctx, sample.Query, nil)
			require.NoError(t, err)
			require.Equal(t, sample.Columns, remote.Columns)
			require.Len(t, remote.Rows, len(local.Rows))
			for i := range local.Rows {
				require.EqualValues(t, local.Rows[i], remote.Rows[i])
			}
			for _, consumer := range []string{"compiled-facts", "admission", "typing"} {
				require.Greater(t, seen[consumer], before[consumer], "consumer not reached for this shape: %s", consumer)
			}
			if !strings.Contains(sample.Name, "empty=true") {
				require.Greater(t, seen["local-execution"], before["local-execution"], "local CALL body not observed")
				if strings.Contains(sample.Query, "comp.remote") {
					require.Greater(t, seen["fabric-transport"], before["fabric-transport"], "remote CALL inputs not observed")
				}
			}

		})
	}
	for _, consumer := range []string{"compiled-facts", "admission", "typing", "local-execution", "fabric-transport"} {
		require.Positive(t, seen[consumer], "consumer never exercised: %s", consumer)
	}
}
