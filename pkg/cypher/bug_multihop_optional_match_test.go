package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestMultiHopOptionalMatch_ReturnsRowsForUnmatchedPattern(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	// Create a Protocol with steps
	_, err := exec.Execute(ctx, `CREATE (p:Protocol {trigger: "end session"})`, nil)
	require.NoError(t, err)

	// Create steps linked to protocol
	for i := 1; i <= 3; i++ {
		_, err = exec.Execute(ctx, fmt.Sprintf(`
			MATCH (p:Protocol {trigger: "end session"})
			CREATE (p)-[:HAS_STEP]->(s:ProtocolStep {title: "step%d"})
		`, i), nil)
		require.NoError(t, err)
	}

	// Create one SUPERSEDES relationship between step1 -> step2
	_, err = exec.Execute(ctx, `
		MATCH (s1:ProtocolStep {title: "step1"})
		MATCH (s2:ProtocolStep {title: "step2"})
		CREATE (s1)-[:SUPERSEDES]->(s2)
	`, nil)
	require.NoError(t, err)

	// Multi-hop MATCH with OPTIONAL MATCH
	res, err := exec.Execute(ctx, `
		MATCH (p:Protocol {trigger: "end session"})-[:HAS_STEP]->(s:ProtocolStep)
		OPTIONAL MATCH (s)-[:SUPERSEDES]->(newer:ProtocolStep)
		RETURN s.title, newer.title
	`, nil)
	require.NoError(t, err)
	t.Logf("Result: columns=%v rows=%v", res.Columns, res.Rows)
	require.Len(t, res.Rows, 3, "should return 3 rows (one per ProtocolStep)")
}
