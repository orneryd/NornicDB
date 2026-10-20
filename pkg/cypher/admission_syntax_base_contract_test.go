package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

// Diagnostic completion is not a parity verdict. Keep every returned value or
// error so supported extensions can be distinguished from grammar limitations.
func TestAdmissionSyntaxBaseContract(t *testing.T) {
	for _, query := range []string{
		"MATCH (n:AdmissionBase WHERE all(x IN [1] WHERE x=1)) RETURN count(*) AS value",
		"MATCH (n:AdmissionBase)-[r:P WHERE any(x IN [1] WHERE x=r.w)]->(m) RETURN count(*) AS value",
		"MATCH ((n:AdmissionBase)-[r:P]->(m) WHERE all(x IN [r] WHERE x.w=1)){1,2} RETURN count(*) AS value",
		"MATCH (n:AdmissionBase {id:head([x IN [1,2] WHERE x=1 | x])}) RETURN count(*) AS value",
	} {
		executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "admissionbase"))
		_, err := executor.Execute(context.Background(), "CREATE (a:AdmissionBase {id:1})-[:P {w:1}]->(b:AdmissionBase {id:2})", nil)
		require.NoError(t, err)
		result, err := executor.Execute(context.Background(), query, nil)
		if err != nil {
			t.Logf("BASE_CONTRACT query=%q error=%v", query, err)
		} else {
			t.Logf("BASE_CONTRACT query=%q columns=%v rows=%v", query, result.Columns, result.Rows)
		}
	}
}
