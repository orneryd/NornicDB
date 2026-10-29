package cypher

import (
	"context"
	"encoding/json"
	"os/exec"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNameEmissionGraphifyStoredType(t *testing.T) {
	output, err := exec.Command("python3", "../../internal/testutil/cyphernames/graphify_probe.py").CombinedOutput()
	require.NoError(t, err, string(output))
	var statements []runtimeNameSubject
	require.NoError(t, json.Unmarshal(output, &statements), string(output))
	require.Len(t, statements, 1)
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "graphify_stored"))
	ctx := context.Background()
	_, err = e.Execute(ctx, "CREATE (a:Code {id:'a', repo:'orneryd/NornicDB', props_hash:'x'}), (b:Code {id:'b', repo:'orneryd/NornicDB', props_hash:'x'}) CREATE (a)-[:A]->(b), (a)-[:B]->(b), (a)-[:`A|B`]->(b)", nil)
	require.NoError(t, err)
	_, err = e.Execute(ctx, statements[0].Text, statements[0].Parameters)
	require.NoError(t, err, statements[0].Text)
	r, err := e.Execute(ctx, "MATCH ()-[r]->() RETURN type(r) ORDER BY type(r)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]any{{"A"}, {"B"}}, r.Rows, statements[0].Text)
	require.Contains(t, statements[0].Text, "[r:`A|B`]")
}
