//go:build cyphernames

package bolt

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDecodedNameUIBoltTransport(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	store := storage.NewNamespacedEngine(base, "nornic")
	direct, err := cypher.NewStorageExecutor(store).Execute(context.Background(), "RETURN {` `:7, nested:{`  `:8, `\t`:9}} AS value", nil)
	require.NoError(t, err)
	require.Contains(t, direct.Rows[0][0].(map[string]interface{}), " ", "the public executor emits the decoded key before Bolt hydration")
	_, port := startBoltIntegrationServer(t, store)
	root, err := filepath.Abs("../..")
	require.NoError(t, err)
	input, err := json.Marshal(map[string]interface{}{"boltUri": fmt.Sprintf("bolt://127.0.0.1:%d", port)})
	require.NoError(t, err)
	file := filepath.Join(t.TempDir(), "ui-bolt.json")
	require.NoError(t, os.WriteFile(file, input, 0600))
	command := exec.Command("python3", filepath.Join(root, "internal/testutil/cyphernames/run_ui_identity.py"), file)
	output, runErr := command.Output()
	if exit, ok := runErr.(*exec.ExitError); ok {
		t.Log(string(exit.Stderr))
	}
	var observed struct {
		Failures []struct{ Name, Message string }
	}
	require.NoError(t, json.Unmarshal(output, &observed), string(output))
	for _, failure := range observed.Failures {
		t.Errorf("UI transport %s: %s", failure.Name, failure.Message)
	}
	require.NoError(t, runErr)
}
