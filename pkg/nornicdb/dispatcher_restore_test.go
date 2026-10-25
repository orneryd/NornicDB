package nornicdb

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The shared graph added JSON Restore to the caller-lock inventory: its
// writes run under DB.mu, while a callback can call GetStorage on the same DB.
func TestDispatcherJSONRestoreListenerReentry(t *testing.T) {
	if os.Getenv("NORNIC_RESTORE_DISPATCH_PROBE") != "child" {
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestDispatcherJSONRestoreListenerReentry$", "-test.timeout=20s", "-test.v")
		cmd.Env = append(os.Environ(), "NORNIC_RESTORE_DISPATCH_PROBE=child")
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", out)
		return
	}
	db, err := Open(t.TempDir(), nil)
	require.NoError(t, err)
	defer db.Close()
	path := filepath.Join(t.TempDir(), "restore.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"version":"1","nodes":[{"id":"restore-listener","labels":["Doc"],"properties":{"v":1}}],"edges":[]}`), 0600))
	base := db.GetBaseStorageForManager()
	called := make(chan struct{}, 1)
	require.NoError(t, storage.SubscribeEvents(base, storage.EventHandlers{NodeCreated: func(n *storage.Node) {
		if strings.HasSuffix(string(n.ID), ":restore-listener") {
			db.GetStorage()
			called <- struct{}{}
		}
	}}))
	require.NoError(t, db.Restore(context.Background(), path))
	select {
	case <-called:
	case <-time.After(5 * time.Second):
		t.Fatal("restore callback missing")
	}
}
