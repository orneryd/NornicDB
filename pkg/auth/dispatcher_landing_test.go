package auth

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDispatcherAuthMutationLanding(t *testing.T) {
	if os.Getenv("NORNIC_AUTH_DISPATCH_PROBE") != "child" {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestDispatcherAuthMutationLanding$", "-test.timeout=12s", "-test.v")
		cmd.Env = append(os.Environ(), "NORNIC_AUTH_DISPATCH_PROBE=child")
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", out)
		return
	}
	b, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour})
	require.NoError(t, err)
	defer b.Close()
	a, err := NewAuthenticator(AuthConfig{SecurityEnabled: false, BcryptCost: 4}, storage.NewNamespacedEngine(b, "system"))
	require.NoError(t, err)
	_, err = a.CreateUser("dispatcher", "password123", []Role{RoleAdmin})
	require.NoError(t, err)
	called := make(chan struct{}, 1)
	b.OnNodeCreated(func(n *storage.Node) {
		if n.ID == "test:auth-trigger" {
			_, getErr := a.GetUser("dispatcher")
			require.NoError(t, getErr)
			called <- struct{}{}
		}
	})
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	_, err = tx.CreateNode(&storage.Node{ID: "test:auth-trigger"})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	require.NoError(t, a.UpdateRolesAndStatus(context.Background(), "dispatcher", []Role{RoleEditor}, nil))
	require.NoError(t, b.FlushWriteBehind())
	<-called
}
