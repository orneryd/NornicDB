package cypher

import (
	"context"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type grantBarrierEngine struct {
	storage.Engine
	entered, release chan struct{}
}

func (e *grantBarrierEngine) WithStoreTransaction(ctx context.Context, mutate func(storage.Engine) error) error {
	return auth.WithStoreTransaction(ctx, e.Engine, func(view storage.Engine) error {
		return mutate(&grantBarrierEngine{Engine: view, entered: e.entered, release: e.release})
	})
}

func (e *grantBarrierEngine) CreateNode(node *storage.Node) (storage.NodeID, error) {
	if strings.HasPrefix(string(node.ID), "proc_priv:") {
		close(e.entered)
		select {
		case <-e.release:
		case <-time.After(10 * time.Second):
			return "", context.DeadlineExceeded
		}
	}
	return e.Engine.CreateNode(node)
}

func TestGrantsAndRoleChangesSerialize(t *testing.T) {
	for _, rename := range []bool{false, true} {
		t.Run(map[bool]string{false: "delete", true: "rename"}[rename], func(t *testing.T) {
			ctx := context.Background()
			system := storage.NewNamespacedEngine(newTestMemoryEngine(t), "system")
			roles := auth.NewRoleStore(system)
			require.NoError(t, roles.CreateRole(ctx, "ops"))
			barrier := &grantBarrierEngine{Engine: system, entered: make(chan struct{}), release: make(chan struct{})}
			exec := NewStorageExecutor(barrier)
			done := make(chan error, 1)
			go func() {
				_, err := exec.Execute(ctx, "GRANT EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS TO ops", nil)
				done <- err
			}()
			select {
			case <-barrier.entered:
			case err := <-done:
				t.Fatalf("grant did not reach its write: %v", err)
			case <-time.After(10 * time.Second):
				t.Fatal("grant did not reach barrier")
			}
			var err error
			if rename {
				err = roles.RenameRole(ctx, "ops", "renamed")
			} else {
				err = roles.DeleteRole(ctx, "ops")
			}
			close(barrier.release)
			require.NoError(t, err)
			require.Error(t, <-done, "the grant's role snapshot must conflict with the role change")
			require.NoError(t, roles.CreateRole(ctx, "ops"))
			allowed, err := auth.NewProcedurePrivilegeStore(system).AllowsBoosted([]string{"ops"}, "db.cdc.query")
			require.NoError(t, err)
			require.False(t, allowed)
		})
	}
}

// A GRANT racing a role delete or rename, with no barrier, never leaves the
// grant behind: either the GRANT or the role change fails, or the grant
// moved with the role. A role created later with the old name starts
// without it. NORNICDB_GRANT_RACE_ITERATIONS raises the iteration count.
func TestGrantRacingRoleChangeNeverLeavesTheGrant(t *testing.T) {
	iterations := 200
	if raw := os.Getenv("NORNICDB_GRANT_RACE_ITERATIONS"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		require.NoError(t, err)
		iterations = parsed
	}
	ctx := context.Background()
	for i := 0; i < iterations; i++ {
		rename := i%2 == 1
		system := storage.NewNamespacedEngine(newTestMemoryEngine(t), "system")
		roles := auth.NewRoleStore(system)
		require.NoError(t, roles.CreateRole(ctx, "ops"))
		exec := NewStorageExecutor(system)
		var wg sync.WaitGroup
		var changeErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = exec.Execute(ctx, "GRANT EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS TO ops", nil)
		}()
		go func() {
			defer wg.Done()
			if rename {
				changeErr = roles.RenameRole(ctx, "ops", "renamed")
			} else {
				changeErr = roles.DeleteRole(ctx, "ops")
			}
		}()
		wg.Wait()
		fresh := auth.NewRoleStore(system)
		require.NoError(t, fresh.Load(ctx))
		if !fresh.Exists("ops") {
			require.NoError(t, fresh.CreateRole(ctx, "ops"))
		}
		allowed, err := auth.NewProcedurePrivilegeStore(system).AllowsBoosted([]string{"ops"}, "db.cdc.query")
		require.NoError(t, err)
		require.False(t, allowed && changeErr == nil, "iteration %d (rename=%v): the role named ops after a successful role change holds the grant", i, rename)
	}
}
