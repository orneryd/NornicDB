package multidb

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type catalogPreparationGate struct {
	*storage.MemoryEngine
	target          string
	entered, resume chan struct{}
}

func (g *catalogPreparationGate) PublishNamespaceCatalogContext(ctx context.Context, mutation storage.NamespaceCatalogMutation) error {
	if mutation.Namespace == g.target {
		close(g.entered)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-g.resume:
		}
	}
	return g.MemoryEngine.PublishNamespaceCatalogContext(ctx, mutation)
}

func TestNamespaceCatalogPreparationsReserveLimitsAndAliases(t *testing.T) {
	for _, kind := range []string{"create-limit", "alias"} {
		t.Run(kind, func(t *testing.T) {
			gate := &catalogPreparationGate{MemoryEngine: storage.NewMemoryEngine(), entered: make(chan struct{}), resume: make(chan struct{})}
			defer func() {
				select {
				case <-gate.resume:
				default:
					close(gate.resume)
				}
			}()
			config := DefaultConfig()
			if kind == "create-limit" {
				config.MaxDatabases = 3
			}
			manager, err := NewDatabaseManager(gate, config)
			require.NoError(t, err)
			defer manager.Close()
			if kind == "alias" {
				require.NoError(t, manager.CreateDatabase("reserve_a"))
				require.NoError(t, manager.CreateDatabase("reserve_b"))
			}
			gate.target = "reserve_a"
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			if kind == "create-limit" {
				go func() { result <- manager.CreateDatabaseContext(ctx, "reserve_a") }()
			} else {
				go func() { result <- manager.CreateAlias("reserved_alias", "reserve_a") }()
			}
			select {
			case <-gate.entered:
			case <-time.After(time.Second):
				t.Fatal("catalog preparation did not enter its storage wait")
			}
			if kind == "create-limit" {
				require.False(t, manager.Exists("reserve_a"), "preparation is private")
				require.ErrorIs(t, manager.CreateDatabase("reserve_b"), ErrMaxDatabasesReached)
				cancel()
				require.ErrorIs(t, <-result, context.Canceled)
				require.NoError(t, manager.CreateDatabase("reserve_b"), "aborted preparation releases its capacity")
			} else {
				require.ErrorIs(t, manager.CreateAlias("reserved_alias", "reserve_b"), ErrAliasExists)
				close(gate.resume)
				require.NoError(t, <-result)
				actual, err := manager.ResolveDatabase("reserved_alias")
				require.NoError(t, err)
				require.Equal(t, "reserve_a", actual)
			}
		})
	}
}
