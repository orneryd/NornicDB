package cypher

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type statementReadDiscoveryEngine struct {
	storage.Engine
	discoveries atomic.Int64
}

func (e *statementReadDiscoveryEngine) ReadCapabilitySource() (storage.Engine, bool) {
	e.discoveries.Add(1)
	return e.Engine, true
}

func (e *statementReadDiscoveryEngine) GetInnerEngine() storage.Engine { return e.Engine }

// Literal statements have no optional MATCH read. Metadata and the read view
// must be bound when the executor is made, rather than walking decorators for
// every statement, including every statement of an explicit transaction.
func TestEngineDecoratorsLiteralRoutingDoesNotRediscoverReadView(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "autocommit", true: "explicit_tx"}[explicit], func(t *testing.T) {
			base := newTestMemoryEngine(t)
			engine := &statementReadDiscoveryEngine{Engine: storage.NewWALEngine(storage.NewTracedEngine(storage.NewNamespacedEngine(base, "tenant")), nil)}
			exec := NewStorageExecutor(engine)
			ctx := context.Background()
			if explicit {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
			}
			engine.discoveries.Store(0)
			for i := 0; i < 3; i++ {
				result, err := exec.Execute(ctx, "RETURN 1", map[string]interface{}{"nonce": i})
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
			}
			require.Zero(t, engine.discoveries.Load(), "literal routing must not rediscover the optional read view")
		})
	}
}

func TestEngineDecoratorsBoundQueryLimitProviderKeepsLiveLimits(t *testing.T) {
	provider := &fakeLimitedEngine{Engine: storage.NewNamespacedEngine(newTestMemoryEngine(t), "tenant"), checker: fakeQueryLimitChecker{limits: fakeMaxResults(2)}}
	exec := NewStorageExecutor(storage.NewWALEngine(storage.NewTracedEngine(provider), nil))
	for _, limit := range []int{2, 3} {
		provider.checker = fakeQueryLimitChecker{limits: fakeMaxResults(limit)}
		result, err := exec.Execute(context.Background(), "UNWIND [1,2,3,4] AS x RETURN x", map[string]interface{}{"nonce": limit})
		require.NoError(t, err)
		require.Len(t, result.Rows, limit, "binding the provider must not freeze its current settings")
	}
}

func (e *statementReadDiscoveryEngine) CapabilityOwnerSource(policy storage.OwnerPolicy) (storage.Engine, bool) {
	return e.Engine, true
}
