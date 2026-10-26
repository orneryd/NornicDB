// Author: Codex,NOR-180 public immutable-outcome contract,29October2026.
package cypher_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/orneryd/nornicdb/internal/testutil/walterminal"
	"github.com/orneryd/nornicdb/pkg/cypher"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestWALTerminalOutcomeValueViews(t *testing.T) {
	for _, profile := range walterminal.ContractProfiles {
		for _, parallel := range []bool{false, true} {
			for _, closed := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/parallel=%v/closed=%v", profile.Name, parallel, closed), func(t *testing.T) {
					c := walterminal.NewOutcomeContract(t, profile, closed, parallel)
					outer := walterminal.New(t)
					exec := cypher.NewStorageExecutor(storage.NewNamespacedEngine(outer, "nornic"))
					result, err := exec.Execute(context.Background(), c.Flow.Query, nil)
					c.RequireNative(t)
					if !closed {
						require.NoError(t, err)
						require.NotNil(t, result)
						return
					}
					require.Error(t, err)
					c.RequireIdentity(t, err)
					for _, view := range []error{err, fmt.Errorf("outer display: %w", err), nornicerrors.MarkStatementTimeout(fmt.Errorf("later deadline: %w", err), nornicerrors.ErrClientTransactionTimeout)} {
						code, message := nornicerrors.Neo4jStatus(view)
						c.RequireView(t, code, message)
						// Compiler-discovered direct status-interface consumers
						// must read the same frozen value as the shared mapper.
						var status interface{ BoltErrorCode() string }
						require.True(t, errors.As(view, &status))
						require.Equal(t, code, status.BoltErrorCode())
						var display interface{ StatusMessage() string }
						require.True(t, errors.As(view, &display))
						c.RequireView(t, status.BoltErrorCode(), display.StatusMessage())
					}
					healthy, err := exec.Execute(context.Background(), "RETURN 42 AS value", nil)
					require.NoError(t, err)
					require.NotNil(t, healthy)
				})
			}
		}
	}
}
