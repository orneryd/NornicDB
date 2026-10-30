package replication

// Author: #1667 implementation lane (Codex).

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type preparationContext struct {
	context.Context
	owner       string
	after, hits int
	cancel      context.CancelFunc
}

func (c *preparationContext) Err() error {
	var pcs [32]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	for {
		frame, more := frames.Next()
		if strings.Contains(frame.Function, "/pkg/storage.") || strings.Contains(frame.Function, "/pkg/replication.") {
			if strings.HasSuffix(frame.Function, "."+c.owner) {
				c.hits++
				if c.hits >= c.after {
					c.cancel()
				}
			}
			break
		}
		if !more {
			break
		}
	}
	return c.Context.Err()
}

func TestReplicatedSubmitPreparationCancellation(t *testing.T) {
	for _, owner := range []string{"semanticTransactionWriter", "EncodeEntityTransactionRequestWithContext", "retainPreparedAuthorityWithContext"} {
		t.Run(owner, func(t *testing.T) {
			b, err := storage.NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { _ = b.Close() })
			fixture := setupTestAdapter(t, adapterFixtureOptions{backing: b, standalone: true, timeout: time.Second})
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			defer tx.Rollback()
			require.NoError(t, tx.SetNamespace("test"))
			for i := 0; i < 20; i++ {
				_, err = tx.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("test:p%d", i))})
				require.NoError(t, err)
			}
			prepared, err := tx.PrepareEntityCommit(context.Background(), storage.EntityCommitRequest{LogicalID: tx.ID})
			require.NoError(t, err)
			defer prepared.Abort()
			position, err := fixture.adapter.GetWALPosition()
			require.NoError(t, err)
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &preparationContext{Context: parent, owner: owner, after: 2, cancel: cancel}
			receipt, err := fixture.engine.SubmitEntityCommit(ctx, prepared, nil)
			if receipt != nil {
				require.NoError(t, receipt.Wait(context.Background()))
			}
			t.Logf("owner=%s hits=%d submit=%v", owner, ctx.hits, err)
			require.GreaterOrEqual(t, ctx.hits, 2)
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, receipt)
			prepared.Abort()
			after, err := fixture.adapter.GetWALPosition()
			require.NoError(t, err)
			require.Equal(t, position, after, "rejected preparation cannot enter replay")
			nodes, err := b.AllNodes()
			require.NoError(t, err)
			require.Empty(t, nodes)
		})
	}
}

func TestLeaderCommandPreparationCancellation(t *testing.T) {
	for _, owner := range []string{"decodeEntityCommandOperationsWithContext", "BeginEntityTransactionWithContext", "PrepareEntityCommand", "ApplyEntityChangeWithContext", "semanticTransactionWriter", "EncodeEntityTransactionRequestWithContext", "retainPreparedAuthorityWithContext"} {
		t.Run(owner, func(t *testing.T) {
			b, err := storage.NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { _ = b.Close() })
			fixture := setupTestAdapter(t, adapterFixtureOptions{backing: b})
			encoded := make([][]byte, 20)
			for i := range encoded {
				encoded[i], err = storage.EncodeNodePayload(&storage.Node{ID: storage.NodeID(fmt.Sprintf("test:p%d", i))})
				require.NoError(t, err)
			}
			data, err := storage.EncodeCommandPayload(encoded)
			require.NoError(t, err)
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			after := 2
			if owner == "BeginEntityTransactionWithContext" {
				after = 1
			}
			ctx := &preparationContext{Context: parent, owner: owner, after: after, cancel: cancel}
			cmd, settle, handled, err := fixture.adapter.PrepareEntityCommand(ctx, &Command{Type: CmdBulkCreateNodes, Data: data})
			if cmd != nil {
				cmd.accepted.prepared.AbortNative()
				settle(storage.CommitOutcome{State: storage.CommitDiscarded})
			}
			t.Logf("owner=%s hits=%d prepare=%v", owner, ctx.hits, err)
			require.True(t, handled)
			require.GreaterOrEqual(t, ctx.hits, after)
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, cmd)
			nodes, err := b.AllNodes()
			require.NoError(t, err)
			require.Empty(t, nodes)
		})
	}
}
