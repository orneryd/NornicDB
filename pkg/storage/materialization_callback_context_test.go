package storage

// Author: #1667 implementation lane (Codex).

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Observe the actual copier only while the selected public materialization
// stage is on the stack. An earlier native read or the outer collection's own
// checkpoint cannot supply credit for the nested image preparation.
type callbackImageContext struct {
	context.Context
	cancel context.CancelFunc
	stage  string
	copier string
	hits   int
}

func (c *callbackImageContext) Err() error {
	var pcs [48]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	first, more := frames.Next()
	copier := c.copier
	if copier == "" {
		copier = "copyNodeWithoutEmbeddingsWithContext"
	}
	if strings.HasSuffix(first.Function, "."+copier) {
		for more {
			frame, next := frames.Next()
			more = next
			if strings.Contains(frame.Function, c.stage) {
				c.hits++
				if c.hits >= 3 {
					c.cancel()
				}
				break
			}
		}
	}
	return c.Context.Err()
}

func TestContextualReadImageOwner(t *testing.T) {
	for _, kind := range []string{"cached-label", "batch", "pending-edge"} {
		t.Run(kind, func(t *testing.T) {
			b, err := NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, b.Close()) })
			properties := map[string]any{}
			for i := 0; i < 20; i++ {
				properties[fmt.Sprintf("property%d", i)] = int64(i)
			}
			for _, id := range []NodeID{"test:a", "test:b"} {
				_, err = b.CreateNode(&Node{ID: id, Labels: []string{"CopyOwner"}, Properties: properties})
				require.NoError(t, err)
			}
			_, err = b.GetNode("test:a")
			require.NoError(t, err)
			parent, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			ctx := &callbackImageContext{Context: parent, cancel: cancel}
			switch kind {
			case "cached-label":
				ctx.stage = ".getNodesByLabelInScopeWithContext"
				rows, readErr := b.GetNodesByLabelWithContext(ctx, "CopyOwner")
				t.Logf("observations=%d rows=%d error=%v", ctx.hits, len(rows), readErr)
				require.GreaterOrEqual(t, ctx.hits, 3)
				require.ErrorIs(t, readErr, context.Canceled)
				require.Empty(t, rows)
			case "batch":
				ctx.stage = ".readNodeBatch"
				rows, readErr := b.BatchGetNodesWithContext(ctx, []NodeID{"test:a", "test:b"})
				t.Logf("observations=%d rows=%d error=%v", ctx.hits, len(rows), readErr)
				require.GreaterOrEqual(t, ctx.hits, 3)
				require.ErrorIs(t, readErr, context.Canceled)
				require.Empty(t, rows)
			case "pending-edge":
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				defer tx.Rollback()
				require.NoError(t, tx.CreateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "COPY", Properties: properties}))
				ctx.stage, ctx.copier = ".streamEdgesByType", "copyEdgeWithContext"
				visits := 0
				readErr := tx.StreamEdgesByType(ctx, "COPY", func(*Edge) error { visits++; return nil })
				t.Logf("observations=%d visits=%d error=%v", ctx.hits, visits, readErr)
				require.GreaterOrEqual(t, ctx.hits, 3)
				require.ErrorIs(t, readErr, context.Canceled)
				require.Zero(t, visits)
			}
		})
	}
}

func TestMaterializationCallbackContext(t *testing.T) {
	for _, stage := range []string{".mergePendingLockedWithContext", ".toUserNodeBatch"} {
		t.Run(stage, func(t *testing.T) {
			b, err := NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, b.Close()) })
			node := &Node{ID: "test:node", Labels: []string{"ProjectionCandidate"}, Properties: map[string]any{}}
			for i := 0; i < 20; i++ {
				node.Properties[fmt.Sprintf("property%d", i)] = int64(i)
			}
			parent, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			ctx := &callbackImageContext{Context: parent, cancel: cancel, stage: stage}
			if strings.Contains(stage, "mergePending") {
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				defer tx.Rollback()
				_, err = tx.CreateNode(node)
				require.NoError(t, err)
				rows, readErr := tx.GetNodesByLabelWithContext(ctx, "ProjectionCandidate")
				t.Logf("stage=%s observations=%d rows=%d error=%v", stage, ctx.hits, len(rows), readErr)
				require.GreaterOrEqual(t, ctx.hits, 3)
				require.ErrorIs(t, readErr, context.Canceled)
				require.Empty(t, rows)
			} else {
				node.ID = "tenant:node"
				_, err = b.CreateNode(node)
				require.NoError(t, err)
				n := NewNamespacedEngine(b, "tenant")
				rows, readErr := n.BatchGetNodesWithContext(ctx, []NodeID{"node"})
				t.Logf("stage=%s observations=%d rows=%d error=%v", stage, ctx.hits, len(rows), readErr)
				require.GreaterOrEqual(t, ctx.hits, 3)
				require.ErrorIs(t, readErr, context.Canceled)
				require.Empty(t, rows)
			}
		})
	}
}
