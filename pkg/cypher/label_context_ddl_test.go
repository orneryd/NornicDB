package cypher

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

type ddlPreparationContext struct {
	context.Context
	owner        string
	cypherOwner  bool
	ownerPackage string
	via          string
	trace        map[string]bool
	hits         int
	after        int
	cancel       context.CancelFunc
}

func (c *ddlPreparationContext) Err() error {
	var pcs [64]uintptr
	n := runtime.Callers(2, pcs[:])
	if n > 0 {
		frames := runtime.CallersFrames(pcs[:n])
		matched, firstStorage, via := false, false, c.via == ""
		for {
			frame, more := frames.Next()
			// A wrapper's own preparation is observed only before entering a
			// lower storage owner; a callee observation cannot impersonate it.
			if c.cypherOwner && !firstStorage && strings.Contains(frame.Function, "/pkg/cypher.") && strings.HasSuffix(frame.Function, c.owner) {
				matched = true
				break
			}
			if c.ownerPackage != "" && !firstStorage && strings.Contains(frame.Function, c.ownerPackage) && strings.HasSuffix(frame.Function, c.owner) {
				matched = true
				break
			}
			// Execute wraps the context. Select the innermost storage owner,
			// so a scan beneath validation cannot impersonate its collection.
			if !firstStorage && strings.Contains(frame.Function, "/pkg/storage.") {
				if c.trace != nil {
					c.trace[frame.Function] = true
				}
				firstStorage = true
				matched = strings.HasSuffix(frame.Function, "."+c.owner) || strings.Contains(c.owner, ")") && strings.HasSuffix(frame.Function, c.owner) || strings.Contains(frame.Function, "/pkg/storage."+c.owner+"[")
			}
			via = via || strings.Contains(frame.Function, c.via)
			if !more {
				break
			}
		}
		if matched && via {
			c.hits++
			after := c.after
			if after == 0 {
				after = 2
			}
			if c.hits == after {
				c.cancel()
			}
		}
	}
	return c.Context.Err()
}

func TestLabelStatementDDLPreparationCancellation(t *testing.T) {
	for _, tc := range []struct{ owner, requirement string }{
		{"validateUniqueConstraintOnCreationWithEngineWithContext", "n.record IS UNIQUE"},
		{"validateNodeKeyConstraintOnCreationWithEngineWithContext", "(n.record, n.start) IS NODE KEY"},
		{"validateExistenceConstraintOnCreationWithEngineWithContext", "n.record IS NOT NULL"},
		{"ValidatePropertyTypeConstraintOnCreationForEngineWithContext", "n.record IS :: INTEGER"},
		{"validateTemporalConstraintOnCreationWithEngineWithContext", "(n.record, n.start, n.end) IS TEMPORAL NO OVERLAP"},
		{"backfillTemporalIndexForLabelWithContext.func2", "(n.record, n.start, n.end) IS TEMPORAL NO OVERLAP"},
	} {
		t.Run(tc.owner, func(t *testing.T) {
			b, err := storage.NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			defer b.Close()
			for i := 0; i < 4; i++ {
				_, err = b.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("test:%d", i)), Labels: []string{"Fact"}, Properties: map[string]interface{}{"record": int64(i), "start": time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), "end": time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)}})
				require.NoError(t, err)
			}
			e := NewStorageExecutor(storage.NewNamespacedEngine(b, "test"))
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &ddlPreparationContext{Context: parent, owner: tc.owner, cancel: cancel}
			_, err = e.Execute(ctx, "CREATE CONSTRAINT canceled FOR (n:Fact) REQUIRE "+tc.requirement, nil)
			require.GreaterOrEqual(t, ctx.hits, 2, "public DDL must enter the collection/body owner: %v", err)
			require.ErrorIs(t, err, context.Canceled)
		})
	}
}
