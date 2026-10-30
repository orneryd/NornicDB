package storage

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBulkDeleteSharedRoutingPolicy(t *testing.T) {
	for _, capture := range []bool{false, true} {
		for _, node := range []bool{false, true} {
			for _, empty := range []bool{false, true} {
				t.Run(fmt.Sprintf("capture=%v/node=%v/empty=%v", capture, node, empty), func(t *testing.T) {
					b := newTestEngine(t)
					require.NoError(t, b.BulkCreateNodes([]*Node{{ID: "test:a"}, {ID: "test:b"}}))
					if !node {
						require.NoError(t, b.CreateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "R"}))
					}
					if capture {
						b.SetChangeCapture("test", ChangeCaptureFull)
					}
					var err error
					if node {
						ids := []NodeID{"test:a", "test:a", "test:missing"}
						if empty {
							ids = append(ids, "")
						}
						err = b.BulkDeleteNodes(ids)
					} else {
						ids := []EdgeID{"test:r", "test:r", "test:missing"}
						if empty {
							ids = append(ids, "")
						}
						err = b.BulkDeleteEdges(ids)
					}
					rejected := capture && empty
					if rejected {
						require.ErrorIs(t, err, ErrInvalidID)
					} else {
						require.NoError(t, err)
					}
					if node {
						_, err = b.GetNode("test:a")
					} else {
						_, err = b.GetEdge("test:r")
					}
					if rejected {
						require.NoError(t, err, "capture input validation rolls back the earlier staged deletion")
					} else {
						require.ErrorIs(t, err, ErrNotFound)
					}
					if capture {
						events, e := b.ChangeEvents("test", 0, 0, 0)
						require.NoError(t, e)
						if rejected {
							require.Empty(t, events)
						} else {
							require.Len(t, events, 1)
							require.Equal(t, byte('d'), events[0].Operation)
						}
					}
				})
			}
		}
	}
}
