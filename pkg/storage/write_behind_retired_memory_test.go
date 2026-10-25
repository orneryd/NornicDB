package storage

import (
	"runtime"
	"testing"
	"time"
	"weak"

	"github.com/stretchr/testify/require"
)

func TestWriteBehindBuffer_RetiredGenerationCollectible(t *testing.T) {
	for _, pendingSuccessor := range []bool{false, true} {
		name := "idle"
		if pendingSuccessor {
			name = "pending_successor"
		}
		t.Run(name, func(t *testing.T) {
			w := NewWriteBehindBuffer(time.Hour, 0, func(*CommitBuffer) error { return nil })
			// Control replay directly so neither the worker nor a later apply
			// can change which generation the queue must release.
			w.Close()
			retire := func() weak.Pointer[CommitBuffer] {
				w.AppendNode(&Node{ID: "test:retired"})
				generation := weak.Make(w.active)
				w.rotate()
				if pendingSuccessor {
					w.AppendNode(&Node{ID: "test:pending"})
					w.rotate()
				}
				w.drainOne(true, nil)
				return generation
			}
			generation := retire()
			runtime.GC()
			require.Nil(t, generation.Value(), "the buffer must not keep a retired generation reachable")
			if pendingSuccessor {
				_, found, deleted := w.GetNode("test:pending")
				require.True(t, found, "the unapplied successor must remain visible")
				require.False(t, deleted)
			}
			runtime.KeepAlive(w)
		})
	}
}
