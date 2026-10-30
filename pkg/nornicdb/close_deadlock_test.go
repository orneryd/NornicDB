package nornicdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDBClose_DoesNotDeadlock(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 3
	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	done := make(chan struct{})
	go func() {
		_ = db.Close()
		close(done)
	}()

	select {
	case <-done:
		// ok
	case <-time.After(5 * time.Second):
		t.Fatal("db.Close() deadlocked or hung")
	}
}
