package storage

// Capture panic release author: Claude (storage-format-owners lane), #1693, 30 October 2026.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A panic inside a capture must surface as that panic and release the view:
// the engine closes afterwards instead of waiting forever for the helper
// transaction, so a crash is a test failure, never a hang.
func TestCaptureReadViewPanicReleasesView(t *testing.T) {
	for _, longLived := range []bool{false, true} {
		t.Run(fmt.Sprintf("longLived=%t", longLived), func(t *testing.T) {
			b, err := NewBadgerEngineInMemory()
			require.NoError(t, err)
			recovered := func() (r any) {
				defer func() { r = recover() }()
				_, _ = b.captureReadView(context.Background(), func(*WriteBehindBuffer) error {
					panic("capture failed")
				}, readViewOptions{longLived: longLived})
				return nil
			}()
			require.Equal(t, "capture failed", recovered)
			closed := make(chan error, 1)
			go func() { closed <- b.Close() }()
			select {
			case err := <-closed:
				require.NoError(t, err)
			case <-time.After(30 * time.Second):
				t.Fatal("engine Close still waits for the view a panicking capture opened")
			}
		})
	}
}
