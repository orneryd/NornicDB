package storage

// Author: Codex (#1636 lane). A public controller callback uses an ordinary
// public host read. Its child read is only a controlled escape from an old lock
// cycle; every child is joined after Close releases the host publication lock.

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type classReentrantController struct {
	MVCCLifecycleController
	engine  *BadgerEngine
	blocked []bool
	readers []<-chan struct{}
}

func (c *classReentrantController) StopLifecycle() {
	done := make(chan struct{})
	c.readers = append(c.readers, done)
	go func() { defer close(done); _, _ = c.engine.NodeCount() }()
	select {
	case <-done:
		c.blocked = append(c.blocked, false)
	case <-time.After(100 * time.Millisecond):
		c.blocked = append(c.blocked, true)
	}
}

func TestOwnerClassStorageRetirementCallbackReentry(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir()}))
	require.NoError(t, err)
	controller := &classReentrantController{engine: engine}
	engine.SetLifecycleController(controller)
	require.NoError(t, engine.Close())
	require.NotEmpty(t, controller.blocked)
	defer func() {
		for _, done := range controller.readers {
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("controlled callback reader did not unwind after host Close")
			}
		}
	}()
	for index, blocked := range controller.blocked {
		require.False(t, blocked, "controller stop callback %d could not reenter its host", index)
	}
}
