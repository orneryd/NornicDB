package heimdall

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// shutdownRecordingPlugin records whether Shutdown ran while an action held
// its callback ticket.
type shutdownRecordingPlugin struct {
	*coverageMinimalPlugin
	actionActive *atomic.Bool
	shutdowns    atomic.Int32
	duringAction atomic.Bool
}

func (p *shutdownRecordingPlugin) Shutdown() error {
	if p.actionActive.Load() {
		p.duringAction.Store(true)
	}
	p.shutdowns.Add(1)
	return nil
}

// Shutdown matrix S1, SubsystemManager with no host lifetime (the be90 F3
// reproduction): ShutdownAll joins an accepted action before disposing the
// plugins, and no replacement owner exists while the old one is stopping.
func TestOwnerMatrixManagerOwnedLifetimeJoinsBeforeDispose(t *testing.T) {
	ResetSubsystemManagerForTests()
	t.Cleanup(ResetSubsystemManagerForTests)
	m := GetSubsystemManager()

	var active atomic.Bool
	plugin := &shutdownRecordingPlugin{coverageMinimalPlugin: newCoverageMinimalPlugin("f3-owned"), actionActive: &active}
	require.NoError(t, m.RegisterPlugin(plugin, "", false))

	entered, release := make(chan struct{}), make(chan struct{})
	RegisterBuiltinAction(ActionFunc{Name: "matrix.f3.hold", Handler: func(ActionContext) (*ActionResult, error) {
		active.Store(true)
		close(entered)
		<-release
		active.Store(false)
		return &ActionResult{}, nil
	}})
	acted := make(chan error, 1)
	go func() {
		_, err := ExecuteAction("matrix.f3.hold", ActionContext{Context: context.Background()})
		acted <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("the action did not start")
	}

	m.mu.RLock()
	owned := m.owned
	m.mu.RUnlock()
	require.NotNil(t, owned, "the manager owns its callback lifetime")

	m.CloseAdmission()
	require.NoError(t, m.StopAll())
	shut := make(chan error, 1)
	go func() { shut <- m.ShutdownAll() }()
	select {
	case err := <-shut:
		close(release)
		t.Fatalf("ShutdownAll returned while an accepted action was running: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	require.Zero(t, plugin.shutdowns.Load(), "plugins were disposed while an accepted action was running")
	require.Same(t, owned, m.lifetime(), "a replacement owner exists before the old one stopped")

	close(release)
	require.NoError(t, <-acted)
	select {
	case err := <-shut:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("ShutdownAll did not complete after the action returned")
	}
	require.EqualValues(t, 1, plugin.shutdowns.Load())
	require.False(t, plugin.duringAction.Load())
}
