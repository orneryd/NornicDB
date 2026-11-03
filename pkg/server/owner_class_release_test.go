package server

// Author: Codex. Same public constructor/consumer schedules on release and head.
import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/stretchr/testify/require"
)

type classReleasePlugin struct {
	heimdall.HeimdallPlugin
	name               string
	stopGate, hookGate chan struct{}
	entered, hookDone  chan struct{}
	stops              sync.Once
	disposals          atomic.Int32
	late               error
}

func (p *classReleasePlugin) Name() string                               { return p.name }
func (p *classReleasePlugin) Type() string                               { return "heimdall" }
func (p *classReleasePlugin) Initialize(heimdall.SubsystemContext) error { return nil }
func (p *classReleasePlugin) Start() error                               { return nil }
func (p *classReleasePlugin) Actions() map[string]heimdall.ActionFunc    { return nil }
func (p *classReleasePlugin) Stop() error {
	if p.stopGate != nil {
		p.stops.Do(func() { close(p.entered) })
		<-p.stopGate
	}
	return nil
}
func (p *classReleasePlugin) Shutdown() error { p.disposals.Add(1); return nil }
func (p *classReleasePlugin) PreExecute(_ *heimdall.PreExecuteContext, done func(heimdall.PreExecuteResult)) {
	close(p.entered)
	go func() {
		<-p.hookGate
		done(heimdall.PreExecuteResult{Continue: true, Error: p.late})
		close(p.hookDone)
	}()
}
func classReleaseHost(t *testing.T) *Server {
	t.Helper()
	heimdall.ResetSubsystemManagerForTests()
	t.Cleanup(heimdall.ResetSubsystemManagerForTests)
	s, _ := setupTestServerWithConfig(t, func(c *Config) { c.PluginsDir = t.TempDir(); c.HeimdallPluginsDir = "" })
	return s
}
func classAwait(t *testing.T, c <-chan struct{}) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(5 * time.Second):
		t.Fatal("public class schedule did not reach its controlled boundary")
	}
}

func TestOwnerClassHTTPReleaseDeadline(t *testing.T) {
	s := classReleaseHost(t)
	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	p := &classReleasePlugin{name: "class_http", stopGate: gate, entered: make(chan struct{})}
	require.NoError(t, heimdall.GetSubsystemManager().RegisterPlugin(p, "", true))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	stopped := make(chan error, 1)
	go func() { stopped <- s.Stop(ctx) }()
	classAwait(t, p.entered)
	var result error
	received := false
	select {
	case result = <-stopped:
		received = true
		require.ErrorIs(t, result, context.DeadlineExceeded)
	case <-time.After(300 * time.Millisecond):
		t.Error("Stop waited for foreign plugin cleanup past its caller deadline")
	}
	release()
	if !received {
		result = <-stopped
	}
	if w, ok := any(s).(interface{ WaitStopped(context.Context) error }); ok {
		require.NoError(t, w.WaitStopped(context.Background()))
	}
	require.EqualValues(t, 1, p.disposals.Load())
}
func TestOwnerClassPluginReleaseActualDone(t *testing.T) {
	s := classReleaseHost(t)
	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	p := &classReleasePlugin{name: "class_async", hookGate: gate, entered: make(chan struct{}), hookDone: make(chan struct{}), late: errors.New("class late plugin outcome")}
	require.NoError(t, heimdall.GetSubsystemManager().RegisterPlugin(p, "", true))
	wrapper := make(chan struct{})
	go func() {
		heimdall.CallPreExecuteHooks(&heimdall.PreExecuteContext{Context: context.Background()})
		close(wrapper)
	}()
	classAwait(t, p.entered)
	select {
	case <-wrapper:
	case <-time.After(6 * time.Second):
		release()
		t.Fatal("public hook wrapper did not preserve its existing timeout")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_ = s.Stop(ctx)
	require.Zero(t, p.disposals.Load(), "plugin disposed while retained done callback still owned it")
	release()
	classAwait(t, p.hookDone)
	if w, ok := any(s).(interface{ WaitStopped(context.Context) error }); ok {
		require.ErrorIs(t, w.WaitStopped(context.Background()), p.late)
	}
	require.EqualValues(t, 1, p.disposals.Load())
}
