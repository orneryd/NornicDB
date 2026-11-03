package heimdall

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// heldGenerator blocks inside Generate until released and records whether its
// Close ran while a call was still inside it.
type heldGenerator struct {
	entered, release chan struct{}
	inside           atomic.Int32
	closedDuringUse  atomic.Bool
	closes           atomic.Int32
}

func (g *heldGenerator) Generate(ctx context.Context, prompt string, params GenerateParams) (string, error) {
	g.inside.Add(1)
	defer g.inside.Add(-1)
	close(g.entered)
	<-g.release
	return "ok", nil
}
func (g *heldGenerator) GenerateStream(ctx context.Context, prompt string, params GenerateParams, callback func(string) error) error {
	return nil
}
func (g *heldGenerator) Close() error {
	if g.inside.Load() > 0 {
		g.closedDuringUse.Store(true)
	}
	g.closes.Add(1)
	return nil
}
func (g *heldGenerator) ModelPath() string { return "held" }

func newHeldManager(t *testing.T) (*Manager, *heldGenerator) {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "held.gguf"), []byte("fake"), 0o644))
	gen := &heldGenerator{entered: make(chan struct{}), release: make(chan struct{})}
	orig := SetGeneratorLoader(func(string, int, int, int, int) (Generator, error) { return gen, nil })
	t.Cleanup(func() { SetGeneratorLoader(orig) })
	manager, err := NewManager(Config{Enabled: true, ModelsDir: dir, Model: "held"})
	require.NoError(t, err)
	require.NotNil(t, manager)
	return manager, gen
}

// Shutdown matrix S1, standalone heimdall Manager (the be90 F4 reproduction):
// Close on a manager built by NewManager, with no host lifetime bound, waits
// for an accepted Generate and closes the generator only after it returned.
func TestOwnerMatrixHeimdallManagerCloseJoinsHeldGenerate(t *testing.T) {
	manager, gen := newHeldManager(t)
	generated := make(chan error, 1)
	go func() {
		_, err := manager.Generate(context.Background(), "prompt", GenerateParams{})
		generated <- err
	}()
	select {
	case <-gen.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("Generate did not reach the generator")
	}
	closed := make(chan error, 1)
	go func() { closed <- manager.Close() }()
	select {
	case err := <-closed:
		close(gen.release)
		t.Fatalf("Close returned while Generate was inside the generator: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	require.Zero(t, gen.closes.Load(), "generator closed while a Generate was accepted")
	_, err := manager.Generate(context.Background(), "late", GenerateParams{})
	require.Error(t, err, "Generate after Close began must be refused")
	close(gen.release)
	require.NoError(t, <-generated)
	require.NoError(t, <-closed)
	require.False(t, gen.closedDuringUse.Load(), "generator closed during an accepted call")
	require.EqualValues(t, 1, gen.closes.Load())
}

// Shutdown matrix S2, heimdall Manager: concurrent and repeated Close callers
// join the one stop and its outcome; the generator closes exactly once.
func TestOwnerMatrixHeimdallManagerConcurrentClose(t *testing.T) {
	manager, gen := newHeldManager(t)
	go func() { _, _ = manager.Generate(context.Background(), "prompt", GenerateParams{}) }()
	select {
	case <-gen.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("Generate did not reach the generator")
	}
	const callers = 4
	results := make(chan error, callers)
	for i := 0; i < callers; i++ {
		go func() { results <- manager.Close() }()
	}
	select {
	case err := <-results:
		close(gen.release)
		t.Fatalf("a Close caller returned while Generate was held: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(gen.release)
	for i := 0; i < callers; i++ {
		require.NoError(t, <-results)
	}
	require.NoError(t, manager.Close())
	require.EqualValues(t, 1, gen.closes.Load())
}
