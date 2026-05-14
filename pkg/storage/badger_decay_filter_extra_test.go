package storage

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// stubAccessAccumulator implements accessIncrementor for the
// SetAccessAccumulator test. We just count IncrementAccess calls.
type stubAccessAccumulator struct {
	count atomic.Int32
	last  string
}

func (s *stubAccessAccumulator) IncrementAccess(entityID string) {
	s.count.Add(1)
	s.last = entityID
}

func TestBadgerEngine_SetAccessAccumulator(t *testing.T) {
	e := NewMemoryEngine()
	t.Cleanup(func() { _ = e.Close() })

	stub := &stubAccessAccumulator{}
	e.SetAccessAccumulator(stub)
	require.Equal(t, accessIncrementor(stub), e.accumulator,
		"SetAccessAccumulator must store the supplied accumulator")

	// Resetting to nil works (e.g. on shutdown).
	e.SetAccessAccumulator(nil)
	require.Nil(t, e.accumulator)
}

func TestBadgerEngine_BeginQueryRevealScope_RevealAndNonReveal(t *testing.T) {
	e := NewMemoryEngine()
	t.Cleanup(func() { _ = e.Close() })

	require.False(t, e.revealAll.Load(), "default state is not revealing")

	// Reveal scope flips the flag and locks exclusively.
	end := e.BeginQueryRevealScope(true)
	require.True(t, e.revealAll.Load(), "reveal scope must enable revealAll")
	end()
	require.False(t, e.revealAll.Load(), "reveal scope end must reset revealAll")

	// Non-reveal scope is a shared lock — flag stays off.
	end = e.BeginQueryRevealScope(false)
	require.False(t, e.revealAll.Load())
	end()
	require.False(t, e.revealAll.Load())
}

func TestBadgerEngine_ScorerForNamespace_ReturnsNilWhenNoSchema(t *testing.T) {
	e := NewMemoryEngine()
	t.Cleanup(func() { _ = e.Close() })

	// No schemas registered yet → scorer for an unknown namespace
	// is nil. The wrapper must not panic when the underlying lookup
	// fails.
	require.Nil(t, e.ScorerForNamespace("nonexistent"))
}
