package storage

import (
	"github.com/stretchr/testify/require"
	"testing"
)

type foundationCaptureOwner struct {
	Engine
	ChangeCaptureEngine
	unwraps int
}

func (o *foundationCaptureOwner) GetInnerEngine() Engine {
	o.unwraps++
	panic("exact capture owner was bypassed")
}

type foundationCaptureCycle struct {
	Engine
	visits int
}

func (o *foundationCaptureCycle) GetInnerEngine() Engine { o.visits++; return o }
func TestFoundationChangeCaptureCommonCursor(t *testing.T) {
	b := NewMemoryEngine()
	defer b.Close()
	issued := &foundationCaptureOwner{Engine: b, ChangeCaptureEngine: b}
	got, ok := ChangeCaptureOf(issued)
	require.True(t, ok)
	require.Same(t, issued, got)
	require.Zero(t, issued.unwraps)
	scoped := NewNamespacedEngine(b, "cdc")
	got, ok = ChangeCaptureOf(scoped)
	require.True(t, ok)
	require.Same(t, b, got)
	cycle := &foundationCaptureCycle{}
	_, ok = ChangeCaptureOf(cycle)
	require.False(t, ok)
	require.Equal(t, 1, cycle.visits)
}
