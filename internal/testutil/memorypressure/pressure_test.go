package memorypressure

import (
	"math"
	"runtime/debug"
	"testing"
)

func TestWithoutRuntimePressureRestoresAmbientLimit(t *testing.T) {
	original := debug.SetMemoryLimit(128 << 20)
	t.Cleanup(func() { debug.SetMemoryLimit(original) })
	t.Run("explicit pressure remains available", func(t *testing.T) {
		WithoutRuntimePressure(t)
		if limit := debug.SetMemoryLimit(-1); limit != math.MaxInt64 {
			t.Fatalf("unpressured phase limit = %d", limit)
		}
		// The runtime-pressure tests must still be able to install a real limit.
		debug.SetMemoryLimit(64 << 20)
		if limit := debug.SetMemoryLimit(-1); limit != 64<<20 {
			t.Fatalf("explicit pressure phase limit = %d", limit)
		}
	})
	if limit := debug.SetMemoryLimit(-1); limit != 128<<20 {
		t.Fatalf("ambient limit was not restored: %d", limit)
	}
}
