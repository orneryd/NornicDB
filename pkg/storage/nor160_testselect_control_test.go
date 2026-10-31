package storage

import (
	"strings"
	"testing"
)

// Controls for the NOR-160 test selection tool; on the tool's branch only.

func TestNOR160SelectControlDirect(t *testing.T) {
	_ = edgeMatchCandidate(&Edge{}, nil)
}

func TestNOR160SelectControlInterface(t *testing.T) {
	var engine Engine = newTestMemoryEngine(t)
	_, _ = engine.GetEdgesByType("T")
}

func TestNOR160SelectControlSubtest(t *testing.T) {
	t.Run("sub", func(t *testing.T) {
		_ = edgeMatchCandidate(&Edge{}, nil)
	})
}

func TestNOR160SelectControlUnrelated(t *testing.T) {
	_ = strings.ToUpper("x")
}
