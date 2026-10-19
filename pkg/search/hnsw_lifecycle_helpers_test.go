package search

import (
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"runtime"
	"strings"
	"testing"
)

func serviceTestGoroutineID() string {
	buf := make([]byte, 128)
	n := runtime.Stack(buf, false)
	return strings.Fields(string(buf[:n]))[1]
}
func serviceStackContains(id string, frames ...string) bool {
	for _, stack := range allGoroutineStacks() {
		if !strings.HasPrefix(stack, "goroutine "+id+" ") {
			continue
		}
		matched := true
		for _, frame := range frames {
			matched = matched && strings.Contains(stack, frame)
		}
		if matched {
			return true
		}
	}
	return false
}
func allGoroutineStacks() []string {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return strings.Split(string(buf[:n]), "\n\n")
		}
		buf = make([]byte, 2*len(buf))
	}
}
func sourceRetirementNode(t *testing.T, svc *Service, id string, vector []float32) {
	t.Helper()
	node := &storage.Node{ID: storage.NodeID(id), ChunkEmbeddings: [][]float32{vector}}
	_, err := svc.engine.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, svc.IndexNode(node))
}
