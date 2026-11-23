package bolt

// Scratch diagnostic (pd-w161-3), not for merge: records where the
// "node ... changed after transaction start" conflict of the link-first
// exclusion is raised.

import (
	"runtime/debug"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
)

func TestPDW1613DiagLinkFirstOutdated(t *testing.T) {
	var mu sync.Mutex
	var raised []string
	localization.NodeChangedHook = func(nodeID string) {
		mu.Lock()
		defer mu.Unlock()
		raised = append(raised, nodeID+"\n"+string(debug.Stack()))
	}
	t.Cleanup(func() { localization.NodeChangedHook = nil })
	err := pdw1613LinkFirstOutdated(t, "diag")
	mu.Lock()
	defer mu.Unlock()
	for i, r := range raised {
		t.Logf("PDW1613 diag raised[%d] node=%s", i, r)
	}
	t.Logf("PDW1613 diag outcome err=%v raised=%d", err, len(raised))
}
