package temporal

import (
	"runtime"
	"strings"
	"testing"
	"time"
	"unsafe"
	"weak"

	"github.com/stretchr/testify/require"
)

func TestRetiredTemporalKeysCollectible(t *testing.T) {
	for _, relationship := range []bool{false, true} {
		owner, reference := func() (any, weak.Pointer[byte]) {
			key := strings.Repeat("retired", 64)
			ref := weak.Make(unsafe.StringData(key))
			if relationship {
				owner := &RelationshipEvolution{accessOrder: []string{key}}
				owner.evictOldest()
				return owner, ref
			}
			owner := &Tracker{accessOrder: []string{key}}
			owner.evictOldest()
			return owner, ref
		}()
		runtime.GC()
		require.Nil(t, reference.Value())
		runtime.KeepAlive(owner)
	}
}

func TestRetiredSessionCollectible(t *testing.T) {
	owner := &SessionDetector{sessionHistory: make(map[string][]*Session)}
	reference := func() weak.Pointer[Session] {
		old := &Session{StartTime: time.Now()}
		history := make([]*Session, 50, 100)
		history[0] = old
		owner.sessionHistory["test"] = history
		ref := weak.Make(old)
		owner.endSession("test", &nodeSessionData{currentSession: &Session{StartTime: time.Now()}}, time.Now())
		return ref
	}()
	runtime.GC()
	require.Nil(t, reference.Value())
	runtime.KeepAlive(owner)
}
