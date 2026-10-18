package heimdall

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"unsafe"
	"weak"

	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/stretchr/testify/require"
)

func TestRetiredWatcherEventCollectible(t *testing.T) {
	owner := &WatcherPlugin{}
	reference := func() weak.Pointer[[64]byte] {
		payload := new([64]byte)
		owner.events = make([]heimdall.SubsystemEvent, 100, 200)
		owner.events[0].Data = map[string]any{"payload": payload}
		ref := weak.Make(payload)
		owner.addEvent("next", "next", nil)
		return ref
	}()
	runtime.GC()
	require.Nil(t, reference.Value())
	runtime.KeepAlive(owner)
}

func TestRetiredWatcherFactCollectible(t *testing.T) {
	owner := &WatcherPlugin{}
	reference := func() weak.Pointer[byte] {
		fact := strings.Repeat("retired", 64)
		owner.facts = make([]string, 50, 100)
		owner.facts[0] = fact
		for i := 1; i < 50; i++ {
			owner.facts[i] = fmt.Sprintf("kept-%d", i)
		}
		ref := weak.Make(unsafe.StringData(fact))
		owner.appendFactLocked("next")
		return ref
	}()
	runtime.GC()
	require.Nil(t, reference.Value())
	runtime.KeepAlive(owner)
}
