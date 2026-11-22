package storage

import (
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type s1Attempt struct {
	what, decoder string
	body          []byte
	names         []string
}

// TestPR1826S1MutatedBodyMemory decodes mutated stored bodies one decoder at a
// time under a watchdog. When the process heap passes 1.5 GiB it prints the
// input being decoded and every goroutine's stack, and ends the process: one
// small stored body may not make a decode take gigabytes.
func TestPR1826S1MutatedBodyMemory(t *testing.T) {
	b, _ := createTestBadgerEngineOnDisk(t)
	ids, sets := s1FuzzFixture(t, b)
	var current atomic.Pointer[s1Attempt]
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(10 * time.Millisecond):
			}
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			if m.HeapAlloc < 1536<<20 {
				continue
			}
			a := current.Load()
			shown := a.body
			if len(shown) > 400 {
				shown = shown[:400]
			}
			fmt.Printf("S1MEM heap=%dMiB decoder=%s input=%s names=%v bodyLen=%d body=%s\n", m.HeapAlloc>>20, a.decoder, a.what, a.names, len(a.body), hex.EncodeToString(shown))
			var out strings.Builder
			_ = pprof.Lookup("goroutine").WriteTo(&out, 2)
			for _, line := range strings.Split(out.String(), "\n") {
				fmt.Println("S1STACK", line)
			}
			os.Exit(3)
		}
	}()
	type top struct {
		bytes uint64
		label string
	}
	var largest top
	decodes := map[string]func(id NodeID, body []byte, names []string){
		"full": func(id NodeID, body []byte, _ []string) {
			_, _ = b.decodeNodeProjected(nil, namespaceForNodeID(id), body, nil)
		},
		"point": func(id NodeID, body []byte, names []string) {
			_, _ = b.decodeNodeProjected(nil, namespaceForNodeID(id), body, propertyProjectionSet(names))
		},
		"scan": func(id NodeID, body []byte, names []string) {
			_, _ = newProjectedNodeDecoder(b, nil, StreamNodesOptions{Projection: names}).decode(nodeKey(id), body)
		},
	}
	judge := func(id NodeID, body []byte, what string) {
		for _, decoder := range []string{"full", "point", "scan"} {
			for si, names := range sets {
				if si > 0 && decoder == "full" {
					break
				}
				current.Store(&s1Attempt{what, decoder, body, names})
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				func() {
					defer func() {
						if p := recover(); p != nil {
							t.Errorf("S1PANIC %s %s: %v", decoder, what, p)
						}
					}()
					decodes[decoder](id, body, names)
				}()
				runtime.ReadMemStats(&after)
				if d := after.TotalAlloc - before.TotalAlloc; d > largest.bytes {
					largest = top{d, fmt.Sprintf("%s %s id=%s names=%v bodyLen=%d", decoder, what, id, names, len(body))}
					if d > 64<<20 {
						t.Logf("S1ALLOC %d MiB: %s", d>>20, largest.label)
					}
				}
			}
		}
	}
	r := rand.New(rand.NewSource(1826))
	for _, id := range ids {
		body := s1StoredBody(t, b, id)
		if len(body) > 4000 {
			continue
		}
		judge(id, body, "valid")
		for i := 0; i < len(body); i++ {
			judge(id, body[:i], fmt.Sprintf("%s truncate %d", id, i))
			for _, x := range []byte{0x00, 0x7f, 0x80, 0xc0, 0xc6, 0xc9, 0xdb, 0xdd, 0xdf, 0xff} {
				m := append([]byte(nil), body...)
				m[i] = x
				judge(id, m, fmt.Sprintf("%s byte %d=%02x", id, i, x))
			}
		}
		for k := 0; k < 500; k++ {
			m := append([]byte(nil), body...)
			m[r.Intn(len(m))] = byte(r.Intn(256))
			m[r.Intn(len(m))] = byte(r.Intn(256))
			judge(id, m, fmt.Sprintf("%s random %d", id, k))
		}
	}
	t.Logf("S1COUNT largest allocation of one decode: %d KiB (%s)", largest.bytes>>10, largest.label)
}
