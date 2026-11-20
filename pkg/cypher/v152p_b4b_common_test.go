package cypher

// Batch 4b probes (untracked; never committed): shared helpers. Identical on
// both builds.

import (
	"fmt"
	"math"
	"os"
	osexec "os/exec"
	"runtime"
	"sort"
	"strings"
	"testing"
)

type b4bStackKey string

type b4bRec struct {
	bytes, objects int64
	stack          []uintptr
}

func b4bMemRecords() map[b4bStackKey]b4bRec {
	var records []runtime.MemProfileRecord
	n, _ := runtime.MemProfile(nil, true)
	for {
		records = make([]runtime.MemProfileRecord, n+64)
		var ok bool
		n, ok = runtime.MemProfile(records, true)
		if ok {
			records = records[:n]
			break
		}
	}
	out := make(map[b4bStackKey]b4bRec, len(records))
	for _, r := range records {
		stack := r.Stack()
		key := b4bStackKey(fmt.Sprint(stack))
		prev := out[key]
		out[key] = b4bRec{bytes: prev.bytes + r.AllocBytes, objects: prev.objects + r.AllocObjects, stack: stack}
	}
	return out
}

func b4bFrames(stack []uintptr) []string {
	frames := runtime.CallersFrames(stack)
	var names []string
	for {
		f, more := frames.Next()
		if f.Function != "" {
			names = append(names, f.Function)
		}
		if !more {
			break
		}
	}
	return names
}

func b4bShort(name string) string {
	return strings.TrimPrefix(name, "github.com/orneryd/nornicdb/")
}

// b4bProfile runs fn with every allocation sampled (MemProfileRate = 1) and
// logs: the TotalAlloc delta; the profiled bytes; the top sites by the first
// non-runtime frame (flat); the top sites by the first frame inside this
// repository (owner); and the top repository functions by cumulative bytes
// (each function counted once per stack). Stacks are those the runtime keeps
// (up to 32 frames).
func b4bProfile(t testing.TB, label string, fn func()) uint64 {
	t.Helper()
	prev := runtime.MemProfileRate
	runtime.MemProfileRate = 1
	runtime.GC()
	runtime.GC()
	before := b4bMemRecords()
	var a, b runtime.MemStats
	runtime.ReadMemStats(&a)
	fn()
	runtime.ReadMemStats(&b)
	runtime.GC()
	runtime.GC()
	after := b4bMemRecords()
	runtime.MemProfileRate = prev

	flat := map[string]int64{}
	owner := map[string]int64{}
	cum := map[string]int64{}
	var profiled, objects int64
	for key, rec := range after {
		d := rec.bytes - before[key].bytes
		o := rec.objects - before[key].objects
		if d <= 0 {
			continue
		}
		profiled += d
		objects += o
		names := b4bFrames(rec.stack)
		flatName, ownerName := "?", "(no repository frame)"
		for _, n := range names {
			if !strings.HasPrefix(n, "runtime.") && !strings.HasPrefix(n, "internal/") {
				flatName = n
				break
			}
		}
		seen := map[string]bool{}
		for _, n := range names {
			if strings.HasPrefix(n, "github.com/orneryd/nornicdb/") {
				if ownerName == "(no repository frame)" {
					ownerName = n
				}
				if !seen[n] {
					seen[n] = true
					cum[n] += d
				}
			}
		}
		flat[flatName] += d
		owner[ownerName] += d
	}
	t.Logf("B4BPROF %s: TotalAlloc=%d profiled=%d objects=%d", label, b.TotalAlloc-a.TotalAlloc, profiled, objects)
	b4bTop(t, label, "flat", flat, 25)
	b4bTop(t, label, "owner", owner, 25)
	b4bTop(t, label, "cum", cum, 40)
	return b.TotalAlloc - a.TotalAlloc
}

func b4bTop(t testing.TB, label, kind string, m map[string]int64, n int) {
	type kv struct {
		k string
		v int64
	}
	var list []kv
	for k, v := range m {
		list = append(list, kv{k, v})
	}
	sort.Slice(list, func(i, j int) bool { return list[i].v > list[j].v })
	if len(list) > n {
		list = list[:n]
	}
	for i, e := range list {
		t.Logf("B4BPROF %s %s #%02d %12d B  %s", label, kind, i+1, e.v, b4bShort(e.k))
	}
}

// b4bLeast: one warm-up, then the least TotalAlloc of 3 runs, each after a GC.
func b4bLeast(before, statement, after func()) uint64 {
	before()
	statement()
	after()
	least := uint64(math.MaxUint64)
	for i := 0; i < 3; i++ {
		before()
		runtime.GC()
		var s, e runtime.MemStats
		runtime.ReadMemStats(&s)
		statement()
		runtime.ReadMemStats(&e)
		after()
		if d := e.TotalAlloc - s.TotalAlloc; d < least {
			least = d
		}
	}
	return least
}

// b4bChild runs this test binary again with only test, in a fresh process,
// with env B4B_CHILD=test, and returns its output.
func b4bChild(t testing.TB, test string, extraEnv ...string) string {
	t.Helper()
	cmd := osexec.Command(os.Args[0], "-test.run=^"+test+"$", "-test.count=1", "-test.v")
	cmd.Env = append(append(os.Environ(), "B4B_CHILD="+test), extraEnv...)
	out, err := cmd.CombinedOutput()
	t.Logf("B4BCHILD %s exit=%v", test, err)
	return string(out)
}
