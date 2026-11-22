package server

// v1.5.2 verification (scratch, test-only): the two memory findings of the
// PR #1826 review on the release, over HTTP and Bolt on an on-disk store.

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func p26Memory() (total uint64, hwmKB int64, rssKB int64) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	if data, err := os.ReadFile("/proc/self/status"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				continue
			}
			value, _ := strconv.ParseInt(fields[1], 10, 64)
			switch fields[0] {
			case "VmHWM:":
				hwmKB = value
			case "VmRSS:":
				rssKB = value
			}
		}
	}
	return m.TotalAlloc, hwmKB, rssKB
}

// F6: one flipped bit in the table file of a closed store turns a one-byte
// length header (bin8, str8) into a four-byte one (bin32, str32).
func TestV152P26FlippedBit(t *testing.T) {
	blob := make([]byte, 100)
	rand.New(rand.NewSource(1826)).Read(blob)
	text := fmt.Sprintf("%x", blob)[:100]
	for _, c := range []struct {
		name  string
		value any
		find  []byte
		to    byte
	}{
		{"bytes", blob, append([]byte{0xc4, 100}, blob...), 0xc6},
		{"string", text, append([]byte{0xd9, 100}, text...), 0xdb},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "v152-p26-*")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(dir) })
			e, err := p26Open(t, dir)
			if err != nil {
				t.Fatal(err)
			}
			e.must("CREATE (:Doc {key:'x', v:$v}), (:Doc {key:'y', v:'fine'})", map[string]any{"v": c.value})
			stopTestServer(t, e.server)
			if err := e.db.Close(); err != nil {
				t.Fatal(err)
			}
			flipped := 0
			var files []string
			_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
				if err != nil || info.IsDir() {
					return nil
				}
				data, err := os.ReadFile(path)
				if err != nil {
					return nil
				}
				found := 0
				if strings.HasSuffix(path, ".sst") || strings.HasSuffix(path, ".vlog") {
					for from := 0; ; {
						at := bytes.Index(data[from:], c.find[:20])
						if at < 0 {
							break
						}
						at += from
						t.Logf("P26ROW F6 %s: one bit flipped in %s at offset %d (%02x -> %02x): the value now claims %d bytes", c.name, filepath.Base(path), at, data[at], c.to, uint32(data[at+1])<<24|uint32(data[at+2])<<16|uint32(data[at+3])<<8|uint32(data[at+4]))
						data[at] = c.to
						found++
						from = at + 1
					}
				}
				files = append(files, fmt.Sprintf("%s(%d bytes, value found raw: %t)", filepath.Base(path), info.Size(), found > 0 || bytes.Contains(data, c.find[:20])))
				if found > 0 {
					flipped += found
					_ = os.Chmod(path, 0o644)
					if err := os.WriteFile(path, data, 0o644); err != nil {
						t.Fatal(err)
					}
				}
				return nil
			})
			if flipped == 0 {
				t.Fatalf("the stored value was not found in a table file: %v", files)
			}
			e2, err := p26Open(t, dir)
			if err != nil {
				_, hwm, _ := p26Memory()
				t.Errorf("P26 F6 %s: the store does not open: %v (peak resident %d MiB)", c.name, err, hwm>>10)
				return
			}
			_, baseHWM, _ := p26Memory()
			t.Logf("P26ROW F6 %s after open: peak resident %d MiB", c.name, baseHWM>>10)
			for _, q := range []string{"MATCH (n:Doc {key:'y'}) RETURN n.v", "MATCH (n:Doc {key:'x'}) RETURN n.key", "MATCH (n:Doc {key:'x'}) RETURN n.v", "MATCH (n:Doc) RETURN n"} {
				for _, s := range []p26Session{e2.httpAuto(), e2.boltAuto()} {
					before, _, _ := p26Memory()
					started := time.Now()
					got := s.run(q, nil)
					after, hwm, rss := p26Memory()
					allocated := (after - before) >> 20
					t.Logf("P26ROW F6 %s %s %q allocated %d MiB in %s, resident %d MiB, peak %d MiB -> %.200s", c.name, s.name, q, allocated, time.Since(started).Round(time.Millisecond), rss>>10, hwm>>10, got)
					if allocated > 256 {
						t.Errorf("P26 F6 %s %s: %q allocates %d MiB on a store of two small nodes", c.name, s.name, q, allocated)
					}
				}
			}
			for i := 0; i < 10; i++ {
				e2.boltAuto().run("MATCH (n:Doc) RETURN n", nil)
			}
			_, hwm, _ := p26Memory()
			t.Logf("P26ROW F6 %s after 10 more reads: peak resident %d MiB", c.name, hwm>>10)
		})
	}
}

func p26Heap() int64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int64(m.HeapAlloc)
}

// R1 #7: numeric lists count 16 bytes in the node caches' byte budget.
func TestV152P26NumericListCache(t *testing.T) {
	const nodes, bytesPerNode = 1000, 480_000
	growth := map[string]int64{}
	for _, kind := range []string{"string", "floats", "ints"} {
		t.Run(kind, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "v152-p26c-*")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(dir) })
			e, err := p26Open(t, dir)
			if err != nil {
				t.Fatal(err)
			}
			e.must("CREATE CONSTRAINT series_key FOR (n:Series) REQUIRE n.key IS UNIQUE", nil)
			store, err := e.server.dbManager.GetStorage(e.database)
			if err != nil {
				t.Fatal(err)
			}
			r := rand.New(rand.NewSource(7))
			for i := 0; i < nodes; i++ {
				var value any
				switch kind {
				case "string":
					raw := make([]byte, bytesPerNode/2)
					r.Read(raw)
					value = fmt.Sprintf("%x", raw)
				case "floats":
					list := make([]float64, bytesPerNode/8)
					for j := range list {
						list[j] = r.Float64()
					}
					value = list
				case "ints":
					list := make([]int64, bytesPerNode/8)
					for j := range list {
						list[j] = r.Int63()
					}
					value = list
				}
				if _, err := store.CreateNode(t.Context(), &storage.Node{ID: storage.NodeID(fmt.Sprintf("s%04d", i)), Labels: []string{"Series"}, Properties: map[string]any{"key": fmt.Sprintf("k%04d", i), "v": value}}); err != nil {
					t.Fatal(err)
				}
			}
			stopTestServer(t, e.server)
			if err := e.db.Close(); err != nil {
				t.Fatal(err)
			}
			e2, err := p26Open(t, dir)
			if err != nil {
				t.Fatal(err)
			}
			before := p26Heap()
			run := e2.httpAuto().run
			for i := 0; i < nodes; i++ {
				if got := run("MATCH (n:Series {key:$k}) RETURN n.key, size(n.v) > 10", map[string]any{"k": fmt.Sprintf("k%04d", i)}); got != fmt.Sprintf(`["k%04d",true]`, i) {
					t.Fatalf("read %d: %.100s", i, got)
				}
			}
			growth[kind] = p26Heap() - before
			t.Logf("P26ROW R7 %s: %d nodes of %d bytes each read once through HTTP; heap kept after two collections: %d MiB (node cache budget 256 MiB)", kind, nodes, bytesPerNode, growth[kind]>>20)
		})
	}
	t.Logf("P26ROW R7 growth MiB string=%d floats=%d ints=%d", growth["string"]>>20, growth["floats"]>>20, growth["ints"]>>20)
	for _, kind := range []string{"floats", "ints"} {
		if growth[kind] > growth["string"]+(100<<20) {
			t.Errorf("P26 R7 %s: the caches keep %d MiB, against %d MiB for strings of the same size", kind, growth[kind]>>20, growth["string"]>>20)
		}
	}
}
