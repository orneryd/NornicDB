// Research-only representations. Never imported by the database runtime.
package main

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"strings"
	"sync"
	"time"
)

const shards = 64
const pageSize = 1024

func shard(id string) uint32 {
	h := uint32(2166136261)
	for i := range len(id) {
		h ^= uint32(id[i])
		h *= 16777619
	}
	return h % shards
}
func hash(id string) uint64 {
	h := uint64(14695981039346656037)
	for i := range len(id) {
		h ^= uint64(id[i])
		h *= 1099511628211
	}
	return mix(h)
}

type dictionary interface {
	Put(string, uint64)
	Get(string) (uint64, bool)
	Reverse(uint64) (string, bool)
	Delete(string)
}
type mapShard struct {
	mu      sync.RWMutex
	forward map[string]uint64
	reverse map[uint64]string
	pages   map[uint64]*[pageSize]string
}
type maps struct {
	sh    [shards]mapShard
	paged bool
}

func newMaps(paged bool) *maps {
	d := &maps{paged: paged}
	for i := range d.sh {
		s := &d.sh[i]
		s.forward = map[string]uint64{}
		if paged {
			s.pages = map[uint64]*[pageSize]string{}
		} else {
			s.reverse = map[uint64]string{}
		}
	}
	return d
}
func (d *maps) Put(id string, num uint64) {
	s := &d.sh[shard(id)]
	s.mu.Lock()
	if _, ok := s.forward[id]; ok {
		s.mu.Unlock()
		return
	}
	s.forward[id] = num
	s.mu.Unlock()
	s = &d.sh[num%shards]
	s.mu.Lock()
	if d.paged {
		n := num / shards
		p := s.pages[n/pageSize]
		if p == nil {
			p = new([pageSize]string)
			s.pages[n/pageSize] = p
		}
		p[n%pageSize] = id
	} else {
		s.reverse[num] = id
	}
	s.mu.Unlock()
}
func (d *maps) Get(id string) (uint64, bool) {
	s := &d.sh[shard(id)]
	s.mu.RLock()
	v, ok := s.forward[id]
	s.mu.RUnlock()
	return v, ok
}
func (d *maps) Reverse(num uint64) (string, bool) {
	s := &d.sh[num%shards]
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !d.paged {
		v, ok := s.reverse[num]
		return v, ok
	}
	n := num / shards
	p := s.pages[n/pageSize]
	if p == nil {
		return "", false
	}
	v := p[n%pageSize]
	return v, v != ""
}
func (d *maps) Delete(id string) {
	s := &d.sh[shard(id)]
	s.mu.Lock()
	num, ok := s.forward[id]
	delete(s.forward, id)
	s.mu.Unlock()
	if !ok {
		return
	}
	s = &d.sh[num%shards]
	s.mu.Lock()
	if d.paged {
		n := num / shards
		if p := s.pages[n/pageSize]; p != nil {
			p[n%pageSize] = ""
		}
	} else {
		delete(s.reverse, num)
	}
	s.mu.Unlock()
}

// Packed records keep arbitrary exact bytes, including UUID case and namespace.
// Offset zero is absent. uint32 per-shard offsets are a prototype limit, not
// a new production limit. Reverse pages contain shard+offset, never pointers.
type packedShard struct {
	mu         sync.RWMutex
	arena      []byte
	slots      []uint32
	live, used int
}
type reverseShard struct {
	mu     sync.RWMutex
	pages  map[uint64]*[pageSize]uint64
	lookup map[uint64]uint64
}
type packed struct {
	hashReverse bool
	sh          [shards]packedShard
	rev         [shards]reverseShard
}

const tomb = ^uint32(0)

func newPacked() *packed {
	d := new(packed)
	for i := range d.sh {
		d.sh[i].arena = []byte{0}
		d.sh[i].slots = make([]uint32, 16)
		d.rev[i].pages = map[uint64]*[pageSize]uint64{}
	}
	return d
}
func newPackedHash() *packed {
	d := newPacked()
	d.hashReverse = true
	for i := range d.rev {
		d.rev[i].lookup = map[uint64]uint64{}
	}
	return d
}

func (s *packedShard) key(off uint32) []byte {
	n := binary.LittleEndian.Uint32(s.arena[int(off)+8:])
	return s.arena[int(off)+12 : int(off)+12+int(n)]
}
func (s *packedShard) find(id string) (int, bool) {
	mask := len(s.slots) - 1
	i := int(hash(id) & uint64(mask))
	first := -1
	for {
		off := s.slots[i]
		if off == 0 {
			if first >= 0 {
				return first, false
			}
			return i, false
		}
		if off == tomb {
			if first < 0 {
				first = i
			}
		} else if string(s.key(off)) == id {
			return i, true
		}
		i = (i + 1) & mask
	}
}
func (s *packedShard) grow() {
	old := s.slots
	s.slots = make([]uint32, len(old)*2)
	for _, off := range old {
		if off == 0 || off == tomb {
			continue
		}
		k := s.key(off)
		h := uint64(14695981039346656037)
		for _, c := range k {
			h ^= uint64(c)
			h *= 1099511628211
		}
		i := int(mix(h) & uint64(len(s.slots)-1))
		for s.slots[i] != 0 {
			i = (i + 1) & (len(s.slots) - 1)
		}
		s.slots[i] = off
	}
	s.used = s.live
}
func (d *packed) Put(id string, num uint64) {
	si := shard(id)
	s := &d.sh[si]
	s.mu.Lock()
	if (s.used+1)*10 >= len(s.slots)*7 {
		s.grow()
	}
	i, ok := s.find(id)
	if ok {
		s.mu.Unlock()
		return
	}
	if uint64(len(s.arena))+12+uint64(len(id)) >= uint64(tomb) {
		panic("prototype per-shard offset limit")
	}
	off := uint32(len(s.arena))
	s.arena = binary.LittleEndian.AppendUint64(s.arena, num)
	s.arena = binary.LittleEndian.AppendUint32(s.arena, uint32(len(id)))
	s.arena = append(s.arena, id...)
	if s.slots[i] == 0 {
		s.used++
	}
	s.slots[i] = off
	s.live++
	s.mu.Unlock()
	r := &d.rev[num%shards]
	r.mu.Lock()
	ref := uint64(si)<<32 | uint64(off)
	if d.hashReverse {
		r.lookup[num] = ref
	} else {
		n := num / shards
		p := r.pages[n/pageSize]
		if p == nil {
			p = new([pageSize]uint64)
			r.pages[n/pageSize] = p
		}
		p[n%pageSize] = ref
	}
	r.mu.Unlock()
}
func (d *packed) Get(id string) (uint64, bool) {
	s := &d.sh[shard(id)]
	s.mu.RLock()
	defer s.mu.RUnlock()
	i, ok := s.find(id)
	if !ok {
		return 0, false
	}
	return binary.LittleEndian.Uint64(s.arena[s.slots[i]:]), true
}
func (d *packed) Reverse(num uint64) (string, bool) {
	r := &d.rev[num%shards]
	r.mu.RLock()
	n := num / shards
	var ref uint64
	if d.hashReverse {
		ref = r.lookup[num]
	} else if p := r.pages[n/pageSize]; p != nil {
		ref = p[n%pageSize]
	}
	r.mu.RUnlock()
	if ref == 0 {
		return "", false
	}
	s := &d.sh[ref>>32]
	s.mu.RLock()
	v := string(s.key(uint32(ref)))
	s.mu.RUnlock()
	return v, true
}
func (d *packed) Delete(id string) {
	s := &d.sh[shard(id)]
	s.mu.Lock()
	i, ok := s.find(id)
	if !ok {
		s.mu.Unlock()
		return
	}
	num := binary.LittleEndian.Uint64(s.arena[s.slots[i]:])
	s.slots[i] = tomb
	s.live--
	s.mu.Unlock()
	r := &d.rev[num%shards]
	r.mu.Lock()
	n := num / shards
	if d.hashReverse {
		delete(r.lookup, num)
	} else if p := r.pages[n/pageSize]; p != nil {
		p[n%pageSize] = 0
	}
	r.mu.Unlock()
}
func mix(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}
func idFor(i int, corpus string) string {
	var raw [16]byte
	binary.BigEndian.PutUint64(raw[:8], mix(uint64(i)))
	binary.BigEndian.PutUint64(raw[8:], mix(uint64(i)^0xd1b54a32d192ed03))
	raw[6] = (raw[6] & 15) | 64
	raw[8] = (raw[8] & 63) | 128
	var out [36]byte
	hex.Encode(out[:8], raw[:4])
	out[8] = '-'
	hex.Encode(out[9:13], raw[4:6])
	out[13] = '-'
	hex.Encode(out[14:18], raw[6:8])
	out[18] = '-'
	hex.Encode(out[19:23], raw[8:10])
	out[23] = '-'
	hex.Encode(out[24:], raw[10:])
	v := string(out[:])
	if corpus == "uuid" {
		return v
	}
	if corpus == "mixed" {
		switch i % 5 {
		case 0:
			return fmt.Sprintf("tenant-%d:arbitrary/%d/\x00", i%17, i)
		case 1:
			return "tenant:" + strings.ToUpper(v)
		case 2:
			return "tenant:" + strings.ReplaceAll(v, "-", "")
		case 3:
			return "tenant:urn:uuid:" + v
		}
	}
	return "nornic:" + v
}
func snapshot() (runtime.MemStats, uint64) {
	runtime.GC()
	debug.FreeOSMemory()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	s := []metrics.Sample{{Name: "/gc/scan/heap:bytes"}}
	metrics.Read(s)
	return m, s[0].Value.Uint64()
}
func rss() map[string]string {
	b, _ := os.ReadFile("/proc/self/status")
	r := map[string]string{}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "VmRSS:") || strings.HasPrefix(line, "VmHWM:") {
			p := strings.SplitN(line, ":", 2)
			r[p[0]] = strings.TrimSpace(p[1])
		}
	}
	return r
}

var sink uint64

func main() {
	mode := flag.String("mode", "baseline", "baseline, paged, packed")
	n := flag.Int("n", 1000000, "entries")
	corpus := flag.String("corpus", "namespace", "uuid, namespace, mixed")
	sparse := flag.Bool("sparse", false, "sparse numeric IDs")
	flag.Parse()
	numFor := func(i int) uint64 {
		if *sparse {
			return uint64(i)*65537 + 1
		}
		return uint64(i) + 1
	}
	var d dictionary
	before, scanBefore := snapshot()
	switch *mode {
	case "baseline":
		d = newMaps(false)
	case "paged":
		d = newMaps(true)
	case "packed":
		d = newPacked()
	case "packed-hash":
		d = newPackedHash()
	default:
		panic(*mode)
	}
	start := time.Now()
	for i := 0; i < *n; i++ {
		d.Put(idFor(i, *corpus), numFor(i))
	}
	build := time.Since(start)
	built, scan := snapshot()
	mem := func(m runtime.MemStats) uint64 { return m.HeapAlloc - before.HeapAlloc }
	result := map[string]any{"mode": *mode, "n": *n, "corpus": *corpus, "sparse": *sparse, "go": runtime.Version(), "gomaxprocs": runtime.GOMAXPROCS(0), "build_ms": float64(build.Microseconds()) / 1000, "retained_bytes": mem(built), "retained_objects": built.HeapObjects - before.HeapObjects, "scan_heap_bytes": scan - scanBefore, "build_alloc_bytes": built.TotalAlloc - before.TotalAlloc, "build_mallocs": built.Mallocs - before.Mallocs, "after_build_process": rss()}
	// Every mapping is checked both ways, including exact strings. Query strings
	// are generated independently of stored keys; no input corpus is retained.
	for i := 0; i < *n; i++ {
		id := idFor(i, *corpus)
		v, ok := d.Get(id)
		if !ok || v != numFor(i) {
			panic("forward mismatch")
		}
		back, ok := d.Reverse(v)
		if !ok || back != id {
			panic("reverse mismatch")
		}
	}
	queries := make([]string, 8192)
	nums := make([]uint64, len(queries))
	misses := make([]string, len(queries))
	for i := range queries {
		j := int(mix(uint64(i)) % uint64(*n))
		queries[i] = idFor(j, *corpus)
		nums[i] = numFor(j)
		misses[i] = "missing:" + queries[i]
	}
	const reps = 262144
	measure := func(fn func(int)) {
		t := time.Now()
		for j := 0; j < reps; j++ {
			fn(j & 8191)
		}
		fmt.Sprint(sink)
		result["last_ns"] = float64(time.Since(t).Nanoseconds()) / reps
	}
	measure(func(j int) {
		v, ok := d.Get(queries[j])
		if !ok {
			panic("miss")
		}
		sink += v
	})
	result["forward_hit_ns"] = result["last_ns"]
	measure(func(j int) {
		_, ok := d.Get(misses[j])
		if ok {
			panic("false hit")
		}
	})
	result["forward_miss_ns"] = result["last_ns"]
	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)
	measure(func(j int) {
		v, ok := d.Reverse(nums[j])
		if !ok {
			panic("reverse miss")
		}
		sink += uint64(len(v))
	})
	runtime.ReadMemStats(&m2)
	result["reverse_hit_ns"] = result["last_ns"]
	result["reverse_bytes_per_op"] = float64(m2.TotalAlloc-m1.TotalAlloc) / reps
	delete(result, "last_ns")
	queries = nil
	nums = nil
	misses = nil
	t := time.Now()
	for i := 0; i < *n; i += 2 {
		d.Delete(idFor(i, *corpus))
	}
	result["delete_ns_per_op"] = float64(time.Since(t).Nanoseconds()) / float64((*n+1)/2)
	deleted, _ := snapshot()
	result["half_deleted_retained_bytes"] = mem(deleted)
	t = time.Now()
	for i := 0; i < *n; i += 2 {
		d.Put(idFor(i+*n, *corpus), numFor(i))
	}
	result["reinsert_ns_per_op"] = float64(time.Since(t).Nanoseconds()) / float64((*n+1)/2)
	churned, _ := snapshot()
	result["churn_retained_bytes"] = mem(churned)
	for i := 0; i < *n; i++ {
		j := i
		if i%2 == 0 {
			j += *n
			if _, ok := d.Get(idFor(i, *corpus)); ok {
				panic("deleted key retained")
			}
		}
		v, ok := d.Get(idFor(j, *corpus))
		if !ok || v != numFor(i) {
			panic("churn forward")
		}
		back, ok := d.Reverse(numFor(i))
		if !ok || back != idFor(j, *corpus) {
			panic("churn reverse")
		}
	}
	result["before_rebuild_process"] = rss()
	if current, ok := d.(*packed); ok {
		t := time.Now()
		fresh := newPacked()
		if current.hashReverse {
			fresh = newPackedHash()
		}
		for i := 0; i < *n; i++ {
			num := numFor(i)
			id, ok := current.Reverse(num)
			if !ok {
				panic("rebuild missing")
			}
			fresh.Put(id, num)
		}
		d = fresh
		current = nil
		rebuilt, _ := snapshot()
		result["rebuilt_retained_bytes"] = mem(rebuilt)
		result["rebuild_ms"] = float64(time.Since(t).Microseconds()) / 1000
		for i := 0; i < *n; i++ {
			j := i
			if i%2 == 0 {
				j += *n
			}
			id := idFor(j, *corpus)
			v, ok := d.Get(id)
			if !ok || v != numFor(i) {
				panic("rebuilt forward")
			}
			back, ok := d.Reverse(v)
			if !ok || back != id {
				panic("rebuilt reverse")
			}
		}
	}
	result["final_process"] = rss()
	result["full_bidirectional_and_churn_check"] = "pass"
	runtime.KeepAlive(d)
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		panic(err)
	}
}
