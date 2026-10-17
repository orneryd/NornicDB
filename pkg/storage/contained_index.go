package storage

import (
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// ContainedIndex answers which of its keys occur in a text: the reverse of a
// text index, which answers n.p CONTAINS $x. Declared by CREATE CONTAINED
// INDEX <name> FOR (n:Label) ON EACH [n.p, ...] (sig/handoff/
// contained-key-index-design.md).
//
// A node's keys are the strings among its indexed properties' values: a
// string, or the string elements of a list; other values have no key, as
// CONTAINS on them is never true. Substring means what CONTAINS evaluates:
// Go strings.Contains on the UTF-8 bytes, case-sensitive, unnormalised. A
// valid UTF-8 key cannot match from inside a multi-byte character, so a byte
// match is exactly a code-point substring. The empty key is contained in
// every text; its nodes are kept apart and always returned.
//
// postings (key → nodes) and nodeKeys (node → its keys) are the source of
// truth, kept under mu by every write (RefileNode). The automaton is an
// Aho-Corasick matcher over the distinct keys of one generation, immutable
// once published. Keys filed since its build are matched directly (delta);
// keys whose nodes all left only lose their postings (dead). A lookup builds
// the next generation when either grows past its limit, one builder at a
// time; other lookups keep the published one.
type ContainedIndex struct {
	Name       string
	Label      string
	Properties []string

	mu        sync.RWMutex
	postings  map[string]map[NodeID]struct{}
	nodeKeys  map[NodeID][]string
	emptyKey  map[NodeID]struct{}
	delta     map[string]struct{}
	dead      int
	automaton atomic.Pointer[containedAutomaton]
	building  atomic.Bool

	// unfilled is set while a new index waits for its backfill; lookups must
	// not use it then (#875).
	unfilled atomic.Bool
}

// ContainedMatch is one key of a node contained in a text.
type ContainedMatch struct {
	NodeID NodeID
	Key    string
}

// containedRebuildMinDelta and containedRebuildDeltaShare bound the keys
// matched directly before the automaton is rebuilt: the larger of 256 keys
// or 2 % of all keys. containedRebuildDeadShare bounds the share of the
// automaton's keys that no node holds any more.
const (
	containedRebuildMinDelta   = 256
	containedRebuildDeltaShare = 50
	containedRebuildDeadShare  = 4
)

func newContainedIndex(name, label string, properties []string) *ContainedIndex {
	return &ContainedIndex{
		Name:       name,
		Label:      label,
		Properties: append([]string(nil), properties...),
		postings:   make(map[string]map[NodeID]struct{}),
		nodeKeys:   make(map[NodeID][]string),
		emptyKey:   make(map[NodeID]struct{}),
		delta:      make(map[string]struct{}),
	}
}

// containedKeys returns the distinct non-empty keys properties give a node
// in an index over indexed, and whether one of them is the empty string.
func containedKeys(indexed []string, properties map[string]interface{}) (keys []string, empty bool) {
	add := func(key string) {
		if key == "" {
			empty = true
			return
		}
		for _, existing := range keys {
			if existing == key {
				return
			}
		}
		keys = append(keys, key)
	}
	for _, property := range indexed {
		switch value := properties[property].(type) {
		case string:
			add(value)
		case []string:
			for _, element := range value {
				add(element)
			}
		case []interface{}:
			for _, element := range value {
				if key, ok := element.(string); ok {
					add(key)
				}
			}
		}
	}
	return keys, empty
}

// RefileNode replaces the keys nodeID filed with those of the property map
// current reads inside the index's critical section: the node's latest
// committed state, nil when the node is gone or out of the label. Every
// publication refiles from committed state under this lock, so writes of one
// node that publish out of order still end at its committed keys
// (F-PUBLISH). If current fails, the keys of fallback (the write's own
// result) are added and nothing is removed: a stale key is re-checked by
// every read, a missing one hides the node.
func (idx *ContainedIndex) RefileNode(nodeID NodeID, current func() (map[string]interface{}, error), fallback map[string]interface{}) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	properties, err := current()
	if err != nil {
		if fallback != nil {
			keys, empty := containedKeys(idx.Properties, fallback)
			_, wasEmpty := idx.emptyKey[nodeID]
			kept := append([]string(nil), idx.nodeKeys[nodeID]...)
			for _, key := range keys {
				if !slices.Contains(kept, key) {
					kept = append(kept, key)
				}
			}
			idx.unfileLocked(nodeID)
			idx.fileKeysLocked(nodeID, kept, empty || wasEmpty)
		}
		return
	}
	idx.unfileLocked(nodeID)
	if properties != nil {
		idx.fileLocked(nodeID, properties)
	}
}

// fileLocked files nodeID under the keys of properties. The caller holds
// idx.mu for writing and has unfiled nodeID.
func (idx *ContainedIndex) fileLocked(nodeID NodeID, properties map[string]interface{}) {
	keys, empty := containedKeys(idx.Properties, properties)
	idx.fileKeysLocked(nodeID, keys, empty)
}

// fileKeysLocked files nodeID under keys (distinct, non-empty) and, when
// empty, under the empty key. The caller holds idx.mu for writing and has
// unfiled nodeID.
func (idx *ContainedIndex) fileKeysLocked(nodeID NodeID, keys []string, empty bool) {
	if empty {
		idx.emptyKey[nodeID] = struct{}{}
	}
	if len(keys) == 0 {
		return
	}
	automaton := idx.automaton.Load()
	for _, key := range keys {
		nodes := idx.postings[key]
		if nodes == nil {
			nodes = make(map[NodeID]struct{}, 1)
			idx.postings[key] = nodes
			if automaton.has(key) {
				idx.dead--
			} else {
				idx.delta[key] = struct{}{}
			}
		}
		nodes[nodeID] = struct{}{}
	}
	idx.nodeKeys[nodeID] = keys
}

// unfileLocked removes every key nodeID filed. The caller holds idx.mu for
// writing.
func (idx *ContainedIndex) unfileLocked(nodeID NodeID) {
	delete(idx.emptyKey, nodeID)
	keys, ok := idx.nodeKeys[nodeID]
	if !ok {
		return
	}
	delete(idx.nodeKeys, nodeID)
	automaton := idx.automaton.Load()
	for _, key := range keys {
		nodes := idx.postings[key]
		delete(nodes, nodeID)
		if len(nodes) > 0 {
			continue
		}
		delete(idx.postings, key)
		if automaton.has(key) {
			idx.dead++
		} else {
			delete(idx.delta, key)
		}
	}
}

// reset empties the index, as an open-time rebuild starts.
func (idx *ContainedIndex) reset() {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.postings = make(map[string]map[NodeID]struct{})
	idx.nodeKeys = make(map[NodeID][]string)
	idx.emptyKey = make(map[NodeID]struct{})
	idx.delta = make(map[string]struct{})
	idx.dead = 0
	idx.automaton.Store(nil)
}

// File files nodeID under the keys of properties, replacing what it filed.
// Loaders (the open-time rebuild) use it; node writes use RefileNode.
func (idx *ContainedIndex) File(nodeID NodeID, properties map[string]interface{}) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.unfileLocked(nodeID)
	idx.fileLocked(nodeID, properties)
}

// Backfill files every node of values (node ID → properties) and marks the
// index filled. Nodes already filed are refiled.
func (idx *ContainedIndex) Backfill(values map[NodeID]map[string]interface{}) {
	idx.mu.Lock()
	for nodeID, properties := range values {
		idx.unfileLocked(nodeID)
		idx.fileLocked(nodeID, properties)
	}
	idx.mu.Unlock()
	idx.unfilled.Store(false)
}

// sameContainedIndexDefinition reports whether two indexes are declared
// alike, so one's contents serve the other.
func sameContainedIndexDefinition(first, second *ContainedIndex) bool {
	return first.Name == second.Name && first.Label == second.Label && slices.Equal(first.Properties, second.Properties)
}

// copyStateFrom replaces idx's contents with a copy of source's, as a
// transaction's staged schema hands a backfilled index to its snapshot. The
// automaton is immutable and shared; a node's key slice is never modified
// once filed and is shared too.
func (idx *ContainedIndex) copyStateFrom(source *ContainedIndex) {
	source.mu.RLock()
	defer source.mu.RUnlock()
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.postings = make(map[string]map[NodeID]struct{}, len(source.postings))
	for key, nodes := range source.postings {
		copied := make(map[NodeID]struct{}, len(nodes))
		for nodeID := range nodes {
			copied[nodeID] = struct{}{}
		}
		idx.postings[key] = copied
	}
	idx.nodeKeys = make(map[NodeID][]string, len(source.nodeKeys))
	for nodeID, keys := range source.nodeKeys {
		idx.nodeKeys[nodeID] = keys
	}
	idx.emptyKey = make(map[NodeID]struct{}, len(source.emptyKey))
	for nodeID := range source.emptyKey {
		idx.emptyKey[nodeID] = struct{}{}
	}
	idx.delta = make(map[string]struct{}, len(source.delta))
	for key := range source.delta {
		idx.delta[key] = struct{}{}
	}
	idx.dead = source.dead
	idx.automaton.Store(source.automaton.Load())
	idx.unfilled.Store(source.unfilled.Load())
}

// Filled reports whether lookups may use the index.
func (idx *ContainedIndex) Filled() bool {
	return !idx.unfilled.Load()
}

// Lookup returns every (node, key) pair of the index with key a substring of
// text, ordered by node ID, then key. Empty-key nodes are returned with the
// key "".
func (idx *ContainedIndex) Lookup(text string) []ContainedMatch {
	idx.rebuildIfStale()

	// The automaton and the delta are read together: a rebuild publishes
	// the one and prunes the other under the write lock, so together they
	// cover every live key.
	idx.mu.RLock()
	automaton := idx.automaton.Load()
	var matched []string
	for key := range idx.delta {
		if strings.Contains(text, key) {
			matched = append(matched, key)
		}
	}
	idx.mu.RUnlock()
	if automaton != nil {
		seen := make(map[int32]struct{})
		automaton.match(text, func(key int32) {
			if _, dup := seen[key]; !dup {
				seen[key] = struct{}{}
				matched = append(matched, automaton.keys[key])
			}
		})
	}

	idx.mu.RLock()
	var out []ContainedMatch
	for _, key := range matched {
		for nodeID := range idx.postings[key] {
			out = append(out, ContainedMatch{NodeID: nodeID, Key: key})
		}
	}
	for nodeID := range idx.emptyKey {
		out = append(out, ContainedMatch{NodeID: nodeID})
	}
	idx.mu.RUnlock()

	sort.Slice(out, func(i, j int) bool {
		if out[i].NodeID != out[j].NodeID {
			return out[i].NodeID < out[j].NodeID
		}
		return out[i].Key < out[j].Key
	})
	return out
}

// LookupNodes returns the distinct nodes with a key contained in text, in ID
// order.
func (idx *ContainedIndex) LookupNodes(text string) []NodeID {
	matches := idx.Lookup(text)
	nodes := make([]NodeID, 0, len(matches))
	for i, match := range matches {
		if i == 0 || match.NodeID != matches[i-1].NodeID {
			nodes = append(nodes, match.NodeID)
		}
	}
	return nodes
}

// KeyCount is the number of distinct non-empty keys nodes hold.
func (idx *ContainedIndex) KeyCount() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.postings)
}

// NodeCount is the number of nodes filed under at least one key.
func (idx *ContainedIndex) NodeCount() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	count := len(idx.nodeKeys)
	for nodeID := range idx.emptyKey {
		if _, keyed := idx.nodeKeys[nodeID]; !keyed {
			count++
		}
	}
	return count
}

// rebuildIfStale builds and publishes the next automaton generation when the
// keys matched directly or the dead keys have grown past their limits and no
// other lookup is building it.
func (idx *ContainedIndex) rebuildIfStale() {
	idx.mu.RLock()
	automaton := idx.automaton.Load()
	live, delta, dead := len(idx.postings), len(idx.delta), idx.dead
	idx.mu.RUnlock()
	stale := delta > max(containedRebuildMinDelta, live/containedRebuildDeltaShare) ||
		(automaton != nil && dead > len(automaton.keys)/containedRebuildDeadShare)
	if !stale || !idx.building.CompareAndSwap(false, true) {
		return
	}
	defer idx.building.Store(false)

	idx.mu.RLock()
	keys := make([]string, 0, len(idx.postings))
	for key := range idx.postings {
		keys = append(keys, key)
	}
	idx.mu.RUnlock()
	next := buildContainedAutomaton(keys)

	// Writes during the build kept the delta against the old generation;
	// recompute it against the new one: every live key next lacks.
	idx.mu.Lock()
	idx.automaton.Store(next)
	idx.delta = make(map[string]struct{})
	for key := range idx.postings {
		if !next.has(key) {
			idx.delta[key] = struct{}{}
		}
	}
	idx.dead = 0
	for _, key := range next.keys {
		if _, live := idx.postings[key]; !live {
			idx.dead++
		}
	}
	idx.mu.Unlock()
}

// containedAutomaton is an Aho-Corasick matcher over bytes. State 0 is the
// root. The edges of state s are edgeByte/edgeNext[edgeStart[s]:edgeStart[s+1]],
// sorted by byte; fail is the failure link, output the key ending at the
// state (-1 for none), and dict the nearest state on the failure chain with
// an output (-1 for none).
type containedAutomaton struct {
	keys      []string
	keySet    map[string]struct{}
	edgeStart []int32
	edgeByte  []byte
	edgeNext  []int32
	fail      []int32
	output    []int32
	dict      []int32
}

// has reports whether the automaton was built with key; a nil automaton has
// no keys.
func (a *containedAutomaton) has(key string) bool {
	if a == nil {
		return false
	}
	_, ok := a.keySet[key]
	return ok
}

// buildContainedAutomaton builds the matcher over the distinct non-empty
// keys.
func buildContainedAutomaton(keys []string) *containedAutomaton {
	sorted := append([]string(nil), keys...)
	sort.Strings(sorted)
	a := &containedAutomaton{keySet: make(map[string]struct{}, len(sorted))}
	children := []map[byte]int32{{}}
	output := []int32{-1}
	for _, key := range sorted {
		if key == "" {
			continue
		}
		if _, dup := a.keySet[key]; dup {
			continue
		}
		a.keySet[key] = struct{}{}
		state := int32(0)
		for i := 0; i < len(key); i++ {
			next, ok := children[state][key[i]]
			if !ok {
				next = int32(len(children))
				children = append(children, map[byte]int32{})
				output = append(output, -1)
				children[state][key[i]] = next
			}
			state = next
		}
		output[state] = int32(len(a.keys))
		a.keys = append(a.keys, key)
	}

	states := len(children)
	a.output = output
	a.fail = make([]int32, states)
	a.dict = make([]int32, states)
	a.edgeStart = make([]int32, states+1)
	for s := range a.dict {
		a.dict[s] = -1
	}
	sortedBytes := func(state int32) []byte {
		out := make([]byte, 0, len(children[state]))
		for c := range children[state] {
			out = append(out, c)
		}
		sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
		return out
	}
	for s := 0; s < states; s++ {
		a.edgeStart[s] = int32(len(a.edgeByte))
		for _, c := range sortedBytes(int32(s)) {
			a.edgeByte = append(a.edgeByte, c)
			a.edgeNext = append(a.edgeNext, children[s][c])
		}
	}
	a.edgeStart[states] = int32(len(a.edgeByte))

	// Breadth-first failure links: a state's link is the longest proper
	// suffix of its string that is also a trie state.
	queue := make([]int32, 0, states)
	for _, c := range sortedBytes(0) {
		queue = append(queue, children[0][c])
	}
	for head := 0; head < len(queue); head++ {
		u := queue[head]
		for _, c := range sortedBytes(u) {
			v := children[u][c]
			f := a.fail[u]
			for {
				if next, ok := children[f][c]; ok && next != v {
					a.fail[v] = next
					break
				}
				if f == 0 {
					a.fail[v] = 0
					break
				}
				f = a.fail[f]
			}
			if link := a.fail[v]; a.output[link] >= 0 {
				a.dict[v] = link
			} else {
				a.dict[v] = a.dict[link]
			}
			queue = append(queue, v)
		}
	}
	return a
}

// step follows state's edge for c.
func (a *containedAutomaton) step(state int32, c byte) (int32, bool) {
	lo, hi := a.edgeStart[state], a.edgeStart[state+1]
	for lo < hi {
		mid := lo + (hi-lo)/2
		switch b := a.edgeByte[mid]; {
		case b == c:
			return a.edgeNext[mid], true
		case b < c:
			lo = mid + 1
		default:
			hi = mid
		}
	}
	return 0, false
}

// match calls emit with the ID of every key occurrence in text (a key may be
// reported once per occurrence).
func (a *containedAutomaton) match(text string, emit func(key int32)) {
	state := int32(0)
	for i := 0; i < len(text); i++ {
		c := text[i]
		for {
			if next, ok := a.step(state, c); ok {
				state = next
				break
			}
			if state == 0 {
				break
			}
			state = a.fail[state]
		}
		if key := a.output[state]; key >= 0 {
			emit(key)
		}
		for d := a.dict[state]; d >= 0; d = a.dict[d] {
			emit(a.output[d])
		}
	}
}
