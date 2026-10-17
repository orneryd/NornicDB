package storage

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// bruteContained is what the evaluator's CONTAINS gives: every (node, key)
// with key a substring of text, ordered like Lookup.
func bruteContained(nodes map[NodeID]map[string]interface{}, properties []string, text string) []ContainedMatch {
	var out []ContainedMatch
	for id, props := range nodes {
		keys, empty := containedKeys(properties, props)
		if empty {
			out = append(out, ContainedMatch{NodeID: id})
		}
		for _, key := range keys {
			if strings.Contains(text, key) {
				out = append(out, ContainedMatch{NodeID: id, Key: key})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].NodeID != out[j].NodeID {
			return out[i].NodeID < out[j].NodeID
		}
		return out[i].Key < out[j].Key
	})
	return out
}

func refile(idx *ContainedIndex, id NodeID, props map[string]interface{}) {
	idx.RefileNode(id, func() (map[string]interface{}, error) { return props, nil }, nil)
}

// Overlapping keys, multi-byte keys, duplicates across nodes, the empty key,
// lists and values without a key.
func TestContainedIndexLookupMatchesContains(t *testing.T) {
	idx := newContainedIndex("keys", "Identity", []string{"name_key", "alias_keys"})
	nodes := map[NodeID]map[string]interface{}{
		"db:1": {"name_key": "ab", "alias_keys": []interface{}{"b", "abc"}},
		"db:2": {"name_key": "b"},
		"db:3": {"name_key": "café", "alias_keys": []string{"é", "fé"}},
		"db:4": {"name_key": "", "alias_keys": []interface{}{"zz", int64(1), nil, []interface{}{"ab"}}},
		"db:5": {"name_key": int64(7), "alias_keys": "x"},
		"db:6": {"other": "ab"},
		"db:7": {"name_key": "日本", "alias_keys": []interface{}{"本語", "日本語"}},
	}
	for id, props := range nodes {
		refile(idx, id, props)
	}
	for _, text := range []string{"", "abc", "xabcx", "b", "le café noir", "é", "日本語を", "nothing", "zzx"} {
		require.Equal(t, bruteContained(nodes, idx.Properties, text), idx.Lookup(text), "text %q", text)
	}
	require.Equal(t, []NodeID{"db:1", "db:2", "db:4"}, idx.LookupNodes("abc"))
}

// Lookups stay exact across automaton generations: before the first build,
// after builds triggered by the delta and by dead keys, and while nodes move
// between keys.
func TestContainedIndexGenerations(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	idx := newContainedIndex("keys", "L", []string{"k"})
	nodes := make(map[NodeID]map[string]interface{})
	word := func() string {
		const letters = "abcdeé"
		runes := []rune(letters)
		n := 1 + rng.Intn(4)
		var b strings.Builder
		for i := 0; i < n; i++ {
			b.WriteRune(runes[rng.Intn(len(runes))])
		}
		return b.String()
	}
	check := func(stage string) {
		t.Helper()
		for i := 0; i < 20; i++ {
			text := word() + word() + word() + word()
			require.Equal(t, bruteContained(nodes, idx.Properties, text), idx.Lookup(text), "%s: %q", stage, text)
		}
	}
	for round := 0; round < 6; round++ {
		for i := 0; i < 400; i++ {
			id := NodeID(fmt.Sprintf("db:%d", rng.Intn(600)))
			switch rng.Intn(4) {
			case 0:
				delete(nodes, id)
				refile(idx, id, nil)
			default:
				props := map[string]interface{}{"k": []interface{}{word(), word() + fmt.Sprint(round)}}
				nodes[id] = props
				refile(idx, id, props)
			}
		}
		check(fmt.Sprintf("round %d", round))
	}
	require.NotNil(t, idx.automaton.Load(), "the delta limit must have triggered a build")
}

// Concurrent writers and lookups (run with -race) end at the brute-force
// answer.
func TestContainedIndexConcurrent(t *testing.T) {
	idx := newContainedIndex("keys", "L", []string{"k"})
	var wg sync.WaitGroup
	final := make([]map[string]interface{}, 8)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			id := NodeID(fmt.Sprintf("db:w%d", w))
			for i := 0; i < 300; i++ {
				props := map[string]interface{}{"k": []interface{}{fmt.Sprintf("key%d-%d", w, i), fmt.Sprintf("shared%d", i%40)}}
				refile(idx, id, props)
				final[w] = props
				if i%7 == 0 {
					_ = idx.Lookup(fmt.Sprintf("text key%d-%d shared%d", w, i, i%40))
				}
			}
		}(w)
	}
	wg.Wait()
	nodes := make(map[NodeID]map[string]interface{})
	for w, props := range final {
		nodes[NodeID(fmt.Sprintf("db:w%d", w))] = props
	}
	for _, text := range []string{"key3-299 and shared19", "shared1", "nothing"} {
		require.Equal(t, bruteContained(nodes, idx.Properties, text), idx.Lookup(text), text)
	}
}
