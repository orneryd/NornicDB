package cypher

import (
	"sort"
	"unicode/utf16"
)

// gdsJavaStringHash owns Java's case-sensitive UTF-16 name hash. GDS adds
// the Objects.hash offset when the key is a RelationshipType rather than text.
func gdsJavaStringHash(name string) uint32 {
	var hash uint32
	for _, character := range name {
		if character <= 0xffff {
			hash = 31*hash + uint32(character)
		} else {
			high, low := utf16.EncodeRune(character)
			hash = 31*(31*hash+uint32(high)) + uint32(low)
		}
	}
	return hash
}

// gdsNativeProjectionMapOrder adapts a Cypher literal's keys to the native
// projection's Java argument map. Neo4j's LiteralMap evaluates a Scala map
// (insertion ordered up to four entries, then a hash trie), followed by
// MapValueBuilder's Java map and the procedure argument map. These stages
// matter when distinct names collide in the later RelationshipType map.
// This adapter is specific to native projection; aggregation builds its
// relationship map from rows instead of a Cypher projection map.
func gdsNativeProjectionMapOrder(keys []string) []string {
	if len(keys) < 2 {
		return keys
	}
	ordered := append([]string(nil), keys...)
	rawHashes := make(map[string]uint32, len(keys))
	improved := make(map[string]uint32, len(keys))
	for _, key := range keys {
		raw := gdsJavaStringHash(key)
		rawHashes[key] = raw ^ (raw >> 16)
		// Scala's hash-trie mixer, expressed with wrapping uint32 math.
		mixed := uint32(0) - 511*raw - 1
		mixed ^= mixed >> 14
		mixed *= 17
		improved[key] = mixed ^ (mixed >> 10)
	}
	if len(keys) > 4 {
		ordered = nil
		var walk func([]string, uint)
		walk = func(items []string, shift uint) {
			if shift >= 32 {
				ordered = append(ordered, items...)
				return
			}
			var groups [32][]string
			for _, item := range items {
				bucket := (improved[item] >> shift) & 31
				groups[bucket] = append(groups[bucket], item)
			}
			// Inline entries precede child nodes in a Scala map trie.
			for _, group := range groups {
				if len(group) == 1 {
					ordered = append(ordered, group[0])
				}
			}
			for _, group := range groups {
				if len(group) > 1 {
					walk(group, shift+5)
				}
			}
		}
		walk(keys, 0)
	}
	orderBuckets := func(capacity int) {
		sort.SliceStable(ordered, func(i, j int) bool {
			return rawHashes[ordered[i]]&uint32(capacity-1) < rawHashes[ordered[j]]&uint32(capacity-1)
		})
	}
	capacity := 16
	for capacity*3/4 < len(keys) {
		capacity *= 2
	}
	orderBuckets(capacity)
	capacity = 1
	for capacity*3/4 < len(keys) {
		capacity *= 2
	}
	orderBuckets(capacity)
	return ordered
}
