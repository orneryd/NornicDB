package cypher

import "strings"

// gdsJavaMapMode describes how a GDS map adds a previously unseen key.
type gdsJavaMapMode uint8

const (
	gdsJavaMapPut gdsJavaMapMode = iota
	gdsJavaMapCompute
	gdsJavaConcurrentMap
)

type gdsJavaMapEntry struct {
	key    string
	hash   uint32
	parent *gdsJavaMapEntry
	child  [2]*gdsJavaMapEntry
	red    bool
}

type gdsJavaMapBucket struct {
	entries []*gdsJavaMapEntry
	root    *gdsJavaMapEntry
}

// gdsJavaMapOrder owns the observable iteration order of the Java maps used
// during GDS projection. It models insertions, load/collision-driven resizing,
// and crowded buckets becoming red-black trees. HashMap moves a tree's root
// to the front of its iteration list; subsequent tree entries follow their
// insertion parent. ConcurrentHashMap instead prepends new tree entries.
// This is an order model only: keys are distinct and values are not stored.
func gdsJavaMapOrder(keys []string, offset uint32, initialCapacity int, mode gdsJavaMapMode) []string {
	if len(keys) < 2 {
		return keys
	}
	capacity := 1
	for capacity < initialCapacity {
		capacity *= 2
	}
	buckets := make([]gdsJavaMapBucket, capacity)
	resize := func() {
		next := make([]gdsJavaMapBucket, len(buckets)*2)
		for index, bucket := range buckets {
			for _, entry := range bucket.entries {
				position := int(entry.hash) & (len(next) - 1)
				next[position].entries = append(next[position].entries, entry)
			}
			if bucket.root != nil {
				low, high := &next[index], &next[index+len(buckets)]
				for _, half := range []*gdsJavaMapBucket{low, high} {
					if len(half.entries) > 6 {
						if len(half.entries) == len(bucket.entries) {
							half.root = bucket.root
						} else {
							half.treeify(mode)
						}
					}
				}
			} else if mode == gdsJavaConcurrentMap && len(bucket.entries) > 1 {
				// CHM reuses the last same-destination run and prepends the
				// preceding entries to their respective split lists.
				last := len(bucket.entries) - 1
				for last > 0 && (bucket.entries[last-1].hash&uint32(len(buckets))) == (bucket.entries[last].hash&uint32(len(buckets))) {
					last--
				}
				next[index].entries, next[index+len(buckets)].entries = nil, nil
				for i := last; i < len(bucket.entries); i++ {
					entry := bucket.entries[i]
					destination := &next[int(entry.hash)&(len(next)-1)]
					destination.entries = append(destination.entries, entry)
				}
				for i := 0; i < last; i++ {
					entry := bucket.entries[i]
					destination := &next[int(entry.hash)&(len(next)-1)]
					destination.entries = append([]*gdsJavaMapEntry{entry}, destination.entries...)
				}
			}
		}
		buckets = next
	}
	for i, key := range keys {
		if mode == gdsJavaMapCompute && i > len(buckets)*3/4 {
			resize()
		}
		hash := gdsJavaStringHash(key) + offset
		hash ^= hash >> 16
		entry := &gdsJavaMapEntry{key: key, hash: hash}
		bucket := &buckets[int(hash)&(len(buckets)-1)]
		if bucket.root != nil {
			parent := bucket.insertTree(entry)
			position := 0
			if mode != gdsJavaConcurrentMap {
				for bucket.entries[position] != parent {
					position++
				}
				position++
			}
			bucket.entries = append(bucket.entries, nil)
			copy(bucket.entries[position+1:], bucket.entries[position:])
			bucket.entries[position] = entry
			bucket.moveRoot(mode)
		} else {
			if mode == gdsJavaMapCompute {
				bucket.entries = append([]*gdsJavaMapEntry{entry}, bucket.entries...)
			} else {
				bucket.entries = append(bucket.entries, entry)
			}
			threshold := 9
			if mode == gdsJavaMapCompute {
				threshold = 8
			}
			if len(bucket.entries) >= threshold {
				if len(buckets) < 64 {
					if mode == gdsJavaConcurrentMap {
						// tryPresize(2*n) chooses capacity with room at 0.75 load.
						target := len(buckets) * 4
						for len(buckets)*3/4 < target {
							resize()
						}
					} else {
						resize()
					}
				} else {
					bucket.treeify(mode)
				}
			}
		}
		if mode != gdsJavaMapCompute && i+1 > len(buckets)*3/4 {
			resize()
		}
	}
	ordered := make([]string, 0, len(keys))
	for _, bucket := range buckets {
		for _, entry := range bucket.entries {
			ordered = append(ordered, entry.key)
		}
	}
	return ordered
}

func (bucket *gdsJavaMapBucket) treeify(mode gdsJavaMapMode) {
	bucket.root = nil
	for _, entry := range bucket.entries {
		entry.parent, entry.child, entry.red = nil, [2]*gdsJavaMapEntry{}, false
		bucket.insertTree(entry)
	}
	bucket.moveRoot(mode)
}

func (bucket *gdsJavaMapBucket) moveRoot(mode gdsJavaMapMode) {
	if mode == gdsJavaConcurrentMap {
		return
	}
	for i, entry := range bucket.entries {
		if entry == bucket.root {
			copy(bucket.entries[1:i+1], bucket.entries[:i])
			bucket.entries[0] = entry
			return
		}
	}
}

// insertTree uses ordinary red-black insertion, comparing signed Java hashes.
// Equal full hashes of non-comparable RelationshipTypes have VM identity order
// in GDS; use a stable name tie-break instead of introducing random output.
func (bucket *gdsJavaMapBucket) insertTree(entry *gdsJavaMapEntry) *gdsJavaMapEntry {
	if bucket.root == nil {
		bucket.root = entry
		return nil
	}
	parent := bucket.root
	for {
		direction := 0
		if int32(entry.hash) > int32(parent.hash) || entry.hash == parent.hash && strings.Compare(entry.key, parent.key) > 0 {
			direction = 1
		}
		if parent.child[direction] == nil {
			parent.child[direction], entry.parent, entry.red = entry, parent, true
			break
		}
		parent = parent.child[direction]
	}
	originalParent := parent
	for entry.parent != nil && entry.parent.red {
		parent = entry.parent
		grandparent := parent.parent
		side := 0
		if grandparent.child[1] == parent {
			side = 1
		}
		uncle := grandparent.child[1-side]
		if uncle != nil && uncle.red {
			parent.red, uncle.red, grandparent.red = false, false, true
			entry = grandparent
			continue
		}
		if parent.child[1-side] == entry {
			bucket.rotate(parent, side)
			entry, parent = parent, entry
		}
		parent.red, grandparent.red = false, true
		bucket.rotate(grandparent, 1-side)
	}
	bucket.root.red = false
	return originalParent
}

// rotate promotes a node's child opposite direction (0 is a left rotation).
func (bucket *gdsJavaMapBucket) rotate(node *gdsJavaMapEntry, direction int) {
	promoted := node.child[1-direction]
	node.child[1-direction] = promoted.child[direction]
	if child := node.child[1-direction]; child != nil {
		child.parent = node
	}
	promoted.parent = node.parent
	if node.parent == nil {
		bucket.root = promoted
	} else if node.parent.child[0] == node {
		node.parent.child[0] = promoted
	} else {
		node.parent.child[1] = promoted
	}
	promoted.child[direction], node.parent = node, promoted
}
