package cypher

import (
	"hash/maphash"
	"math/bits"
)

var pipelineStringMapSeed = maphash.MakeSeed()

type pipelineStringMapEntry[T any] struct {
	key   string
	value T
}
type pipelineStringMapPage[T any] struct {
	values [64]pipelineStringMapEntry[T]
	used   int
	next   *pipelineStringMapPage[T]
}
type pipelineStringMapBucket[T any] struct {
	first      pipelineStringMapEntry[T]
	head, tail *pipelineStringMapPage[T]
}
type pipelineStringMapNode[T any] struct {
	hash     uint64
	bit      int
	children [2]*pipelineStringMapNode[T]
	bucket   *pipelineStringMapBucket[T]
}

// All keys use one bounded directory. Integer hashes alone do not make a
// native map insertion bounded: its directory can grow with its cardinality.
// Strictly increasing crit-bit positions limit a lookup to 64 branch decisions.
// Collision pages never recopy their accumulated prefix.
type pipelineStringMap[T any] struct {
	probe *pipelineCancellation
	root  *pipelineStringMapNode[T]
	size  int
}

func (index *pipelineStringMap[T]) Len() int { return index.size }
func (index *pipelineStringMap[T]) Clear()   { index.root = nil; index.size = 0 }

func (index *pipelineStringMap[T]) SetWithCancellation(key string, value T, probe *pipelineCancellation) bool {
	borrowed := *index
	borrowed.probe = probe
	if !borrowed.Set(key, value) {
		return false
	}
	index.root, index.size = borrowed.root, borrowed.size
	return true
}

func (index *pipelineStringMap[T]) DeleteWithCancellation(key string, probe *pipelineCancellation) bool {
	if probe != nil && !probe.check() {
		return false
	}
	hash := hashPipelineString(key, pipelineStringMapSeed, probe)
	if probe != nil && probe.err != nil {
		return false
	}
	link := &index.root
	var parentLink **pipelineStringMapNode[T]
	var parent *pipelineStringMapNode[T]
	var direction uint64
	for *link != nil && (*link).bit >= 0 {
		if probe != nil && !probe.check() {
			return false
		}
		parentLink, parent = link, *link
		direction = (hash >> uint(63-parent.bit)) & 1
		link = &parent.children[direction]
	}
	if *link == nil || (*link).hash != hash {
		return false
	}
	borrowed := *index
	borrowed.probe = probe
	bucket := (*link).bucket
	entry := borrowed.find(bucket, key)
	if entry == nil {
		return false
	}
	page := bucket.tail
	var previous *pipelineStringMapPage[T]
	if page != nil && page.used == 1 {
		for candidate := bucket.head; candidate != page; candidate = candidate.next {
			if probe != nil && !probe.check() {
				return false
			}
			previous = candidate
		}
	}
	if probe != nil && !probe.check() {
		return false
	}
	// Prepared constant-size commit; no callback after changing the entry.
	if page == nil {
		if parent == nil {
			index.root = nil
		} else {
			*parentLink = parent.children[1-direction]
		}
	} else {
		last := &page.values[page.used-1]
		*entry = *last
		var empty pipelineStringMapEntry[T]
		*last = empty
		page.used--
		if page.used == 0 {
			if previous == nil {
				bucket.head = nil
			} else {
				previous.next = nil
			}
			bucket.tail = previous
		}
	}
	index.size--
	return true
}

func (index *pipelineStringMap[T]) continueWork() bool {
	return index.probe == nil || index.probe.check()
}
func (index *pipelineStringMap[T]) lookup(hash uint64) *pipelineStringMapBucket[T] {
	node := index.root
	for node != nil && node.bit >= 0 {
		if !index.continueWork() {
			return nil
		}
		node = node.children[(hash>>uint(63-node.bit))&1]
	}
	if node != nil && node.hash == hash {
		return node.bucket
	}
	return nil
}
func (index *pipelineStringMap[T]) find(bucket *pipelineStringMapBucket[T], key string) *pipelineStringMapEntry[T] {
	if bucket == nil {
		return nil
	}
	equal := func(entry *pipelineStringMapEntry[T]) bool {
		if !index.continueWork() {
			return false
		}
		order := comparePipelineStrings(entry.key, key, index.probe)
		return (index.probe == nil || index.probe.err == nil) && order == 0
	}
	if equal(&bucket.first) {
		return &bucket.first
	}
	if index.probe != nil && index.probe.err != nil {
		return nil
	}
	for page := bucket.head; page != nil; page = page.next {
		for item := 0; item < page.used; item++ {
			if equal(&page.values[item]) {
				return &page.values[item]
			}
			if index.probe != nil && index.probe.err != nil {
				return nil
			}
		}
	}
	return nil
}
func (index *pipelineStringMap[T]) Get(key string) (value T, exists bool) {
	if !index.continueWork() {
		return value, false
	}
	hash := hashPipelineString(key, pipelineStringMapSeed, index.probe)
	if index.probe != nil && index.probe.err != nil {
		return value, false
	}
	entry := index.find(index.lookup(hash), key)
	if entry != nil {
		return entry.value, true
	}
	return value, false
}
func (index *pipelineStringMap[T]) Set(key string, value T) bool {
	if !index.continueWork() {
		return false
	}
	hash := hashPipelineString(key, pipelineStringMapSeed, index.probe)
	if index.probe != nil && index.probe.err != nil {
		return false
	}
	bucket := index.lookup(hash)
	if entry := index.find(bucket, key); entry != nil {
		entry.value = value
		return true
	}
	if index.probe != nil && index.probe.err != nil {
		return false
	}
	if bucket != nil {
		page := bucket.tail
		if page == nil || page.used == len(page.values) {
			prepared := &pipelineStringMapPage[T]{}
			prepared.values[0] = pipelineStringMapEntry[T]{key, value}
			prepared.used = 1
			if !index.continueWork() {
				return false
			}
			if page == nil {
				bucket.head = prepared
			} else {
				page.next = prepared
			}
			bucket.tail = prepared
		} else {
			if !index.continueWork() {
				return false
			}
			page.values[page.used] = pipelineStringMapEntry[T]{key, value}
			page.used++
		}
		index.size++
		return true
	}
	prepared := &pipelineStringMapNode[T]{hash: hash, bit: -1, bucket: &pipelineStringMapBucket[T]{first: pipelineStringMapEntry[T]{key, value}}}
	if index.root == nil {
		if !index.continueWork() {
			return false
		}
		index.root = prepared
		index.size++
		return true
	}
	node := index.root
	for node.bit >= 0 {
		if !index.continueWork() {
			return false
		}
		node = node.children[(hash>>uint(63-node.bit))&1]
	}
	bit := bits.LeadingZeros64(node.hash ^ hash)
	link := &index.root
	for (*link).bit >= 0 && (*link).bit < bit {
		if !index.continueWork() {
			return false
		}
		link = &(*link).children[(hash>>uint(63-(*link).bit))&1]
	}
	branch := &pipelineStringMapNode[T]{bit: bit}
	direction := (hash >> uint(63-bit)) & 1
	branch.children[direction] = prepared
	branch.children[1-direction] = *link
	if !index.continueWork() {
		return false
	}
	*link = branch
	index.size++
	return true
}
