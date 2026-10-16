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
		return true
	}
	prepared := &pipelineStringMapNode[T]{hash: hash, bit: -1, bucket: &pipelineStringMapBucket[T]{first: pipelineStringMapEntry[T]{key, value}}}
	if index.root == nil {
		if !index.continueWork() {
			return false
		}
		index.root = prepared
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
	return true
}
