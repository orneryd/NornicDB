// Copyright (c) 2026 NornicDB contributors.
// Use of this file is governed by the BSD 3-clause license in LICENSE.

package antlr

import "math/bits"

func chargeWork(check func()) {
	if check != nil {
		check()
	}
}

// scalarDirectory uses existing integer hashes unchanged. Strictly increasing
// branch positions bound each lookup/search to 64 decisions; insertion commits
// one prepared parent link, without a native map directory resize.
type scalarDirectory[V any] struct{ root *scalarDirectoryNode[V] }
type scalarDirectoryNode[V any] struct {
	key      uint64
	bit      int
	children [2]*scalarDirectoryNode[V]
	value    *V
}

func (directory *scalarDirectory[V]) get(key int, check func()) *V {
	current := directory.root
	for current != nil && current.bit >= 0 {
		chargeWork(check)
		current = current.children[(uint64(key)>>uint(63-current.bit))&1]
	}
	if current != nil && current.key == uint64(key) {
		return current.value
	}
	return nil
}

func (directory *scalarDirectory[V]) put(key int, value *V, check func()) {
	chargeWork(check)
	current := directory.root
	if current == nil {
		prepared := &scalarDirectoryNode[V]{key: uint64(key), bit: -1, value: value}
		chargeWork(check)
		directory.root = prepared
		return
	}
	for current.bit >= 0 {
		chargeWork(check)
		current = current.children[(uint64(key)>>uint(63-current.bit))&1]
	}
	if current.key == uint64(key) {
		chargeWork(check)
		current.value = value
		return
	}
	bit := bits.LeadingZeros64(current.key ^ uint64(key))
	link := &directory.root
	for (*link).bit >= 0 && (*link).bit < bit {
		chargeWork(check)
		link = &(*link).children[(uint64(key)>>uint(63-(*link).bit))&1]
	}
	leaf := &scalarDirectoryNode[V]{key: uint64(key), bit: -1, value: value}
	branch := &scalarDirectoryNode[V]{bit: bit}
	direction := (uint64(key) >> uint(63-bit)) & 1
	branch.children[direction] = leaf
	branch.children[1-direction] = *link
	chargeWork(check)
	*link = branch
}

func (directory *scalarDirectory[V]) each(check func(), visit func(*V) bool) bool {
	var walk func(*scalarDirectoryNode[V]) bool
	walk = func(node *scalarDirectoryNode[V]) bool {
		if node == nil {
			return true
		}
		chargeWork(check)
		if node.bit < 0 {
			return visit(node.value)
		}
		return walk(node.children[0]) && walk(node.children[1])
	}
	return walk(directory.root)
}

// Collision storage has one inline entry and fixed pages. Preparing a new
// page never copies an old prefix; its link/count commit is callback-free.
type collisionBucket[T any] struct {
	first      T
	count      int
	head, tail *collisionPage[T]
}
type collisionPage[T any] struct {
	entries [64]T
	used    int
	next    *collisionPage[T]
}

func (bucket *collisionBucket[T]) append(value T, check func()) {
	chargeWork(check)
	if bucket.count == 0 {
		bucket.first = value
		bucket.count = 1
		return
	}
	page := bucket.tail
	if page == nil || page.used == len(page.entries) {
		prepared := &collisionPage[T]{}
		prepared.entries[0] = value
		prepared.used = 1
		chargeWork(check)
		if page == nil {
			bucket.head = prepared
		} else {
			page.next = prepared
		}
		bucket.tail = prepared
		bucket.count++
		return
	}
	page.entries[page.used] = value
	page.used++
	bucket.count++
}

func (bucket *collisionBucket[T]) each(check func(), visit func(T) bool) bool {
	if bucket == nil || bucket.count == 0 {
		return true
	}
	chargeWork(check)
	if !visit(bucket.first) {
		return false
	}
	for page := bucket.head; page != nil; page = page.next {
		for i := 0; i < page.used; i++ {
			chargeWork(check)
			if !visit(page.entries[i]) {
				return false
			}
		}
	}
	return true
}

// CollectionOperation is transient work for one canonical collection call.
// It is never retained on a collection, comparator or published value.
type CollectionOperation[T any] struct {
	Check  func()
	Hash   func(T, func()) int
	Equals func(T, T, func()) bool
}

func operationCheck[T any](operation *CollectionOperation[T]) func() {
	if operation == nil {
		return nil
	}
	return operation.Check
}
func operationHash[T any](comparator Comparator[T], value T, operation *CollectionOperation[T]) int {
	if operation == nil {
		return comparator.Hash1(value)
	}
	chargeWork(operation.Check)
	if operation.Hash != nil {
		return operation.Hash(value, operation.Check)
	}
	return comparator.Hash1(value)
}
func operationEqual[T any](comparator Comparator[T], left, right T, operation *CollectionOperation[T]) bool {
	if operation == nil {
		return comparator.Equals2(left, right)
	}
	chargeWork(operation.Check)
	if operation.Equals != nil {
		return operation.Equals(left, right, operation.Check)
	}
	return comparator.Equals2(left, right)
}
