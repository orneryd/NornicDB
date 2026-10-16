package antlr

// Copyright (c) 2012-2022 The ANTLR Project. All rights reserved.
// Use of this file is governed by the BSD 3-clause license that
// can be found in the LICENSE.txt file in the project root.

import (
	"container/list"
	"runtime/debug"
	"sort"
	"unsafe"
)

// Collectable is an interface that a struct should implement if it is to be
// usable as a key in these collections.
type Collectable[T any] interface {
	Hash() int
	Equals(other Collectable[T]) bool
}

type Comparator[T any] interface {
	Hash1(o T) int
	Equals2(T, T) bool
}

type CollectionSource int
type CollectionDescriptor struct {
	SybolicName string
	Description string
}

const (
	UnknownCollection CollectionSource = iota
	ATNConfigLookupCollection
	ATNStateCollection
	DFAStateCollection
	ATNConfigCollection
	PredictionContextCollection
	SemanticContextCollection
	ClosureBusyCollection
	PredictionVisitedCollection
	MergeCacheCollection
	PredictionContextCacheCollection
	AltSetCollection
	ReachSetCollection
)

var CollectionDescriptors = map[CollectionSource]CollectionDescriptor{
	UnknownCollection: {
		SybolicName: "UnknownCollection",
		Description: "Unknown collection type. Only used if the target author thought it was an unimportant collection.",
	},
	ATNConfigCollection: {
		SybolicName: "ATNConfigCollection",
		Description: "ATNConfig collection. Used to store the ATNConfigs for a particular state in the ATN." +
			"For instance, it is used to store the results of the closure() operation in the ATN.",
	},
	ATNConfigLookupCollection: {
		SybolicName: "ATNConfigLookupCollection",
		Description: "ATNConfigLookup collection. Used to store the ATNConfigs for a particular state in the ATN." +
			"This is used to prevent duplicating equivalent states in an ATNConfigurationSet.",
	},
	ATNStateCollection: {
		SybolicName: "ATNStateCollection",
		Description: "ATNState collection. This is used to store the states of the ATN.",
	},
	DFAStateCollection: {
		SybolicName: "DFAStateCollection",
		Description: "DFAState collection. This is used to store the states of the DFA.",
	},
	PredictionContextCollection: {
		SybolicName: "PredictionContextCollection",
		Description: "PredictionContext collection. This is used to store the prediction contexts of the ATN and cache computes.",
	},
	SemanticContextCollection: {
		SybolicName: "SemanticContextCollection",
		Description: "SemanticContext collection. This is used to store the semantic contexts of the ATN.",
	},
	ClosureBusyCollection: {
		SybolicName: "ClosureBusyCollection",
		Description: "ClosureBusy collection. This is used to check and prevent infinite recursion right recursive rules." +
			"It stores ATNConfigs that are currently being processed in the closure() operation.",
	},
	PredictionVisitedCollection: {
		SybolicName: "PredictionVisitedCollection",
		Description: "A map that records whether we have visited a particular context when searching through cached entries.",
	},
	MergeCacheCollection: {
		SybolicName: "MergeCacheCollection",
		Description: "A map that records whether we have already merged two particular contexts and can save effort by not repeating it.",
	},
	PredictionContextCacheCollection: {
		SybolicName: "PredictionContextCacheCollection",
		Description: "A map that records whether we have already created a particular context and can save effort by not computing it again.",
	},
	AltSetCollection: {
		SybolicName: "AltSetCollection",
		Description: "Used to eliminate duplicate alternatives in an ATN config set.",
	},
	ReachSetCollection: {
		SybolicName: "ReachSetCollection",
		Description: "Used as merge cache to prevent us needing to compute the merge of two states if we have already done it.",
	},
}

// JStore implements a container that allows the use of a struct to calculate the key
// for a collection of values akin to map. This is not meant to be a full-blown HashMap but just
// serve the needs of the ANTLR Go runtime.
//
// For ease of porting the logic of the runtime from the master target (Java), this collection
// operates in a similar way to Java, in that it can use any struct that supplies a Hash() and Equals()
// function as the key. Hashes index a fixed-depth scalar directory; collisions
// occupy fixed-size pages. Growing either owner never copies an accumulated
// prefix while a query cancellation callback is suspended. This collection
// serves the ANTLR runtime rather than a general-purpose map API.
type JStore[T any, C Comparator[T]] struct {
	store      scalarDirectory[collisionBucket[T]]
	len        int
	comparator Comparator[T]
	stats      *JStatRec
}

func NewJStore[T any, C Comparator[T]](comparator Comparator[T], cType CollectionSource, desc string) *JStore[T, C] {

	if comparator == nil {
		panic("comparator cannot be nil")
	}

	s := &JStore[T, C]{
		comparator: comparator,
	}
	if collectStats {
		s.stats = &JStatRec{
			Source:      cType,
			Description: desc,
		}

		// Track where we created it from  if we are being asked to do so
		if runtimeConfig.statsTraceStacks {
			s.stats.CreateStack = debug.Stack()
		}
		Statistics.AddJStatRec(s.stats)
	}
	return s
}

// Put will store given value in the collection. Note that the key for storage is generated from
// the value itself - this is specifically because that is what ANTLR needs - this would not be useful
// as any kind of general collection.
//
// If the key has a hash conflict, then the value will be added to the slice of values associated with the
// hash, unless the value is already in the slice, in which case the existing value is returned. Value equivalence is
// tested by calling the equals() method on the key.
//
// # If the given value is already present in the store, then the existing value is returned as v and exists is set to true
//
// If the given value is not present in the store, then the value is added to the store and returned as v and exists is set to false.
func (s *JStore[T, C]) Put(value T) (T, bool) { return s.PutWithOperation(value, nil) }
func (s *JStore[T, C]) PutWithOperation(value T, operation *CollectionOperation[T]) (T, bool) {
	check := operationCheck(operation)
	if collectStats {
		s.stats.Puts++
	}
	hash := operationHash(s.comparator, value, operation)
	bucket := s.store.get(hash, check)
	found := false
	var existing T
	bucket.each(check, func(candidate T) bool {
		if collectStats {
			s.stats.PutHashConflicts++
		}
		if operationEqual(s.comparator, value, candidate, operation) {
			existing = candidate
			found = true
			if collectStats {
				s.stats.PutHits++
			}
			return false
		}
		if collectStats {
			s.stats.PutMisses++
		}
		return true
	})
	if found {
		return existing, true
	}
	if bucket == nil {
		bucket = &collisionBucket[T]{}
		bucket.append(value, check)
		s.store.put(hash, bucket, check)
	} else {
		bucket.append(value, check)
	}
	// No callback between publication and corresponding metadata.
	s.len++
	if collectStats {
		s.stats.CurSize = s.len
		if s.len > s.stats.MaxSize {
			s.stats.MaxSize = s.len
		}
		if bucket.count > s.stats.MaxSlotSize {
			s.stats.MaxSlotSize = bucket.count
		}
	}
	return value, false
}
func (s *JStore[T, C]) Get(key T) (T, bool) { return s.GetWithOperation(key, nil) }
func (s *JStore[T, C]) GetWithOperation(key T, operation *CollectionOperation[T]) (T, bool) {
	check := operationCheck(operation)
	if collectStats {
		s.stats.Gets++
	}
	hash := operationHash(s.comparator, key, operation)
	bucket := s.store.get(hash, check)
	found := false
	result := key
	bucket.each(check, func(candidate T) bool {
		if collectStats {
			s.stats.GetHashConflicts++
		}
		if operationEqual(s.comparator, key, candidate, operation) {
			result = candidate
			found = true
			if collectStats {
				s.stats.GetHits++
			}
			return false
		}
		if collectStats {
			s.stats.GetMisses++
		}
		return true
	})
	if collectStats && !found {
		s.stats.GetNoEnt++
	}
	return result, found
}
func (s *JStore[T, C]) Contains(key T) bool { _, ok := s.Get(key); return ok }
func (s *JStore[T, C]) SortedSlice(less func(i, j T) bool) []T {
	values := s.Values()
	sort.Slice(values, func(i, j int) bool { return less(values[i], values[j]) })
	return values
}
func (s *JStore[T, C]) Each(visit func(T) bool) {
	s.store.each(nil, func(bucket *collisionBucket[T]) bool {
		bucket.each(nil, func(value T) bool { visit(value); return true })
		return true
	})
}
func (s *JStore[T, C]) Len() int { return s.len }
func (s *JStore[T, C]) Values() []T {
	values := make([]T, 0, s.len)
	s.store.each(nil, func(bucket *collisionBucket[T]) bool {
		bucket.each(nil, func(value T) bool { values = append(values, value); return true })
		return true
	})
	return values
}
func (s *JStore[T, C]) eachWithWork(check func(), visit func(T) bool) bool {
	return s.store.each(check, func(bucket *collisionBucket[T]) bool { return bucket.each(check, visit) })
}

type entry[K, V any] struct {
	key K
	val V
}

type JMap[K, V any, C Comparator[K]] struct {
	store      scalarDirectory[collisionBucket[*entry[K, V]]]
	len        int
	comparator Comparator[K]
	stats      *JStatRec
}

func NewJMap[K, V any, C Comparator[K]](comparator Comparator[K], cType CollectionSource, desc string) *JMap[K, V, C] {
	m := &JMap[K, V, C]{
		comparator: comparator,
	}
	if collectStats {
		m.stats = &JStatRec{
			Source:      cType,
			Description: desc,
		}
		// Track where we created it from  if we are being asked to do so
		if runtimeConfig.statsTraceStacks {
			m.stats.CreateStack = debug.Stack()
		}
		Statistics.AddJStatRec(m.stats)
	}
	return m
}

func (m *JMap[K, V, C]) Put(key K, val V) (V, bool) { return m.PutWithOperation(key, val, nil) }
func (m *JMap[K, V, C]) PutWithOperation(key K, val V, operation *CollectionOperation[K]) (V, bool) {
	check := operationCheck(operation)
	if collectStats {
		m.stats.Puts++
	}
	hash := operationHash(m.comparator, key, operation)
	bucket := m.store.get(hash, check)
	found := false
	var result V
	bucket.each(check, func(candidate *entry[K, V]) bool {
		if collectStats {
			m.stats.PutHashConflicts++
		}
		if operationEqual(m.comparator, candidate.key, key, operation) {
			result = candidate.val
			found = true
			if collectStats {
				m.stats.PutHits++
			}
			return false
		}
		if collectStats {
			m.stats.PutMisses++
		}
		return true
	})
	if found {
		return result, true
	}
	prepared := &entry[K, V]{key: key, val: val}
	if bucket == nil {
		bucket = &collisionBucket[*entry[K, V]]{}
		bucket.append(prepared, check)
		m.store.put(hash, bucket, check)
	} else {
		bucket.append(prepared, check)
	}
	m.len++
	if collectStats {
		m.stats.CurSize = m.len
		if m.len > m.stats.MaxSize {
			m.stats.MaxSize = m.len
		}
		if bucket.count > m.stats.MaxSlotSize {
			m.stats.MaxSlotSize = bucket.count
		}
	}
	return val, false
}
func (m *JMap[K, V, C]) Get(key K) (V, bool) { return m.GetWithOperation(key, nil) }
func (m *JMap[K, V, C]) GetWithOperation(key K, operation *CollectionOperation[K]) (V, bool) {
	check := operationCheck(operation)
	if collectStats {
		m.stats.Gets++
	}
	hash := operationHash(m.comparator, key, operation)
	bucket := m.store.get(hash, check)
	found := false
	var result V
	bucket.each(check, func(candidate *entry[K, V]) bool {
		if collectStats {
			m.stats.GetHashConflicts++
		}
		if operationEqual(m.comparator, candidate.key, key, operation) {
			result = candidate.val
			found = true
			if collectStats {
				m.stats.GetHits++
			}
			return false
		}
		if collectStats {
			m.stats.GetMisses++
		}
		return true
	})
	if collectStats && !found {
		m.stats.GetNoEnt++
	}
	return result, found
}
func (m *JMap[K, V, C]) Values() []V {
	values := make([]V, 0, m.len)
	m.store.each(nil, func(bucket *collisionBucket[*entry[K, V]]) bool {
		bucket.each(nil, func(item *entry[K, V]) bool { values = append(values, item.val); return true })
		return true
	})
	return values
}
func (m *JMap[K, V, C]) Len() int { return m.len }
func (m *JMap[K, V, C]) valuesWithWork(check func()) *workSequence[V] {
	values := &workSequence[V]{}
	m.store.each(check, func(bucket *collisionBucket[*entry[K, V]]) bool {
		bucket.each(check, func(item *entry[K, V]) bool { values.Append(item.val, check); return true })
		return true
	})
	return values
}
func (m *JMap[K, V, C]) Delete(key K) { m.DeleteWithOperation(key, nil) }
func (m *JMap[K, V, C]) DeleteWithOperation(key K, operation *CollectionOperation[K]) {
	check := operationCheck(operation)
	hash := operationHash(m.comparator, key, operation)
	bucket := m.store.get(hash, check)
	prepared := &collisionBucket[*entry[K, V]]{}
	removed := false
	bucket.each(check, func(item *entry[K, V]) bool {
		if !removed && operationEqual(m.comparator, item.key, key, operation) {
			removed = true
		} else {
			prepared.append(item, check)
		}
		return true
	})
	if removed {
		m.store.put(hash, prepared, check)
		m.len--
	}
}
func (m *JMap[K, V, C]) Clear() {
	m.store = scalarDirectory[collisionBucket[*entry[K, V]]]{}
}

type JPCMap struct {
	store *JMap[*PredictionContext, *JMap[*PredictionContext, *PredictionContext, *ObjEqComparator[*PredictionContext]], *ObjEqComparator[*PredictionContext]]
	size  int
	stats *JStatRec
}

func NewJPCMap(cType CollectionSource, desc string) *JPCMap {
	m := &JPCMap{
		store: NewJMap[*PredictionContext, *JMap[*PredictionContext, *PredictionContext, *ObjEqComparator[*PredictionContext]], *ObjEqComparator[*PredictionContext]](pContextEqInst, cType, desc),
	}
	if collectStats {
		m.stats = &JStatRec{
			Source:      cType,
			Description: desc,
		}
		// Track where we created it from  if we are being asked to do so
		if runtimeConfig.statsTraceStacks {
			m.stats.CreateStack = debug.Stack()
		}
		Statistics.AddJStatRec(m.stats)
	}
	return m
}

func (pcm *JPCMap) Get(k1, k2 *PredictionContext) (*PredictionContext, bool) {
	return pcm.getWithWork(k1, k2, nil)
}
func (pcm *JPCMap) getWithWork(k1, k2 *PredictionContext, check func()) (*PredictionContext, bool) {
	if collectStats {
		pcm.stats.Gets++
	}
	// Do we have a map stored by k1?
	//
	operation := predictionContextOperation(check)
	m2, present := pcm.store.GetWithOperation(k1, operation)
	if present {
		if collectStats {
			pcm.stats.GetHits++
		}
		// We found a map of values corresponding to k1, so now we need to look up k2 in that map
		//
		return m2.GetWithOperation(k2, operation)
	}
	if collectStats {
		pcm.stats.GetMisses++
	}
	return nil, false
}

func (pcm *JPCMap) Put(k1, k2, v *PredictionContext) {
	pcm.putWithWork(k1, k2, v, nil)
}
func (pcm *JPCMap) putWithWork(k1, k2, v *PredictionContext, check func()) {

	if collectStats {
		pcm.stats.Puts++
	}
	// First does a map already exist for k1?
	//
	operation := predictionContextOperation(check)
	if m2, present := pcm.store.GetWithOperation(k1, operation); present {
		if collectStats {
			pcm.stats.PutHits++
		}
		_, present = m2.PutWithOperation(k2, v, operation)
		if !present {
			pcm.size++
			if collectStats {
				pcm.stats.CurSize = pcm.size
				if pcm.size > pcm.stats.MaxSize {
					pcm.stats.MaxSize = pcm.size
				}
			}
		}
	} else {
		// No map found for k1, so we create it, add in our value, then store is
		//
		if collectStats {
			pcm.stats.PutMisses++
			m2 = NewJMap[*PredictionContext, *PredictionContext, *ObjEqComparator[*PredictionContext]](pContextEqInst, pcm.stats.Source, pcm.stats.Description+" map entry")
		} else {
			m2 = NewJMap[*PredictionContext, *PredictionContext, *ObjEqComparator[*PredictionContext]](pContextEqInst, PredictionContextCacheCollection, "map entry")
		}

		m2.PutWithOperation(k2, v, operation)
		pcm.store.PutWithOperation(k1, m2, operation)
		pcm.size++
	}
}

type JPCMap2 struct {
	store map[int][]JPCEntry
	size  int
	stats *JStatRec
}

type JPCEntry struct {
	k1, k2, v *PredictionContext
}

func NewJPCMap2(cType CollectionSource, desc string) *JPCMap2 {
	m := &JPCMap2{
		store: make(map[int][]JPCEntry, 1000),
	}
	if collectStats {
		m.stats = &JStatRec{
			Source:      cType,
			Description: desc,
		}
		// Track where we created it from  if we are being asked to do so
		if runtimeConfig.statsTraceStacks {
			m.stats.CreateStack = debug.Stack()
		}
		Statistics.AddJStatRec(m.stats)
	}
	return m
}

func dHash(k1, k2 *PredictionContext) int {
	return k1.cachedHash*31 + k2.cachedHash
}

func (pcm *JPCMap2) Get(k1, k2 *PredictionContext) (*PredictionContext, bool) {
	if collectStats {
		pcm.stats.Gets++
	}

	h := dHash(k1, k2)
	var hClash bool
	for _, e := range pcm.store[h] {
		hClash = true
		if e.k1.Equals(k1) && e.k2.Equals(k2) {
			if collectStats {
				pcm.stats.GetHits++
				pcm.stats.GetHashConflicts++
			}
			return e.v, true
		}
		if collectStats {
			pcm.stats.GetMisses++
		}
	}
	if collectStats {
		if hClash {
			pcm.stats.GetHashConflicts++
		}
		pcm.stats.GetNoEnt++
	}
	return nil, false
}

func (pcm *JPCMap2) Put(k1, k2, v *PredictionContext) (*PredictionContext, bool) {
	if collectStats {
		pcm.stats.Puts++
	}
	h := dHash(k1, k2)
	var hClash bool
	for _, e := range pcm.store[h] {
		hClash = true
		if e.k1.Equals(k1) && e.k2.Equals(k2) {
			if collectStats {
				pcm.stats.PutHits++
				pcm.stats.PutHashConflicts++
			}
			return e.v, true
		}
		if collectStats {
			pcm.stats.PutMisses++
		}
	}
	if collectStats {
		if hClash {
			pcm.stats.PutHashConflicts++
		}
	}
	pcm.store[h] = append(pcm.store[h], JPCEntry{k1, k2, v})
	pcm.size++
	if collectStats {
		pcm.stats.CurSize = pcm.size
		if pcm.size > pcm.stats.MaxSize {
			pcm.stats.MaxSize = pcm.size
		}
	}
	return nil, false
}

type VisitEntry struct {
	k *PredictionContext
	v *PredictionContext
}
type VisitRecord struct {
	store scalarDirectory[VisitEntry]
	len   int
	stats *JStatRec
}

type VisitList struct {
	cache *list.List
	lock  RWMutex
}

var visitListPool = VisitList{
	cache: list.New(),
	lock:  RWMutex{},
}

// NewVisitRecord returns a new VisitRecord instance from the pool if available.
// Note that this "map" uses a pointer as a key because we are emulating the behavior of
// IdentityHashMap in Java, which uses the `==` operator to compare whether the keys are equal,
// which means is the key the same reference to an object rather than is it .equals() to another
// object.
func NewVisitRecord() *VisitRecord { return newVisitRecordWithWork(nil) }
func newVisitRecordWithWork(check func()) *VisitRecord {
	visitListPool.lock.lockWithWork(check)
	defer visitListPool.lock.Unlock()
	el := visitListPool.cache.Front()
	var vr *VisitRecord
	if el == nil {
		vr = &VisitRecord{}
		if collectStats {
			vr.stats = &JStatRec{
				Source:      PredictionContextCacheCollection,
				Description: "VisitRecord",
			}
			// Track where we created it from  if we are being asked to do so
			if runtimeConfig.statsTraceStacks {
				vr.stats.CreateStack = debug.Stack()
			}
		}
	} else {
		vr = el.Value.(*VisitRecord)
		visitListPool.cache.Remove(el)
		vr.store = scalarDirectory[VisitEntry]{}
	}
	if collectStats {
		Statistics.AddJStatRec(vr.stats)
	}
	return vr
}

func (vr *VisitRecord) Release() {
	vr.len = 0
	vr.store = scalarDirectory[VisitEntry]{}
	if collectStats {
		vr.stats.MaxSize = 0
		vr.stats.CurSize = 0
		vr.stats.Gets = 0
		vr.stats.GetHits = 0
		vr.stats.GetMisses = 0
		vr.stats.GetHashConflicts = 0
		vr.stats.GetNoEnt = 0
		vr.stats.Puts = 0
		vr.stats.PutHits = 0
		vr.stats.PutMisses = 0
		vr.stats.PutHashConflicts = 0
		vr.stats.MaxSlotSize = 0
	}
	visitListPool.lock.Lock()
	visitListPool.cache.PushBack(vr)
	visitListPool.lock.Unlock()
}

func (vr *VisitRecord) Get(k *PredictionContext) (*PredictionContext, bool) {
	return vr.getWithWork(k, nil)
}
func (vr *VisitRecord) getWithWork(k *PredictionContext, check func()) (*PredictionContext, bool) {
	if collectStats {
		vr.stats.Gets++
	}
	entry := vr.store.get(int(uintptr(unsafe.Pointer(k))), check)
	var v *PredictionContext
	if entry != nil {
		v = entry.v
	}
	if v != nil {
		if collectStats {
			vr.stats.GetHits++
		}
		return v, true
	}
	if collectStats {
		vr.stats.GetNoEnt++
	}
	return nil, false
}

func (vr *VisitRecord) Put(k, v *PredictionContext) (*PredictionContext, bool) {
	return vr.putWithWork(k, v, nil)
}
func (vr *VisitRecord) putWithWork(k, v *PredictionContext, check func()) (*PredictionContext, bool) {
	if collectStats {
		vr.stats.Puts++
	}
	prepared := &VisitEntry{k: k, v: v}
	vr.store.put(int(uintptr(unsafe.Pointer(k))), prepared, check)
	vr.len++
	if collectStats {
		vr.stats.CurSize = vr.len
		if vr.len > vr.stats.MaxSize {
			vr.stats.MaxSize = vr.len
		}
	}
	return v, false
}
