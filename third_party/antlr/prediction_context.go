// Copyright (c) 2012-2022 The ANTLR Project. All rights reserved.
// Use of this file is governed by the BSD 3-clause license that
// can be found in the LICENSE.txt file in the project root.

package antlr

import (
	"strconv"
)

var _emptyPredictionContextHash int

func init() {
	_emptyPredictionContextHash = murmurInit(1)
	_emptyPredictionContextHash = murmurFinish(_emptyPredictionContextHash, 0)
}

func calculateEmptyHash() int {
	return _emptyPredictionContextHash
}

const (
	// BasePredictionContextEmptyReturnState represents {@code $} in an array in full context mode, $
	// doesn't mean wildcard:
	//
	//   $ + x = [$,x]
	//
	// Here,
	//
	//   $ = EmptyReturnState
	BasePredictionContextEmptyReturnState = 0x7FFFFFFF
)

// TODO: JI These are meant to be atomics - this does not seem to match the Java runtime here
//
//goland:noinspection GoUnusedGlobalVariable
var (
	BasePredictionContextglobalNodeCount = 1
	BasePredictionContextid              = BasePredictionContextglobalNodeCount
)

const (
	PredictionContextEmpty = iota
	PredictionContextSingleton
	PredictionContextArray
)

// PredictionContext is a go idiomatic implementation of PredictionContext that does not rty to
// emulate inheritance from Java, and can be used without an interface definition. An interface
// is not required because no user code will ever need to implement this interface.
type PredictionContext struct {
	cachedHash   int
	pcType       int
	parentCtx    *PredictionContext
	returnState  int
	parents      []*PredictionContext
	returnStates []int
	parentValues *workSequence[*PredictionContext]
	returnValues *workSequence[int]
}

func NewEmptyPredictionContext() *PredictionContext {
	nep := &PredictionContext{}
	nep.cachedHash = calculateEmptyHash()
	nep.pcType = PredictionContextEmpty
	nep.returnState = BasePredictionContextEmptyReturnState
	return nep
}

func NewBaseSingletonPredictionContext(parent *PredictionContext, returnState int) *PredictionContext {
	pc := &PredictionContext{}
	pc.pcType = PredictionContextSingleton
	pc.returnState = returnState
	pc.parentCtx = parent
	if parent != nil {
		pc.cachedHash = calculateHash(parent, returnState)
	} else {
		pc.cachedHash = calculateEmptyHash()
	}
	return pc
}

func SingletonBasePredictionContextCreate(parent *PredictionContext, returnState int) *PredictionContext {
	if returnState == BasePredictionContextEmptyReturnState && parent == nil {
		// someone can pass in the bits of an array ctx that mean $
		return BasePredictionContextEMPTY
	}
	return NewBaseSingletonPredictionContext(parent, returnState)
}

func NewArrayPredictionContext(parents []*PredictionContext, returnStates []int) *PredictionContext {
	return NewArrayPredictionContextWithWork(parents, returnStates, nil)
}
func NewArrayPredictionContextWithWork(parents []*PredictionContext, returnStates []int, check func()) *PredictionContext {
	// Parent can be nil only if full ctx mode and we make an array
	// from {@link //EMPTY} and non-empty. We merge {@link //EMPTY} by using
	// nil parent and
	// returnState == {@link //EmptyReturnState}.
	hash := murmurInit(1)
	for _, parent := range parents {
		chargeWork(check)
		hash = murmurUpdate(hash, parent.Hash())
	}
	for _, returnState := range returnStates {
		chargeWork(check)
		hash = murmurUpdate(hash, returnState)
	}
	hash = murmurFinish(hash, len(parents)<<1)

	nec := &PredictionContext{}
	nec.cachedHash = hash
	nec.pcType = PredictionContextArray
	nec.parents = parents
	nec.returnStates = returnStates
	return nec
}

func (p *PredictionContext) Hash() int {
	if p == nil {
		return 0
	}
	return p.cachedHash
}

func (p *PredictionContext) Equals(other Collectable[*PredictionContext]) bool {
	return p.EqualsWithWork(other, nil)
}
func (p *PredictionContext) EqualsWithWork(other Collectable[*PredictionContext], check func()) bool {
	chargeWork(check)
	if p == other {
		return true
	}
	if p == nil {
		return false
	}
	switch p.pcType {
	case PredictionContextEmpty:
		if other == nil {
			return true
		}
		otherP := other.(*PredictionContext)
		return other == nil || otherP == nil || otherP.isEmpty()
	case PredictionContextSingleton:
		return p.singletonEqualsWithWork(other, check)
	case PredictionContextArray:
		return p.arrayEqualsWithWork(other, check)
	}
	return false
}

func (p *PredictionContext) ArrayEquals(o Collectable[*PredictionContext]) bool {
	return p.arrayEqualsWithWork(o, nil)
}
func (p *PredictionContext) arrayEqualsWithWork(o Collectable[*PredictionContext], check func()) bool {
	chargeWork(check)
	if o == nil {
		return false
	}
	other := o.(*PredictionContext)
	if other == nil || other.pcType != PredictionContextArray {
		return false
	}
	if p.cachedHash != other.Hash() {
		return false // can't be same if hash is different
	}

	// Must compare the actual array elements and not just the array address
	//
	if p.length() != other.length() {
		return false
	}
	for i := 0; i < p.length(); i++ {
		chargeWork(check)
		if p.getReturnState(i) != other.getReturnState(i) {
			return false
		}
	}
	for i := 0; i < p.length(); i++ {
		chargeWork(check)
		if !p.GetParent(i).EqualsWithWork(other.GetParent(i), check) {
			return false
		}
	}
	return true
}

func (p *PredictionContext) SingletonEquals(other Collectable[*PredictionContext]) bool {
	return p.singletonEqualsWithWork(other, nil)
}
func (p *PredictionContext) singletonEqualsWithWork(other Collectable[*PredictionContext], check func()) bool {
	chargeWork(check)
	if other == nil {
		return false
	}
	otherP := other.(*PredictionContext)
	if otherP == nil || otherP.pcType != PredictionContextSingleton {
		return false
	}

	if p.cachedHash != otherP.Hash() {
		return false // Can't be same if hash is different
	}

	if p.returnState != otherP.getReturnState(0) {
		return false
	}

	// Both parents must be nil if one is
	if p.parentCtx == nil {
		return otherP.parentCtx == nil
	}

	return p.parentCtx.EqualsWithWork(otherP.parentCtx, check)
}

func (p *PredictionContext) GetParent(i int) *PredictionContext {
	switch p.pcType {
	case PredictionContextEmpty:
		return nil
	case PredictionContextSingleton:
		return p.parentCtx
	case PredictionContextArray:
		if p.parentValues != nil {
			return p.parentValues.At(i, nil)
		}
		return p.parents[i]
	}
	return nil
}

func (p *PredictionContext) getReturnState(i int) int {
	switch p.pcType {
	case PredictionContextArray:
		if p.returnValues != nil {
			return p.returnValues.At(i, nil)
		}
		return p.returnStates[i]
	default:
		return p.returnState
	}
}

func (p *PredictionContext) GetReturnStates() []int {
	switch p.pcType {
	case PredictionContextArray:
		if p.returnValues != nil {
			values := make([]int, p.returnValues.Len())
			for i := range values {
				values[i] = p.getReturnState(i)
			}
			return values
		}
		return p.returnStates
	default:
		return []int{p.returnState}
	}
}

func (p *PredictionContext) length() int {
	switch p.pcType {
	case PredictionContextArray:
		if p.returnValues != nil {
			return p.returnValues.Len()
		}
		return len(p.returnStates)
	default:
		return 1
	}
}

func (p *PredictionContext) hasEmptyPath() bool {
	switch p.pcType {
	case PredictionContextSingleton:
		return p.returnState == BasePredictionContextEmptyReturnState
	}
	return p.getReturnState(p.length()-1) == BasePredictionContextEmptyReturnState
}

func (p *PredictionContext) String() string {
	switch p.pcType {
	case PredictionContextEmpty:
		return "$"
	case PredictionContextSingleton:
		var up string

		if p.parentCtx == nil {
			up = ""
		} else {
			up = p.parentCtx.String()
		}

		if len(up) == 0 {
			if p.returnState == BasePredictionContextEmptyReturnState {
				return "$"
			}

			return strconv.Itoa(p.returnState)
		}

		return strconv.Itoa(p.returnState) + " " + up
	case PredictionContextArray:
		if p.isEmpty() {
			return "[]"
		}

		s := "["
		for i := 0; i < p.length(); i++ {
			if i > 0 {
				s = s + ", "
			}
			if p.getReturnState(i) == BasePredictionContextEmptyReturnState {
				s = s + "$"
				continue
			}
			s = s + strconv.Itoa(p.getReturnState(i))
			if !p.GetParent(i).isEmpty() {
				s = s + " " + p.GetParent(i).String()
			} else {
				s = s + "nil"
			}
		}
		return s + "]"

	default:
		return "unknown"
	}
}

func (p *PredictionContext) isEmpty() bool {
	switch p.pcType {
	case PredictionContextEmpty:
		return true
	case PredictionContextArray:
		// since EmptyReturnState can only appear in the last position, we
		// don't need to verify that size==1
		return p.getReturnState(0) == BasePredictionContextEmptyReturnState
	default:
		return false
	}
}

func (p *PredictionContext) Type() int {
	return p.pcType
}

func calculateHash(parent *PredictionContext, returnState int) int {
	h := murmurInit(1)
	h = murmurUpdate(h, parent.Hash())
	h = murmurUpdate(h, returnState)
	return murmurFinish(h, 2)
}

// Convert a {@link RuleContext} tree to a {@link BasePredictionContext} graph.
// Return {@link //EMPTY} if {@code outerContext} is empty or nil.
// /
func predictionContextFromRuleContext(a *ATN, outerContext RuleContext) *PredictionContext {
	return predictionContextFromRuleContextWithWork(a, outerContext, nil)
}
func predictionContextFromRuleContextWithWork(a *ATN, outerContext RuleContext, check func()) *PredictionContext {
	chargeWork(check)
	if outerContext == nil {
		outerContext = ParserRuleContextEmpty
	}
	// if we are in RuleContext of start rule, s, then BasePredictionContext
	// is EMPTY. Nobody called us. (if we are empty, return empty)
	if outerContext.GetParent() == nil || outerContext == ParserRuleContextEmpty {
		return BasePredictionContextEMPTY
	}
	// If we have a parent, convert it to a BasePredictionContext graph
	parent := predictionContextFromRuleContextWithWork(a, outerContext.GetParent().(RuleContext), check)
	state := a.states[outerContext.GetInvokingState()]
	transition := state.GetTransitions()[0]

	return SingletonBasePredictionContextCreate(parent, transition.(*RuleTransition).followState.GetStateNumber())
}

func merge(a, b *PredictionContext, rootIsWildcard bool, cache *JPCMap) *PredictionContext {
	return mergeWithWork(a, b, rootIsWildcard, cache, nil)
}
func mergeWithWork(a, b *PredictionContext, rootIsWildcard bool, mergeCache *JPCMap, check func()) *PredictionContext {
	chargeWork(check)

	// Share same graph if both same
	//
	if a == b || a.EqualsWithWork(b, check) {
		return a
	}

	if a.pcType == PredictionContextSingleton && b.pcType == PredictionContextSingleton {
		return mergeSingletonsWithWork(a, b, rootIsWildcard, mergeCache, check)
	}
	// At least one of a or b is array
	// If one is $ and rootIsWildcard, return $ as wildcard
	if rootIsWildcard {
		if a.isEmpty() {
			return a
		}
		if b.isEmpty() {
			return b
		}
	}

	// Convert either Singleton or Empty to arrays, so that we can merge them
	//
	ara := convertToArray(a)
	arb := convertToArray(b)
	return mergeArraysWithWork(ara, arb, rootIsWildcard, mergeCache, check)
}

func convertToArray(pc *PredictionContext) *PredictionContext {
	switch pc.Type() {
	case PredictionContextEmpty:
		return NewArrayPredictionContext([]*PredictionContext{}, []int{})
	case PredictionContextSingleton:
		return NewArrayPredictionContext([]*PredictionContext{pc.GetParent(0)}, []int{pc.getReturnState(0)})
	default:
		// Already an array
	}
	return pc
}

// mergeSingletons merges two Singleton [PredictionContext] instances.
//
// Stack tops equal, parents merge is same return left graph.
// <embed src="images/SingletonMerge_SameRootSamePar.svg"
// type="image/svg+xml"/></p>
//
// <p>Same stack top, parents differ merge parents giving array node, then
// remainders of those graphs. A new root node is created to point to the
// merged parents.<br>
// <embed src="images/SingletonMerge_SameRootDiffPar.svg"
// type="image/svg+xml"/></p>
//
// <p>Different stack tops pointing to same parent. Make array node for the
// root where both element in the root point to the same (original)
// parent.<br>
// <embed src="images/SingletonMerge_DiffRootSamePar.svg"
// type="image/svg+xml"/></p>
//
// <p>Different stack tops pointing to different parents. Make array node for
// the root where each element points to the corresponding original
// parent.<br>
// <embed src="images/SingletonMerge_DiffRootDiffPar.svg"
// type="image/svg+xml"/></p>
//
// @param a the first {@link SingletonBasePredictionContext}
// @param b the second {@link SingletonBasePredictionContext}
// @param rootIsWildcard {@code true} if this is a local-context merge,
// otherwise false to indicate a full-context merge
// @param mergeCache
// /
func mergeSingletons(a, b *PredictionContext, rootIsWildcard bool, cache *JPCMap) *PredictionContext {
	return mergeSingletonsWithWork(a, b, rootIsWildcard, cache, nil)
}
func mergeSingletonsWithWork(a, b *PredictionContext, rootIsWildcard bool, mergeCache *JPCMap, check func()) *PredictionContext {
	chargeWork(check)
	if mergeCache != nil {
		previous, present := mergeCache.getWithWork(a, b, check)
		if present {
			return previous
		}
		previous, present = mergeCache.getWithWork(b, a, check)
		if present {
			return previous
		}
	}

	rootMerge := mergeRoot(a, b, rootIsWildcard)
	if rootMerge != nil {
		if mergeCache != nil {
			mergeCache.putWithWork(a, b, rootMerge, check)
		}
		return rootMerge
	}
	if a.returnState == b.returnState {
		parent := mergeWithWork(a.parentCtx, b.parentCtx, rootIsWildcard, mergeCache, check)
		// if parent is same as existing a or b parent or reduced to a parent,
		// return it
		if parent.EqualsWithWork(a.parentCtx, check) {
			return a // ax + bx = ax, if a=b
		}
		if parent.EqualsWithWork(b.parentCtx, check) {
			return b // ax + bx = bx, if a=b
		}
		// else: ax + ay = a'[x,y]
		// merge parents x and y, giving array node with x,y then remainders
		// of those graphs. dup a, a' points at merged array.
		// New joined parent so create a new singleton pointing to it, a'
		spc := SingletonBasePredictionContextCreate(parent, a.returnState)
		if mergeCache != nil {
			mergeCache.putWithWork(a, b, spc, check)
		}
		return spc
	}
	// a != b payloads differ
	// see if we can collapse parents due to $+x parents if local ctx
	var singleParent *PredictionContext
	if a.EqualsWithWork(b, check) || (a.parentCtx != nil && a.parentCtx.EqualsWithWork(b.parentCtx, check)) { // ax +
		// bx =
		// [a,b]x
		singleParent = a.parentCtx
	}
	if singleParent != nil { // parents are same
		// sort payloads and use same parent
		payloads := []int{a.returnState, b.returnState}
		if a.returnState > b.returnState {
			payloads[0] = b.returnState
			payloads[1] = a.returnState
		}
		parents := []*PredictionContext{singleParent, singleParent}
		apc := NewArrayPredictionContext(parents, payloads)
		if mergeCache != nil {
			mergeCache.putWithWork(a, b, apc, check)
		}
		return apc
	}
	// parents differ and can't merge them. Just pack together
	// into array can't merge.
	// ax + by = [ax,by]
	payloads := []int{a.returnState, b.returnState}
	parents := []*PredictionContext{a.parentCtx, b.parentCtx}
	if a.returnState > b.returnState { // sort by payload
		payloads[0] = b.returnState
		payloads[1] = a.returnState
		parents = []*PredictionContext{b.parentCtx, a.parentCtx}
	}
	apc := NewArrayPredictionContext(parents, payloads)
	if mergeCache != nil {
		mergeCache.putWithWork(a, b, apc, check)
	}
	return apc
}

// Handle case where at least one of {@code a} or {@code b} is
// {@link //EMPTY}. In the following diagrams, the symbol {@code $} is used
// to represent {@link //EMPTY}.
//
// <h2>Local-Context Merges</h2>
//
// <p>These local-context merge operations are used when {@code rootIsWildcard}
// is true.</p>
//
// <p>{@link //EMPTY} is superset of any graph return {@link //EMPTY}.<br>
// <embed src="images/LocalMerge_EmptyRoot.svg" type="image/svg+xml"/></p>
//
// <p>{@link //EMPTY} and anything is {@code //EMPTY}, so merged parent is
// {@code //EMPTY} return left graph.<br>
// <embed src="images/LocalMerge_EmptyParent.svg" type="image/svg+xml"/></p>
//
// <p>Special case of last merge if local context.<br>
// <embed src="images/LocalMerge_DiffRoots.svg" type="image/svg+xml"/></p>
//
// <h2>Full-Context Merges</h2>
//
// <p>These full-context merge operations are used when {@code rootIsWildcard}
// is false.</p>
//
// <p><embed src="images/FullMerge_EmptyRoots.svg" type="image/svg+xml"/></p>
//
// <p>Must keep all contexts {@link //EMPTY} in array is a special value (and
// nil parent).<br>
// <embed src="images/FullMerge_EmptyRoot.svg" type="image/svg+xml"/></p>
//
// <p><embed src="images/FullMerge_SameRoot.svg" type="image/svg+xml"/></p>
//
// @param a the first {@link SingletonBasePredictionContext}
// @param b the second {@link SingletonBasePredictionContext}
// @param rootIsWildcard {@code true} if this is a local-context merge,
// otherwise false to indicate a full-context merge
// /
func mergeRoot(a, b *PredictionContext, rootIsWildcard bool) *PredictionContext {
	if rootIsWildcard {
		if a.pcType == PredictionContextEmpty {
			return BasePredictionContextEMPTY // // + b =//
		}
		if b.pcType == PredictionContextEmpty {
			return BasePredictionContextEMPTY // a +// =//
		}
	} else {
		if a.isEmpty() && b.isEmpty() {
			return BasePredictionContextEMPTY // $ + $ = $
		} else if a.isEmpty() { // $ + x = [$,x]
			payloads := []int{b.getReturnState(-1), BasePredictionContextEmptyReturnState}
			parents := []*PredictionContext{b.GetParent(-1), nil}
			return NewArrayPredictionContext(parents, payloads)
		} else if b.isEmpty() { // x + $ = [$,x] ($ is always first if present)
			payloads := []int{a.getReturnState(-1), BasePredictionContextEmptyReturnState}
			parents := []*PredictionContext{a.GetParent(-1), nil}
			return NewArrayPredictionContext(parents, payloads)
		}
	}
	return nil
}

// Merge two {@link ArrayBasePredictionContext} instances.
//
// <p>Different tops, different parents.<br>
// <embed src="images/ArrayMerge_DiffTopDiffPar.svg" type="image/svg+xml"/></p>
//
// <p>Shared top, same parents.<br>
// <embed src="images/ArrayMerge_ShareTopSamePar.svg" type="image/svg+xml"/></p>
//
// <p>Shared top, different parents.<br>
// <embed src="images/ArrayMerge_ShareTopDiffPar.svg" type="image/svg+xml"/></p>
//
// <p>Shared top, all shared parents.<br>
// <embed src="images/ArrayMerge_ShareTopSharePar.svg"
// type="image/svg+xml"/></p>
//
// <p>Equal tops, merge parents and reduce top to
// {@link SingletonBasePredictionContext}.<br>
// <embed src="images/ArrayMerge_EqualTop.svg" type="image/svg+xml"/></p>
//
//goland:noinspection GoBoolExpressions
func mergeArrays(a, b *PredictionContext, rootIsWildcard bool, cache *JPCMap) *PredictionContext {
	return mergeArraysWithWork(a, b, rootIsWildcard, cache, nil)
}

// Make pass over all M parents and merge any Equals() ones.
// Note that we pass a pointer to the slice as we want to modify it in place.
//
//goland:noinspection GoUnusedFunction
func combineCommonParents(parents *[]*PredictionContext) {
	uniqueParents := NewJStore[*PredictionContext, Comparator[*PredictionContext]](pContextEqInst, PredictionContextCollection, "combineCommonParents for PredictionContext")

	for p := 0; p < len(*parents); p++ {
		parent := (*parents)[p]
		_, _ = uniqueParents.Put(parent)
	}
	for q := 0; q < len(*parents); q++ {
		pc, _ := uniqueParents.Get((*parents)[q])
		(*parents)[q] = pc
	}
}

func getCachedBasePredictionContext(context *PredictionContext, cache *PredictionContextCache, visited *VisitRecord) *PredictionContext {
	return getCachedBasePredictionContextWithWork(context, cache, visited, nil)
}
