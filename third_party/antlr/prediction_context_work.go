// Copyright (c) 2026 NornicDB contributors.
// Use of this file is governed by the BSD 3-clause license in LICENSE.

package antlr

func predictionContextOperation(check func()) *CollectionOperation[*PredictionContext] {
	if check == nil {
		return nil
	}
	return &CollectionOperation[*PredictionContext]{Check: check, Equals: func(left, right *PredictionContext, work func()) bool { return left.EqualsWithWork(right, work) }}
}

func arrayContextFromSequences(parents *workSequence[*PredictionContext], states *workSequence[int], check func()) *PredictionContext {
	hash := murmurInit(1)
	for i := 0; i < parents.Len(); i++ {
		hash = murmurUpdate(hash, parents.At(i, check).Hash())
	}
	for i := 0; i < states.Len(); i++ {
		hash = murmurUpdate(hash, states.At(i, check))
	}
	hash = murmurFinish(hash, parents.Len()<<1)
	chargeWork(check)
	return &PredictionContext{cachedHash: hash, pcType: PredictionContextArray, parentValues: parents, returnValues: states}
}

func mergeArraysWithWork(a, b *PredictionContext, rootIsWildcard bool, cache *JPCMap, check func()) *PredictionContext {
	chargeWork(check)
	if cache != nil {
		if prior, ok := cache.getWithWork(a, b, check); ok {
			return prior
		}
		if prior, ok := cache.getWithWork(b, a, check); ok {
			return prior
		}
	}
	parents := &workSequence[*PredictionContext]{}
	states := &workSequence[int]{}
	i, j := 0, 0
	appendPair := func(parent *PredictionContext, state int) { parents.Append(parent, check); states.Append(state, check) }
	for i < a.length() && j < b.length() {
		chargeWork(check)
		left, right := a.getReturnState(i), b.getReturnState(j)
		switch {
		case left == right:
			ap, bp := a.GetParent(i), b.GetParent(j)
			bothEmpty := left == BasePredictionContextEmptyReturnState && ap == nil && bp == nil
			if bothEmpty || ap != nil && bp != nil && ap.EqualsWithWork(bp, check) {
				appendPair(ap, left)
			} else {
				appendPair(mergeWithWork(ap, bp, rootIsWildcard, cache, check), left)
			}
			i++
			j++
		case left < right:
			appendPair(a.GetParent(i), left)
			i++
		default:
			appendPair(b.GetParent(j), right)
			j++
		}
	}
	for ; i < a.length(); i++ {
		appendPair(a.GetParent(i), a.getReturnState(i))
	}
	for ; j < b.length(); j++ {
		appendPair(b.GetParent(j), b.getReturnState(j))
	}
	if states.Len() == 1 {
		result := SingletonBasePredictionContextCreate(parents.At(0, check), states.At(0, check))
		if cache != nil {
			cache.putWithWork(a, b, result, check)
		}
		return result
	}
	// Intern private parents before publishing the immutable candidate. Its
	// hash uses the same parent hashes, so canonicalization preserves identity.
	unique := NewJStore[*PredictionContext, Comparator[*PredictionContext]](pContextEqInst, PredictionContextCollection, "merge parents")
	operation := predictionContextOperation(check)
	for i := 0; i < parents.Len(); i++ {
		parent := parents.At(i, check)
		canonical, _ := unique.PutWithOperation(parent, operation)
		parents.Set(i, canonical, check)
	}
	result := arrayContextFromSequences(parents, states, check)
	if result.EqualsWithWork(a, check) {
		result = a
	} else if result.EqualsWithWork(b, check) {
		result = b
	}
	if cache != nil {
		cache.putWithWork(a, b, result, check)
	}
	return result
}

func getCachedBasePredictionContextWithWork(context *PredictionContext, cache *PredictionContextCache, visited *VisitRecord, check func()) *PredictionContext {
	chargeWork(check)
	// The empty-path entry in a full-context array has a nil parent.
	if context == nil || context.isEmpty() {
		return context
	}
	if existing, ok := visited.getWithWork(context, check); ok {
		return existing
	}
	if existing, ok := cache.getWithWork(context, check); ok {
		visited.putWithWork(context, existing, check)
		return existing
	}
	parents := &workSequence[*PredictionContext]{}
	states := &workSequence[int]{}
	changed := false
	for i := 0; i < context.length(); i++ {
		chargeWork(check)
		original := context.GetParent(i)
		parent := getCachedBasePredictionContextWithWork(original, cache, visited, check)
		if !parent.EqualsWithWork(original, check) {
			changed = true
		}
		parents.Append(parent, check)
		states.Append(context.getReturnState(i), check)
	}
	if !changed {
		cache.addWithWork(context, check)
		visited.putWithWork(context, context, check)
		return context
	}
	var updated *PredictionContext
	switch parents.Len() {
	case 0:
		updated = BasePredictionContextEMPTY
	case 1:
		updated = SingletonBasePredictionContextCreate(parents.At(0, check), states.At(0, check))
	default:
		updated = arrayContextFromSequences(parents, states, check)
	}
	cache.addWithWork(updated, check)
	visited.putWithWork(updated, updated, check)
	visited.putWithWork(context, updated, check)
	return updated
}
