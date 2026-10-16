// Copyright (c) 2012-2022 The ANTLR Project. All rights reserved.
// Use of this file is governed by the BSD 3-clause license that
// can be found in the LICENSE.txt file in the project root.

package antlr

var ATNSimulatorError = NewDFAState(0x7FFFFFFF, NewATNConfigSet(false))

type IATNSimulator interface {
	SharedContextCache() *PredictionContextCache
	ATN() *ATN
	DecisionToDFA() []*DFA
}

type BaseATNSimulator struct {
	atn                *ATN
	sharedContextCache *PredictionContextCache
	decisionToDFA      []*DFA
	cancellationCheck  func()
}

// SetCancellationCheck installs work on this query-owned simulator only. The
// callback may panic to abort synchronous prediction; shared caches never retain
// it. Callers must clear it before returning an instance to a pool.
func (b *BaseATNSimulator) SetCancellationCheck(check func()) { b.cancellationCheck = check }
func (b *BaseATNSimulator) CheckCancellation()                { chargeWork(b.cancellationCheck) }

func (b *BaseATNSimulator) getCachedContext(context *PredictionContext) *PredictionContext {
	if b.sharedContextCache == nil {
		return context
	}

	//visited := NewJMap[*PredictionContext, *PredictionContext, Comparator[*PredictionContext]](pContextEqInst, PredictionVisitedCollection, "Visit map in getCachedContext()")
	visited := newVisitRecordWithWork(b.cancellationCheck)
	defer visited.Release()
	return getCachedBasePredictionContextWithWork(context, b.sharedContextCache, visited, b.cancellationCheck)
}

func (b *BaseATNSimulator) SharedContextCache() *PredictionContextCache {
	return b.sharedContextCache
}

func (b *BaseATNSimulator) ATN() *ATN {
	return b.atn
}

func (b *BaseATNSimulator) DecisionToDFA() []*DFA {
	return b.decisionToDFA
}
