package antlr

import "testing"

func TestCancellationRecursiveSemanticContexts(t *testing.T) {
	left, right := SemanticContext(NewPredicate(1, 1, false)), SemanticContext(NewPredicate(1, 1, false))
	for i := 2; i < 100; i++ {
		left = SemanticContextandContext(left, NewPredicate(1, i, false))
		right = SemanticContextandContextWithWork(right, NewPredicate(1, i, false), func() {})
	}
	if !semanticEqualWithWork(left, right, func() {}) || semanticHashWithWork(left, func() {}) != right.Hash() {
		t.Fatal("AND checked parity changed")
	}
	and := left.(*AND)
	if !and.Equals(right) || and.Equals(NewOR(left, right)) {
		t.Fatal("AND equality changed")
	}
	or := NewOR(NewOR(NewPredicate(2, 1, false), NewPredicate(2, 2, false)), NewOR(NewPredicate(2, 3, false), NewPredicate(2, 4, false)))
	if !or.Equals(NewORWithWork(or, or, func() {})) || or.Equals(and) {
		t.Fatal("OR checked parity changed")
	}
	_ = or.Hash()
	_ = and.String()
	_ = or.String()
	for _, operation := range []func(func()){
		func(check func()) { NewANDWithWork(and, and, check) },
		func(check func()) { NewORWithWork(or, or, check) },
		func(check func()) { semanticHashWithWork(NewOR(and, or), check) },
		func(check func()) { semanticEqualWithWork(left, right, check) },
	} {
		replayRuntimeCancellation(t, operation)
	}
	parser := NewBaseParser(nil)
	parser.Interpreter = NewParserATNSimulator(parser, NewATN(ATNTypeParser, 1), nil, NewPredictionContextCache())
	if !and.evaluate(parser, nil) || !or.evaluate(parser, nil) || and.evalPrecedence(parser, nil) != and || or.evalPrecedence(parser, nil) != or {
		t.Fatal("ordinary predicate evaluation changed")
	}
	for _, context := range []SemanticContext{and, or} {
		replayRuntimeCancellation(t, func(check func()) { parser.Interpreter.SetCancellationCheck(check); context.evaluate(parser, nil) })
		replayRuntimeCancellation(t, func(check func()) {
			parser.Interpreter.SetCancellationCheck(check)
			context.evalPrecedence(parser, nil)
		})
	}
	parser.Interpreter.SetCancellationCheck(nil)
	for _, constructor := range []func(SemanticContext, SemanticContext) SemanticContext{
		SemanticContextandContext, SemanticContextorContext,
	} {
		_ = constructor(nil, left)
		_ = constructor(left, nil)
		_ = constructor(SemanticContextNone, left)
		_ = constructor(left, SemanticContextNone)
		_ = constructor(left, left)
	}
	for _, pair := range [][2]SemanticContext{{NewPrecedencePredicate(0), NewPrecedencePredicate(1)}, {NewPredicate(1, 2, false), NewPrecedencePredicate(0)}, {NewPredicate(1, 2, false), NewPrecedencePredicate(-1)}} {
		and := NewAND(pair[0], pair[1])
		or := NewOR(pair[0], pair[1])
		_ = and.evalPrecedence(parser, nil)
		_ = or.evalPrecedence(parser, nil)
	}
	set := NewJStore[SemanticContext, Comparator[SemanticContext]](semctxEqInst, SemanticContextCollection, "test")
	set.Put(NewPrecedencePredicate(1))
	if len(PrecedencePredicatefilterPrecedencePredicates(set)) != 1 {
		t.Fatal("precedence filter changed")
	}
}

func TestCancellationPredictionModeCompatibility(t *testing.T) {
	state := NewBasicState()
	state.SetStateNumber(1)
	stop := NewRuleStopState()
	stop.SetStateNumber(2)
	configs := NewATNConfigSet(false)
	for _, s := range []ATNState{state, stop} {
		for alt := 1; alt <= 3; alt++ {
			configs.Add(NewATNConfig6(s, alt, BasePredictionContextEMPTY), nil)
		}
	}
	sets := PredictionModegetConflictingAltSubsets(configs)
	if len(sets) != 2 {
		t.Fatal("conflict partition changed")
	}
	_ = PredictionModegetSingleViableAlt(sets)
	_ = PredictionModeGetAlts(sets)
	_ = PredictionModegetUniqueAlt(sets)
	_ = PredictionModeallSubsetsEqual(sets)
	_ = PredictionModehasConflictingAltSet(sets)
	_ = PredictionModehasNonConflictingAltSet(sets)
	_ = PredictionModeallSubsetsConflict(sets)
	_ = PredictionModeresolvesToJustOneViableAlt(sets)
	_ = PredictionModehasStateAssociatedWithOneAlt(configs)
	_ = PredictionModeGetStateToAltMap(configs)
	_ = PredictionModeallConfigsInRuleStopStates(configs)
	_ = PredictionModehasConfigInRuleStopState(configs)
	for _, mode := range []int{PredictionModeSLL, PredictionModeLL, PredictionModeLLExactAmbigDetection} {
		_ = PredictionModehasSLLConflictTerminatingPrediction(mode, configs)
	}
	configs.hasSemanticContext = true
	replayRuntimeCancellation(t, func(check func()) {
		PredictionModehasSLLConflictTerminatingPredictionWithWork(PredictionModeSLL, configs, check)
	})
	one := NewBitSet()
	one.add(1)
	two := NewBitSet()
	two.add(2)
	for _, groups := range [][]*BitSet{nil, {one}, {one, two}, {one, one}} {
		sequence := sequenceFromSlice(groups)
		if PredictionModegetSingleViableAlt(groups) != PredictionModegetSingleViableAltWithWork(sequence, func() {}) || PredictionModeallSubsetsEqual(groups) != PredictionModeallSubsetsEqualWithWork(sequence, func() {}) {
			t.Fatal("prediction compatibility changed")
		}
		_ = PredictionModehasNonConflictingAltSet(groups)
		_ = PredictionModeallSubsetsConflict(groups)
		_ = PredictionModeGetAlts(groups)
	}
}
