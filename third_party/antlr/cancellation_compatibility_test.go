package antlr

import "testing"

func TestCancellationComparatorCompatibility(t *testing.T) {
	state := NewBasicState()
	state.SetStateNumber(7)
	left := NewATNConfig6(state, 1, BasePredictionContextEMPTY)
	right := NewATNConfig6(state, 1, NewEmptyPredictionContext())
	left.semanticContext = NewAND(NewPredicate(1, 1, false), NewPredicate(2, 2, false))
	right.semanticContext = NewAND(NewPredicate(1, 1, false), NewPredicate(2, 2, false))
	for _, comparator := range []Comparator[*ATNConfig]{aConfEqInst, aConfCompInst, atnConfCompInst, atnAltCfgEqInst} {
		operation := configLookupOperation(comparator, func() {})
		if operation.Hash(left, func() {}) != comparator.Hash1(left) || operation.Equals(left, right, func() {}) != comparator.Equals2(left, right) {
			t.Fatal("config comparator parity changed")
		}
		if !operation.Equals(left, left, func() {}) || operation.Equals(left, nil, func() {}) {
			t.Fatal("config comparator identity changed")
		}
		replayRuntimeCancellation(t, func(check func()) { operation.Equals(left, right, check) })
	}
	if !left.PEquals(right) || left.PHash() != left.Hash() {
		t.Fatal("parser config wrappers changed")
	}
	lhs := NewLexerATNConfig6(state, 1, BasePredictionContextEMPTY)
	rhs := NewLexerATNConfig6(state, 1, BasePredictionContextEMPTY)
	if !lhs.LEquals(rhs) || lhs.LHash() != lhs.Hash() {
		t.Fatal("lexer config wrappers changed")
	}
	lhs.passedThroughNonGreedyDecision = true
	if lhs.LEquals(rhs) {
		t.Fatal("non-greedy config difference ignored")
	}
	lhs.passedThroughNonGreedyDecision = false
	lhs.lexerActionExecutor = NewLexerActionExecutor([]LexerAction{NewLexerChannelAction(1)})
	if lhs.LEquals(rhs) {
		t.Fatal("missing action executor ignored")
	}
	rhs.lexerActionExecutor = NewLexerActionExecutor([]LexerAction{NewLexerChannelAction(2)})
	if lhs.LEquals(rhs) {
		t.Fatal("different action executor ignored")
	}
	for _, pair := range [][2]SemanticContext{
		{NewAND(NewPredicate(1, 1, false), NewPredicate(2, 2, false)), NewPredicate(1, 1, false)},
		{NewOR(NewPredicate(1, 1, false), NewPredicate(2, 2, false)), NewPredicate(1, 1, false)},
		{NewAND(NewPredicate(1, 1, false), NewPredicate(2, 2, false)), NewAND(NewPredicate(1, 1, false), NewPredicate(1, 1, false))},
		{NewOR(NewPredicate(1, 1, false), NewPredicate(2, 2, false)), NewOR(NewPredicate(1, 1, false), NewPredicate(3, 3, false))},
	} {
		if semanticEqualWithWork(pair[0], pair[1], func() {}) {
			t.Fatal("different semantic context accepted")
		}
	}
	if SemanticContextandContext(NewPredicate(1, 1, false), nil) == nil {
		t.Fatal("right-nil conjunction changed")
	}
	parser, _ := cancellationPrecedenceParser()
	first, second := SemanticContext(NewPredicate(1, 1, false)), SemanticContext(NewPredicate(2, 2, false))
	and := NewAND(NewAND(first, second), NewPrecedencePredicate(0))
	or := NewOR(NewOR(first, second), NewPrecedencePredicate(-1))
	if and.evalPrecedence(parser.parser, nil) == nil || or.evalPrecedence(parser.parser, nil) == nil {
		t.Fatal("precedence reduction lost non-precedence operands")
	}
	if SemanticContextandContext(first, first) != first {
		t.Fatal("idempotent conjunction changed")
	}
	// Equal-hash executors still compare their elements.
	rhs.lexerActionExecutor.cachedHash = lhs.lexerActionExecutor.cachedHash
	if lhs.lexerActionExecutor.Equals(rhs.lexerActionExecutor) {
		t.Fatal("action hash collision accepted")
	}
	set := NewATNConfigSet(false)
	set.Add(left, nil)
	set.readOnly = true
	set.cachedHash = -1
	_ = set.Hash()
	set.freezeWithWork(func() {})
	listener := NewDiagnosticErrorListener(true)
	if !listener.getConflictingAlts(nil, set).contains(1) {
		t.Fatal("diagnostic alternative collection changed")
	}
	identities := configIdentitySet{}
	if identities.add(left, nil) || !identities.add(left, nil) {
		t.Fatal("closure identity dedup changed")
	}
}

func TestCancellationTokenAndLookaheadCompatibility(t *testing.T) {
	stream := cancellationTokenStream()
	stream.SetCancellationCheck(func() {})
	if stream.getNumberOfOnChannelTokens() != 2 {
		t.Fatal("on-channel count changed")
	}
	if stream.GetHiddenTokensToLeft(0, -1) != nil {
		t.Fatal("hidden tokens before start")
	}
	if stream.filterForChannel(1, 258, 999) != nil {
		t.Fatal("nonmatching channel accepted")
	}
	types := NewIntervalSet()
	types.addOne(1)
	if len(stream.GetTokens(0, 259, types)) != 259 {
		t.Fatal("typed token selection changed")
	}
	if stream.GetTextFromTokens(nil, nil) != "" {
		t.Fatal("nil token text changed")
	}
	if stream.GetTextFromTokens(stream.tokens.At(0, nil), stream.tokens.At(1, nil)) != "xx" {
		t.Fatal("token text interval changed")
	}
	stream.Seek(0)
	stream.Consume()
	if stream.Index() != 259 {
		t.Fatal("consume did not skip hidden channel")
	}
	atn := NewATN(ATNTypeParser, 2)
	decision := NewBasicBlockStartState()
	atn.addState(decision)
	stop := NewRuleStopState()
	atn.addState(stop)
	decision.AddTransition(NewAtomTransition(stop, 1), -1)
	look := NewLL1Analyzer(atn).getDecisionLookahead(decision)
	if len(look) != 1 {
		t.Fatal("decision lookahead count changed")
	}
	if NewLL1Analyzer(atn).getDecisionLookahead(nil) != nil {
		t.Fatal("nil decision changed")
	}
	if !NewLL1Analyzer(atn).Look(decision, nil, nil).contains(1) {
		t.Fatal("nil lookahead wrapper changed")
	}
}
