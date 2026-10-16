package antlr

import (
	"testing"
	"time"
)

func TestCancellationPublishedConfigBorrow(t *testing.T) {
	atn := NewATN(ATNTypeParser, 2)
	stop := NewRuleStopState()
	atn.addState(stop)
	config := NewATNConfig6(stop, 1, BasePredictionContextEMPTY)
	config.reachesIntoOuterContext = 7
	config.precedenceFilterSuppressed = true
	source := NewATNConfigSet(false)
	source.Add(config, nil)
	source.freezeWithWork(nil)
	before := *config
	parser := NewParserATNSimulator(nil, atn, nil, NewPredictionContextCache())
	reach := parser.computeReachSet(source, TokenEOF, false)
	borrowed := reach.configs.At(0, nil)
	if borrowed == config || borrowed.owner != reach {
		t.Fatal("published EOF config was not privately cloned")
	}
	borrowed.SetContext(NewBaseSingletonPredictionContext(BasePredictionContextEMPTY, 9))
	borrowed.SetReachesIntoOuterContext(8)
	if *config != before {
		t.Fatal("published source config changed")
	}
	for stopAt := 1; stopAt <= 30; stopAt++ {
		calls := 0
		parser.SetCancellationCheck(func() {
			calls++
			if calls == stopAt {
				panic(cancellationTestStop{})
			}
		})
		func() {
			defer func() {
				if value := recover(); value != nil {
					if _, ok := value.(cancellationTestStop); !ok {
						panic(value)
					}
				}
			}()
			parser.computeReachSet(source, TokenEOF, false)
		}()
		if *config != before {
			t.Fatal("cancelled EOF transfer changed published source")
		}
	}
	parser.SetCancellationCheck(nil)
	if parser.computeReachSet(source, TokenEOF, false) == nil {
		t.Fatal("cancelled transfer poisoned future prediction")
	}
}

func TestCancellationOptimizeBorrowedConfig(t *testing.T) {
	state := NewBasicState()
	state.SetStateNumber(1)
	context := BasePredictionContextEMPTY
	for i := 0; i < 256; i++ {
		context = NewBaseSingletonPredictionContext(context, i+1)
	}
	config := NewATNConfig6(state, 1, context)
	source := NewATNConfigSet(false)
	source.Add(config, nil)
	source.freezeWithWork(nil)
	before := *config
	atn := NewATN(ATNTypeParser, 2)
	parser := NewParserATNSimulator(nil, atn, nil, NewPredictionContextCache())
	for stopAt := 1; stopAt <= 30; stopAt++ {
		candidate := NewATNConfigSet(false)
		candidate.Add(config, nil)
		calls := 0
		parser.SetCancellationCheck(func() {
			calls++
			if calls == stopAt {
				panic(cancellationTestStop{})
			}
		})
		func() {
			defer func() {
				if value := recover(); value != nil {
					if _, ok := value.(cancellationTestStop); !ok {
						panic(value)
					}
				}
			}()
			atn.stateMu.lockWithWork(parser.cancellationCheck)
			defer atn.stateMu.Unlock()
			candidate.OptimizeConfigs(&parser.BaseATNSimulator)
		}()
		if *config != before {
			t.Fatal("cancelled optimization changed published config")
		}
		if !atn.stateMu.mu.TryLock() {
			t.Fatal("state lock survived cancellation")
		}
		atn.stateMu.Unlock()
	}
	parser.SetCancellationCheck(nil)
	candidate := NewATNConfigSet(false)
	candidate.Add(config, nil)
	atn.stateMu.Lock()
	candidate.OptimizeConfigs(&parser.BaseATNSimulator)
	atn.stateMu.Unlock()
	if *config != before {
		t.Fatal("healthy optimization changed source")
	}
}

func TestCancellationNextTokensPublishesOnlyComplete(t *testing.T) {
	atn := NewATN(ATNTypeParser, 2)
	states := make([]*BasicState, 512)
	for i := range states {
		states[i] = NewBasicState()
		atn.addState(states[i])
	}
	stop := NewRuleStopState()
	atn.addState(stop)
	for i := 0; i < len(states)-1; i++ {
		states[i].AddTransition(NewEpsilonTransition(states[i+1], -1), -1)
	}
	states[len(states)-1].AddTransition(NewAtomTransition(stop, 1), -1)
	calls := 0
	expectWorkStop(t, func() {
		atn.NextTokensWithWork(states[0], nil, func() {
			calls++
			if calls == 100 {
				panic(cancellationTestStop{})
			}
		})
	})
	if states[0].GetNextTokenWithinRule() != nil {
		t.Fatal("partial lookahead published")
	}
	if !atn.mu.mu.TryLock() {
		t.Fatal("ATN lock survived cancellation")
	}
	atn.mu.Unlock()
	if !atn.NextTokens(states[0], nil).contains(1) {
		t.Fatal("cancelled lookahead poisoned cache")
	}
}

func TestCancellationExpectedTokensParserAndExceptionGlue(t *testing.T) {
	atn := NewATN(ATNTypeParser, 2)
	stop := NewRuleStopState()
	atn.addState(stop)
	invoke := NewBasicState()
	atn.addState(invoke)
	start := NewRuleStartState()
	atn.addState(start)
	invoke.AddTransition(NewRuleTransition(start, 0, 0, stop), -1)
	context := NewBaseParserRuleContext(nil, -1)
	for i := 0; i < 1024; i++ {
		context = NewBaseParserRuleContext(context, invoke.GetStateNumber())
	}
	parser := NewBaseParser(nil)
	parser.Interpreter = NewParserATNSimulator(parser, atn, nil, NewPredictionContextCache())
	parser.ctx = context
	parser.SetState(stop.GetStateNumber())
	exception := NewBaseRecognitionException("test", parser, nil, context)
	for _, run := range []func(){func() { parser.GetExpectedTokens() }, func() { exception.getExpectedTokens() }, func() { parser.IsExpectedToken(TokenEOF) }} {
		calls := 0
		parser.Interpreter.SetCancellationCheck(func() {
			calls++
			if calls == 100 {
				panic(cancellationTestStop{})
			}
		})
		expectWorkStop(t, run)
	}
	parser.Interpreter.SetCancellationCheck(nil)
	if !parser.GetExpectedTokens().contains(TokenEOF) {
		t.Fatal("expected EOF after complete context walk")
	}
	if !parser.IsExpectedToken(TokenEOF) || parser.IsExpectedToken(1) {
		t.Fatal("context-sensitive expected token changed")
	}
	if !parser.GetExpectedTokensWithinCurrentRule().contains(TokenEpsilon) {
		t.Fatal("within-rule lookahead changed")
	}
	if !atn.NextTokensNoContext(stop).contains(TokenEpsilon) || !atn.NextTokensInContext(stop, context).contains(TokenEOF) {
		t.Fatal("legacy lookahead wrappers changed")
	}
	if !atn.getExpectedTokens(stop.GetStateNumber(), context).contains(TokenEOF) {
		t.Fatal("legacy expected-token wrapper changed")
	}
	parser.SetState(invoke.GetStateNumber())
	_ = parser.GetExpectedTokensWithinCurrentRule()
	replayRuntimeCancellation(t, func(check func()) { predictionContextFromRuleContextWithWork(atn, context, check) })
}

func TestCancellationContendedRuntimeLocks(t *testing.T) {
	var mutex Mutex
	mutex.Lock()
	started := time.Now()
	expectWorkStop(t, func() { mutex.lockWithWork(func() { panic(cancellationTestStop{}) }) })
	if time.Since(started) > 250*time.Millisecond {
		t.Fatal("wait ignored cancellation")
	}
	mutex.Unlock()
	var rw RWMutex
	rw.Lock()
	expectWorkStop(t, func() { rw.rLockWithWork(func() { panic(cancellationTestStop{}) }) })
	expectWorkStop(t, func() { rw.lockWithWork(func() { panic(cancellationTestStop{}) }) })
	rw.Unlock()
}

func TestCancellationTransitionIntervalScan(t *testing.T) {
	intervals := NewIntervalSet()
	for i := 0; i < 4096; i++ {
		intervals.addOne(i * 2)
	}
	state := NewBasicState()
	for _, transition := range []Transition{NewSetTransition(state, intervals), NewNotSetTransition(state, intervals)} {
		calls := 0
		expectWorkStop(t, func() {
			transitionMatchesWithWork(transition, 8191, 0, 10000, func() {
				calls++
				if calls == 100 {
					panic(cancellationTestStop{})
				}
			})
		})
		for _, symbol := range []int{-1, 0, 2, 8190, 8191, 10001} {
			if transitionMatchesWithWork(transition, symbol, 0, 10000, func() {}) != transition.Matches(symbol, 0, 10000) {
				t.Fatal("changed set transition result", symbol)
			}
		}
	}
	if !transitionMatchesWithWork(NewAtomTransition(state, 3), 3, 0, 10000, func() {}) {
		t.Fatal("changed atom result")
	}
}

func TestCancellationPredictionContextEmptyArrayParents(t *testing.T) {
	for _, check := range []func(){nil, func() {}} {
		left := NewArrayPredictionContext([]*PredictionContext{BasePredictionContextEMPTY, nil}, []int{1, BasePredictionContextEmptyReturnState})
		right := NewArrayPredictionContext([]*PredictionContext{BasePredictionContextEMPTY, nil}, []int{1, BasePredictionContextEmptyReturnState})
		if !left.EqualsWithWork(right, check) {
			t.Fatal("empty-path array equality changed")
		}
		visited := NewVisitRecord()
		cached := getCachedBasePredictionContextWithWork(left, NewPredictionContextCache(), visited, check)
		visited.Release()
		if !cached.EqualsWithWork(left, check) {
			t.Fatal("empty-path cache changed context")
		}
		merged := mergeArraysWithWork(left, right, false, nil, check)
		if !merged.EqualsWithWork(left, check) {
			t.Fatal("empty-path merge changed context")
		}
	}
}
