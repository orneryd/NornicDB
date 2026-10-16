package antlr

import "testing"

func cancellationPrecedenceParser() (*ParserATNSimulator, *DFA) {
	atn := NewATN(ATNTypeParser, 2)
	stop := NewRuleStopState()
	stop.SetRuleIndex(0)
	atn.addState(stop)
	start := NewStarLoopEntryState()
	start.precedenceRuleDecision = true
	start.SetRuleIndex(0)
	atn.addState(start)
	start.AddTransition(NewEpsilonTransition(stop, -1), -1)
	atn.ruleToStopState = []*RuleStopState{stop}
	dfa := NewDFA(start, 0)
	base := NewBaseParser(nil)
	parser := NewParserATNSimulator(base, atn, []*DFA{dfa}, NewPredictionContextCache())
	base.Interpreter = parser
	return parser, dfa
}

func TestCancellationPrecedencePublication(t *testing.T) {
	parser, dfa := cancellationPrecedenceParser()
	first := parser.initializeStartState(dfa)
	if first == nil || parser.startState(dfa) != first || parser.initializeStartState(dfa) != first {
		t.Fatal("precedence winner changed")
	}
	if !dfa.s0.configs.readOnly || !first.configs.readOnly {
		t.Fatal("mutable precedence configs published")
	}
	if _, found := dfa.Get(first); !found {
		t.Fatal("interned state missing")
	}
	if len(first.GetAltSet()) != 1 {
		t.Fatal("state alternatives changed")
	}
	dfa.Put(first)
	dfa.setPrecedenceStartState(-1, first)
	if dfa.getPrecedenceStartState(-1) != nil {
		t.Fatal("negative precedence accepted")
	}
	for stopAt := 1; stopAt <= 150; stopAt++ {
		parser, dfa := cancellationPrecedenceParser()
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
			parser.initializeStartState(dfa)
		}()
		if !parser.atn.stateMu.mu.TryLock() {
			t.Fatal("state lock leaked")
		}
		parser.atn.stateMu.Unlock()
		if !parser.atn.edgeMu.mu.TryLock() {
			t.Fatal("edge lock leaked")
		}
		parser.atn.edgeMu.Unlock()
		if state := dfa.getPrecedenceStartState(0); state != nil && (!state.configs.readOnly || !dfa.s0.configs.readOnly) {
			t.Fatal("partial precedence state visible")
		}
		parser.SetCancellationCheck(nil)
		if parser.initializeStartState(dfa) == nil {
			t.Fatal("cancelled preparation poisoned prediction")
		}
	}
	dfa.setPrecedenceDfa(false)
	if dfa.getS0() != nil {
		t.Fatal("precedence reset retained start")
	}
	dfa.setPrecedenceDfa(true)
	dfa.setPrecedenceStartState(1, first)
	if dfa.getPrecedenceStartState(1) != first {
		t.Fatal("precedence nil wrapper changed")
	}
}

func TestCancellationConfigAndFilterCompatibility(t *testing.T) {
	parser, _ := cancellationPrecedenceParser()
	state := NewBasicState()
	state.SetStateNumber(10)
	set := NewATNConfigSet(false)
	for _, alt := range []int{1, 2, 3} {
		set.Add(NewATNConfig6(state, alt, BasePredictionContextEMPTY), nil)
	}
	filtered := parser.applyPrecedenceFilter(set)
	if filtered.configs.Len() != 1 || set.configs.Len() != 3 {
		t.Fatal("precedence filtering or ownership changed")
	}
	replayRuntimeCancellation(t, func(check func()) { parser.SetCancellationCheck(check); parser.applyPrecedenceFilter(set) })
	parser.SetCancellationCheck(nil)
	for _, configs := range []*ATNConfigSet{set, filtered} {
		if configs.GetStates().Len() != 1 {
			t.Fatal("config state materialization changed")
		}
		_ = configs.GetPredicates()
		_ = configs.String()
		_ = configs.hashCodeConfigs()
		if !configs.Compare(configs) || !configs.Equals(configs) {
			t.Fatal("config equality changed")
		}
	}
	set.Clear()
	if set.configs.Len() != 0 {
		t.Fatal("config clearing changed")
	}
	stop := parser.atn.ruleToStopState[0]
	input := NewATNConfigSet(false)
	input.Add(NewATNConfig6(stop, 1, BasePredictionContextEMPTY), nil)
	_ = parser.removeAllConfigsNotInRuleStopState(input, true)
	busy := NewClosureBusy("test")
	busy.Put(input.configs.At(0, nil))
	cache := NewPredictionContextCache()
	context := NewBaseSingletonPredictionContext(BasePredictionContextEMPTY, 2)
	if cache.add(context) != context {
		t.Fatal("context add changed")
	}
	if cached, found := cache.Get(context); !found || cached != context {
		t.Fatal("context cache nil wrapper changed")
	}
}
