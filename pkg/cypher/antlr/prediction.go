package antlr

import (
	"sync"
	"sync/atomic"

	"github.com/antlr4-go/antlr/v4"
)

// parserPrediction is one generation of the parser's shared prediction
// state: a DFA per grammar decision and the prediction-context cache.
//
// ANTLR caches every prediction it computes in these DFAs so a later parse
// of the same shape is fast. The generated parser keeps a single process-wide
// set that only grows: each new statement shape adds states, and large,
// deeply nested statements add many (a 99 KB statement adds about 200
// MiB). A server that sees many distinct statements kept gigabytes of
// prediction state for the life of the process.
//
// Parsers built here use the current generation instead. Once its DFAs hold
// more than maxParserDFAStates states, the next parse starts a fresh
// generation and the old one is freed when the last parse using it ends.
// The DFAs are a cache: a fresh generation recomputes predictions on demand
// and accepts exactly the same statements, only the first parse of each
// shape after a reset is slower.
type parserPrediction struct {
	// mu is held shared by every parse using this generation; counting the
	// DFA states takes it exclusively, because ANTLR adds states under its
	// own lock, which this package cannot take.
	mu            sync.RWMutex
	decisionToDFA []*antlr.DFA
	contextCache  *antlr.PredictionContextCache

	// busyChecks counts size checks in a row that found p busy; draining is
	// set while drain measures p.
	busyChecks atomic.Uint32
	draining   atomic.Bool
}

// maxParserDFAStates bounds the DFA states of one prediction generation.
// Measured on Go 1.26 (antlr4-go v4.13.1), a DFA state with its
// configurations and cached contexts holds about 20 KiB, so 25,000 states
// are about 500 MiB. A single 99 KB application statement builds about
// 9,600 states. Tests lower it.
var maxParserDFAStates = 25_000

// maxBusyPredictionChecks is how many size checks in a row may find a
// generation busy before it is drained and measured. A generation can
// overshoot maxParserDFAStates by what the parses in that window add (a
// large statement adds thousands of states), so the window is short; each
// drain costs new parses a wait for at most the parses already running.
// Tests lower it.
var maxBusyPredictionChecks uint32 = 8

var (
	currentPrediction atomic.Pointer[parserPrediction]
	predictionMu      sync.Mutex
	// predictionDrains tracks running drains, so tests can wait for them.
	predictionDrains sync.WaitGroup
)

func newParserPrediction() *parserPrediction {
	CypherParserInit()
	atn := CypherParserParserStaticData.atn
	dfas := make([]*antlr.DFA, len(atn.DecisionToState))
	for decision, state := range atn.DecisionToState {
		dfas[decision] = antlr.NewDFA(state, decision)
	}
	return &parserPrediction{decisionToDFA: dfas, contextCache: antlr.NewPredictionContextCache()}
}

// acquirePrediction returns the current generation, held for one parse;
// the caller releases it with release.
func acquirePrediction() *parserPrediction {
	p := currentPrediction.Load()
	if p == nil {
		predictionMu.Lock()
		if p = currentPrediction.Load(); p == nil {
			p = newParserPrediction()
			currentPrediction.Store(p)
		}
		predictionMu.Unlock()
	}
	p.mu.RLock()
	return p
}

// release ends a parse using p and starts a new generation when p has
// grown past maxParserDFAStates. Counting is a sum over the grammar's
// decisions, cheap next to a parse; it is checked after every parse because
// one large statement can add thousands of states.
//
// Counting needs p to be idle. Under steady concurrent load some parse
// nearly always holds p, so TryLock keeps failing; after
// maxBusyPredictionChecks failures in a row, p is measured by draining it
// instead (drain). A parse holds one generation at a time and never takes
// another while holding it, so the pending lock cannot deadlock.
func (p *parserPrediction) release() {
	p.mu.RUnlock()
	// Never wait for a busy generation here.
	if !p.mu.TryLock() {
		if p.busyChecks.Add(1) >= maxBusyPredictionChecks && p.draining.CompareAndSwap(false, true) {
			p.drain()
		}
		return
	}
	p.busyChecks.Store(0)
	states := p.dfaStates()
	p.mu.Unlock()
	if states > maxParserDFAStates {
		currentPrediction.CompareAndSwap(p, newParserPrediction())
	}
}

// drain measures a generation that is never idle, in the background. The
// pending exclusive lock holds new parses of p back until the parses already
// running end; the count then takes microseconds and they continue on p, or
// on a fresh generation when p is past maxParserDFAStates.
//
// Waiting for one in-flight parse keeps p's predictions. The alternative,
// sending new parses to a fresh generation while p drains and restoring p if
// it was small, makes every parse in that window recompute its predictions
// from nothing (seconds for a large statement) and discards them after.
func (p *parserPrediction) drain() {
	predictionDrains.Add(1)
	go func() {
		defer predictionDrains.Done()
		p.mu.Lock()
		states := p.dfaStates()
		if states > maxParserDFAStates {
			currentPrediction.CompareAndSwap(p, newParserPrediction())
		}
		p.busyChecks.Store(0)
		p.draining.Store(false)
		p.mu.Unlock()
	}()
}

// dfaStates counts the states of every decision's DFA. The caller holds
// p.mu exclusively.
func (p *parserPrediction) dfaStates() int {
	n := 0
	for _, dfa := range p.decisionToDFA {
		n += dfa.Len()
	}
	return n
}

// usePrediction points parser at generation p (a no-op when it already
// uses it) and returns p.
func usePrediction(parser *CypherParser, current, p *parserPrediction) *parserPrediction {
	if current != p {
		parser.Interpreter = antlr.NewParserATNSimulator(parser, CypherParserParserStaticData.atn, p.decisionToDFA, p.contextCache)
	}
	return p
}
