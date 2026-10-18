package antlr

import (
	"math"
	"reflect"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/antlr4-go/antlr/v4"
)

// parserPrediction is one generation of the parser's shared prediction
// state: a DFA per grammar decision and the prediction-context cache.
//
// ANTLR caches every prediction it computes in these DFAs so a later parse
// of the same shape is fast. The generated parser keeps a single process-wide
// set that only grows: each new statement shape adds states, and large,
// deeply nested statements add many (a 99 KB statement adds about 200 MiB).
// A server that sees many distinct statements kept gigabytes of prediction
// state for the life of the process.
//
// Parsers built here use a generation from the ledger (predictions) instead,
// and a generation that has grown past the budget is replaced by a fresh
// one; the old one is freed when the last parse using it ends. The DFAs are
// a cache: a fresh generation recomputes predictions on demand and accepts
// exactly the same statements, only the first parse of each shape after a
// reset is slower. Taking or giving back a generation never waits for
// another parse.
type parserPrediction struct {
	decisionToDFA []*antlr.DFA
	contextCache  *antlr.PredictionContextCache

	// The fields below belong to the ledger: they change only under
	// predictions.mu. shared is false for a private generation, which one
	// parse uses alone and drops at its release. readers counts the parses
	// holding a shared generation, and states is its DFA state count at the
	// last release. retired is set once it is no longer current.
	shared  bool
	readers int
	states  int64
	retired bool
}

// predictionLedger is the one owner of the shared generations and their
// account. Every role change and every change to the account happens under
// mu, together, so the account always covers exactly the generations that
// can still grow or are still held:
//
//   - the generations are current, plus every retired one a parse still
//     holds; a retired one leaves when its last parse ends;
//   - states is the sum of their counts, each taken at a release;
//   - a parse gets a shared generation only while states fits the budget.
//     Past it, the current generation is retired, and while the retired ones
//     still held fill the budget alone, a parse predicts with a private
//     generation that nothing else grows.
//
// So shared prediction state stays within the budget plus what the parses in
// flight add, however many generations long parses hold. mu is never held
// during a parse or a count.
type predictionLedger struct {
	mu      sync.Mutex
	current *parserPrediction
	retired map[*parserPrediction]struct{}
	states  int64
	made    uint64
}

var predictions predictionLedger

// bytesPerDFAState is the measured heap of one DFA state with its
// configurations and cached prediction contexts (Go 1.26, antlr4-go
// v4.13.1): a 99 KB application statement builds 9,630 states and 197 MiB.
const bytesPerDFAState = 20 << 10

// defaultPredictionBudget is the prediction memory of a process without a
// memory limit, and predictionBudgetShare the share of the runtime memory
// limit (NORNICDB_MEMORY_LIMIT, GOMEMLIMIT or the cgroup limit) it may use.
// An application's working set of large statements has to fit: each 99 KB
// statement holds about 200 MiB, and a generation that is too small rotates
// every few statements, so their first runs pay cold prediction again.
const (
	defaultPredictionBudget = 2 << 30
	predictionBudgetShare   = 4
)

// maxParserDFAStates overrides the DFA state budget when positive; tests
// lower it. Zero derives it from the memory budget.
var maxParserDFAStates atomic.Int64

// parserDFAStateLimit bounds the DFA states of all shared prediction
// generations together: a quarter of the runtime memory limit, or
// defaultPredictionBudget without one, in states.
func parserDFAStateLimit() int64 {
	if n := maxParserDFAStates.Load(); n > 0 {
		return n
	}
	budget := int64(defaultPredictionBudget)
	if limit := debug.SetMemoryLimit(-1); limit > 0 && limit < math.MaxInt64 {
		budget = limit / predictionBudgetShare
	}
	return budget / bytesPerDFAState
}

// atnStateLock is the lock antlr4-go adds every parser DFA state under: the
// parser ATN's unexported stateMu (v4.13.1: AdaptivePredict and addDFAEdge
// take it exclusively around addDFAState, the only writer of a DFA's state
// set). Holding it shared, a release counts a generation's states exactly
// while other parses still use it. It is nil if the runtime's ATN has no
// such field (TestATNStateLockResolves); counts then stay zero and the
// generation is never replaced, as before generations existed. Resolved on
// the first count, so a process that never parses with ANTLR never
// deserializes the ATN for it.
var atnStateLock = sync.OnceValue(func() *antlr.RWMutex {
	CypherParserInit()
	field := reflect.ValueOf(CypherParserParserStaticData.atn).Elem().FieldByName("stateMu")
	if !field.IsValid() || field.Type() != reflect.TypeOf(antlr.RWMutex{}) {
		return nil
	}
	return (*antlr.RWMutex)(unsafe.Pointer(field.UnsafeAddr()))
})

func newParserPrediction() *parserPrediction {
	CypherParserInit()
	atn := CypherParserParserStaticData.atn
	dfas := make([]*antlr.DFA, len(atn.DecisionToState))
	for decision, state := range atn.DecisionToState {
		dfas[decision] = antlr.NewDFA(state, decision)
	}
	return &parserPrediction{decisionToDFA: dfas, contextCache: antlr.NewPredictionContextCache()}
}

// acquirePrediction returns the generation one parse uses; the caller
// releases it with release.
func acquirePrediction() *parserPrediction {
	budget := parserDFAStateLimit()
	l := &predictions
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.states > budget {
		if l.current != nil && l.current.states > 0 {
			l.retireCurrentLocked()
		}
		if l.states > budget {
			return newParserPrediction()
		}
	}
	if l.current == nil {
		l.current = newParserPrediction()
		l.current.shared = true
		l.made++
	}
	l.current.readers++
	return l.current
}

// release ends a parse using p. It counts p's states, which only grow, so
// the larger of this count and the last one is p's share; a retired p leaves
// the account with its last parse.
func (p *parserPrediction) release() {
	if !p.shared {
		return
	}
	states := p.countStates()
	l := &predictions
	l.mu.Lock()
	if states > p.states {
		l.states += states - p.states
		p.states = states
	}
	p.readers--
	if p.retired && p.readers == 0 {
		delete(l.retired, p)
		l.states -= p.states
	}
	l.mu.Unlock()
}

// retireCurrentLocked takes the current generation out of service. A held
// one stays in the account until its last parse ends. The caller holds l.mu.
func (l *predictionLedger) retireCurrentLocked() {
	p := l.current
	l.current = nil
	if p.readers == 0 {
		l.states -= p.states
		return
	}
	p.retired = true
	if l.retired == nil {
		l.retired = make(map[*parserPrediction]struct{})
	}
	l.retired[p] = struct{}{}
}

// countStates counts the states of every decision's DFA under the ATN's
// state lock, so parses still using p may keep adding states.
func (p *parserPrediction) countStates() int64 {
	lock := atnStateLock()
	if lock == nil {
		return 0
	}
	lock.RLock()
	defer lock.RUnlock()
	n := 0
	for _, dfa := range p.decisionToDFA {
		n += dfa.Len()
	}
	return int64(n)
}

// usePrediction points parser at generation p for one parse.
func usePrediction(parser *CypherParser, p *parserPrediction) {
	parser.Interpreter = antlr.NewParserATNSimulator(parser, CypherParserParserStaticData.atn, p.decisionToDFA, p.contextCache)
}
