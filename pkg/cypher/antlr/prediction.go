package antlr

import (
	"math"
	"runtime/debug"
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
// Parsers built here use the current generation instead, and a generation
// that has grown too large is replaced by a fresh one; the old one is freed
// when the last parse using it ends. The DFAs are a cache: a fresh
// generation recomputes predictions on demand and accepts exactly the same
// statements, only the first parse of each shape after a reset is slower.
// Replacing a generation never waits for, or holds back, any parse.
//
// One budget covers every live generation: the current one, those draining,
// the spare, and retired ones that parses still hold, each at its last exact
// count or estimate (liveStates). The current generation fits only while
// the total does, so while long parses hold large generations the current
// one stays small instead of growing to the budget again beside each of
// them.
//
// Only an exact count retires a generation. While parses always hold the
// current one, its size is estimated from new-shape tokens, which overcounts
// (a new shape built from known parts adds almost no states). A generation
// past the budget by the estimate is drained instead: new parses use a
// stand-in until its last parse ends, then it is counted, and it is current
// again if it fits. A generation only short parses hold drains in
// milliseconds, so stand-ins drain the same way and keep their own warm
// predictions, and the stand-in a restore replaces is kept as the spare that
// the next drain or replacement hands new parses to, warm. Without this, 8
// workers of distinct small statements next to one long parse replaced the
// generation 20 times in 13 s, and the long statement parsed cold on every
// run.
type parserPrediction struct {
	// mu is held shared by every parse using this generation; counting the
	// DFA states takes it exclusively, and only when no parse holds it
	// (TryLock), because ANTLR adds states under its own lock, which this
	// package cannot take. readers counts the parses holding it.
	mu            sync.RWMutex
	readers       atomic.Int32
	decisionToDFA []*antlr.DFA
	contextCache  *antlr.PredictionContextCache

	// shapes holds the fingerprints of the statement shapes parsed with
	// this generation, and newShapeTokens their total token count.
	shapesMu       sync.Mutex
	shapes         map[uint64]struct{}
	newShapeTokens atomic.Int64

	// exactStates is the DFA state count of the last exact count, and
	// tokensAtExact newShapeTokens at that moment. While the generation is
	// never idle its size is estimated from them (estimatedStates).
	exactStates   atomic.Int64
	tokensAtExact atomic.Int64

	// accounted is this generation's share of liveStates.
	accounted atomic.Int64

	// seq orders generations by when they became current, by creation or as
	// the spare, and standIn marks one made by a drain or replacing one that
	// was: a drained generation that fits is current again in place of a
	// stand-in that became current after it. draining is set from a drain to
	// the exact count that ends it.
	seq      atomic.Uint64
	standIn  bool
	draining atomic.Bool
}

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
var maxParserDFAStates = 0

// parserDFAStateLimit bounds the DFA states of all live prediction
// generations together: a quarter of the runtime memory limit, or
// defaultPredictionBudget without one, in states.
func parserDFAStateLimit() int64 {
	if maxParserDFAStates > 0 {
		return int64(maxParserDFAStates)
	}
	budget := int64(defaultPredictionBudget)
	if limit := debug.SetMemoryLimit(-1); limit > 0 && limit < math.MaxInt64 {
		budget = limit / predictionBudgetShare
	}
	return budget / bytesPerDFAState
}

// statesPerNewShapeToken is the growth estimate for a generation that is
// never idle. Predictions depend only on token types, so a shape the
// generation has parsed before adds no states. A new shape added 0.27
// states per token for a 99 KB application statement and at most 1.56 for
// small cold statements, so continuously busy generations stay within
// about 1.6x the budget even when every statement is new and small.
const statesPerNewShapeToken = 1

// maxPredictionShapes bounds the fingerprint set of one generation.
const maxPredictionShapes = 1 << 18

// sparePrediction is the stand-in the last restore replaced, which the next
// drain or replacement makes current. liveStates is the sum of every live
// generation's accounted states.
var (
	currentPrediction atomic.Pointer[parserPrediction]
	sparePrediction   atomic.Pointer[parserPrediction]
	liveStates        atomic.Int64
	predictionSeq     atomic.Uint64
	predictionMu      sync.Mutex
)

func newParserPrediction(standIn bool) *parserPrediction {
	CypherParserInit()
	atn := CypherParserParserStaticData.atn
	dfas := make([]*antlr.DFA, len(atn.DecisionToState))
	for decision, state := range atn.DecisionToState {
		dfas[decision] = antlr.NewDFA(state, decision)
	}
	p := &parserPrediction{decisionToDFA: dfas, contextCache: antlr.NewPredictionContextCache(), standIn: standIn}
	p.seq.Store(predictionSeq.Add(1))
	return p
}

// acquirePrediction returns the current generation, held for one parse;
// the caller releases it with release.
func acquirePrediction() *parserPrediction {
	p := currentPrediction.Load()
	if p == nil {
		predictionMu.Lock()
		if p = currentPrediction.Load(); p == nil {
			p = newParserPrediction(false)
			currentPrediction.Store(p)
		}
		predictionMu.Unlock()
	}
	p.readers.Add(1)
	p.mu.RLock()
	return p
}

// release ends a parse using p of the statement tokens holds, and replaces p
// when the live generations have grown past parserDFAStateLimit. When no
// parse holds p its DFA states are counted exactly; while parses always hold
// it, which TryLock can't get past, the count is estimated from the last
// exact count and the new-shape tokens since (estimatedStates), and an
// estimate past the budget drains p (drain). Neither check waits for other
// parses, and a generation is only replaced when an exact count is past the
// budget, so a working set that fits keeps its warm predictions.
func (p *parserPrediction) release(tokens *antlr.CommonTokenStream) {
	p.mu.RUnlock()
	p.readers.Add(-1)
	if tokens != nil {
		if shape, count := statementShape(tokens); p.addShape(shape) {
			p.newShapeTokens.Add(int64(count))
		}
	}
	budget := parserDFAStateLimit()
	if p.mu.TryLock() {
		p.count(budget)
		return
	}
	p.account(p.estimatedStates())
	if currentPrediction.Load() == p && overBudget(p, budget) {
		p.drain(budget)
	}
}

// count counts p's DFA states exactly and accounts them. A current p past
// the budget is replaced and a spare one dropped; a drained p is restored if
// it fits and retired if not. The caller holds p.mu, which count releases.
func (p *parserPrediction) count(budget int64) {
	tokensNow := p.newShapeTokens.Load()
	states := int64(p.dfaStates())
	p.mu.Unlock()
	p.exactStates.Store(states)
	p.tokensAtExact.Store(tokensNow)
	p.account(states)
	if !p.live() {
		return
	}
	fits := !overBudget(p, budget)
	switch {
	case p.draining.CompareAndSwap(true, false):
		if fits {
			p.restore()
		} else {
			p.settle()
		}
	case fits:
	case sparePrediction.CompareAndSwap(p, nil):
		p.settle()
	default:
		p.retire()
		p.settle()
	}
}

// drain makes the spare, or a fresh stand-in without one, current so that
// p's parses end and the last one counts p exactly.
func (p *parserPrediction) drain(budget int64) {
	if !p.draining.CompareAndSwap(false, true) {
		return
	}
	if !p.replace(true) {
		p.draining.Store(false)
		return
	}
	// p's parses may all have ended already, and then no release counts it.
	if p.mu.TryLock() {
		p.count(budget)
	}
}

// replace makes the spare current in place of p, or without a spare a fresh
// generation that is a stand-in when standIn is set. It reports false when p
// was no longer current.
func (p *parserPrediction) replace(standIn bool) bool {
	next := sparePrediction.Swap(nil)
	if next == nil {
		next = newParserPrediction(standIn)
	} else {
		next.seq.Store(predictionSeq.Add(1))
	}
	if !currentPrediction.CompareAndSwap(p, next) {
		if !sparePrediction.CompareAndSwap(nil, next) {
			next.settle()
		}
		return false
	}
	next.account(next.estimatedStates())
	return true
}

// restore makes drained generation p current again in place of a stand-in
// that became current after it, which becomes the spare; p is retired when
// no such stand-in is current.
func (p *parserPrediction) restore() {
	for {
		c := currentPrediction.Load()
		if c == nil || c == p || !c.standIn || c.seq.Load() < p.seq.Load() {
			p.settle()
			return
		}
		if currentPrediction.CompareAndSwap(c, p) {
			p.account(p.exactStates.Load())
			if old := sparePrediction.Swap(c); old != nil && old != c {
				old.settle()
			}
			return
		}
	}
}

// overBudget reports whether the live generations hold more states than
// budget. When dropping the spare, unless it is keep, would make room, it
// drops it; otherwise the spare is kept for the drain or replacement that
// follows.
func overBudget(keep *parserPrediction, budget int64) bool {
	live := liveStates.Load()
	if live <= budget {
		return false
	}
	s := sparePrediction.Load()
	if s == nil || s == keep || live-s.accounted.Load() > budget {
		return true
	}
	if sparePrediction.CompareAndSwap(s, nil) {
		s.settle()
	}
	return liveStates.Load() > budget
}

// live reports whether p counts against the budget: it is current, the
// spare or draining, or a parse holds it.
func (p *parserPrediction) live() bool {
	return currentPrediction.Load() == p || sparePrediction.Load() == p || p.draining.Load() || p.readers.Load() > 0
}

// account sets p's share of liveStates to n states, or to none while p is
// not live. Whoever makes p not live settles it, and account checks
// liveness again after its update, so a racing update leaves no share
// behind.
func (p *parserPrediction) account(n int64) {
	if !p.live() {
		n = 0
	}
	liveStates.Add(n - p.accounted.Swap(n))
	if n != 0 && !p.live() {
		liveStates.Add(-p.accounted.Swap(0))
	}
}

// settle drops p's share of liveStates once p is not live, and keeps it while
// a parse still holds p: the parse's release accounts it again, and its last
// one drops it.
func (p *parserPrediction) settle() {
	p.account(p.accounted.Load())
}

// estimatedStates is p's DFA state count from its last exact count plus
// statesPerNewShapeToken for every new-shape token parsed since.
func (p *parserPrediction) estimatedStates() int64 {
	return p.exactStates.Load() + (p.newShapeTokens.Load()-p.tokensAtExact.Load())*statesPerNewShapeToken
}

// retire makes the spare, or a fresh generation that stands in where p did,
// current if p still is.
func (p *parserPrediction) retire() {
	p.replace(p.standIn)
}

// addShape records shape and reports whether it is new to p. A full set
// counts every further shape as new.
func (p *parserPrediction) addShape(shape uint64) bool {
	p.shapesMu.Lock()
	defer p.shapesMu.Unlock()
	if _, seen := p.shapes[shape]; seen {
		return false
	}
	if p.shapes == nil {
		p.shapes = make(map[uint64]struct{})
	}
	if len(p.shapes) < maxPredictionShapes {
		p.shapes[shape] = struct{}{}
	}
	return true
}

// statementShape is the FNV-1a fingerprint of the token types tokens holds,
// and their count.
func statementShape(tokens *antlr.CommonTokenStream) (uint64, int) {
	all := tokens.GetAllTokens()
	hash := uint64(14695981039346656037)
	for _, token := range all {
		tokenType := uint32(token.GetTokenType())
		for shift := 0; shift < 32; shift += 8 {
			hash ^= uint64(byte(tokenType >> shift))
			hash *= 1099511628211
		}
	}
	return hash, len(all)
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

// usePrediction points parser at generation p for one parse.
func usePrediction(parser *CypherParser, p *parserPrediction) {
	parser.Interpreter = antlr.NewParserATNSimulator(parser, CypherParserParserStaticData.atn, p.decisionToDFA, p.contextCache)
}
