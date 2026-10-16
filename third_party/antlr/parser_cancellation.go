// Copyright (c) 2026 NornicDB contributors.
// Use of this file is governed by the BSD 3-clause license in LICENSE.

package antlr

import "unsafe"

type configIdentitySet struct{ entries scalarDirectory[ATNConfig] }

func (set *configIdentitySet) add(config *ATNConfig, check func()) bool {
	key := int(uintptr(unsafe.Pointer(config)))
	if set.entries.get(key, check) != nil {
		return true
	}
	set.entries.put(key, config, check)
	return false
}

func (p *ParserATNSimulator) startStateLocked(dfa *DFA) *DFAState {
	if !dfa.getPrecedenceDfa() {
		return dfa.getS0()
	}
	p.atn.edgeMu.rLockWithWork(p.cancellationCheck)
	defer p.atn.edgeMu.RUnlock()
	return dfa.getPrecedenceStartState(p.parser.GetPrecedence())
}
func (p *ParserATNSimulator) startState(dfa *DFA) *DFAState {
	p.atn.stateMu.rLockWithWork(p.cancellationCheck)
	defer p.atn.stateMu.RUnlock()
	return p.startStateLocked(dfa)
}
func (p *ParserATNSimulator) initializeStartState(dfa *DFA) *DFAState {
	raw := p.computeStartState(dfa.atnStartState, ParserRuleContextEmpty, false)
	filtered := raw
	if dfa.getPrecedenceDfa() {
		filtered = p.applyPrecedenceFilter(raw)
	}
	p.atn.stateMu.lockWithWork(p.cancellationCheck)
	defer p.atn.stateMu.Unlock()
	if winner := p.startStateLocked(dfa); winner != nil {
		return winner
	}
	if dfa.getPrecedenceDfa() {
		raw.OptimizeConfigs(&p.BaseATNSimulator)
		raw.freezeWithWork(p.cancellationCheck)
	}
	state := p.addDFAState(dfa, NewDFAState(-1, filtered))
	if dfa.getPrecedenceDfa() {
		p.atn.edgeMu.lockWithWork(p.cancellationCheck)
		defer p.atn.edgeMu.Unlock()
		// Directory preparation may poll. Do it before publishing the complete
		// raw diagnostic closure; no published config is later optimized.
		dfa.setPrecedenceStartStateWithWork(p.parser.GetPrecedence(), state, p.cancellationCheck)
		dfa.s0.configs = raw
	} else {
		dfa.setS0(state)
	}
	return state
}
func (p *ParserATNSimulator) internState(dfa *DFA, state *DFAState) *DFAState {
	p.atn.stateMu.lockWithWork(p.cancellationCheck)
	defer p.atn.stateMu.Unlock()
	return p.addDFAState(dfa, state)
}
