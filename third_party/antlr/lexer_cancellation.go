// Copyright (c) 2026 NornicDB contributors.
// Use of this file is governed by the BSD 3-clause license in LICENSE.

package antlr

func (l *LexerATNSimulator) cachedStartState(dfa *DFA) *DFAState {
	l.atn.stateMu.rLockWithWork(l.cancellationCheck)
	defer l.atn.stateMu.RUnlock()
	return dfa.getS0()
}
