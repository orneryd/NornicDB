// Copyright (c) 2026 NornicDB contributors.
// Use of this file is governed by the BSD 3-clause license in LICENSE.

package antlr

// transitionMatchesWithWork keeps the public Transition interface unchanged.
// Only set transitions scan a variable-size character/token interval list.
func transitionMatchesWithWork(transition Transition, symbol, minimum, maximum int, check func()) bool {
	if check == nil {
		return transition.Matches(symbol, minimum, maximum)
	}
	switch value := transition.(type) {
	case *NotSetTransition:
		return symbol >= minimum && symbol <= maximum && !value.intervalSet.containsWithWork(symbol, check)
	case *SetTransition:
		return value.intervalSet.containsWithWork(symbol, check)
	default:
		return transition.Matches(symbol, minimum, maximum)
	}
}
