// Copyright (c) 2026 NornicDB contributors.
// Use of this file is governed by the BSD 3-clause license in LICENSE.

package antlr

func dfaStateOperation(check func()) *CollectionOperation[*DFAState] {
	if check == nil {
		return nil
	}
	return &CollectionOperation[*DFAState]{Check: check,
		Hash: func(state *DFAState, work func()) int {
			hash := murmurInit(7)
			hash = murmurUpdate(hash, state.configs.hashWithWork(work))
			return murmurFinish(hash, 1)
		},
		Equals: func(left, right *DFAState, work func()) bool {
			return left == right || left.configs.equalsWithWork(right.configs, work)
		},
	}
}

func semanticHashWithWork(context SemanticContext, check func()) int {
	chargeWork(check)
	var operands workSequence[SemanticContext]
	seed := 37
	switch value := context.(type) {
	case *AND:
		operands = value.opnds
	case *OR:
		operands = value.opnds
		seed = 41
	default:
		return context.Hash()
	}
	hash := murmurInit(seed)
	for i := 0; i < operands.Len(); i++ {
		hash = murmurUpdate(hash, semanticHashWithWork(operands.At(i, check), check))
	}
	return murmurFinish(hash, operands.Len())
}
func semanticEqualWithWork(left, right SemanticContext, check func()) bool {
	chargeWork(check)
	if left == right {
		return true
	}
	var lhs, rhs workSequence[SemanticContext]
	switch value := left.(type) {
	case *AND:
		other, ok := right.(*AND)
		if !ok {
			return false
		}
		lhs, rhs = value.opnds, other.opnds
	case *OR:
		other, ok := right.(*OR)
		if !ok {
			return false
		}
		lhs, rhs = value.opnds, other.opnds
	default:
		return left.Equals(right)
	}
	if lhs.Len() != rhs.Len() {
		return false
	}
	for i := 0; i < lhs.Len(); i++ {
		if !semanticEqualWithWork(lhs.At(i, check), rhs.At(i, check), check) {
			return false
		}
	}
	return true
}

func semanticOperation(check func()) *CollectionOperation[SemanticContext] {
	if check == nil {
		return nil
	}
	return &CollectionOperation[SemanticContext]{Check: check, Hash: semanticHashWithWork, Equals: semanticEqualWithWork}
}

func configLookupOperation(comparator Comparator[*ATNConfig], check func()) *CollectionOperation[*ATNConfig] {
	if check == nil {
		return nil
	}
	operation := &CollectionOperation[*ATNConfig]{Check: check}
	operation.Hash = func(config *ATNConfig, work func()) int {
		switch comparator.(type) {
		case *ATNConfigComparator[*ATNConfig]:
			return 31*(31*(31*7+config.state.GetStateNumber())+config.alt) + semanticHashWithWork(config.semanticContext, work)
		case *ATNAltConfigComparator[*ATNConfig]:
			hash := murmurInit(7)
			hash = murmurUpdate(hash, config.state.GetStateNumber())
			hash = murmurUpdate(hash, config.context.Hash())
			return murmurFinish(hash, 2)
		default:
			return config.HashWithWork(work)
		}
	}
	operation.Equals = func(left, right *ATNConfig, work func()) bool {
		if left == right {
			return true
		}
		if left == nil || right == nil {
			return false
		}
		switch comparator.(type) {
		case *ATNConfigComparator[*ATNConfig], *BaseATNConfigComparator[*ATNConfig]:
			return left.state.GetStateNumber() == right.state.GetStateNumber() && left.alt == right.alt && semanticEqualWithWork(left.semanticContext, right.semanticContext, work)
		case *ATNAltConfigComparator[*ATNConfig]:
			return left.state.GetStateNumber() == right.state.GetStateNumber() && left.context.EqualsWithWork(right.context, work)
		default:
			return left.EqualsWithWork(right, work)
		}
	}
	return operation
}
