// Package admittedcheck keeps a typed checker behind its constructor.
// Consumers cannot name its type or initialize its private callback. A usable
// checker therefore originates in New; an inferred zero has no callback.
package admittedcheck

type checker[Frame, Input, Result any] struct {
	frame Frame
	run   func(Frame, Input) (Result, error)
}

// New retains the complete admitted frame, with no second source of facts.
func New[Frame, Input, Result any](frame Frame, run func(Frame, Input) (Result, error)) checker[Frame, Input, Result] {
	return checker[Frame, Input, Result]{frame: frame, run: run}
}

func (value checker[Frame, Input, Result]) Check(input Input) (Result, error) {
	return value.run(value.frame, input)
}
