package cypher

import (
	"context"
	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
)

// constantArguments preserves the public error and empty-argument contracts of
// the two namespaces while sharing the immutable value-returning implementation.
type constantArguments uint8

const (
	coreConstantArguments constantArguments = iota
	gdsConstantArguments
)

func constantFunction(name string, value interface{}, arguments constantArguments) cypherfn.Func {
	return func(_ context.Context, _ cypherfn.Context, args []string) (interface{}, error) {
		if arguments == gdsConstantArguments {
			if err := gdsArity(name, args, 0, 0); err != nil {
				return nil, err
			}
		} else if len(args) != 0 {
			return nil, argumentCountError(name, "0", len(args))
		}
		return value, nil
	}
}
