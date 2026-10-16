package fn

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// ArgumentTypeError reports a function argument of a type the function does
// not accept (labels() of a map, a number, ...). Callers turn it into a
// Neo.ClientError.Statement.TypeError.
type ArgumentTypeError struct {
	Function string
	Value    interface{}
}

func (e *ArgumentTypeError) Error() string {
	return fmt.Sprintf("%s() received an invalid %T argument", e.Function, e.Value)
}

// TypeMismatchError reports a function argument whose type the function's
// signature excludes (size() of a map, a node, a number, ...). Neo4j rejects
// these while planning; callers turn it into
// Neo.ClientError.Statement.SyntaxError "Type mismatch: expected <Expected>
// but was <type of Value>".
type TypeMismatchError struct {
	Function string
	Expected string
	Value    interface{}
}

func (e *TypeMismatchError) Error() string {
	return fmt.Sprintf("%s() expected %s but received %T", e.Function, e.Expected, e.Value)
}

// Func evaluates a Cypher function call.
//
// args are the raw argument expressions (not pre-evaluated). Use ctx.Eval for
// evaluation to preserve Cypher semantics like lazy coalesce.
type Func func(ctx Context, args []string) (interface{}, error)

var (
	mu       sync.RWMutex
	registry = map[string]Func{}
	classes  = map[string]PrivilegeClass{}
)

// Register registers a function implementation for the given name.
// Names are stored in lower-case.
func Register(name string, class PrivilegeClass, fn Func) {
	if strings.TrimSpace(name) == "" {
		panic("fn.Register: empty name")
	}
	if fn == nil {
		panic("fn.Register: nil function")
	}
	if !class.Valid() {
		panic("fn.Register: explicit privilege class required")
	}
	key := strings.ToLower(strings.TrimSpace(name))
	mu.Lock()
	defer mu.Unlock()
	registry[key] = fn
	classes[key] = class
}

// Classes returns a snapshot of every implementation's declared contract,
// including internal parser helpers that are not listed by SHOW FUNCTIONS.
func Classes() map[string]PrivilegeClass {
	mu.RLock()
	defer mu.RUnlock()
	out := make(map[string]PrivilegeClass, len(classes))
	for name, class := range classes {
		out[name] = class
	}
	return out
}

// Class reports the contract of a registered implementation.
func Class(name string) (PrivilegeClass, bool) {
	mu.RLock()
	defer mu.RUnlock()
	class, found := classes[strings.ToLower(strings.TrimSpace(name))]
	return class, found
}

// EvaluateFunction evaluates a function by name.
//
// Returns:
//   - value: the function result
//   - found: whether the function name is registered
//   - err: evaluation error (only meaningful if found is true)
func EvaluateFunction(name string, args []string, ctx Context) (value interface{}, found bool, err error) {
	key := strings.ToLower(strings.TrimSpace(name))
	mu.RLock()
	fn, ok := registry[key]
	mu.RUnlock()
	if !ok {
		return nil, false, nil
	}
	if ctx.Eval == nil {
		return nil, true, fmt.Errorf("fn.Context.Eval is required")
	}
	if ctx.Now == nil {
		ctx.Now = func() time.Time { return time.Now() }
	}
	val, err := fn(ctx, args)
	return val, true, err
}
