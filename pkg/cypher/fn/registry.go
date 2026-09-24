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

// Func evaluates a Cypher function call.
//
// args are the raw argument expressions (not pre-evaluated). Use ctx.Eval for
// evaluation to preserve Cypher semantics like lazy coalesce.
type Func func(ctx Context, args []string) (interface{}, error)

var (
	mu       sync.RWMutex
	registry = map[string]Func{}
)

// Register registers a function implementation for the given name.
// Names are stored in lower-case.
func Register(name string, fn Func) {
	if strings.TrimSpace(name) == "" {
		panic("fn.Register: empty name")
	}
	if fn == nil {
		panic("fn.Register: nil function")
	}
	key := strings.ToLower(strings.TrimSpace(name))
	mu.Lock()
	defer mu.Unlock()
	registry[key] = fn
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
