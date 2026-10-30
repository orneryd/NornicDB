package fn

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/internal/cyphertext"
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
// evaluation to preserve Cypher semantics like lazy coalesce. Expression hosts
// evaluate external Eval(text) as a fresh expression in the current row scope;
// the callback never identifies an occurrence in the enclosing query. Context
// documents the existing value-bound host adapter.
type Func func(ctx Context, args []string) (interface{}, error)

// InvocationFunc is an internal function over retained host arguments. The
// strings are derived metadata; evaluation selects a position through ctx.Eval.
type InvocationFunc func(ctx InvocationContext, args []string) (interface{}, error)

type registeredFunction struct {
	invocation InvocationFunc
	external   Func
}

var (
	mu       sync.RWMutex
	registry = map[string]registeredFunction{}
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
	key := cyphertext.NameLower(strings.TrimSpace(name))
	mu.Lock()
	defer mu.Unlock()
	registry[key] = registeredFunction{external: fn}
}

// RegisterInvocation registers an internal argument-identified implementation.
func RegisterInvocation(name string, function InvocationFunc) {
	if strings.TrimSpace(name) == "" {
		panic("fn.RegisterInvocation: empty name")
	}
	if function == nil {
		panic("fn.RegisterInvocation: nil function")
	}
	key := cyphertext.NameLower(strings.TrimSpace(name))
	mu.Lock()
	defer mu.Unlock()
	registry[key] = registeredFunction{invocation: function}
}

// EvaluateFunction evaluates a function by name.
//
// Returns:
//   - value: the function result
//   - found: whether the function name is registered
//   - err: evaluation error (only meaningful if found is true)
func EvaluateFunction(name string, args []string, ctx Context) (value interface{}, found bool, err error) {
	var evaluate func(int) (interface{}, error)
	var part func(int, cyphertext.Span) (interface{}, error)
	if ctx.Eval != nil {
		evaluate = func(index int) (interface{}, error) { return ctx.Eval(args[index]) }
		part = func(index int, span cyphertext.Span) (interface{}, error) {
			return ctx.Eval(args[index][span.Start:span.End])
		}
	}
	return EvaluateInvocation(name, args, InvocationContext{Context: ctx, Eval: evaluate, EvalSpan: part})
}

// EvaluateInvocation is the internal host entry. Only the external Func
// adapter receives Context's standalone-expression callback.
func EvaluateInvocation(name string, args []string, ctx InvocationContext) (value interface{}, found bool, err error) {
	key := cyphertext.NameLower(strings.TrimSpace(name))
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
	var val interface{}
	if fn.invocation != nil {
		val, err = fn.invocation(ctx, args)
	} else {
		val, err = fn.external(ctx.Context, args)
	}
	return FunctionResult(val), true, err
}
