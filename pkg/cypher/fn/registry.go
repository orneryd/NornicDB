package fn

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/internal/queryprofile"
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
// workCtx is the caller's statement context, the context the statement
// execution was given: work that outlasts argument evaluation observes its
// cancellation and deadline there.
//
// args are the raw argument expressions (not pre-evaluated). Use ctx.Eval for
// evaluation to preserve Cypher semantics like lazy coalesce.
type Func func(workCtx context.Context, ctx Context, args []string) (interface{}, error)

// ReadObservation is the read-observation policy a registration declares for
// the database access its implementation performs. It is the one policy type
// every callback registration shares (see internal/queryprofile), so the
// policy a source registration states is the policy an admitted callable
// stores. The zero value is OpaqueReads, so a registration that doesn't state
// a policy is never trusted with an unobserved read.
type ReadObservation = queryprofile.ReadObservation

const (
	// OpaqueReads declares a callback whose complete database access the
	// caller's reader observations cannot see. Every actual invocation is
	// reported through queryprofile.Callback.
	OpaqueReads = queryprofile.OpaqueReads
	// CanonicalReads declares an implementation whose reads are complete
	// without the opaque-callback hook.
	CanonicalReads = queryprofile.CanonicalReads
)

// registration is one function and the read-observation policy declared for
// it. Both are replaced together under the registry lock, so a call never
// observes one registration's implementation with another's policy.
type registration struct {
	implementation Func
	reads          ReadObservation
}

var (
	mu       sync.RWMutex
	registry = map[string]registration{}
)

// Register registers a function implementation and its read-observation policy
// for the given name. Names are stored in lower-case.
//
// Registering an existing name replaces both the implementation and the
// policy: the name carries no trust of its own, so a replacement of a built-in
// is opaque unless its own registration says otherwise. A custom caller passes
// OpaqueReads; a built-in passes CanonicalReads only after source review shows
// its reads are complete without the opaque-callback hook.
func Register(name string, implementation Func, observation ReadObservation) {
	if strings.TrimSpace(name) == "" {
		panic("fn.Register: empty name")
	}
	if implementation == nil {
		panic("fn.Register: nil function")
	}
	if !observation.Valid() {
		panic("fn.Register: unknown read observation")
	}
	key := cyphertext.NameLower(strings.TrimSpace(name))
	mu.Lock()
	defer mu.Unlock()
	registry[key] = registration{implementation: implementation, reads: observation}
}

// EvaluateFunction evaluates a function by name.
//
// workCtx is the caller's statement context; it is the context the registered
// function receives. It must be the statement's own context, not a
// background context.
//
// Returns:
//   - value: the function result
//   - found: whether the function name is registered
//   - err: evaluation error (only meaningful if found is true)
func EvaluateFunction(workCtx context.Context, name string, args []string, ctx Context) (value interface{}, found bool, err error) {
	key := cyphertext.NameLower(strings.TrimSpace(name))
	mu.RLock()
	entry, ok := registry[key]
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
	// Only an opaque registration reports the opaque-callback hook; a
	// canonical one carries its own complete observation.
	if entry.reads == OpaqueReads {
		queryprofile.Callback(workCtx, "registered function callback", key)
	}
	val, err := entry.implementation(workCtx, ctx, args)
	return FunctionResult(val), true, err
}
