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
//
// It says nothing about the row bindings an implementation may read; InputImage
// owns that, and a canonical read observation never implies a narrow input
// image.
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

// InputImage is the input-image demand a registration declares for the row
// bindings a call may read: how much of a binding's stored image the
// implementation reads. The statement planner reads it to decide which property
// bodies a scan must decode before the call.
//
// It is independent of ReadObservation, which says whether the implementation's
// database access is observable. A registration stores both policies with its
// implementation under the registry lock. A replacement completed before
// statement planning supplies its own demand, independently of the old name.
//
// The zero value is FullInputImage: a registration that doesn't state a demand
// is never trusted with a narrower image than a custom callback can read.
type InputImage uint8

const (
	// FullInputImage declares an implementation that may read any visible row
	// binding completely, including bindings its own arguments never name: it
	// receives the call context, not only its arguments' values. The statement
	// keeps every visible binding's whole image for it. This is the
	// conservative declaration for a public custom registration.
	FullInputImage InputImage = iota
	// ArgumentInputImage declares an implementation that reads only the values
	// of its own argument expressions, never another binding. A node argument
	// is read as a value, so its whole property body is part of that value; a
	// property argument (n.key) is that property alone. Native node arguments
	// include stored embedding sidecars when they can reach this callback.
	ArgumentInputImage
	// HeaderInputImage declares an implementation that reads only identity,
	// labels and existence from its argument values: id(v), elementId(v),
	// labels(v), type(r).
	HeaderInputImage
	// StructuralInputImage declares a reviewed built-in whose reads of an
	// argument follow the planner's canonical structural contract for that
	// name: one of its elements, its cardinality, or its whole property body
	// (head(), size(), collect()). It is a promise about a built-in
	// implementation, not a way to rename transform semantics: a name the
	// planner has no structural contract for keeps the whole argument value,
	// and a custom implementation of a structural built-in's name declares
	// ArgumentInputImage or FullInputImage instead.
	StructuralInputImage
	// MetadataArgumentInputImage reads only argument values without embedding
	// sidecars or implicit row bindings. Reviewed Cypher value built-ins use
	// this contract; a custom argument reader that can inspect stored vectors
	// must use ArgumentInputImage instead.
	MetadataArgumentInputImage
)

// Valid reports whether image names a declared input-image demand. Register
// validates through this owner, so an unknown value can't be stored as one
// demand and read as another.
func (image InputImage) Valid() bool {
	switch image {
	case FullInputImage, ArgumentInputImage, HeaderInputImage, StructuralInputImage, MetadataArgumentInputImage:
		return true
	}
	return false
}

// registration is one function and the policies declared for it. All of them
// are replaced together under the registry lock, so a call never observes one
// registration's implementation with another's policy.
type registration struct {
	implementation Func
	reads          ReadObservation
	image          InputImage
}

var (
	mu       sync.RWMutex
	registry = map[string]registration{}
)

// Register registers a function implementation and the two policies declared
// for it: the read observation of its database access and the input-image
// demand of the row bindings it reads.
//
// Registering an existing name replaces the implementation and both policies:
// the name carries no trust of its own, so a replacement of a built-in is
// opaque and full-scope unless its own registration says otherwise. A custom
// caller passes OpaqueReads and FullInputImage. A built-in passes
// CanonicalReads only after source review shows its reads are complete without
// the opaque-callback hook, and states the narrowest input image its reviewed
// implementation reads: ArgumentInputImage for a function that reads its
// arguments' values, HeaderInputImage for the entity scalars, and
// StructuralInputImage for the list and property built-ins whose argument reads
// the planner's structural contract owns.
func Register(name string, implementation Func, observation ReadObservation, image InputImage) {
	if strings.TrimSpace(name) == "" {
		panic("fn.Register: empty name")
	}
	if implementation == nil {
		panic("fn.Register: nil function")
	}
	if !observation.Valid() {
		panic("fn.Register: unknown read observation")
	}
	if !image.Valid() {
		panic("fn.Register: unknown input image")
	}
	key := cyphertext.NameLower(strings.TrimSpace(name))
	mu.Lock()
	defer mu.Unlock()
	registry[key] = registration{implementation: implementation, reads: observation, image: image}
}

// InputImageOf returns the input-image demand the registration of name
// declares, and whether a function is registered under that name at all. It
// reads the live registration: a statement's planning sees the demand of the
// implementation its calls resolve to, without a snapshot or a version cache.
//
// found is false for a name the registry doesn't own (an aggregate, an
// evaluator built-in implemented elsewhere, or a native plugin function). Such
// a call has no registration to promise anything, so the planner decides from
// the owner it has; an unregistered call never reports a narrower promise than
// that owner states.
func InputImageOf(name string) (image InputImage, found bool) {
	key := cyphertext.NameLower(strings.TrimSpace(name))
	mu.RLock()
	entry, ok := registry[key]
	mu.RUnlock()
	if !ok {
		return FullInputImage, false
	}
	return entry.image, true
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
