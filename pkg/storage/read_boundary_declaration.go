package storage

import (
	"reflect"
	"strings"
	"sync"
)

// CapabilityDisposition states how a boundary handles an optional operation.
type CapabilityDisposition uint8

const (
	// CapabilityOwned admits an operation whose scope is enforced by the view.
	CapabilityOwned CapabilityDisposition = iota + 1
	// CapabilityTransparent delegates reads to the declared source. Mutation
	// and decorator metadata owners retain their existing execution contracts.
	CapabilityTransparent
	// CapabilityRefused keeps an operation behind this read boundary.
	CapabilityRefused
)

// CapabilityDeclaration assigns one policy to each named method. Scope describes
// the owned scope, transparent source, or reason for refusal. The go/types
// completeness test checks these declarations against the actual method sets;
// adding an operation cannot silently acquire a default policy.
type CapabilityDeclaration struct {
	Methods     string
	Disposition CapabilityDisposition
	Scope       string
}

// BoundaryReadPolicy is the boundary's complete optional-method policy, declared
// beside its type. Admission is derived from this policy rather than a second
// list. Core Engine operations retain their existing view ownership.
type BoundaryReadPolicy []CapabilityDeclaration

// ReadCapabilityDeclaration exposes a view's policy for completeness checking.
// Transparent decorators also declare their implemented forwards explicitly.
type ReadCapabilityDeclaration interface {
	ReadCapabilityPolicy() BoundaryReadPolicy
}

// Copy returns declarations without exposing the view's admission table to mutation.
func (policy BoundaryReadPolicy) Copy() BoundaryReadPolicy {
	return append(BoundaryReadPolicy(nil), policy...)
}

// Scoped reports whether every requested optional operation is owned and
// admitted by this view. Unknown methods and declared refusals remain closed.
func (policy BoundaryReadPolicy) Scoped(request reflect.Type) bool {
	if request.Kind() != reflect.Interface {
		return true
	}
	if len(policy) == 0 {
		return request.NumMethod() == 0
	}
	key := &policy[0]
	cached, ok := compiledBoundaryPolicies.Load(key)
	if !ok {
		table := &compiledBoundaryPolicy{owned: map[string]bool{}}
		for _, d := range policy {
			if d.Disposition == CapabilityOwned {
				for _, name := range strings.Fields(d.Methods) {
					table.owned[name] = true
				}
			}
		}
		cached, _ = compiledBoundaryPolicies.LoadOrStore(key, table)
	}
	table := cached.(*compiledBoundaryPolicy)
	if result, ok := table.requests.Load(request); ok {
		return result.(bool)
	}
	core := reflect.TypeFor[Engine]()
	admitted := true
	for i := 0; i < request.NumMethod(); i++ {
		name := request.Method(i).Name
		if _, ok := core.MethodByName(name); !ok && !table.owned[name] {
			admitted = false
			break
		}
	}
	table.requests.Store(request, admitted)
	return admitted
}

// Declarations are immutable and static. Compile once per declaration table,
// then once per structural request type, never once per row.
var compiledBoundaryPolicies sync.Map

type compiledBoundaryPolicy struct {
	owned    map[string]bool
	requests sync.Map
}
