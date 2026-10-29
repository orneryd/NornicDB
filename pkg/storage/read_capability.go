package storage

import (
	"reflect"
)

// ReadCapabilityAvailability describes conditional optional data reads on a view.
// Implementing an interface promises that its operations can be served; an
// owner whose forwarding methods are conditional must additionally declare
// their live availability here. This is a selection hint, not a reservation.
// ReadWithCapability and VisitWithCapability own execution and handle refusal
// before rows; a historical operation never chooses a current Engine fallback.
// The request is structural: named, anonymous and combined interfaces have the
// same availability. This contract never grants access past a view boundary.
type ReadCapabilityAvailability interface {
	ReadCapabilityAvailable(request reflect.Type) bool
}

// ReadCapabilityIncludes reports whether request includes the operations of T.
// View owners use narrow interfaces to describe their actual dependencies,
// rather than recognizing the names or spellings used by individual consumers.
func ReadCapabilityIncludes[T any](request reflect.Type) bool {
	capabilityTypeCheck()
	return request.Kind() == reflect.Interface && request.Implements(reflect.TypeFor[T]())
}

// ReadCapabilitySource declares whether an engine delegates its read view.
// A transparent decorator returns its inner engine and true. A namespace,
// transaction, composite or filtered view returns nil and false: capabilities
// must be supplied by that view, never by its underlying engine. Engines that
// do not declare this contract are also treated as read-view boundaries.
// A nil or self source ends delegation without exposing an inner read view.
type ReadCapabilitySource interface {
	ReadCapabilitySource() (Engine, bool)
}

// ReadCapabilityScope is the closed admission policy of a view boundary.
// Data reads and read-view metadata must be owned by that view. Wrapper-owned
// metadata has a separate exact-provider policy. Adding a method to a boundary
// does not implicitly expose a new data-read capability.
type ReadCapabilityScope interface{ ReadCapabilityScoped(reflect.Type) bool }

// readCapabilityForEngine finds an optional capability on the engine that owns
// this read view. Transparent decorators declare their source through the one
// ReadCapabilitySource contract; their forwarding method sets do not advertise
// support that the underlying view lacks. Conditional view owners declare live
// ReadCapabilityAvailability. Discovery does not reserve dependencies: execute
// optional reads through ReadWithCapability or VisitWithCapability so refusal
// and a same-view, same-version fallback belong to the same operation.
//
// Namespace, transaction, composite and other view-changing engines are
// boundaries: the capability must belong to that view, never its inner engine.
// T is a data-read interface, for example ProjectedLabelNodeReader. View identity
// and visibility/revision metadata use ViewMetadataForEngine, independently of
// optional data-read availability. Writes and decorator-owned operations continue
// through the original Engine. Existing concrete view requests are also accepted.
func readCapabilityForEngine[T any](engine any) (T, bool) {
	return engineCapability[T](engine, capabilityReadView, nil)
}

// ViewMetadataForEngine finds metadata on the actual read-view owner. Like data
// discovery it passes transparent decorators and stops at view boundaries, but
// disabling an optional data read cannot hide the namespace/composite identity,
// visibility policy or live graph revision of that same view. Wrapper-owned WAL
// and limit metadata use MetadataCapabilityForEngine instead.
func ViewMetadataForEngine[T any](engine any) (T, bool) {
	return engineCapability[T](engine, capabilityViewMetadata, nil)
}

// OwnedCapability discovers a capability on its exact operation owner. Writes,
// transaction control, lifecycle mutation and narrower snapshot views use this
// policy so discovering a capability cannot bypass WAL, accounting or a view.
func OwnedCapability[T any](owner any) (T, bool) {
	capabilityTypeCheck()
	capability, supported := owner.(T)
	return capability, supported
}

// MetadataCapabilityForEngine finds the outermost owner of read-only wrapper
// metadata, such as its WAL or query-limit controller. An absent capability may
// pass a declared transparent layer, but never a namespace/transaction boundary.
// Data reads use readCapabilityForEngine, and mutations use OwnedCapability.
func MetadataCapabilityForEngine[T any](engine any) (T, bool) {
	return engineCapability[T](engine, capabilityMetadataOwner, nil)
}

// VisitPhysicalEngineOwners inspects exact physical owners through the common
// cursor, outermost first. Returning false stops inspection. This topology-only
// operation grants no data-read, mutation or commit authority: the caller keeps
// its original issued view and applies its own publication/admission protocol.
// visit must be non-nil. Physical namespace rescoping remains an explicit caller
// responsibility; logical read and membership discovery use their own policies.
func VisitPhysicalEngineOwners(engine Engine, visit func(Engine) bool) {
	engineCapability[Engine](engine, capabilityIdentityOwner, func(owner Engine) bool { return !visit(owner) })
}

type capabilityPolicy uint8

const (
	capabilityReadView capabilityPolicy = iota
	capabilityViewMetadata
	capabilityMetadataOwner
	capabilityIdentityOwner
	capabilityMembershipOwner
	capabilityCommitOwner
	capabilityBindReadView
)

// engineCapability is the single discovery cursor. Data reads use the declared
// read view; identity lookup follows physical wrappers until its predicate finds
// the owning namespace. Exact operations never leave their original owner.
func engineCapability[T any](owner any, policy capabilityPolicy, accept func(T) bool) (T, bool) {
	var zero T
	viewOwner := policy == capabilityReadView || policy == capabilityViewMetadata || policy == capabilityBindReadView
	var request reflect.Type
	checkAvailability := false
	if policy == capabilityReadView {
		request = reflect.TypeFor[T]()
		// Binding an Engine or discovering a concrete view identity is not
		// selecting an optional read. In particular, executor setup/cloning
		// must not perform availability lookups for literal statements.
		checkAvailability = request != reflect.TypeFor[Engine]() && request.Kind() == reflect.Interface && !reflect.TypeFor[Engine]().Implements(request)
	}
	for depth := 0; owner != nil && depth < 128; depth++ {
		capabilityProviderVisit()
		if policy == capabilityMembershipOwner || policy == capabilityCommitOwner {
			if capability, ok := OwnedCapability[T](owner); ok {
				return capability, true
			}
			source, ok := OwnedCapability[CapabilityOwnerSource](owner)
			if !ok {
				return zero, false
			}
			edge := MembershipOwnerPolicy
			if policy == capabilityCommitOwner {
				edge = CommitOwnerPolicy
			}
			inner, delegated := source.CapabilityOwnerSource(edge)
			if !delegated || inner == nil || sameCapabilityOwner(inner, owner) {
				return zero, false
			}
			owner = inner
			continue
		}
		if policy == capabilityBindReadView && accept != nil {
			accept(owner.(T))
		}
		if policy == capabilityReadView || policy == capabilityViewMetadata {
			if provider, ok := OwnedCapability[ReadViewBindingProvider](owner); ok {
				if bound := provider.ReadCapabilityBinding(); bound != nil && bound.issuer == reflect.TypeOf(owner) {
					if bound.owner == nil {
						return zero, false
					}
					if checkAvailability {
						if availability, ok := OwnedCapability[ReadCapabilityAvailability](owner); ok && !availability.ReadCapabilityAvailable(request) {
							return zero, false
						}
						if batch, ok := OwnedCapability[BatchNodeWithoutEmbeddingsCapability](owner); ok && ReadCapabilityIncludes[BatchNodeWithoutEmbeddingsReader](request) && !batch.BatchGetNodesWithoutEmbeddingsSupported() {
							return zero, false
						}
						if !bound.admits(request) {
							return zero, false
						}
					}
					owner = bound.owner
					if scope, ok := OwnedCapability[ReadCapabilityScope](owner); ok && !scope.ReadCapabilityScoped(reflect.TypeFor[T]()) {
						return zero, false
					}
					capability, ok := OwnedCapability[T](owner)
					return capability, ok
				}
			}
		}
		var inner Engine
		transparent := false
		if policy != capabilityIdentityOwner {
			if source, declared := OwnedCapability[ReadCapabilitySource](owner); declared {
				inner, transparent = source.ReadCapabilitySource()
			}
		}
		if policy == capabilityMetadataOwner {
			if source, declared := OwnedCapability[CapabilityOwnerSource](owner); declared {
				inner, transparent = source.CapabilityOwnerSource(WrapperMetadataPolicy)
			}
		}
		// Wrapper metadata belongs to the exact outer provider. Its method
		// may be declared by a decorator embedding a scoped view (for example
		// a query-limit controller); the embedded view's read policy must not
		// hide it. An absent provider still stops at that view boundary below.
		if !transparent && (policy == capabilityReadView || policy == capabilityViewMetadata) {
			if scope, ok := OwnedCapability[ReadCapabilityScope](owner); ok && !scope.ReadCapabilityScoped(reflect.TypeFor[T]()) {
				return zero, false
			}
		}
		if checkAvailability {
			availability, declared := OwnedCapability[ReadCapabilityAvailability](owner)
			batchAvailability, batchDeclared := OwnedCapability[BatchNodeWithoutEmbeddingsCapability](owner)
			if declared && !availability.ReadCapabilityAvailable(request) {
				return zero, false
			}
			// Legacy support=false is authoritative at every layer,
			// including a transparent decorator. Delegation must not
			// discard a live operation-availability signal.
			if batchDeclared && ReadCapabilityIncludes[BatchNodeWithoutEmbeddingsReader](request) && !batchAvailability.BatchGetNodesWithoutEmbeddingsSupported() {
				return zero, false
			}
		}
		if !viewOwner || !transparent {
			capability, supported := OwnedCapability[T](owner)
			if supported && (policy == capabilityBindReadView || accept == nil || accept(capability)) {
				return capability, true
			}
			if policy != capabilityIdentityOwner && (policy != capabilityMetadataOwner || !transparent) {
				return zero, false
			}
		}
		if policy == capabilityIdentityOwner {
			wrapper, supported := OwnedCapability[EngineUnwrapper](owner)
			if !supported {
				return zero, false
			}
			inner = wrapper.GetInnerEngine()
		}
		// Returning oneself is the existing terminal-wrapper convention. A
		// delegating wrapper with no inner view cannot supply a read capability.
		if inner == nil || sameCapabilityOwner(inner, owner) {
			return zero, false
		}
		owner = inner
	}
	return zero, false
}

func (t *TracedEngine) ReadCapabilitySource() (Engine, bool)     { return t.GetInnerEngine(), true }
func (w *WALEngine) ReadCapabilitySource() (Engine, bool)        { return w.GetInnerEngine(), true }
func (n *NamespacedEngine) ReadCapabilitySource() (Engine, bool) { return nil, false }
func (c *CompositeEngine) ReadCapabilitySource() (Engine, bool)  { return nil, false }

func sameCapabilityOwner(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	typ := reflect.TypeOf(a)
	return typ == reflect.TypeOf(b) && typ.Comparable() && a == b
}

// readSourceIdentity uses the physical-identity cursor solely for provenance
// deduplication. Namespace components compose; captured/filtered/transaction
// boundaries remain distinct even when their underlying physical store agrees.
func readSourceIdentity(engine Engine) (Engine, string) {
	path := ""
	source, ok := engineCapability[Engine](engine, capabilityIdentityOwner, func(layer Engine) bool {
		if namespace, ok := OwnedCapability[*NamespacedEngine](layer); ok {
			path += "\x00" + namespace.scope
			return false
		}
		if declaration, ok := OwnedCapability[ReadCapabilitySource](layer); ok {
			_, transparent := declaration.ReadCapabilitySource()
			if !transparent {
				return true
			}
		}
		wrapper, ok := OwnedCapability[EngineUnwrapper](layer)
		if !ok {
			return true
		}
		inner := wrapper.GetInnerEngine()
		return inner == nil || sameCapabilityOwner(inner, layer)
	})
	if !ok {
		return engine, path
	}
	return source, path
}
