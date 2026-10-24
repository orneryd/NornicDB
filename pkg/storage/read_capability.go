package storage

import "reflect"

// ReadCapabilityAvailability describes conditional optional data reads on a view.
// Implementing an interface promises that its operations can be served; an
// owner whose forwarding methods are conditional must additionally declare
// their live availability here. False means callers must choose their core
// Engine fallback before invoking the optional operation or visiting any rows.
// The request is structural: named, anonymous and combined interfaces have the
// same availability. This contract never grants access past a view boundary.
type ReadCapabilityAvailability interface {
	ReadCapabilityAvailable(request reflect.Type) bool
}

// ReadCapabilityIncludes reports whether request includes the operations of T.
// View owners use narrow interfaces to describe their actual dependencies,
// rather than recognizing the names or spellings used by individual consumers.
func ReadCapabilityIncludes[T any](request reflect.Type) bool {
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

// ReadCapabilityForEngine finds an optional capability on the engine that owns
// this read view. Transparent decorators declare their source through the one
// ReadCapabilitySource contract; their forwarding method sets do not advertise
// support that the underlying view lacks. Conditional view owners declare live
// ReadCapabilityAvailability; an unavailable dependency retains the caller's
// core fallback before the optional operation can deliver any rows.
//
// Namespace, transaction, composite and other view-changing engines are
// boundaries: the capability must belong to that view, never its inner engine.
// T is a data-read interface, for example ProjectedLabelNodeReader. View identity
// and visibility/revision metadata use ViewMetadataForEngine, independently of
// optional data-read availability. Writes and decorator-owned operations continue
// through the original Engine. Existing concrete view requests are also accepted.
func ReadCapabilityForEngine[T any](engine any) (T, bool) {
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
	capability, supported := owner.(T)
	return capability, supported
}

// MetadataCapabilityForEngine finds the outermost owner of read-only wrapper
// metadata, such as its WAL or query-limit controller. An absent capability may
// pass a declared transparent layer, but never a namespace/transaction boundary.
// Data reads use ReadCapabilityForEngine, and mutations use OwnedCapability.
func MetadataCapabilityForEngine[T any](engine any) (T, bool) {
	return engineCapability[T](engine, capabilityMetadataOwner, nil)
}

type capabilityPolicy uint8

const (
	capabilityReadView capabilityPolicy = iota
	capabilityViewMetadata
	capabilityMetadataOwner
	capabilityIdentityOwner
)

// engineCapability is the single discovery cursor. Data reads use the declared
// read view; identity lookup follows physical wrappers until its predicate finds
// the owning namespace. Exact operations never leave their original owner.
func engineCapability[T any](owner any, policy capabilityPolicy, accept func(T) bool) (T, bool) {
	var zero T
	viewOwner := policy == capabilityReadView || policy == capabilityViewMetadata
	var request reflect.Type
	checkAvailability := false
	if policy == capabilityReadView {
		request = reflect.TypeFor[T]()
		// Binding an Engine or discovering a concrete view identity is not
		// selecting an optional read. In particular, executor setup/cloning
		// must not perform availability lookups for literal statements.
		checkAvailability = request != reflect.TypeFor[Engine]() && request.Kind() == reflect.Interface && !reflect.TypeFor[Engine]().Implements(request)
	}
	for {
		var inner Engine
		transparent := false
		if policy != capabilityIdentityOwner {
			if source, declared := OwnedCapability[ReadCapabilitySource](owner); declared {
				inner, transparent = source.ReadCapabilitySource()
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
			if supported && (accept == nil || accept(capability)) {
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
		if inner == nil || inner == owner {
			return zero, false
		}
		owner = inner
	}
}

func (t *TracedEngine) ReadCapabilitySource() (Engine, bool)     { return t.GetInnerEngine(), true }
func (w *WALEngine) ReadCapabilitySource() (Engine, bool)        { return w.GetInnerEngine(), true }
func (n *NamespacedEngine) ReadCapabilitySource() (Engine, bool) { return nil, false }
func (c *CompositeEngine) ReadCapabilitySource() (Engine, bool)  { return nil, false }
