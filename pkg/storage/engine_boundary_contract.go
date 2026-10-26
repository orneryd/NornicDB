package storage

// EngineBoundaryKind classifies every Engine implementation, independently of
// whether it opted into optional-read forwarding.
type EngineBoundaryKind uint8

const (
	EngineLeaf EngineBoundaryKind = iota + 1
	EngineDecorator
	EngineView
	EngineContainer
)

type BindingDisposition uint8

const (
	BindingOwned BindingDisposition = iota + 1
	BindingCarried
	BindingDelegated
	BindingUnowned
	BindingRefused
)

// EngineBoundaryPolicy declares logical obligations that method sets cannot
// infer. Every field must have an explicit disposition. FullRead describes the
// intentional materialized/export API, not permission to use it as fallback.
type EngineBoundaryPolicy struct {
	Kind                                                                  EngineBoundaryKind
	ReadState, Membership, Controller, PendingActions, SchemaPublication  BindingDisposition
	ReadLease, StatementMode, Identity, Enforcement, CompoundDependencies BindingDisposition
	SameVersionFallback, Delivery, ExactEffects, FullRead                 BindingDisposition
	ObservedNode, ObservedEdge                                            BindingDisposition
}
type EngineBoundaryDeclaration interface{ EngineCapabilityPolicy() EngineBoundaryPolicy }

func (*BadgerEngine) EngineCapabilityPolicy() EngineBoundaryPolicy {
	return EngineBoundaryPolicy{Kind: EngineLeaf, ReadState: BindingOwned, Membership: BindingUnowned, Controller: BindingOwned, PendingActions: BindingOwned, SchemaPublication: BindingOwned, ReadLease: BindingOwned, StatementMode: BindingUnowned, Identity: BindingOwned, Enforcement: BindingUnowned, CompoundDependencies: BindingOwned, SameVersionFallback: BindingOwned, Delivery: BindingOwned, ExactEffects: BindingOwned, FullRead: BindingOwned, ObservedNode: BindingRefused, ObservedEdge: BindingRefused}
}
func (*MemoryEngine) EngineCapabilityPolicy() EngineBoundaryPolicy {
	return EngineBoundaryPolicy{Kind: EngineLeaf, ReadState: BindingOwned, Membership: BindingUnowned, Controller: BindingOwned, PendingActions: BindingOwned, SchemaPublication: BindingOwned, ReadLease: BindingOwned, StatementMode: BindingUnowned, Identity: BindingOwned, Enforcement: BindingUnowned, CompoundDependencies: BindingOwned, SameVersionFallback: BindingOwned, Delivery: BindingOwned, ExactEffects: BindingOwned, FullRead: BindingOwned, ObservedNode: BindingRefused, ObservedEdge: BindingRefused}
}
func (*RemoteEngine) EngineCapabilityPolicy() EngineBoundaryPolicy {
	return EngineBoundaryPolicy{Kind: EngineLeaf, ReadState: BindingOwned, Membership: BindingCarried, Controller: BindingCarried, PendingActions: BindingCarried, SchemaPublication: BindingCarried, ReadLease: BindingCarried, StatementMode: BindingCarried, Identity: BindingOwned, Enforcement: BindingCarried, CompoundDependencies: BindingOwned, SameVersionFallback: BindingRefused, Delivery: BindingOwned, ExactEffects: BindingOwned, FullRead: BindingOwned, ObservedNode: BindingRefused, ObservedEdge: BindingRefused}
}
func (*TracedEngine) EngineCapabilityPolicy() EngineBoundaryPolicy {
	return EngineBoundaryPolicy{Kind: EngineDecorator, ReadState: BindingDelegated, Membership: BindingDelegated, Controller: BindingDelegated, PendingActions: BindingDelegated, SchemaPublication: BindingDelegated, ReadLease: BindingDelegated, StatementMode: BindingDelegated, Identity: BindingDelegated, Enforcement: BindingDelegated, CompoundDependencies: BindingDelegated, SameVersionFallback: BindingDelegated, Delivery: BindingDelegated, ExactEffects: BindingOwned, FullRead: BindingOwned, ObservedNode: BindingRefused, ObservedEdge: BindingRefused}
}
func (*WALEngine) EngineCapabilityPolicy() EngineBoundaryPolicy {
	return EngineBoundaryPolicy{Kind: EngineDecorator, ReadState: BindingDelegated, Membership: BindingDelegated, Controller: BindingDelegated, PendingActions: BindingDelegated, SchemaPublication: BindingDelegated, ReadLease: BindingDelegated, StatementMode: BindingDelegated, Identity: BindingDelegated, Enforcement: BindingOwned, CompoundDependencies: BindingDelegated, SameVersionFallback: BindingDelegated, Delivery: BindingDelegated, ExactEffects: BindingOwned, FullRead: BindingOwned, ObservedNode: BindingRefused, ObservedEdge: BindingRefused}
}
func (*NamespacedEngine) EngineCapabilityPolicy() EngineBoundaryPolicy {
	return EngineBoundaryPolicy{Kind: EngineView, ReadState: BindingOwned, Membership: BindingCarried, Controller: BindingDelegated, PendingActions: BindingCarried, SchemaPublication: BindingCarried, ReadLease: BindingCarried, StatementMode: BindingCarried, Identity: BindingOwned, Enforcement: BindingCarried, CompoundDependencies: BindingOwned, SameVersionFallback: BindingOwned, Delivery: BindingOwned, ExactEffects: BindingOwned, FullRead: BindingOwned, ObservedNode: BindingRefused, ObservedEdge: BindingRefused}
}
func (*CompositeEngine) EngineCapabilityPolicy() EngineBoundaryPolicy {
	return EngineBoundaryPolicy{Kind: EngineContainer, ReadState: BindingOwned, Membership: BindingCarried, Controller: BindingCarried, PendingActions: BindingCarried, SchemaPublication: BindingCarried, ReadLease: BindingCarried, StatementMode: BindingCarried, Identity: BindingOwned, Enforcement: BindingCarried, CompoundDependencies: BindingOwned, SameVersionFallback: BindingOwned, Delivery: BindingOwned, ExactEffects: BindingOwned, FullRead: BindingOwned, ObservedNode: BindingRefused, ObservedEdge: BindingRefused}
}
