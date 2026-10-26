package cypher

import (
	"reflect"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// These engines own a read view: transaction state, filtered relationships or
// the system graph. Capability discovery must never delegate past that view.
func (w *transactionStorageWrapper) ReadCapabilitySource() (storage.Engine, bool) { return nil, false }
func (r *algorithmRelationshipReader) ReadCapabilitySource() (storage.Engine, bool) {
	return nil, false
}
func (systemGraphView) ReadCapabilitySource() (storage.Engine, bool) { return nil, false }

func (w *transactionStorageWrapper) ReadCapabilityScoped(request reflect.Type) bool {
	return transactionStorageWrapperReadPolicy.Scoped(request)
}
func (r *algorithmRelationshipReader) ReadCapabilityScoped(request reflect.Type) bool {
	return algorithmRelationshipReaderReadPolicy.Scoped(request)
}
func (systemGraphView) ReadCapabilityScoped(request reflect.Type) bool {
	return systemGraphViewReadPolicy.Scoped(request)
}
func (*graphIdentityStorage) ReadCapabilitySource() (storage.Engine, bool) { return nil, false }
func (*graphIdentityStorage) ReadCapabilityScoped(request reflect.Type) bool {
	return graphIdentityStorageReadPolicy.Scoped(request)
}

// ReadCapabilityAvailable checks this view's conditional forwards while
// retaining the transaction's namespace conversion and pending state.
func (w *transactionStorageWrapper) ReadCapabilityAvailable(request reflect.Type) bool {
	if storage.ReadCapabilityIncludes[storage.PropertyKeyLookup](request) {
		direct := storage.HasReadCapability[storage.PropertyKeyLookup](w.underlying)
		scoped := storage.HasReadCapability[storage.NamespacePropertyKeyLookup](w.underlying)
		if !direct && !scoped {
			return false
		}
	}
	if storage.ReadCapabilityIncludes[interface {
		GetNodesByLabelVisibleAt(string, storage.MVCCVersion) ([]*storage.Node, error)
	}](request) && !storage.HasReadCapability[interface {
		GetNodesByLabelVisibleAt(string, storage.MVCCVersion) ([]*storage.Node, error)
	}](w.underlying) {
		return false
	}
	if storage.ReadCapabilityIncludes[interface {
		GetOutgoingEdgesVisibleAt(storage.NodeID, storage.MVCCVersion) ([]*storage.Edge, error)
	}](request) && !storage.HasReadCapability[interface {
		GetOutgoingEdgesVisibleAt(storage.NodeID, storage.MVCCVersion) ([]*storage.Edge, error)
	}](w.underlying) {
		return false
	}
	if storage.ReadCapabilityIncludes[interface {
		GetIncomingEdgesVisibleAt(storage.NodeID, storage.MVCCVersion) ([]*storage.Edge, error)
	}](request) && !storage.HasReadCapability[interface {
		GetIncomingEdgesVisibleAt(storage.NodeID, storage.MVCCVersion) ([]*storage.Edge, error)
	}](w.underlying) {
		return false
	}
	if storage.ReadCapabilityIncludes[interface {
		GetEdgesByTypeVisibleAt(string, storage.MVCCVersion) ([]*storage.Edge, error)
	}](request) && !storage.HasReadCapability[interface {
		GetEdgesByTypeVisibleAt(string, storage.MVCCVersion) ([]*storage.Edge, error)
	}](w.underlying) {
		return false
	}
	if storage.ReadCapabilityIncludes[interface {
		GetEdgesBetweenVisibleAt(storage.NodeID, storage.NodeID, storage.MVCCVersion) ([]*storage.Edge, error)
	}](request) && !storage.HasReadCapability[interface {
		GetEdgesBetweenVisibleAt(storage.NodeID, storage.NodeID, storage.MVCCVersion) ([]*storage.Edge, error)
	}](w.underlying) {
		return false
	}

	return true
}

func (w *transactionStorageWrapper) IsDecayEnabled() bool {
	policy, ok := storage.ViewMetadataForEngine[interface{ IsDecayEnabled() bool }](w.underlying)
	return ok && policy.IsDecayEnabled()
}

func (w *transactionStorageWrapper) HasManualSuppressions() bool {
	policy, ok := storage.ViewMetadataForEngine[interface{ HasManualSuppressions() bool }](w.underlying)
	return ok && policy.HasManualSuppressions()
}

// BindReadCapabilityGuard compiles structural dependencies at issuance. Only
// actual provider values are consulted by the retained operation afterwards.
func (w *transactionStorageWrapper) BindReadCapabilityGuard(request reflect.Type) func() bool {
	var guards []func() bool
	if storage.ReadCapabilityIncludes[storage.PropertyKeyLookup](request) {
		a := storage.BindReadOperation[storage.PropertyKeyLookup](w.underlying)
		b := storage.BindReadOperation[storage.NamespacePropertyKeyLookup](w.underlying)
		guards = append(guards, func() bool { return a.Available() || b.Available() })
	}
	if storage.ReadCapabilityIncludes[interface {
		GetNodesByLabelVisibleAt(string, storage.MVCCVersion) ([]*storage.Node, error)
	}](request) {
		op := storage.BindReadOperation[interface {
			GetNodesByLabelVisibleAt(string, storage.MVCCVersion) ([]*storage.Node, error)
		}](w.underlying)
		guards = append(guards, op.Available)
	}
	if storage.ReadCapabilityIncludes[interface {
		GetOutgoingEdgesVisibleAt(storage.NodeID, storage.MVCCVersion) ([]*storage.Edge, error)
	}](request) {
		op := storage.BindReadOperation[interface {
			GetOutgoingEdgesVisibleAt(storage.NodeID, storage.MVCCVersion) ([]*storage.Edge, error)
		}](w.underlying)
		guards = append(guards, op.Available)
	}
	if storage.ReadCapabilityIncludes[interface {
		GetIncomingEdgesVisibleAt(storage.NodeID, storage.MVCCVersion) ([]*storage.Edge, error)
	}](request) {
		op := storage.BindReadOperation[interface {
			GetIncomingEdgesVisibleAt(storage.NodeID, storage.MVCCVersion) ([]*storage.Edge, error)
		}](w.underlying)
		guards = append(guards, op.Available)
	}
	if storage.ReadCapabilityIncludes[interface {
		GetEdgesByTypeVisibleAt(string, storage.MVCCVersion) ([]*storage.Edge, error)
	}](request) {
		op := storage.BindReadOperation[interface {
			GetEdgesByTypeVisibleAt(string, storage.MVCCVersion) ([]*storage.Edge, error)
		}](w.underlying)
		guards = append(guards, op.Available)
	}
	if storage.ReadCapabilityIncludes[interface {
		GetEdgesBetweenVisibleAt(storage.NodeID, storage.NodeID, storage.MVCCVersion) ([]*storage.Edge, error)
	}](request) {
		op := storage.BindReadOperation[interface {
			GetEdgesBetweenVisibleAt(storage.NodeID, storage.NodeID, storage.MVCCVersion) ([]*storage.Edge, error)
		}](w.underlying)
		guards = append(guards, op.Available)
	}
	if len(guards) == 0 {
		return nil
	}
	return func() bool {
		for _, guard := range guards {
			if !guard() {
				return false
			}
		}
		return true
	}
}
