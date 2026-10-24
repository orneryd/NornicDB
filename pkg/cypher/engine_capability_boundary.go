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

// ReadCapabilityAvailable checks this view's conditional forwards while
// retaining the transaction's namespace conversion and pending state.
func (w *transactionStorageWrapper) ReadCapabilityAvailable(request reflect.Type) bool {
	if storage.ReadCapabilityIncludes[storage.PropertyKeyLookup](request) {
		_, direct := storage.ReadCapabilityForEngine[storage.PropertyKeyLookup](w.underlying)
		_, scoped := storage.ReadCapabilityForEngine[storage.NamespacePropertyKeyLookup](w.underlying)
		if !direct && !scoped {
			return false
		}
	}
	if storage.ReadCapabilityIncludes[interface {
		GetNodesByLabelVisibleAt(string, storage.MVCCVersion) ([]*storage.Node, error)
	}](request) ||
		storage.ReadCapabilityIncludes[interface {
			GetOutgoingEdgesVisibleAt(storage.NodeID, storage.MVCCVersion) ([]*storage.Edge, error)
		}](request) ||
		storage.ReadCapabilityIncludes[interface {
			GetIncomingEdgesVisibleAt(storage.NodeID, storage.MVCCVersion) ([]*storage.Edge, error)
		}](request) ||
		storage.ReadCapabilityIncludes[interface {
			GetEdgesByTypeVisibleAt(string, storage.MVCCVersion) ([]*storage.Edge, error)
		}](request) ||
		storage.ReadCapabilityIncludes[interface {
			GetEdgesBetweenVisibleAt(storage.NodeID, storage.NodeID, storage.MVCCVersion) ([]*storage.Edge, error)
		}](request) {
		_, available := storage.ReadCapabilityForEngine[storage.MVCCIndexedVisibilityEngine](w.underlying)
		if !available {
			return false
		}
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
