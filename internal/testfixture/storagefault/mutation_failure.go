package storagefault

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// MutationOperation selects the engine call a MutationFailureEngine fails. The
// empty operation injects nothing, so the same real engine can stage writes
// and serve rollback verification.
type MutationOperation string

const (
	MutationCreate MutationOperation = "create"
	MutationUpdate MutationOperation = "update"
	MutationDelete MutationOperation = "delete"
	MutationScan   MutationOperation = "scan"
	MutationGet    MutationOperation = "get"
	MutationList   MutationOperation = "list"
)

// MutationFailureEngine decorates a real engine and fails exactly the selected
// operation with the supplied error; every other call reaches that engine
// unchanged, so earlier staged writes are already in the same real
// transaction when the selected failure is injected.
//
// Nth counts calls of the selected operation only: 0 fails every call of it,
// n fails its nth call. Failure, Nth and ID are plain fields so a test can
// change the selection between calls; set MutationFailureEngine.ID to match
// one node ID (empty matches any).
//
// RejectUpdate optionally fails an update whose after-image it rejects. It is
// independent of Operation, ID and Nth: a rejected update fails the call
// before the operation selection is counted, so an update that is both
// rejected and selected still consumes no selection count.
type MutationFailureEngine struct {
	storage.Engine
	Operation    MutationOperation
	ID           storage.NodeID
	Nth          int
	Failure      error
	RejectUpdate func(*storage.Node) bool
	calls        int
	run          func(context.Context, storage.Engine, func(storage.Engine) error) error
}

// NewMutationFailureEngine decorates an existing engine. The caller retains
// ownership of the real engine's lifetime. Pass the canonical transaction
// owner (auth.WithStoreTransaction) explicitly; this fixture supplies no
// alternate transaction implementation or storage fallback.
func NewMutationFailureEngine(engine storage.Engine, operation MutationOperation, failure error, nth int, run func(context.Context, storage.Engine, func(storage.Engine) error) error) *MutationFailureEngine {
	return &MutationFailureEngine{Engine: engine, Operation: operation, Failure: failure, Nth: nth, run: run}
}

// Select replaces the injected failure and the calls of it to fail, keeping
// the call count, so one decorator serves a test's successive selections.
func (e *MutationFailureEngine) Select(operation MutationOperation, id storage.NodeID, nth int) {
	e.Operation, e.ID, e.Nth = operation, id, nth
}

// Fails reports whether the current call is the selected one: the operation
// matches, the ID matches when one is set, and it is the nth such call (0
// means every call). It counts only the calls it is asked about.
func (e *MutationFailureEngine) Fails(operation MutationOperation, id storage.NodeID) bool {
	if operation != e.Operation || (e.ID != "" && e.ID != id) {
		return false
	}
	e.calls++
	return e.Nth == 0 || e.calls == e.Nth
}

// WithStoreTransaction keeps the decorated engine's real transaction and
// hands the staged view to the same decorator, so the selected operation fails
// inside that transaction after the operations before it were staged.
func (e *MutationFailureEngine) WithStoreTransaction(ctx context.Context, mutate func(storage.Engine) error) error {
	return e.run(ctx, e.Engine, func(view storage.Engine) error {
		staged := *e
		staged.Engine = view
		return mutate(&staged)
	})
}

func (e *MutationFailureEngine) CreateNode(node *storage.Node) (storage.NodeID, error) {
	if e.Fails(MutationCreate, node.ID) {
		return "", e.Failure
	}
	return e.Engine.CreateNode(node)
}

func (e *MutationFailureEngine) UpdateNode(updateRequest storage.NodeUpdateRequest) error {
	id := storage.NodeID("")
	if updateRequest.After != nil {
		id = updateRequest.After.ID
	}
	// An update the caller rejects fails before the selection is counted, so
	// the rejection never consumes a selected call.
	if e.RejectUpdate != nil && e.RejectUpdate(updateRequest.After) {
		return e.Failure
	}
	if e.Fails(MutationUpdate, id) {
		return e.Failure
	}
	return e.Engine.UpdateNode(updateRequest)
}

func (e *MutationFailureEngine) DeleteNode(id storage.NodeID) error {
	if e.Fails(MutationDelete, id) {
		return e.Failure
	}
	return e.Engine.DeleteNode(id)
}

func (e *MutationFailureEngine) GetNode(id storage.NodeID) (*storage.Node, error) {
	if e.Fails(MutationGet, id) {
		return nil, e.Failure
	}
	return e.Engine.GetNode(id)
}

func (e *MutationFailureEngine) GetNodesByLabel(label string) ([]*storage.Node, error) {
	if e.Fails(MutationScan, "") {
		return nil, e.Failure
	}
	return e.Engine.GetNodesByLabel(label)
}

func (e *MutationFailureEngine) AllNodes() ([]*storage.Node, error) {
	if e.Fails(MutationList, "") {
		return nil, e.Failure
	}
	return e.Engine.AllNodes()
}
