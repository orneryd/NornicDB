package capturecensus

// Context-owner and envelope-metadata convergence author: Claude (storage-format-owners lane), #1693, 30 October 2026.

import (
	_ "embed"
	"encoding/json"

	"github.com/orneryd/nornicdb/internal/workcensus"
)

//go:embed storage_distinct.json
var storageDecisions []byte

//go:embed storage_cross_unit_owners.json
var storageCrossUnitOwners []byte

//go:embed storage_nil_dispositions.json
var storageNilDispositions []byte

//go:embed storage_contract_scope.json
var storageContractScope []byte

// StoragePolicy is the native storage client's contract. Live entities and
// selectors consume the capture owner; explicit logical versions retain the
// release MVCC owner, heads retain their metadata owner, degrees retain the
// scalar prefix counter. Publication roots are inventoried alongside readers.
// No reader function names participate in discovering membership.
func StoragePolicy() Policy {
	const pkg = "github.com/orneryd/nornicdb/pkg/storage"
	method := func(name string) string { return "(*" + pkg + ".BadgerEngine)." + name }
	var reviewed struct {
		Decisions map[string]Disposition `json:"decisions"`
	}
	if err := json.Unmarshal(storageDecisions, &reviewed); err != nil {
		panic(err)
	}
	var cross struct {
		Owners map[string]CrossUnitOwner `json:"owners"`
	}
	if err := json.Unmarshal(storageCrossUnitOwners, &cross); err != nil {
		panic(err)
	}
	var scope struct {
		OutOfContract    map[string]ScopeEntry        `json:"outOfContract"`
		ExcludedPackages map[string]ScopeEntry        `json:"excludedPackages"`
		TrackedDefects   map[string]ScopeEntry        `json:"trackedDefects"`
		DeferredPublic   ScopeEntry                   `json:"deferredPublicAPI"`
		PublicPackages   []string                     `json:"publicPackages"`
		PublicEntries    []string                     `json:"publicEntries"`
		ReviewedHeld     map[string]ReviewedHeldValue `json:"reviewedHeldValues"`
	}
	if err := json.Unmarshal(storageContractScope, &scope); err != nil {
		panic(err)
	}
	var nilDispositions struct {
		Dispositions map[string]NilDisposition `json:"dispositions"`
		Detached     map[string]NilDisposition `json:"detached"`
	}
	if err := json.Unmarshal(storageNilDispositions, &nilDispositions); err != nil {
		panic(err)
	}
	return Policy{
		Package: pkg, Native: "BadgerEngine",
		Entities: []string{"Node", "Edge"}, IDs: []string{"NodeID", "EdgeID"}, Views: []string{"ReadSnapshotView"},
		Capture:                []string{method("captureReadView")},
		Historical:             []string{method("beginMVCCSnapshotRead")},
		Head:                   []string{method("withView")},
		Scalar:                 []string{method("countByPrefix")},
		Publication:            []string{method("publishNodeOutcome"), method("dispatchNodeUpdatedOutcome"), pkg + ".commitEmbeddingOutcome", "(" + pkg + ".embeddingPublicationProof).matches", "(*" + pkg + ".commitWriter).commitOutcome"},
		BorrowedOptions:        []string{"readViewOptions"},
		CrossUnitOwners:        cross.Owners,
		OutOfContract:          scope.OutOfContract,
		ExcludedPackages:       scope.ExcludedPackages,
		TrackedDefects:         scope.TrackedDefects,
		DeferredPublicAPI:      scope.DeferredPublic,
		PublicPackages:         scope.PublicPackages,
		PublicEntries:          scope.PublicEntries,
		ReviewedHeldValues:     scope.ReviewedHeld,
		NilDetached:            nilDispositions.Detached,
		NilContextDispositions: nilDispositions.Dispositions,
		ContextSources: []ContextSource{{
			Source: pkg + ".commitNotificationContext", Root: workcensus.Root{Package: pkg, Name: "drainCommitNotifications", Receiver: "BadgerEngine", Required: true}, Disposition: "drainer-owned",
			Reason: "the ordered drainer publishes queued notifications of any commit, not only its caller's; publication is not cancellable by contract",
		}, {
			Source: pkg + ".pendingIndexContext", Root: workcensus.Root{Package: pkg, Name: "trackPendingValues", Receiver: "WriteBehindBuffer", Required: true}, Disposition: "maintenance-owned",
			Reason: "the buffer's shared pending value index serves every request; a request's cancellation would leave it partial for later readers",
		}, {
			Source: pkg + ".writeOrderContext", Root: workcensus.Root{Package: pkg, Name: "holdsEntities", Receiver: "WriteBehindBuffer", Required: true}, Disposition: "write-order-owned",
			Reason: "write ordering is decided on shared buffer state; a request's cancellation would report the entities as not held and misorder the write",
		}, {
			Source: pkg + ".namespaceMaintenanceContext", Root: workcensus.Root{Package: pkg, Name: "ReconcileDecaySuppression", Receiver: "BadgerEngine", Required: true}, Disposition: "namespace-maintenance-owned",
			Reason: "a namespace's first schema use reconciles decay suppression, shared state later readers depend on; a request's cancellation would leave it half done",
		}},
		ContextOwners: []string{method("captureReadView"), method("pendingReadConflict"), method("readBufferedEntities"), "(*" + pkg + ".WriteBehindBuffer).visitEntityOverlayLocked"},
		ExtraResults:  []TypeName{{"github.com/orneryd/nornicdb/pkg/knowledgepolicy", "EntityMeta"}},
		Distinct:      reviewed.Decisions,
	}
}
