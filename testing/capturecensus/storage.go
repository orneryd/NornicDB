package capturecensus

// Context-owner and envelope-metadata convergence author: Claude (storage-format-owners lane), #1693, 30 October 2026.

import (
	_ "embed"
	"encoding/json"
)

//go:embed storage_distinct.json
var storageDecisions []byte

//go:embed storage_cross_unit_owners.json
var storageCrossUnitOwners []byte

//go:embed storage_nil_dispositions.json
var storageNilDispositions []byte

//go:embed storage_non_nil_results.json
var storageNonNilResults []byte

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
	var nonNilResults map[string]ReviewedNonNilResult
	if err := json.Unmarshal(storageNonNilResults, &nonNilResults); err != nil {
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
		NonNilResults:          nonNilResults,
		NilDetached:            nilDispositions.Detached,
		NilContextDispositions: nilDispositions.Dispositions,
		// The four former helper sources no longer exist. Their source-pinned
		// tombstones are in storage_reconciliation.json. Fresh contexts in
		// their former roots remain subject to the ordinary request census;
		// retirement grants no owner-lifetime exception.
		ContextOwners: []string{method("captureReadView"), method("pendingReadConflict"), method("readBufferedEntities"), "(*" + pkg + ".WriteBehindBuffer).visitEntityOverlayLocked"},
		ExtraResults:  []TypeName{{"github.com/orneryd/nornicdb/pkg/knowledgepolicy", "EntityMeta"}},
		Distinct:      reviewed.Decisions,
	}
}
