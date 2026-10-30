package multidb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Probe storage retains the canonical native preparation and receipt owner.
// Faults enter at the authoritative submitter. APIs receive the actual operation
// context; acceptance and terminal settlement use the existing storage receipt.
type f25CatalogReceiptProbe struct {
	storage.Engine
	native                    *storage.BadgerEngine
	mode                      string
	faultAfter, metadataCalls int
	ownerContext              context.Context
	prepared                  *storage.PreparedEntityCommit
	receipt                   *storage.CommitReceipt
	receiptOwner              storage.CommitReceiptOwner
	submitted                 chan struct{}
	beforeSubmit              func()
	closeStarted              chan struct{}
}

func (e *f25CatalogReceiptProbe) GetInnerEngine() storage.Engine { return e.Engine }
func (e *f25CatalogReceiptProbe) Close() error {
	if e.closeStarted != nil {
		close(e.closeStarted)
	}
	return e.Engine.Close()
}

func (e *f25CatalogReceiptProbe) SubmitEntityCommit(ctx context.Context, prepared *storage.PreparedEntityCommit, settle func(storage.CommitOutcome)) (*storage.CommitReceipt, error) {
	if prepared.Namespace() != "system" || e.mode == "" {
		return e.native.SubmitEntityCommit(ctx, prepared, settle)
	}
	e.metadataCalls++
	if e.faultAfter > 0 && e.metadataCalls < e.faultAfter {
		return e.native.SubmitEntityCommit(ctx, prepared, settle)
	}
	if e.mode == "rejected" {
		return nil, storage.ErrConflict
	}
	if e.beforeSubmit != nil {
		e.beforeSubmit()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if e.mode == "preaccept-cancelled" {
		// The request ended, but this submission runs under the owner's
		// lifetime, so the real engine takes it.
		return e.native.SubmitEntityCommit(ctx, prepared, settle)
	}
	e.prepared = prepared
	e.receipt, e.receiptOwner = storage.NewCommitReceipt(settle)
	// The test may change mode once submitted is closed; this submission keeps
	// the mode it was admitted under.
	mode := e.mode
	if e.submitted != nil {
		close(e.submitted)
	}
	if mode == "applied-ack-error" {
		if err := storagefault.FinishPreparedReceipt(e.prepared, e.receiptOwner, storage.CommitOutcome{State: storage.CommitApplied, Err: storage.ErrConflict}); err != nil {
			return nil, err
		}
	} else if mode != "accepted-wait" {
		if err := e.receiptOwner.Resolve(storage.CommitOutcome{State: storage.CommitRecoveryPending, Err: storage.ErrConflict}); err != nil {
			return nil, err
		}
	}
	return e.receipt, nil
}

func TestF25CompositeCatalogPublicationPrototype(t *testing.T) {
	for _, operation := range []string{"create", "add", "remove", "drop"} {
		for _, phase := range []string{"rejected", "preaccept-cancelled", "applied-ack-error", "pending-applied", "pending-discarded"} {
			t.Run(operation+"/"+phase, func(t *testing.T) {
				dir := filepath.Join(t.TempDir(), "store")
				base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dir, LowMemory: true}))
				require.NoError(t, err)
				fixtureowner.Own(t, base)
				backing := &f25CatalogReceiptProbe{Engine: base, native: base, ownerContext: context.Background()}
				manager, err := NewDatabaseManager(context.Background(), backing, nil)
				require.NoError(t, err)
				t.Cleanup(func() { _ = manager.Close() })
				for _, name := range []string{"firstdb", "lastdb"} {
					require.NoError(t, manager.CreateDatabase(backing.ownerContext, DatabaseCreateRequest{Name: name}))
				}
				refs := []ConstituentRef{{Alias: "Winner", Type: "local", AccessMode: "read_write", DatabaseName: "firstdb"}}
				if operation == "remove" {
					refs = append(refs, ConstituentRef{Alias: "winner", Type: "local", AccessMode: "read_write", DatabaseName: "lastdb"})
				}
				if operation != "create" {
					require.NoError(t, manager.CreateCompositeDatabase(backing.ownerContext, "probe", refs))
				}
				var oldScope *StatementCacheScope
				if operation != "create" {
					oldScope, err = manager.StatementCacheScope("probe.WINNER")
					require.NoError(t, err)
				}
				beforeID, beforeCreated := manager.ServerIdentity()
				call := func() error {
					switch operation {
					case "create":
						return manager.CreateCompositeDatabase(backing.ownerContext, "probe", refs)
					case "add":
						return manager.AddConstituent(backing.ownerContext, "probe", ConstituentRef{Alias: "winner", Type: "local", AccessMode: "read_write", DatabaseName: "lastdb"})
					case "remove":
						return manager.RemoveConstituent(backing.ownerContext, "probe", "WINNER")
					default:
						return manager.DropCompositeDatabase(backing.ownerContext, "probe")
					}
				}
				backing.mode = phase
				if phase == "preaccept-cancelled" {
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					backing.ownerContext = ctx
				}
				err = call()
				require.Error(t, err)
				if phase == "preaccept-cancelled" {
					require.ErrorIs(t, err, context.Canceled)
				} else if phase == "rejected" {
					require.ErrorIs(t, err, storage.ErrConflict)
				} else {
					// Author: NOR-288 lane (Codex), 2026-10-27. Accepted
					// diagnostics cannot classify as a fresh retryable conflict.
					var accepted *storage.AcceptedCommitError
					require.ErrorAs(t, err, &accepted)
					requireCatalogAcceptedDiagnostic(t, accepted, storage.ErrConflict)
				}
				pending := phase == "pending-applied" || phase == "pending-discarded"
				applied := phase == "applied-ack-error"
				inspect := func(label string, m *DatabaseManager, wantApplied bool) {
					t.Helper()
					wantExists := operation != "create"
					if wantApplied {
						wantExists = operation != "drop"
					}
					assert.Equal(t, wantExists, m.Exists("probe"), label+" catalog existence")
					if !wantExists {
						return
					}
					got, readErr := m.GetCompositeConstituents("probe")
					if !assert.NoError(t, readErr, label) {
						return
					}
					wantOwner := "firstdb"
					if operation == "remove" && !wantApplied || operation == "add" && wantApplied {
						wantOwner = "lastdb"
					}
					owner, ownerErr := m.ResolveConnectionLimitOwner("probe.WINNER")
					assert.NoError(t, ownerErr, label+" owner")
					assert.Equal(t, wantOwner, owner, label+" physical owner")
					if assert.Len(t, got, 1, label+" canonical references") {
						assert.Equal(t, wantOwner, got[0].DatabaseName, label+" constituent")
					}
					view, viewErr := m.GetStorage("probe.WINNER")
					assert.NoError(t, viewErr, label+" storage route")
					if viewErr == nil {
						assert.NotNil(t, view)
					}
				}
				inspect("live after return", manager, applied)
				if oldScope != nil {
					assert.Equal(t, !applied, oldScope.Valid(), "cache token follows physical publication, not return error alone")
				}
				afterID, afterCreated := manager.ServerIdentity()
				assert.Equal(t, beforeID, afterID)
				assert.Equal(t, beforeCreated, afterCreated)
				if pending {
					state := storage.CommitDiscarded
					if phase == "pending-applied" {
						state = storage.CommitApplied
					}
					require.NoError(t, storagefault.FinishPreparedReceipt(backing.prepared, backing.receiptOwner, storage.CommitOutcome{State: state, Err: storage.ErrConflict}))
					applied = state == storage.CommitApplied
					inspect("live after terminal receipt", manager, applied)
					if oldScope != nil {
						assert.Equal(t, !applied, oldScope.Valid(), "receipt terminal outcome owns cache publication")
					}
				}
				backing.mode = ""
				require.NoError(t, manager.Close())
				require.NoError(t, base.Close())
				reopened, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dir, LowMemory: true}))
				require.NoError(t, err)
				fixtureowner.Own(t, reopened)
				reloaded, err := NewDatabaseManager(context.Background(), reopened, nil)
				require.NoError(t, err)
				t.Cleanup(func() { _ = reloaded.Close() })
				inspect(fmt.Sprintf("reopened applied=%v", applied), reloaded, applied)
				reloadID, reloadCreated := reloaded.ServerIdentity()
				assert.Equal(t, beforeID, reloadID)
				assert.Equal(t, beforeCreated, reloadCreated)
			})
		}
	}
}

func TestF25OtherCatalogPublicationPrototype(t *testing.T) {
	for _, operation := range []string{"create-database", "create-alias", "drop-alias", "status", "limits", "option"} {
		for _, phase := range []string{"rejected", "preaccept-cancelled", "applied-ack-error", "pending-applied", "pending-discarded"} {
			t.Run(operation+"/"+phase, func(t *testing.T) {
				dir := filepath.Join(t.TempDir(), "store")
				base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dir, LowMemory: true}))
				require.NoError(t, err)
				fixtureowner.Own(t, base)
				backing := &f25CatalogReceiptProbe{Engine: base, native: base, ownerContext: context.Background()}
				manager, err := NewDatabaseManager(context.Background(), backing, nil)
				require.NoError(t, err)
				t.Cleanup(func() { _ = manager.Close() })
				if operation != "create-database" {
					require.NoError(t, manager.CreateDatabase(backing.ownerContext, DatabaseCreateRequest{Name: "target"}))
					_, err = manager.GetStorage("target")
					require.NoError(t, err)
				}
				var oldScope *StatementCacheScope
				if operation == "drop-alias" {
					require.NoError(t, manager.CreateAlias(backing.ownerContext, "selected", "target"))
					oldScope, err = manager.StatementCacheScope("selected")
					require.NoError(t, err)
				}
				live := func(m *DatabaseManager) any {
					info, err := m.GetDatabase("target")
					if errors.Is(err, ErrDatabaseNotFound) {
						return nil
					}
					require.NoError(t, err)
					return info
				}
				before := live(manager)
				// JSON materialization freezes nested values before any mutation.
				beforeBytes, err := json.Marshal(before)
				require.NoError(t, err)
				durable := func() json.RawMessage {
					node, err := base.GetNode("system:databases:metadata")
					require.NoError(t, err)
					var records map[string]json.RawMessage
					require.NoError(t, json.Unmarshal([]byte(node.Properties["data"].(string)), &records))
					if record, ok := records["target"]; ok {
						return record
					}
					return json.RawMessage("null")
				}
				inspect := func(label string, wantApplied bool, m *DatabaseManager) {
					t.Helper()
					expected := json.RawMessage(beforeBytes)
					if wantApplied {
						expected = durable()
					}
					actual, err := json.Marshal(live(m))
					require.NoError(t, err)
					assert.JSONEq(t, string(expected), string(actual), label+" metadata must follow physical result")
					if operation == "option" {
						want := storage.ChangeCaptureOff
						if wantApplied {
							want = storage.ChangeCaptureFull
						}
						assert.Equal(t, want, base.ChangeCapture("target"), label+" capture runtime")
					}
					if operation == "status" {
						_, err := m.GetStorage("target")
						if wantApplied {
							assert.ErrorIs(t, err, ErrDatabaseOffline, label)
						} else {
							assert.NoError(t, err, label)
						}
					}
					if oldScope != nil && m == manager {
						assert.Equal(t, !wantApplied, oldScope.Valid(), label+" alias cache")
					}
				}
				backing.mode = phase
				if phase == "preaccept-cancelled" {
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					backing.ownerContext = ctx
				}
				switch operation {
				case "create-database":
					err = manager.CreateDatabase(backing.ownerContext, DatabaseCreateRequest{Name: "target"})
				case "create-alias":
					err = manager.CreateAlias(backing.ownerContext, "selected", "target")
				case "drop-alias":
					err = manager.DropAlias(backing.ownerContext, "selected")
				case "status":
					err = manager.SetDatabaseStatus(backing.ownerContext, "target", "offline")
				case "limits":
					err = manager.SetDatabaseLimits(backing.ownerContext, "target", &Limits{Storage: StorageLimits{MaxNodes: 1}})
				default:
					err = manager.SetDatabaseOption(backing.ownerContext, "target", OptionTxLogEnrichment, "FULL")
				}
				require.Error(t, err)
				if phase == "preaccept-cancelled" {
					require.ErrorIs(t, err, context.Canceled)
				} else if phase == "rejected" {
					require.ErrorIs(t, err, storage.ErrConflict)
				} else {
					var accepted *storage.AcceptedCommitError
					require.ErrorAs(t, err, &accepted)
					requireCatalogAcceptedDiagnostic(t, accepted, storage.ErrConflict)
				}
				applied := phase == "applied-ack-error"
				inspect("live return", applied, manager)
				if phase == "pending-applied" || phase == "pending-discarded" {
					state := storage.CommitDiscarded
					if phase == "pending-applied" {
						state = storage.CommitApplied
					}
					require.NoError(t, storagefault.FinishPreparedReceipt(backing.prepared, backing.receiptOwner, storage.CommitOutcome{State: state, Err: storage.ErrConflict}))
					applied = state == storage.CommitApplied
					inspect("live terminal", applied, manager)
				}
				backing.mode = ""
				// Check durable state through an actual reopen, not another live handle.
				expected := json.RawMessage(beforeBytes)
				if applied {
					expected = durable()
				}
				require.NoError(t, manager.Close())
				require.NoError(t, base.Close())
				reopened, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dir, LowMemory: true}))
				require.NoError(t, err)
				fixtureowner.Own(t, reopened)
				reloaded, err := NewDatabaseManager(context.Background(), reopened, nil)
				require.NoError(t, err)
				t.Cleanup(func() { _ = reloaded.Close() })
				actual, err := json.Marshal(live(reloaded))
				require.NoError(t, err)
				assert.JSONEq(t, string(expected), string(actual), "reopened metadata follows physical result")
			})
		}
	}
}

func TestF25StandardDropCompletionPrototype(t *testing.T) {
	for _, phase := range []string{"rejected", "preaccept-cancelled", "applied-ack-error", "pending-applied", "pending-discarded"} {
		t.Run(phase, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "store")
			base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dir, LowMemory: true}))
			require.NoError(t, err)
			fixtureowner.Own(t, base)
			backing := &f25CatalogReceiptProbe{Engine: base, native: base, ownerContext: context.Background()}
			manager, err := NewDatabaseManager(context.Background(), backing, nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = manager.Close() })
			for _, name := range []string{"target", "peer"} {
				require.NoError(t, manager.CreateDatabase(backing.ownerContext, DatabaseCreateRequest{Name: name}))
				view, err := manager.GetStorage(name)
				require.NoError(t, err)
				_, err = view.CreateNode(&storage.Node{ID: "keep"})
				require.NoError(t, err)
			}
			scope, err := manager.StatementCacheScope("target")
			require.NoError(t, err)
			backing.mode = phase
			backing.faultAfter = 2
			if phase == "preaccept-cancelled" {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				backing.ownerContext = ctx
				backing.beforeSubmit = cancel
			}
			err = manager.DropDatabase(backing.ownerContext, "target")
			applied := phase == "applied-ack-error"
			if phase == "preaccept-cancelled" {
				// The request ends just before the final change, after the
				// intent was accepted: the owner still finishes the drop. Its
				// completion holds the DROP admission until it ends.
				requireAcceptedDropOutcome(t, err)
				release, admissionErr := manager.dropAdmission.acquire(context.Background(), true, false)
				require.NoError(t, admissionErr)
				release()
				applied = true
			} else {
				require.Error(t, err)
			}
			inspect := func(label string) {
				assert.Equal(t, !applied, manager.Exists("target"), label+" catalog existence")
				assert.Equal(t, !applied, manager.pendingDrops["target"], label+" pending intent")
				assert.False(t, scope.Valid(), label+" admitted DROP retires graph cache even if completion fails")
				_, err := manager.GetStorage("target")
				require.Error(t, err)
				_, err = base.GetNode("target:keep")
				assert.ErrorIs(t, err, storage.ErrNotFound, label+" completed physical removal cannot be rolled back")
				_, err = base.GetNode("peer:keep")
				assert.NoError(t, err, label+" peer remains")
			}
			inspect("return")
			if phase == "pending-applied" || phase == "pending-discarded" {
				state := storage.CommitDiscarded
				if phase == "pending-applied" {
					state = storage.CommitApplied
				}
				require.NoError(t, storagefault.FinishPreparedReceipt(backing.prepared, backing.receiptOwner, storage.CommitOutcome{State: state, Err: storage.ErrConflict}))
				applied = state == storage.CommitApplied
				inspect("terminal")
			}
			backing.mode = ""
			require.NoError(t, manager.Close())
			require.NoError(t, base.Close())
			reopened, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dir, LowMemory: true}))
			require.NoError(t, err)
			fixtureowner.Own(t, reopened)
			reloaded, err := NewDatabaseManager(context.Background(), reopened, nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = reloaded.Close() })
			assert.False(t, reloaded.Exists("target"), "durable pending intent completes during writable reopen")
			_, err = reopened.GetNode("peer:keep")
			assert.NoError(t, err)
		})
	}
}

// Author: NOR-288 lane (Codex), 2026-10-29. Accepted catalog composition
// can add more than one opaque boundary; inspect diagnostics explicitly while
// preserving the public prohibition on retryable/rejected cause identity.
func catalogAcceptedDiagnosticContains(root error, target error) bool {
	pending := []error{root}
	for len(pending) > 0 {
		err := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if err == nil {
			continue
		}
		if accepted, ok := err.(*storage.AcceptedCommitError); ok {
			pending = append(pending, accepted.Cause())
			continue
		}
		if errors.Is(err, target) {
			return true
		}
		if joined, ok := err.(interface{ Unwrap() []error }); ok {
			pending = append(pending, joined.Unwrap()...)
		} else if cause := errors.Unwrap(err); cause != nil {
			pending = append(pending, cause)
		}
	}
	return false
}

func requireCatalogAcceptedDiagnostic(t *testing.T, accepted *storage.AcceptedCommitError, target error, message ...any) {
	t.Helper()
	require.NotErrorIs(t, accepted, target)
	require.True(t, catalogAcceptedDiagnosticContains(accepted, target), "accepted diagnostic must retain %v: %v", target, message)
}

func TestCatalogAcceptedWaitCancellationRetainsPublicationAuthority(t *testing.T) {
	for _, terminal := range []storage.CommitOutcomeState{storage.CommitApplied, storage.CommitDiscarded} {
		t.Run(fmt.Sprint(terminal), func(t *testing.T) {
			manager, backing := newCatalogPublicationProbe(t)
			t.Cleanup(func() { _ = manager.Close() })
			backing.mode = "accepted-wait"
			backing.submitted = make(chan struct{})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- manager.CreateDatabase(ctx, DatabaseCreateRequest{Name: "accepted"}) }()
			<-backing.submitted
			assert.False(t, manager.Exists("accepted"))
			assert.Zero(t, len(manager.catalogGate), "accepted receipt retains catalog serialization")
			cancel()
			err := <-result
			var accepted *storage.AcceptedCommitError
			require.ErrorAs(t, err, &accepted)
			requireCatalogAcceptedDiagnostic(t, accepted, context.Canceled)
			require.Equal(t, storage.CommitRecoveryPending, accepted.State)
			require.NotEmpty(t, accepted.LogicalID)
			assert.Zero(t, len(manager.catalogGate), "wait cancellation cannot release accepted authority")
			assert.False(t, manager.Exists("accepted"))
			denied, stop := context.WithCancel(t.Context())
			stop()
			require.ErrorIs(t, manager.CreateDatabase(denied, DatabaseCreateRequest{Name: "must-not-appear"}), context.Canceled)
			require.NoError(t, storagefault.FinishPreparedReceipt(backing.prepared, backing.receiptOwner, storage.CommitOutcome{State: terminal, Err: storage.ErrConflict}))
			assert.Equal(t, terminal == storage.CommitApplied, manager.Exists("accepted"))
			assert.Equal(t, 1, len(manager.catalogGate), "only terminal settlement releases serialization")
			backing.mode = ""
			backing.submitted = nil
			require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "next"}))
			assert.True(t, manager.Exists("next"))
			assert.False(t, manager.Exists("must-not-appear"))
		})
	}
}

func TestCatalogAcceptedPublicationAfterManagerClosePreservesClosedCache(t *testing.T) {
	manager, backing := newCatalogPublicationProbe(t)
	scope, err := manager.StatementCacheScope("nornic")
	require.NoError(t, err)
	backing.mode = "pending-applied"
	var accepted *storage.AcceptedCommitError
	require.ErrorAs(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "accepted"}), &accepted)
	backing.closeStarted = make(chan struct{})
	closed := make(chan error, 1)
	go func() { closed <- manager.Close() }()
	<-backing.closeStarted
	unlocked := manager.mu.TryRLock()
	assert.True(t, unlocked, "Close must not retain metadata lock while waiting on native accepted work")
	if unlocked {
		manager.mu.RUnlock()
	}
	require.NoError(t, storagefault.FinishPreparedReceipt(backing.prepared, backing.receiptOwner, storage.CommitOutcome{State: storage.CommitApplied, Err: storage.ErrConflict}))
	require.NoError(t, <-closed)
	require.False(t, scope.Valid())
	assert.True(t, manager.Exists("accepted"), "physical accepted outcome still owns catalog publication")
	_, err = manager.StatementCacheScope("accepted")
	assert.ErrorIs(t, err, ErrDatabaseNotFound, "settlement cannot reopen closed cache resources")
	assert.True(t, manager.statementCachesClosed)
	assert.Empty(t, manager.limitCheckers, "terminal publication cannot recreate closed checker resources")
}

func newCatalogPublicationProbe(t *testing.T) (*DatabaseManager, *f25CatalogReceiptProbe) {
	t.Helper()
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	probe := &f25CatalogReceiptProbe{Engine: base, native: base.BadgerEngine, ownerContext: context.Background()}
	manager, err := NewDatabaseManager(t.Context(), probe, nil)
	require.NoError(t, err)
	return manager, probe
}
