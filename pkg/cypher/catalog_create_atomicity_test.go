package cypher

import (
	"context"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Real native preparation/receipts are retained; faults enter the authoritative
// system submitter, never a manager mock or a direct CRUD side channel.
type createOptionsSubmissionProbe struct {
	storage.Engine
	native       *storage.BadgerEngine
	mode         string
	submissions  int
	submitted    chan struct{}
	beforeSubmit func()
	prepared     *storage.PreparedEntityCommit
	receipt      *storage.CommitReceipt
	receiptOwner storage.CommitReceiptOwner
}

func (p *createOptionsSubmissionProbe) GetInnerEngine() storage.Engine { return p.Engine }
func (p *createOptionsSubmissionProbe) SubmitEntityCommit(ctx context.Context, prepared *storage.PreparedEntityCommit, settle func(storage.CommitOutcome)) (*storage.CommitReceipt, error) {
	if prepared.Namespace() != "system" || p.mode == "" {
		return p.native.SubmitEntityCommit(ctx, prepared, settle)
	}
	p.submissions++
	if p.mode == "preaccept-canceled" {
		p.beforeSubmit()
		return nil, ctx.Err()
	}
	if p.mode == "rejected" {
		return nil, storage.ErrConflict
	}
	if p.mode == "native" {
		return p.native.SubmitEntityCommit(ctx, prepared, settle)
	}
	p.prepared = prepared
	p.receipt, p.receiptOwner = storage.NewCommitReceipt(settle)
	if p.mode == "applied-ack-error" {
		if err := storagefault.FinishPreparedReceipt(p.prepared, p.receiptOwner, storage.CommitOutcome{State: storage.CommitApplied, Err: storage.ErrConflict}); err != nil {
			return nil, err
		}
	}
	if p.submitted != nil {
		close(p.submitted)
	}
	return p.receipt, nil
}

// Historical fail-first evidence preserves the old second-submit and option-
// boundary defects. CREATE now has one publication, including initial options.
func TestCreateDatabaseOptionsFailurePublishesNothing(t *testing.T) {
	for _, mode := range []string{"canceled-before-create", "preaccept-canceled", "rejected", "native", "applied-ack-error", "pending-applied", "pending-discarded"} {
		t.Run(mode, func(t *testing.T) {
			native, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), LowMemory: true}))
			require.NoError(t, err)
			backing := &createOptionsSubmissionProbe{Engine: native, native: native}
			manager, err := multidb.NewDatabaseManager(t.Context(), backing, nil)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, manager.Close()) })
			adapter := &catalogRequestContextAdapter{optionsTestManager: optionsTestManager{&testDatabaseManagerAdapter{manager: manager}}}
			executor := NewStorageExecutor(storage.NewNamespacedEngine(backing, "nornic"))
			executor.SetDatabaseManager(adapter)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			backing.mode = mode
			backing.beforeSubmit = cancel
			if mode == "canceled-before-create" {
				adapter.beforeCreate = func(received context.Context) { cancel(); assert.ErrorIs(t, received.Err(), context.Canceled) }
			}
			execute := func() error {
				_, err := executor.Execute(ctx, "CREATE DATABASE atomic_options OPTIONS {txLogEnrichment: 'FULL'}", nil)
				return err
			}
			pending := mode == "pending-applied" || mode == "pending-discarded"
			if pending {
				backing.submitted = make(chan struct{})
				finished := make(chan error, 1)
				go func() { finished <- execute() }()
				<-backing.submitted
				require.False(t, manager.Exists("atomic_options"), "accepted but unsettled create must not publish")
				require.Equal(t, storage.ChangeCaptureOff, native.ChangeCapture("atomic_options"))
				cancel()
				err = <-finished
				var accepted *storage.AcceptedCommitError
				require.ErrorAs(t, err, &accepted)
				require.Equal(t, storage.CommitRecoveryPending, accepted.State)
				state := storage.CommitApplied
				if mode == "pending-discarded" {
					state = storage.CommitDiscarded
				}
				require.NoError(t, storagefault.FinishPreparedReceipt(backing.prepared, backing.receiptOwner, storage.CommitOutcome{State: state, Err: storage.ErrConflict}))
			} else {
				err = execute()
			}
			applied := mode == "native" || mode == "applied-ack-error" || mode == "pending-applied"
			switch mode {
			case "canceled-before-create":
				require.ErrorIs(t, err, context.Canceled)
				require.Zero(t, backing.submissions)
			case "preaccept-canceled":
				require.ErrorIs(t, err, context.Canceled)
			case "rejected":
				require.ErrorIs(t, err, storage.ErrConflict)
			case "native":
				require.NoError(t, err)
			case "applied-ack-error":
				var accepted *storage.AcceptedCommitError
				require.ErrorAs(t, err, &accepted)
				require.Equal(t, storage.CommitApplied, accepted.State)
			}
			if mode != "canceled-before-create" {
				require.Equal(t, 1, backing.submissions, "CREATE OPTIONS must have exactly one authoritative submission")
			}
			require.Equal(t, applied, manager.Exists("atomic_options"))
			expectedMode := storage.ChangeCaptureOff
			if applied {
				expectedMode = storage.ChangeCaptureFull
				options, err := manager.DatabaseOptions("atomic_options")
				require.NoError(t, err)
				require.Equal(t, map[string]string{multidb.OptionTxLogEnrichment: "FULL"}, options)
			}
			require.Equal(t, expectedMode, native.ChangeCapture("atomic_options"))
			backing.mode = ""
			reloaded, err := multidb.NewDatabaseManager(t.Context(), backing, nil)
			require.NoError(t, err)
			require.Equal(t, applied, reloaded.Exists("atomic_options"), "native persisted catalog agrees with terminal publication")
			if applied {
				options, err := reloaded.DatabaseOptions("atomic_options")
				require.NoError(t, err)
				require.Equal(t, map[string]string{multidb.OptionTxLogEnrichment: "FULL"}, options)
			}
		})
	}
}
