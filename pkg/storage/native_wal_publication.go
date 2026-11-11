package storage

import (
	"context"
	"errors"

	"github.com/dgraph-io/badger/v4"
)

// nativeWALKey binds one wrapper call to the native final-publication owner.
// It is not a stored context or a new acceptance controller.
type nativeWALKey struct{}

func (cw *commitWriter) prepareNativeWAL() (*valueEncoder, error) {
	admission, _ := cw.ctx.Value(nativeWALKey{}).(*nativeWALAdmission)
	if admission == nil || len(cw.entityOperations) == 0 || cw.metadata {
		return nil, nil
	}
	if admission.state.Load() != 0 {
		return nil, ErrTransactionClosed
	}
	wal := admission.owner.wal
	// Existing journal admission is interruptible before the semantic decision;
	// never retain the WAL mutex across an outer decision or physical action.
	if err := wal.lockAdmission(cw.ctx); err != nil {
		return nil, err
	}
	wal.mu.Unlock()
	operations := cw.entityOperations
	namespace := firstOperationNamespace(operations)
	logicalID := cw.entityLogicalID
	if logicalID == "" {
		logicalID = generateTxID()
	}
	cw.entityLogicalID = logicalID
	cw.configureStoredEntityPublication(logicalID, false)
	mode := cw.entityMode
	encoder := getValueEncoder()
	payload, err := encoder.entityTransaction(EntityTransactionRequest{Namespace: namespace, LogicalID: logicalID, Operations: operations, Mode: mode, keptVectors: cw.entityKeptVectors})
	if err != nil {
		return encoder, err
	}
	// The replay receipt belongs to the SAME physical commit as the graph.
	// Stage it before selecting the final transaction: this write may spill.
	key := bufferedCommitMarkerKey(namespace, "", logicalID)
	if err := cw.write(cw.ctx, func(txn *badger.Txn) error { return txn.Set(key, []byte{1}) }); err != nil {
		return encoder, err
	}
	// The semantic callback sees all original caller values, but cannot hand
	// this private call's journal authority to a nested native write.
	original := context.WithValue(cw.ctx, nativeWALKey{}, (*nativeWALAdmission)(nil))
	cw.ctx = original
	cw.entityWAL, cw.entityJournalData = wal, payload
	if cw.large != nil {
		cw.large.ctx = cw.ctx
	}
	return encoder, nil
}

func firstOperationNamespace(operations []Operation) string {
	for _, operation := range operations {
		switch operation.Type {
		case OpCreateNode, OpUpdateNode:
			if operation.Node != nil {
				return namespaceForNodeID(operation.Node.ID)
			}
		case OpDeleteNode:
			return namespaceForNodeID(operation.NodeID)
		case OpCreateEdge, OpUpdateEdge:
			if operation.Edge != nil {
				return namespaceForEdgeID(operation.Edge.ID)
			}
		case OpDeleteEdge:
			return namespaceForEdgeID(operation.EdgeID)
		}
	}
	return ""
}

// lockAdmission is the existing WAL closed cut, held through an accepted
// command's registration or released before an external semantic decision.
func (w *WAL) lockAdmission(ctx context.Context) error {
	if err := w.mu.LockContext(ctx); err != nil {
		return err
	}
	if w.work.Closing() {
		w.mu.Unlock()
		return ErrWALClosed
	}
	return nil
}

// acceptedNativeWALError fences accepted replay intent before its native
// authority/commit gate can release. Reopen and recovery own resumption.
func (cw *commitWriter) acceptedNativeWALError(err error) error {
	cw.db.oracle.fail(err)
	var accepted *AcceptedCommitError
	if errors.As(err, &accepted) {
		return err
	}
	return &AcceptedCommitError{State: CommitRecoveryPending, LogicalID: cw.entityLogicalID, Err: err}
}

func (cw *commitWriter) acceptNativeWAL() error {
	sequence, err := cw.entityWAL.AppendWithDatabaseReturningSeq(OpEntityCommit, &walEntityCommitData{Payload: cw.entityJournalData}, firstOperationNamespace(cw.entityOperations))
	if err != nil {
		var rejection *walRecordRejection
		if errors.Is(err, ErrWALClosed) || errors.As(err, &rejection) {
			return err
		}
	} else if sequence == 0 {
		return nil
	}
	cw.entityJournalAccepted = true
	if err != nil {
		return cw.acceptedNativeWALError(err)
	}
	return nil
}

// nativePublicationOracle captures only stable metadata under the native state
// lock. The oracle remains valid after the database handle is closed; failure
// inspection never reads the released Badger handle.
func nativePublicationOracle(engine Engine) (*commitOracle, bool) {
	native, ok := UnwrapEngine(engine).(*BadgerEngine)
	if !ok {
		return nil, false
	}
	native.mu.RLock()
	defer native.mu.RUnlock()
	if native.db == nil {
		return nil, true
	}
	return native.db.oracle, true
}

// checkNativeSnapshotPublication prevents a snapshot's WAL cut from discarding
// accepted intent absent from the readable last-published graph.
func checkNativeSnapshotPublication(engine Engine) error {
	oracle, native := nativePublicationOracle(engine)
	if !native {
		return nil
	}
	if oracle == nil {
		return ErrStorageClosed
	}
	return oracle.failure()
}
