package storage

import (
 "context"
 "fmt"
 "github.com/orneryd/nornicdb/pkg/util"
)

// WAL owns the replay obligations independently of graph snapshots. Only an
// intrinsic completion marker discharges an admitted lifecycle record. Rebuild
// from durable records at Open, then reconcile against the physical markers.
func (w *WAL) setNamespaceObligation(sequence uint64, namespace string, complete bool) {
	w.lifecycleMu.Lock()
	defer w.lifecycleMu.Unlock()
	if complete {
		delete(w.lifecyclePending, sequence)
		return
	}
	if w.lifecyclePending == nil {
		w.lifecyclePending = make(map[uint64]string)
	}
	w.lifecyclePending[sequence] = namespace
}

func (w *WAL) reconcileNamespaceObligations(owner *BadgerEngine) {
	w.lifecycleMu.Lock()
	pending := make(map[uint64]string, len(w.lifecyclePending))
	for sequence, namespace := range w.lifecyclePending {
		pending[sequence] = namespace
	}
	w.lifecycleMu.Unlock()
	for sequence, namespace := range pending {
		if done, err := owner.namespaceCompleted(namespace, sequence); err == nil && done {
			w.setNamespaceObligation(sequence, "", true)
		}
	}
}

func (w *WAL) checkNamespaceSnapshot(sequence uint64) error {
	w.lifecycleMu.Lock()
	defer w.lifecycleMu.Unlock()
	for pending, namespace := range w.lifecyclePending {
		if pending <= sequence {
			return fmt.Errorf("%w: snapshot/truncation would pass unfinished namespace %q lifecycle record %d", ErrConflict, namespace, pending)
		}
	}
	return nil
}

// Snapshot publication must be recovery-complete, not merely a consistent graph
// image. This check also protects snapshot-transfer and direct streaming callers.
func checkNamespaceSnapshot(engine Engine) error {
	owner := namespaceLifecycleOwner(engine)
	if owner == nil {
		return nil
	}
	if state := owner.namespaceLifetimes.Load(); state != nil {
		for namespace, op := range state.active {
			if op.Committed() {
				return fmt.Errorf("%w: snapshot includes unfinished namespace %q lifecycle", ErrConflict, namespace)
			}
		}
	}
	if owner.bufferedCommitWAL != nil {
		return owner.bufferedCommitWAL.checkNamespaceSnapshot(owner.bufferedCommitWAL.sequence.Load())
	}
	return nil
}

// AdmitRecoverySnapshot prevents lifecycle publication until a recovery image is
// complete. Wrapper WAL leases precede physical publication, matching mutation
// admission. A caller already holding its WAL lease passes the inner engine.
func AdmitRecoverySnapshot(ctx context.Context, engine Engine) (func(), error) {
 if ctx == nil { ctx = context.Background() }
 if wal,ok:=engine.(*WALEngine);ok {
  releaseWAL,err:=util.LockContext(ctx,wal.mutationMu.Lock,wal.mutationMu.Unlock)
  if err!=nil{return nil,err}
  release,err:=AdmitRecoverySnapshot(ctx,wal.engine)
  if err!=nil{releaseWAL();return nil,err}
  return func(){release();releaseWAL()},nil
 }
 if wrapper,ok:=engine.(EngineUnwrapper);ok {
  return AdmitRecoverySnapshot(ctx,wrapper.GetInnerEngine())
 }
 owner:=namespaceLifecycleOwner(engine)
 if owner==nil{return func(){},ctx.Err()}
 release,err:=owner.beginNamespaceMutation(ctx,nil,nil,false,namespacePublicationFresh)
 if err!=nil{return nil,err}
 if err:=checkNamespaceSnapshot(engine);err!=nil{release();return nil,err}
 return release,nil
}