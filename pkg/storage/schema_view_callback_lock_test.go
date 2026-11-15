package storage

import (
	"github.com/stretchr/testify/require"
	"testing"
)

// Identity callbacks may consult their transaction. Refuse the callback under
// its lock rather than leaking a deadlocked goroutine in the regression test.
type reentrantSchemaIdentityView struct {
	*schemaStatementView
	calledUnderLock bool
}

func (v *reentrantSchemaIdentityView) identityAvailable() bool {
	if !v.tx.mu.TryLock() {
		v.calledUnderLock = true
		return false
	}
	v.tx.mu.Unlock()
	return true
}

func (v *reentrantSchemaIdentityView) GetInnerEngine() Engine {
	if !v.identityAvailable() {
		return nil
	}
	_ = v.tx.Namespace()
	return v.Engine
}

func (v *reentrantSchemaIdentityView) Namespace() string {
	if !v.identityAvailable() {
		return "wrong-locked-scope"
	}
	return v.tx.Namespace()
}

func TestSchemaViewIdentityCallbacksRunOutsideTransactionLock(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()
	tx, base := beginTemporalDDLStatement(t, engine)
	defer func() { require.NoError(t, tx.Rollback()) }()
	view := &reentrantSchemaIdentityView{schemaStatementView: base}
	require.True(t, tx.ownsSchemaView(view, base.GetSchema()))
	require.False(t, view.calledUnderLock, "wrapper identity may read transaction metadata")
}
