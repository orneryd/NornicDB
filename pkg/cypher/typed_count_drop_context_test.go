package cypher

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type typedCountContextDropManager struct {
	*testDatabaseManagerAdapter
	entered bool
}

func (m *typedCountContextDropManager) DropDatabaseContext(ctx context.Context, name string) error {
	m.entered = true
	<-ctx.Done()
	return ctx.Err()
}

func TestTypedCountDropForwardsStatementContext(t *testing.T) {
	f := openTypedCountScope(t, t.TempDir(), true)
	typedCountRun(t, f.admin, "CREATE DATABASE scope_cancel")
	manager := &typedCountContextDropManager{testDatabaseManagerAdapter: &testDatabaseManagerAdapter{manager: f.manager}}
	f.admin.SetDatabaseManager(manager)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := f.admin.Execute(ctx, "DROP DATABASE scope_cancel", nil)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.True(t, manager.entered)
	require.True(t, f.manager.Exists("scope_cancel"))
}
