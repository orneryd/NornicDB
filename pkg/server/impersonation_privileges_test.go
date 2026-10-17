package server

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// ImpersonationChecker decides Bolt imp_user from the IMPERSONATE
// privileges; without a privilege store no one may impersonate.
func TestImpersonationChecker(t *testing.T) {
	var nilServer *Server
	require.False(t, nilServer.ImpersonationChecker([]string{"admin"}, true, "alice"))
	require.False(t, (&Server{}).ImpersonationChecker([]string{"admin"}, true, "alice"))

	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	store := auth.NewImpersonatePrivilegeStore(engine)
	require.NoError(t, store.Set(context.Background(), "svc", auth.ImpersonateUser("alice"), false, false))
	s := &Server{impersonatePrivileges: store}
	require.True(t, s.ImpersonationChecker([]string{"svc"}, false, "alice"))
	require.False(t, s.ImpersonationChecker([]string{"svc"}, false, "bob"))
	require.True(t, s.ImpersonationChecker([]string{"ops"}, true, "bob"))
}
