package bolt

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/stretchr/testify/require"
)

func TestFineGrainedBoltBufferedResultRetainsItsPrincipal(t *testing.T) {
	ctx := t.Context()
	server := &Server{}
	server.SetFineGrainedAdmission(func(ctx context.Context, roles []string) (*auth.FineGrainedRequest, error) {
		identity := auth.RequestIdentityFromContext(ctx)
		return &auth.FineGrainedRequest{Fingerprint: identity.ID + ":" + roles[0]}, nil
	})
	session := &Session{server: server, authResult: &BoltAuthResult{
		Authenticated: true, Username: "first", PrincipalID: "sub:first", Roles: []string{"reader"},
	}}
	admitted, _, err := session.fineGrainedPrincipal(ctx, map[string]any{"imp_user": nil})
	require.NoError(t, err)
	scope := newFineGrainedResultScope(admitted, nil)

	current, _, err := session.validateFineGrainedResult(ctx, scope)
	require.NoError(t, err)
	require.Equal(t, admitted.Fingerprint, current.Fingerprint)

	session.authResult = &BoltAuthResult{
		Authenticated: true, Username: "second", PrincipalID: "sub:second", Roles: []string{"reader"},
	}
	current, target, err := session.validateFineGrainedResult(ctx, scope)
	require.ErrorIs(t, err, auth.ErrFineGrainedForbidden)
	require.Nil(t, current)
	require.Nil(t, target)
}

func TestFineGrainedBoltImpersonationUnavailableWithoutResolver(t *testing.T) {
	session := &Session{server: &Server{}}
	for _, metadata := range []map[string]any{nil, {"imp_user": nil}} {
		principal, user, err := session.fineGrainedPrincipal(t.Context(), metadata)
		require.NoError(t, err)
		require.Nil(t, principal)
		require.Nil(t, user)
	}
	principal, user, err := session.fineGrainedPrincipal(t.Context(), map[string]any{"imp_user": "other"})
	require.ErrorIs(t, err, auth.ErrFineGrainedForbidden)
	require.Nil(t, principal)
	require.Nil(t, user)
}
