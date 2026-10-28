package auth

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClusterTokenIssuanceContract(t *testing.T) {
	for _, test := range []struct {
		name, node, details     string
		role                    Role
		expiry                  time.Duration
		explicit, missingSecret bool
	}{
		{"default expiry", "node-2", "cluster token generated for node node-2 with role admin", RoleAdmin, time.Hour, false, false},
		{"default no expiry", "node-2", "cluster token generated for node node-2 with role viewer", RoleViewer, 0, false, false},
		{"explicit expiry", "node-2", "cluster token generated for node node-2 with role admin, expiry=168h0m0s", RoleAdmin, 7 * 24 * time.Hour, true, false},
		{"explicit no expiry", "node-2", "cluster token generated for node node-2 with role viewer, expiry=0s", RoleViewer, 0, true, false},
		{"explicit negative expiry", "node-2", "cluster token generated for node node-2 with role editor, expiry=-1h0m0s", RoleEditor, -time.Hour, true, false},
		{"opaque node and role", "tenant/λ", "cluster token generated for node tenant/λ with role custom:role", Role("custom:role"), time.Hour, false, false},
		{"default missing secret", "node-2", "", RoleAdmin, time.Hour, false, true},
		{"explicit missing secret", "node-2", "", RoleAdmin, time.Hour, true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			a := newTestAuthenticator(t)
			a.config.TokenExpiry = test.expiry
			if test.missingSecret {
				a.config.JWTSecret = nil
			}
			var events []AuditEvent
			a.SetAuditLogger(func(event AuditEvent) { events = append(events, event) })
			before := time.Now()
			var token string
			var err error
			if test.explicit {
				token, err = a.GenerateClusterTokenWithExpiry(test.node, test.role, test.expiry)
			} else {
				token, err = a.GenerateClusterToken(test.node, test.role)
			}
			after := time.Now()
			if test.missingSecret {
				require.Same(t, ErrMissingSecret, err)
				require.Empty(t, token)
				require.Empty(t, events)
				return
			}
			require.NoError(t, err)
			require.Len(t, events, 1)
			event := events[0]
			require.Equal(t, "cluster_token_generated", event.EventType)
			require.Equal(t, test.node, event.Username)
			require.Equal(t, "cluster-"+test.node, event.UserID)
			require.True(t, event.Success)
			require.Equal(t, test.details, event.Details)
			require.False(t, event.Timestamp.Before(before))
			require.False(t, event.Timestamp.After(after))
			claims, err := a.ValidateToken(token)
			require.NoError(t, err)
			require.Equal(t, "cluster-"+test.node, claims.Sub)
			require.Equal(t, test.node, claims.Username)
			require.Equal(t, []string{string(test.role)}, claims.Roles)
			if test.expiry > 0 {
				require.Equal(t, claims.Iat+int64(test.expiry.Seconds()), claims.Exp)
			} else {
				require.Zero(t, claims.Exp)
			}
		})
	}
}
