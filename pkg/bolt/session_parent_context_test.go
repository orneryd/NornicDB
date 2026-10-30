package bolt

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// RUN, BEGIN and COMMIT start their work under the session's lifetime: the
// connection context first, else the session span, else the Bolt server's
// lifetime, which the server's stop ends (never a substituted Background).
func TestSessionParentContextFollowsTheSessionOwner(t *testing.T) {
	t.Run("connection first", func(t *testing.T) {
		conn, cancel := context.WithCancel(t.Context())
		defer cancel()
		span := t.Context()
		session := &Session{server: &Server{}, connCtx: conn, spanCtx: span}
		require.Equal(t, conn, session.parentContext())
	})
	t.Run("span next", func(t *testing.T) {
		span := t.Context()
		session := &Session{server: &Server{}, spanCtx: span}
		require.Equal(t, span, session.parentContext())
	})
	t.Run("server lifetime last", func(t *testing.T) {
		server := &Server{}
		session := &Session{server: server}
		ctx := session.parentContext()
		require.NoError(t, ctx.Err())
		server.lifetime().CloseAdmission()
		server.lifetime().Cancel()
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
			t.Fatal("the session's work outlived the Bolt server's stop")
		}
	})
	t.Run("commit falls back to the session", func(t *testing.T) {
		var l transactionLifecycle
		session := t.Context()
		require.Equal(t, session, l.commitContext(session))
	})
}
