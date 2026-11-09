package server

// From the round-2 review of #1768 (gpt-6.1-sol, TestReview1768RoleMutation
// DoesNotFreezeAuthenticatedHTTP): while a role rename or delete waits on
// storage, unrelated users' Bearer and cached-Basic requests still answer.
// Formatted by the prune-lockup lane (Claude Opus 5.5), 9 November 2026.

import (
	"context"
	"encoding/base64"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type review1768RoleStore struct {
	storage.Engine
	stall   atomic.Bool
	entered chan struct{}
	gate    chan struct{}
}

func (s *review1768RoleStore) GetInnerEngine() storage.Engine { return s.Engine }

func (s *review1768RoleStore) Namespace() string { return "system" }

func (s *review1768RoleStore) WithStoreTransaction(ctx context.Context, mutate func(storage.Engine) error) error {
	if s.stall.Load() {
		s.entered <- struct{}{}
		<-s.gate
	}
	return auth.WithStoreTransaction(ctx, s.Engine, mutate)
}

func TestReview1768RoleMutationDoesNotFreezeAuthenticatedHTTP(t *testing.T) {
	for _, method := range []string{http.MethodPatch, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			s, users := setupTestServer(t)
			wrapped := &review1768RoleStore{Engine: storage.NewNamespacedEngine(s.db.GetBaseStorageForManager(), "system"), entered: make(chan struct{}, 1), gate: make(chan struct{})}
			s.roleStore = auth.NewRoleStore(wrapped)
			require.NoError(t, s.roleStore.CreateRole(t.Context(), "ops"))
			require.NoError(t, users.UpdateRoles(t.Context(), "admin", []auth.Role{auth.RoleAdmin, "ops"}))
			admin := "Bearer " + getAuthToken(t, users, "admin")
			bearer := "Bearer " + getAuthToken(t, users, "reader")
			basic := "Basic " + base64.StdEncoding.EncodeToString([]byte("reader:password123"))
			require.Equal(t, http.StatusOK, makeRequest(t, s, http.MethodGet, "/auth/me", nil, basic).Code)
			require.Equal(t, http.StatusOK, makeRequest(t, s, http.MethodGet, "/auth/me", nil, bearer).Code)
			wrapped.stall.Store(true)
			released := false
			release := func() {
				if !released {
					released = true
					close(wrapped.gate)
				}
			}
			t.Cleanup(release)
			mutation := make(chan int, 1)
			go func() {
				mutation <- makeRequest(t, s, method, "/auth/roles/ops", map[string]any{"name": "operators"}, admin).Code
			}()
			select {
			case <-wrapped.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("role HTTP write never reached storage")
			}
			responses := make(chan int, 2)
			for _, header := range []string{bearer, basic} {
				go func(header string) {
					responses <- makeRequest(t, s, http.MethodGet, "/auth/me", nil, header).Code
				}(header)
			}
			got := 0
			deadline := time.After(200 * time.Millisecond)
		collecting:
			for got < 2 {
				select {
				case code := <-responses:
					require.Equal(t, http.StatusOK, code)
					got++
				case <-deadline:
					break collecting
				}
			}
			release()
			require.Equal(t, http.StatusOK, <-mutation)
			for i := got; i < 2; i++ {
				require.Equal(t, http.StatusOK, <-responses)
			}
			require.Equal(t, 2, got, "Bearer and cached Basic HTTP requests stalled behind unrelated role storage I/O")
		})
	}
}
