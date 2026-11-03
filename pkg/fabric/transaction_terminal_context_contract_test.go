package fabric

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type terminalContractKey struct{}

func TestTerminalContextsPreserveCommitIdentityAndCanceledAdmission(t *testing.T) {
	for _, override := range []bool{false, true} {
		t.Run(map[bool]string{false: "bound", true: "override"}[override], func(t *testing.T) {
			commit, cancel := context.WithCancel(context.WithValue(context.Background(), terminalContractKey{}, "admitted"))
			cancel()
			cleanup := context.WithValue(context.Background(), terminalContractKey{}, "cleanup")
			tx := NewFabricTransaction("context-identity")
			_, err := tx.GetOrOpen("writer", true)
			require.NoError(t, err)
			calls := 0
			commitFn := func(got context.Context, _ *SubTransaction) error {
				calls++
				require.True(t, got == commit, "exact admitted context")
				require.Equal(t, "admitted", got.Value(terminalContractKey{}))
				require.ErrorIs(t, got.Err(), context.Canceled)
				return nil
			}
			rollbackFn := func(context.Context, *SubTransaction) error {
				t.Fatal("healthy accepted commit must not roll back")
				return nil
			}
			if override {
				require.NoError(t, tx.BindParticipantCallbacks("writer", ParticipantCallbacks{Commit: func(context.Context, *SubTransaction) error {
					t.Fatal("global override must replace bound callback")
					return nil
				}}))
			} else {
				require.NoError(t, tx.BindParticipantCallbacks("writer", ParticipantCallbacks{Commit: commitFn, Rollback: rollbackFn}))
				commitFn = nil
				rollbackFn = nil
			}
			require.NoError(t, tx.Commit(TerminalContexts{Commit: commit, Cleanup: cleanup}, commitFn, rollbackFn))
			require.Equal(t, 1, calls)
			require.Equal(t, "committed", tx.State())
		})
	}
}

func TestTerminalContextsInvalidInputsHaveNoEffects(t *testing.T) {
	for _, mode := range []string{"commit", "cleanup", "both"} {
		t.Run(mode, func(t *testing.T) {
			tx := NewFabricTransaction("invalid-context")
			_, err := tx.GetOrOpen("writer", true)
			require.NoError(t, err)
			lifetime := tx.BindContext(context.Background())
			calls := 0
			require.NoError(t, tx.BindParticipantCallbacks("writer", ParticipantCallbacks{Commit: func(context.Context, *SubTransaction) error { calls++; return nil }, Rollback: func(context.Context, *SubTransaction) error { calls++; return nil }}))
			scopes := TerminalContexts{Commit: context.Background(), Cleanup: context.Background()}
			switch mode {
			case "commit":
				scopes.Commit = nil
			case "cleanup":
				scopes.Cleanup = nil
			case "both":
				scopes = TerminalContexts{}
			}
			err = tx.Commit(scopes, nil, nil)
			require.Error(t, err)
			require.Zero(t, calls)
			require.Equal(t, "open", tx.State())
			require.Equal(t, "open", tx.SubTransactions()["writer"].State)
			require.NoError(t, lifetime.Err(), "invalid input must not cancel the transaction lifetime")
			require.NoError(t, tx.Rollback(context.Background(), nil))
			require.Equal(t, 1, calls)
			require.ErrorIs(t, lifetime.Err(), context.Canceled)
		})
	}
}

func TestTerminalContextsCompensationUsesIndependentCleanup(t *testing.T) {
	for _, scenario := range []string{"reader-error", "reader-panic", "writer-error", "writer-panic"} {
		t.Run(scenario, func(t *testing.T) {
			request, cancel := context.WithCancel(context.WithValue(context.Background(), terminalContractKey{}, "commit"))
			defer cancel()
			cleanup := context.WithValue(context.Background(), terminalContractKey{}, "cleanup")
			tx := NewFabricTransaction("compensation-context")
			_, err := tx.GetOrOpen("reader", false)
			require.NoError(t, err)
			_, err = tx.GetOrOpen("writer", true)
			require.NoError(t, err)
			failure := errors.New(scenario)
			commits := []string{}
			rollbacks := []string{}
			for _, name := range []string{"reader", "writer"} {
				name := name
				require.NoError(t, tx.BindParticipantCallbacks(name, ParticipantCallbacks{
					Commit: func(got context.Context, _ *SubTransaction) error {
						commits = append(commits, name)
						require.True(t, got == request)
						require.Equal(t, "commit", got.Value(terminalContractKey{}))
						if scenario == name+"-error" || scenario == name+"-panic" {
							cancel()
							if scenario == name+"-panic" {
								panic(failure)
							}
							return failure
						}
						return nil
					},
					Rollback: func(got context.Context, _ *SubTransaction) error {
						rollbacks = append(rollbacks, name)
						require.True(t, got == cleanup, "compensation must use cleanup, not canceled commit input")
						require.Equal(t, "cleanup", got.Value(terminalContractKey{}))
						require.NoError(t, got.Err())
						require.ErrorIs(t, request.Err(), context.Canceled)
						return nil
					},
					Outcome: func() string { return "rolledback" },
				}))
			}
			run := func() { err = tx.Commit(TerminalContexts{Commit: request, Cleanup: cleanup}, nil, nil) }
			if scenario == "reader-panic" || scenario == "writer-panic" {
				require.PanicsWithValue(t, failure, run)
			} else {
				run()
				require.ErrorIs(t, err, failure)
			}
			if scenario == "reader-error" || scenario == "reader-panic" {
				require.Equal(t, []string{"reader"}, commits)
				require.Equal(t, []string{"reader", "writer"}, rollbacks)
			} else {
				require.Equal(t, []string{"reader", "writer"}, commits)
				require.Equal(t, []string{"writer"}, rollbacks)
			}
			require.Equal(t, "rolledback", tx.State())
		})
	}
}

func TestTerminalRollbackUsesCallerCleanupAfterRequestCancellation(t *testing.T) {
	request, cancel := context.WithCancel(context.Background())
	cancel()
	cleanup := context.WithValue(context.Background(), terminalContractKey{}, "independent-cleanup")
	tx := NewFabricTransaction("rollback-context")
	_, err := tx.GetOrOpen("writer", true)
	require.NoError(t, err)
	calls := 0
	require.NoError(t, tx.BindParticipantCallbacks("writer", ParticipantCallbacks{Rollback: func(got context.Context, _ *SubTransaction) error {
		calls++
		require.True(t, got == cleanup)
		require.Equal(t, "independent-cleanup", got.Value(terminalContractKey{}))
		require.NoError(t, got.Err())
		require.ErrorIs(t, request.Err(), context.Canceled)
		return nil
	}}))
	require.NoError(t, tx.Rollback(cleanup, nil))
	require.Equal(t, 1, calls)
	require.Equal(t, "rolledback", tx.State())
}

func TestTerminalCleanupDispatchMayBeCanceledWhileParticipantScopeSurvives(t *testing.T) {
	request, cancel := context.WithCancel(context.WithValue(context.Background(), terminalContractKey{}, "request"))
	boundCleanup := context.WithoutCancel(request)
	cancel()
	tx := NewFabricTransaction("bound-cleanup-policy")
	_, err := tx.GetOrOpen("reader", false)
	require.NoError(t, err)
	calls := 0
	require.NoError(t, tx.BindParticipantCallbacks("reader", ParticipantCallbacks{
		Rollback: func(input context.Context, _ *SubTransaction) error {
			calls++
			require.True(t, input == request)
			require.ErrorIs(t, input.Err(), context.Canceled)
			require.NoError(t, boundCleanup.Err(), "participant retains its independently bound policy")
			require.Equal(t, "request", boundCleanup.Value(terminalContractKey{}))
			return nil
		},
	}))
	require.NoError(t, tx.Rollback(request, nil))
	require.Equal(t, 1, calls)
	require.Equal(t, "rolledback", tx.State())
}
