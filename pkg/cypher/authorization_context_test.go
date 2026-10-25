package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAuthorizeQueryCanceledClassification(t *testing.T) {
	const query = "CREATE (:AuthorizationCancellationProbe)"
	for _, mode := range []string{"canceled", "deadline", "reader checkpoint"} {
		t.Run(mode, func(t *testing.T) {
			var ctx context.Context
			var want error
			switch mode {
			case "canceled":
				canceled, cancel := context.WithCancel(t.Context())
				cancel()
				ctx, want = canceled, context.Canceled
			case "deadline":
				expired, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
				defer cancel()
				ctx, want = expired, context.DeadlineExceeded
			case "reader checkpoint":
				checkpoint := newCheckpointContext(2, nil)
				defer checkpoint.cancel()
				ctx, want = checkpoint, context.Canceled
			}
			checks := 0
			ctx = WithPermissionChecker(ctx, func(permission string) bool { checks++; return permission == "read" })
			require.ErrorIs(t, AuthorizeQuery(ctx, query), want)
			require.ErrorIs(t, ctx.Err(), want)
			require.Zero(t, checks, "incomplete requirements must not reach the entitlement decision")
		})
	}
	// Absence of a checker deliberately disables authorization, including when
	// the embedded caller has an expired or canceled execution context.
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	require.NoError(t, AuthorizeQuery(canceled, query))
}

func TestAuthorizeQueryLiveClassificationControls(t *testing.T) {
	const query = "CREATE (:AuthorizationCancellationProbe)"
	readOnly := WithPermissionChecker(t.Context(), func(permission string) bool { return permission == "read" })
	var denied *PermissionDeniedError
	require.ErrorAs(t, AuthorizeQuery(readOnly, query), &denied)
	require.Equal(t, "write", denied.Permission)
	require.NoError(t, AuthorizeQuery(WithPermissionChecker(t.Context(), func(permission string) bool { return permission == "write" }), query))
	require.NoError(t, AuthorizeQuery(readOnly, "RETURN 1 AS value"))
	require.ErrorAs(t, AuthorizeQuery(readOnly, "CALL db.cdc.query()"), &denied)
	require.Equal(t, "admin", denied.Permission)
	granted := WithProcedureGrantChecker(readOnly, func(procedure string) bool { return procedure == "db.cdc.query" })
	require.NoError(t, AuthorizeQuery(granted, "CALL db.cdc.query()"))
	require.ErrorAs(t, AuthorizeQuery(granted, "CALL db.txlog.entries()"), &denied)
	require.Equal(t, "admin", denied.Permission)
}
