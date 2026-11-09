package cypher

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/stretchr/testify/require"
)

// Only the exact immutable context errors are safe without projection. An
// errors.Is match is insufficient: its wrapper can contain source credentials.
func TestAdminCredentialCancellationProjection(t *testing.T) {
	const secret = "projection-probe-741928"
	for _, query := range []string{
		"CREATE USER private SET PASSWORD '" + secret + "'",
		"CREATE ALIAS remote FOR DATABASE nornic AT 'https://user:" + secret + "@example.invalid'",
	} {
		for _, owner := range []struct {
			name string
			run  func(string, *error)
		}{
			{"entry", func(query string, err *error) { redactAdminCommandError(query, err) }},
			{"source", func(query string, err *error) {
				redactAdminCommandErrorSource(cyphertext.NormalizeSource(query), query, err)
			}},
		} {
			t.Run(owner.name+"/"+fmt.Sprint(len(query)), func(t *testing.T) {
				var empty error
				owner.run(query, &empty)
				require.NoError(t, empty)
				for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
					exact := cause
					owner.run(query, &exact)
					require.True(t, exact == cause, "immutable context error must keep exact identity")
					wrapped := fmt.Errorf("%s: %w", secret, cause)
					owner.run(query, &wrapped)
					require.ErrorIs(t, wrapped, cause)
					require.NotContains(t, wrapped.Error(), secret)
					// errors.Join is also a wrapper, not an immutable context error.
					joined := errors.Join(cause, errors.New(secret))
					owner.run(query, &joined)
					require.ErrorIs(t, joined, cause)
					require.NotContains(t, joined.Error(), secret)
				}
			})
		}
	}
}
