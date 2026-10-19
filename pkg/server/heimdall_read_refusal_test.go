package server

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
)

func TestHeimdallReadRefusalPreservesAdmissionError(t *testing.T) {
	router := newHeimdallDBRouter(nil, nil, nil)
	ctx := auth.WithRequestDatabaseScope(context.Background(), auth.NewRequestDatabaseScope("tenant", map[string]string{"tenant": "tenant"}))
	ctx = auth.WithRequestEntitlementResolver(ctx, func(_ string, permission auth.Permission) bool { return permission != auth.PermRead })
	for _, call := range []struct {
		name string
		run  func() error
	}{
		{"Stats", func() error { _, err := router.Stats(ctx, "tenant"); return err }},
		{"Discover", func() error { _, err := router.Discover(ctx, "tenant", "text", nil, 1, 1); return err }},
	} {
		t.Run(call.name, func(t *testing.T) {
			err := call.run()
			require.EqualError(t, err, (&cypher.PermissionDeniedError{Permission: "read"}).Error())
		})
	}
}
