package server

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
)

func BenchmarkSelectedAliasAdmission(b *testing.B) {
	s, _ := setupTestServer(b)
	const role = "alias-bench-reader"
	ctx := context.Background()
	if err := s.dbManager.CreateAlias("selected_alias", "nornic"); err != nil {
		b.Fatal(err)
	}
	if err := s.roleStore.CreateRole(ctx, role); err != nil {
		b.Fatal(err)
	}
	if err := s.roleEntitlementsStore.Set(ctx, role, []string{"read"}); err != nil {
		b.Fatal(err)
	}
	if err := s.allowlistStore.SaveRoleDatabases(ctx, role, []string{"nornic", "selected_alias"}); err != nil {
		b.Fatal(err)
	}
	if err := s.privilegesStore.SavePrivilege(ctx, role, "nornic", true, false); err != nil {
		b.Fatal(err)
	}
	req := httptest.NewRequest("GET", "/api/bifrost/status", nil).WithContext(context.WithValue(ctx, contextKeyClaims, &auth.JWTClaims{Roles: []string{role}}))
	principal := s.withBifrostRBAC(req).Context()
	scope := auth.RequestDatabaseScopeFromContext(principal)
	router := newHeimdallDBRouter(s.db, s.dbManager, nil)
	for _, selection := range []string{"nornic", "selected_alias"} {
		b.Run("resolve/"+selection, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, ok := scope.Resolve(selection); !ok {
					b.Fatal("selection denied")
				}
			}
		})
		b.Run("bound-read/"+selection, func(b *testing.B) {
			_, allows := auth.BindRequestDatabaseRead(principal, selection)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if !allows() {
					b.Fatal("read denied")
				}
			}
		})
	}
	b.Run("request-setup", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if s.withBifrostRBAC(req) == nil {
				b.Fatal("no request")
			}
		}
	})
	b.Run("heimdall-stats-alias", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := router.Stats(principal, "selected_alias"); err != nil {
				b.Fatal(err)
			}
		}
	})
}
