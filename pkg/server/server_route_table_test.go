package server

import (
	"net/http"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
)

// BenchmarkServerRouteTableRegistration pins the startup cost of wiring the
// authenticated route tables through registerRouteTable. Registration happens
// once per server start, so this is informational rather than a hot path.
func BenchmarkServerRouteTableRegistration(b *testing.B) {
	s := &Server{}
	routes := []routeSpec{
		{"/nornicdb/search", auth.PermRead, s.handleSearch},
		{"/nornicdb/decay", auth.PermRead, s.handleDecay},
		{"/admin/stats", auth.PermAdmin, s.handleAdminStats},
		{"/gdpr/export", auth.PermRead, s.handleGDPRExport},
		{"/auth/me", auth.PermRead, s.handleMe},
		{"/admin/retention/policies", auth.PermAdmin, func(w http.ResponseWriter, r *http.Request) { s.handleRetention(w, r, retentionPoliciesRoute) }},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mux := http.NewServeMux()
		s.registerRouteTable(mux, routes)
	}
}
