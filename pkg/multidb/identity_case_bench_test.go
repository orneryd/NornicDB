package multidb

import (
	"testing"
)

func BenchmarkIdentityManagerPublicLookup(b *testing.B) {
	manager := identityCaseManagerFixture(b, []ConstituentRef{{Alias: "Region", DatabaseName: "identity_one", Type: "local", AccessMode: "read_write"}, {Alias: "Ärea", DatabaseName: "identity_two", Type: "local", AccessMode: "read_write"}})

	for _, selection := range []string{"identity_cmp.Region", "identity_cmp.REGION", "identity_cmp.Ärea", "identity_cmp.äREA"} {
		b.Run(selection, func(b *testing.B) {
			if engine, err := manager.GetStorage(selection); err != nil || engine == nil {
				b.Fatalf("lookup failed: %v", err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				engine, err := manager.GetStorage(selection)
				if err != nil || engine == nil {
					b.Fatalf("lookup failed: %v", err)
				}
			}
		})
	}
	b.Run("Snapshot", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			refs, err := manager.GetCompositeConstituents("identity_cmp")
			if err != nil || len(refs) != 2 {
				b.Fatalf("snapshot failed: %v", err)
			}
		}
	})
}
