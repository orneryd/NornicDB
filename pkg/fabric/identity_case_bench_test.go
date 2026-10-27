package fabric

import "testing"

func BenchmarkIdentityCatalogPublicLookup(b *testing.B) {
	catalog := NewCatalog()
	catalog.Register("identity.Region", &LocationLocal{DBName: "one"})
	catalog.Register("identity.Ärea", &LocationLocal{DBName: "two"})
	for _, name := range []string{"identity.region", "IDENTITY.REGION", "identity.ärea", "IDENTITY.ÄREA"} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				location, err := catalog.Resolve(name)
				if err != nil || location == nil {
					b.Fatalf("lookup failed: %v", err)
				}
			}
		})
	}
}
