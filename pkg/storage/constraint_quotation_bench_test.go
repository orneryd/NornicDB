package storage

import "testing"

// Both products accept these exact inputs. Escaped-input correctness is
// established separately by the public matrix, not by timing a baseline error.
func BenchmarkConstraintQuotation(b *testing.B) {
	for _, expr := range []string{"n.name IN ['Acme, Sons', 'Other']", "n.name IN [1, 2, 3]"} {
		b.Run(expr, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				ok, _, _, err := parsePropertyInExpression(expr)
				if err != nil || !ok {
					b.Fatalf("parse failed: %v", err)
				}
			}
		})
	}
}
