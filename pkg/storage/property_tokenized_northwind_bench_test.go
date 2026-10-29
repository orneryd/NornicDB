package storage

import "testing"

var benchmarkTokenizedPropertiesSink []byte

// BenchmarkEncodeTokenizedPropertiesNorthwind measures the property shapes
// of the Northwind order-line relationships and product nodes after their
// dictionary names have already been published.
func BenchmarkEncodeTokenizedPropertiesNorthwind(b *testing.B) {
	cases := []struct {
		name  string
		props map[string]any
	}{
		{name: "order_line", props: map[string]any{"quantity": int64(17), "discount": 12.75}},
		{name: "product", props: map[string]any{
			"productID": int64(12345), "productName": "Sample Product 12345", "sku": "SKU-012345-AB",
			"unitPrice": 19.75, "unitsInStock": int64(320), "discontinued": false,
			"description": "A representative Northwind product description for bulk ingestion.",
			"tags":        []any{"fresh", "sample", "food"},
		}},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			engine := benchEngine(b)
			if _, err := engine.CreateNode(&Node{ID: "bench:seed", Properties: tc.props}); err != nil {
				b.Fatal(err)
			}
			txn, readTS, err := engine.db.beginTxn(true)
			if err != nil {
				b.Fatal(err)
			}
			defer engine.db.endRead(readTS)
			defer txn.Discard()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchmarkTokenizedPropertiesSink, err = engine.encodeTokenizedProperties(txn, "bench", tc.props)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
