package bolt

import "testing"

func BenchmarkIssue1327RecordMap(b *testing.B) {
	for _, tc := range []struct {
		name  string
		value map[string]any
	}{
		{"ordinary", map[string]any{"id": int64(7), "name": "example", "nested": map[string]any{"ok": true}}},
		{"marker", map[string]any{"_pathResult": true, "id": int64(7), "name": "example"}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, err := encodeRecordValueInto(b.Context(), nil, tc.value, true, true, "test")
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
