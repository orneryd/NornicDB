package cypher

import "testing"

func TestIsCacheableReadQuery_NewPrimitives(t *testing.T) {
	tests := []struct {
		name   string
		query  string
		expect bool
	}{
		{
			name:   "retrieve cacheable",
			query:  "CALL db.retrieve({query: 'alpha', limit: 10})",
			expect: true,
		},
		{
			name:   "rretrieve cacheable",
			query:  "CALL db.rretrieve({query: 'alpha', limit: 10})",
			expect: true,
		},
		{
			name:   "rerank cacheable",
			query:  "CALL db.rerank({query: 'alpha', candidates: [{id: '1', content: 'x', score: 1.0}]})",
			expect: true,
		},
		{
			name:   "vector embed cacheable",
			query:  "CALL db.index.vector.embed('alpha') YIELD embedding",
			expect: true,
		},
		{
			name:   "localized procedure metadata not cacheable",
			query:  "SHOW PROCEDURES",
			expect: false,
		},
		{
			name:   "infer not cacheable by default",
			query:  "CALL db.infer({prompt: 'hello'})",
			expect: false,
		},
		{
			name:   "infer cache opt-in via cache true",
			query:  "CALL db.infer({prompt: 'hello', cache: true})",
			expect: true,
		},
		{
			name:   "infer cache_enabled ignored",
			query:  "CALL db.infer({prompt: 'hello', cache_enabled: true})",
			expect: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isCacheableReadQuery(tt.query)
			if got != tt.expect {
				t.Fatalf("isCacheableReadQuery(%q) = %v, want %v", tt.query, got, tt.expect)
			}
		})
	}
}
