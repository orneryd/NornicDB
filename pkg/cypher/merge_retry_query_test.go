package cypher

import "testing"

func TestIsRetrySafeMergeCommitQuery(t *testing.T) {
	t.Parallel()

	analyzer := NewQueryAnalyzer(16)
	tests := []struct {
		name  string
		query string
		want  bool
	}{
		{
			name:  "merge with set remains retryable",
			query: "MERGE (n:TerraformResource {uid: $uid}) SET n.name = $name RETURN n",
			want:  true,
		},
		{
			name:  "match then merge remains retryable",
			query: "MATCH (a:Account {id: $id}) MERGE (a)-[:HAS]->(r:TerraformResource {uid: $uid}) RETURN r",
			want:  true,
		},
		{
			name:  "create mixed with merge is not retryable",
			query: "CREATE (a:Audit {id: $id}) WITH a MERGE (n:TerraformResource {uid: $uid}) SET n.auditId = a.id",
			want:  false,
		},
		{
			name:  "call mixed with merge is not retryable",
			query: "CALL custom.writeProc() YIELD value MERGE (n:TerraformResource {uid: value.uid}) RETURN n",
			want:  false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			info := analyzer.Analyze(tt.query)
			if got := IsRetrySafeMergeCommitQuery(info); got != tt.want {
				t.Fatalf("IsRetrySafeMergeCommitQuery(%q) = %v, want %v", tt.query, got, tt.want)
			}
		})
	}
}