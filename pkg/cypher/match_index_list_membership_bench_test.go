package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// benchIdentityPopulation is the identity population of the list-membership
// benchmarks: 7,300 identities, each with a name key and four alias keys.
const benchIdentityPopulation = 7300

func newBenchIdentityExecutor(b *testing.B) *StorageExecutor {
	b.Helper()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "bench_member"))
	ctx := context.Background()
	for _, ddl := range []string{
		"CREATE INDEX bench_identity_alias_keys FOR (n:Identity) ON (n.alias_keys)",
		"CREATE INDEX bench_identity_name_key FOR (n:Identity) ON (n.name_key)",
		"CREATE INDEX bench_identity_id FOR (n:Identity) ON (n.id)",
	} {
		if _, err := exec.Execute(ctx, ddl, nil); err != nil {
			b.Fatal(err)
		}
	}
	_, err := exec.Execute(ctx, `UNWIND range(0, $count - 1) AS i
CREATE (:GraphNode:Identity {id: 'id' + toString(i), name_key: 'name' + toString(i),
  alias_keys: ['alias' + toString(i), 'short' + toString(i), 'alt' + toString(i), 'shared' + toString(i % 500)],
  current: i % 9 <> 0})`, map[string]interface{}{"count": benchIdentityPopulation})
	if err != nil {
		b.Fatal(err)
	}
	return exec
}

// BenchmarkListMembershipIdentityRead is the identity read of a mention
// match: identities named by ID, name key or alias key, at 7,300 identities.
func BenchmarkListMembershipIdentityRead(b *testing.B) {
	exec := newBenchIdentityExecutor(b)
	ctx := context.Background()
	for _, query := range []struct{ name, text string }{
		{"member", "MATCH (i:Identity) WHERE $alias IN i.alias_keys RETURN i.id"},
		{"any", "MATCH (i:Identity) WHERE any(k IN $names WHERE k IN i.alias_keys) RETURN i.id"},
		{"disjunction", "MATCH (i:GraphNode:Identity) WHERE coalesce(i.current, true) AND (i.id IN $ids OR i.name_key IN $names OR any(k IN $names WHERE k IN i.alias_keys)) RETURN i.id"},
	} {
		b.Run(query.name, func(b *testing.B) {
			exec.cache = nil
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				n := i % benchIdentityPopulation
				params := map[string]interface{}{
					"alias": fmt.Sprintf("alt%d", n),
					"ids":   []interface{}{fmt.Sprintf("id%d", (n+1)%benchIdentityPopulation)},
					"names": []interface{}{fmt.Sprintf("name%d", (n+2)%benchIdentityPopulation), fmt.Sprintf("alias%d", n), "nothing", fmt.Sprintf("shared%d", n%500)},
				}
				if _, err := exec.Execute(ctx, query.text, params); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkListMembershipIndexedWrite is the write cost of an index on a
// list property: create and update identities with alias lists.
func BenchmarkListMembershipIndexedWrite(b *testing.B) {
	exec := newBenchIdentityExecutor(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		params := map[string]interface{}{"id": fmt.Sprintf("w%d", i), "aliases": []interface{}{fmt.Sprintf("wa%d", i), fmt.Sprintf("wb%d", i), "shared1"}}
		if _, err := exec.Execute(ctx, "CREATE (:GraphNode:Identity {id: $id, name_key: $id, alias_keys: $aliases})", params); err != nil {
			b.Fatal(err)
		}
		if _, err := exec.Execute(ctx, "MATCH (i:Identity {id: $id}) SET i.alias_keys = i.alias_keys + ['late']", params); err != nil {
			b.Fatal(err)
		}
	}
}
