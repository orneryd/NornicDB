package cypher

// A repeated MATCH (n:Doc {kind: $v}) never returns a stale label membership
// (NOR-407). The scan behind it reads through the node read cache and, once
// it repeats, a remembered walk of the label; the label index stays the one
// owner of label membership, so every write that changes it is seen by the
// next statement: in auto-commit, inside the writing transaction, and by
// others only once the transaction commits.

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRepeatedLabelScanSeesEveryLabelWrite(t *testing.T) {
	engine := newTestMemoryEngine(t).BadgerEngine
	session := func(database string) *StorageExecutor {
		return NewStorageExecutor(storage.NewNamespacedEngine(engine, database), QueryCachePolicy{MaxEntries: 0, TTL: 0})
	}
	writer, reader, elsewhere := session("nornic"), session("nornic"), session("other")
	run := func(exec *StorageExecutor, query string) {
		t.Helper()
		_, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err, query)
	}
	// documents is what the scan returns, read three times: a first scan, a
	// scan that repeats, and a scan of what the repeat kept all agree.
	documents := func(exec *StorageExecutor) []string {
		t.Helper()
		var ids []string
		for round := 0; round < 3; round++ {
			result, err := exec.Execute(context.Background(), "MATCH (n:Doc {kind: 'k'}) RETURN n.id AS id", nil)
			require.NoError(t, err)
			got := make([]string, 0, len(result.Rows))
			for _, row := range result.Rows {
				got = append(got, fmt.Sprint(row[0]))
			}
			sort.Strings(got)
			if round > 0 {
				require.Equal(t, ids, got, "round %d", round)
			}
			ids = got
		}
		return ids
	}
	// The label is a small part of the database, as a label the scan reads
	// through the cache is.
	want := map[string]bool{}
	for i := 0; i < 12; i++ {
		run(writer, fmt.Sprintf("CREATE (:Doc {id: 'd%d', kind: 'k'})", i))
		want[fmt.Sprintf("d%d", i)] = true
	}
	for i := 0; i < 40; i++ {
		run(writer, fmt.Sprintf("CREATE (:Other {id: 'o%d', kind: 'k'})", i))
	}
	expected := func() []string {
		ids := make([]string, 0, len(want))
		for id, present := range want {
			if present {
				ids = append(ids, id)
			}
		}
		sort.Strings(ids)
		return ids
	}
	check := func(step string) {
		t.Helper()
		require.Equal(t, expected(), documents(reader), step)
		require.Equal(t, expected(), documents(writer), step+" (the writing session)")
	}
	check("the fixture")

	run(writer, "CREATE (:Doc {id: 'created', kind: 'k'})")
	want["created"] = true
	check("a node created with the label")

	run(writer, "MATCH (n:Doc {id: 'd3'}) DETACH DELETE n")
	want["d3"] = false
	check("a node of the label deleted")

	run(writer, "MATCH (n:Other {id: 'o1'}) SET n:Doc")
	want["o1"] = true
	check("the label added to a node")

	run(writer, "MATCH (n:Doc {id: 'd5'}) REMOVE n:Doc")
	want["d5"] = false
	check("the label removed from a node")

	run(writer, "MATCH (n:Doc {id: 'd6'}) SET n.kind = 'archived'")
	want["d6"] = false
	check("a property the pattern tests changed")
	run(writer, "MATCH (n:Doc {id: 'd6'}) SET n.kind = 'k'")
	want["d6"] = true
	check("and changed back")

	// Another database has its own label.
	run(elsewhere, "CREATE (:Doc {id: 'elsewhere', kind: 'k'})")
	check("a node created in another database")
	require.Equal(t, []string{"elsewhere"}, documents(elsewhere))
	_, _, err := engine.DeleteByPrefix("other:")
	require.NoError(t, err)
	require.Empty(t, documents(elsewhere), "the other database dropped")
	check("another database dropped")

	// A transaction that commits: its writes are its own until then.
	run(writer, "BEGIN")
	run(writer, "CREATE (:Doc {id: 'committed', kind: 'k'})")
	run(writer, "MATCH (n:Doc {id: 'd7'}) DETACH DELETE n")
	run(writer, "MATCH (n:Other {id: 'o2'}) SET n:Doc")
	run(writer, "MATCH (n:Doc {id: 'd8'}) REMOVE n:Doc")
	require.Equal(t, expected(), documents(reader), "an open transaction's writes are not another session's")
	want["committed"], want["d7"], want["o2"], want["d8"] = true, false, true, false
	require.Equal(t, expected(), documents(writer), "a transaction sees its own writes")
	run(writer, "COMMIT")
	check("the transaction committed")

	// A transaction that rolls back changes nothing.
	run(writer, "BEGIN")
	run(writer, "CREATE (:Doc {id: 'rolled back', kind: 'k'})")
	run(writer, "MATCH (n:Doc {id: 'd9'}) DETACH DELETE n")
	run(writer, "MATCH (n:Other {id: 'o3'}) SET n:Doc")
	run(writer, "MATCH (n:Doc {id: 'd10'}) REMOVE n:Doc")
	inTransaction := append([]string{"o3", "rolled back"}, expected()...)
	sort.Strings(inTransaction)
	for _, gone := range []string{"d9", "d10"} {
		index := sort.SearchStrings(inTransaction, gone)
		inTransaction = append(inTransaction[:index], inTransaction[index+1:]...)
	}
	require.Equal(t, inTransaction, documents(writer), "a transaction sees its own writes")
	require.Equal(t, expected(), documents(reader), "an open transaction's writes are not another session's")
	run(writer, "ROLLBACK")
	check("the transaction rolled back")
}
