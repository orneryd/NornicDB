// Close-out lane L8, round 6 (2026-11-22): owner step for L-H5-001. The
// stress runs show a read returning count 0 for an existing node exactly when
// DROP INDEX overlaps it. The seek reads candidates through
// SchemaManager.PropertyIndexLookup (storage/schema.go:2186), called from
// propertyIndexLookup (transaction_storage_wrapper.go:63) by the planned seek
// (node_index_candidate_plan.go:473). This run calls that lookup for an
// existing node while the index exists and after it is dropped (and while a
// re-created index is still being filled): it answers (nil, nil) in both
// later cases, the same answer as "no node has this value", so a seek planned
// while the index existed returns no rows instead of falling back to a scan.
// Test-only, not for merge.
package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestL8Owner_H5_001_LookupOfVanishedIndex(t *testing.T) {
	ctx := context.Background()
	exec := l8Exec(t, "CREATE INDEX h5_rb IF NOT EXISTS FOR (n:H5RB) ON (n.key)", "CREATE (:H5RB {key: 'k1'})")
	schema := exec.storage.GetSchema()
	lookup := func(step string) {
		ids, err := schema.PropertyIndexLookup(ctx, "H5RB", "key", "k1")
		seekable, _ := schema.SeekableCompositeIndexesForLabel(ctx, "H5RB")
		names := []string{}
		for _, idx := range seekable {
			names = append(names, idx.Name)
		}
		o := l8Run(t, exec, "MATCH (n:H5RB {key: 'k1'}) RETURN count(n) AS c", nil)
		l8Owner(t, "L-H5-001", step, map[string]any{"PropertyIndexLookup_ids": len(ids), "PropertyIndexLookup_nil": ids == nil, "err": fmt.Sprint(err), "seekable": names, "statement_rows": o.Rows})
	}
	lookup("index present and filled")
	l8Run(t, exec, "DROP INDEX h5_rb IF EXISTS", nil)
	lookup("after DROP INDEX")
	parked, release := make(chan struct{}), make(chan struct{})
	armed := true
	restore := storage.SetSchemaIndexFillHookForTesting(func(name, label string) error {
		if armed && label == "H5RB" {
			armed = false
			close(parked)
			<-release
		}
		return nil
	})
	defer restore()
	done := make(chan struct{})
	go func() {
		_, _ = exec.Execute(ctx, "CREATE INDEX h5_rb IF NOT EXISTS FOR (n:H5RB) ON (n.key)", nil)
		close(done)
	}()
	<-parked
	ids, err := schema.PropertyIndexLookup(ctx, "H5RB", "key", "k1")
	l8Owner(t, "L-H5-001", "re-created index, fill parked", map[string]any{"PropertyIndexLookup_ids": len(ids), "PropertyIndexLookup_nil": ids == nil, "err": fmt.Sprint(err)})
	close(release)
	<-done
	lookup("re-created index filled")
	// For comparison: a label that never had an index gives the same answer.
	noIndex, noErr := schema.PropertyIndexLookup(ctx, "NoSuchLabel", "key", "k1")
	l8Owner(t, "L-H5-001", "label without an index", map[string]any{"PropertyIndexLookup_ids": len(noIndex), "PropertyIndexLookup_nil": noIndex == nil, "err": fmt.Sprint(noErr)})
}
