//go:build closeout_l2_e53_6

package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// E53.6: in a namespace whose name contains a colon ("a:b"), a node created
// through the storage API is missed by the executor's first
// MATCH (n:T) RETURN count(n). The parser is the process's (NORNICDB_PARSER).

type l2E53_6Fixture struct {
	store *storage.NamespacedEngine
	exec  *StorageExecutor
}

func l2E53_6New(t *testing.T, namespace string, cache QueryCachePolicy) l2E53_6Fixture {
	t.Helper()
	engine, err := testfixture.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	store := storage.NewNamespacedEngine(engine, namespace)
	return l2E53_6Fixture{store: store, exec: NewStorageExecutor(store, cache)}
}

func l2E53_6Count(t *testing.T, exec *StorageExecutor) interface{} {
	t.Helper()
	res, err := exec.Execute(context.Background(), "MATCH (n:T) RETURN count(n) AS c", nil)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if len(res.Rows) != 1 || len(res.Rows[0]) != 1 {
		return res.Rows
	}
	return res.Rows[0][0]
}

func l2E53_6CreateAndCount(t *testing.T, namespace string, cache QueryCachePolicy) (interface{}, l2E53_6Fixture) {
	t.Helper()
	f := l2E53_6New(t, namespace, cache)
	if _, err := f.store.CreateNode(context.Background(), &storage.Node{ID: "one", Labels: []string{"T"}}); err != nil {
		t.Fatalf("CreateNode: %v", err)
	}
	return l2E53_6Count(t, f.exec), f
}

func TestL2_e53_6_Repro(t *testing.T) {
	got, _ := l2E53_6CreateAndCount(t, "a:b", QueryCachePolicy{MaxEntries: 100, TTL: time.Hour})
	verdict := "OK"
	if got != int64(1) {
		verdict = "DEFECT"
	}
	t.Logf("L2RESULT E53.6 %s parser=%s namespace=a:b first_count=%v (%T)", verdict, config.GetParserType(), got, got)
	if verdict == "DEFECT" {
		t.Errorf("first count in namespace a:b = %v, want 1", got)
	}
}

// Control: the same steps in a namespace without a colon count 1.
func TestL2_e53_6_Control(t *testing.T) {
	got, _ := l2E53_6CreateAndCount(t, "ab", QueryCachePolicy{MaxEntries: 100, TTL: time.Hour})
	t.Logf("control namespace=ab first_count=%v", got)
	if got != int64(1) {
		t.Errorf("control: count = %v, want 1", got)
	}
}

// Cause: which reader disagrees. For "a:b" it logs the storage-side answers
// (NodeCountByLabel, GetNodesByLabel, NodeCount, AllNodes and the prefixed
// ID), the executor's count with the result cache on (first and second run)
// and off, so the stale source (label counter keyed by the namespace parsed at
// the first ':' vs. the query cache vs. the scan) is visible.
func TestL2_e53_6_Cause(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct {
		name  string
		cache QueryCachePolicy
	}{
		{"cache-on", QueryCachePolicy{MaxEntries: 100, TTL: time.Hour}},
		{"cache-off", QueryCachePolicy{MaxEntries: 0, TTL: 0}},
	} {
		first, f := l2E53_6CreateAndCount(t, "a:b", c.cache)
		second := l2E53_6Count(t, f.exec)
		byLabel, byLabelErr := f.store.NodeCountByLabel(ctx, "T")
		nodes, nodesErr := f.store.GetNodesByLabel(ctx, "T")
		total, totalErr := f.store.NodeCount(ctx)
		all, allErr := f.store.AllNodes(ctx)
		t.Logf("cause parser=%s %s prefixed=%s first=%v second=%v NodeCountByLabel=%d(%v) GetNodesByLabel=%d(%v) NodeCount=%d(%v) AllNodes=%d(%v)",
			config.GetParserType(), c.name, f.store.PrefixNodeID("one"), first, second, byLabel, byLabelErr, len(nodes), nodesErr, total, totalErr, len(all), allErr)
	}
}
