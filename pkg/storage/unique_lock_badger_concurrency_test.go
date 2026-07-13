package storage

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

const (
	badgerUniqueLockWriters       = 8
	badgerUniqueLockKeysPerWriter = 100
)

// TestBadgerUniqueConstraintCommit_DisjointFunctionBatchesAreParallel proves
// the exact lock contract through real BadgerTransaction.Commit calls. The
// retained Eshu route updates existing Function nodes with disjoint uid values,
// so unrelated writers must all reach commit-time UNIQUE validation together.
func TestBadgerUniqueConstraintCommit_DisjointFunctionBatchesAreParallel(t *testing.T) {
	engine := newTestEngine(t)
	schema := engine.GetSchemaForNamespace("test")
	if err := schema.AddUniqueConstraint("function_uid", "Function", "uid"); err != nil {
		t.Fatalf("AddUniqueConstraint: %v", err)
	}

	seedBadgerUniqueFunctions(t, engine)
	if err := engine.rebuildUniqueConstraintValues("test", schema); err != nil {
		t.Fatalf("rebuildUniqueConstraintValues: %v", err)
	}

	transactions := make([]*BadgerTransaction, badgerUniqueLockWriters)
	for writer := range badgerUniqueLockWriters {
		tx, err := engine.BeginTransaction()
		if err != nil {
			t.Fatalf("BeginTransaction writer %d: %v", writer, err)
		}
		if err := tx.SetDeferredConstraintValidation(true); err != nil {
			t.Fatalf("SetDeferredConstraintValidation writer %d: %v", writer, err)
		}
		for key := range badgerUniqueLockKeysPerWriter {
			node := badgerUniqueFunctionNode(writer, key, 1)
			if err := tx.UpdateNode(node); err != nil {
				t.Fatalf("UpdateNode writer %d key %d: %v", writer, key, err)
			}
		}
		transactions[writer] = tx
	}

	schema.mu.RLock()
	constraint := schema.uniqueConstraints["Function:uid"]
	schema.mu.RUnlock()
	if constraint == nil {
		t.Fatal("Function.uid UNIQUE constraint missing")
	}
	constraint.mu.Lock()
	constraint.valuesCacheComplete = false
	constraint.mu.Unlock()

	enteredValidation := make(chan struct{}, len(transactions))
	releaseValidation := make(chan struct{})
	restoreHook := setUniqueConstraintScanHook(func() {
		select {
		case enteredValidation <- struct{}{}:
		default:
		}
		<-releaseValidation
	})
	defer restoreHook()

	start := make(chan struct{})
	errors := make(chan error, len(transactions))
	var wg sync.WaitGroup
	for _, tx := range transactions {
		tx := tx
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errors <- tx.Commit()
		}()
	}
	close(start)

	admitted := 0
	deadline := time.NewTimer(2 * time.Second)
collect:
	for admitted < len(transactions) {
		select {
		case <-enteredValidation:
			admitted++
		case <-deadline.C:
			break collect
		}
	}
	close(releaseValidation)
	if !deadline.Stop() {
		select {
		case <-deadline.C:
		default:
		}
	}
	wg.Wait()
	close(errors)

	if admitted != len(transactions) {
		t.Fatalf("real disjoint Badger commits entered UNIQUE validation concurrently = %d/%d, want %d/%d", admitted, len(transactions), len(transactions), len(transactions))
	}
	for err := range errors {
		if err != nil {
			t.Fatalf("disjoint Badger commit: %v", err)
		}
	}
	assertBadgerUniqueFunctionExactness(t, engine, schema, 1)
}

func TestBadgerUniqueConstraintCommit_SameUIDRetryPreservesExactness(t *testing.T) {
	engine := newTestEngine(t)
	schema := engine.GetSchemaForNamespace("test")
	if err := schema.AddUniqueConstraint("function_uid", "Function", "uid"); err != nil {
		t.Fatalf("AddUniqueConstraint: %v", err)
	}

	nodeID := NodeID("test:same-uid")
	uid := "content-entity:same-uid"
	if _, err := engine.CreateNode(&Node{
		ID:         nodeID,
		Labels:     []string{"Function"},
		Properties: map[string]interface{}{"uid": uid, "revision": 0},
	}); err != nil {
		t.Fatalf("CreateNode seed: %v", err)
	}
	if err := engine.rebuildUniqueConstraintValues("test", schema); err != nil {
		t.Fatalf("rebuildUniqueConstraintValues: %v", err)
	}

	type commitResult struct {
		revision int
		err      error
	}
	transactions := make([]*BadgerTransaction, 2)
	for index := range transactions {
		tx, err := engine.BeginTransaction()
		if err != nil {
			t.Fatalf("BeginTransaction revision %d: %v", index+1, err)
		}
		if err := tx.SetDeferredConstraintValidation(true); err != nil {
			t.Fatalf("SetDeferredConstraintValidation revision %d: %v", index+1, err)
		}
		if err := tx.UpdateNode(&Node{
			ID:         nodeID,
			Labels:     []string{"Function"},
			Properties: map[string]interface{}{"uid": uid, "revision": index + 1},
		}); err != nil {
			t.Fatalf("UpdateNode revision %d: %v", index+1, err)
		}
		transactions[index] = tx
	}

	start := make(chan struct{})
	results := make(chan commitResult, len(transactions))
	for index, tx := range transactions {
		revision := index + 1
		tx := tx
		go func() {
			<-start
			results <- commitResult{revision: revision, err: tx.Commit()}
		}()
	}
	close(start)

	var winner, loser commitResult
	for range transactions {
		result := <-results
		if result.err == nil {
			if winner.revision != 0 {
				t.Fatalf("same-UID initial commits both succeeded: revisions %d and %d", winner.revision, result.revision)
			}
			winner = result
		} else {
			if loser.revision != 0 {
				t.Fatalf("same-UID initial commits both failed: %v; %v", loser.err, result.err)
			}
			loser = result
		}
	}
	if winner.revision == 0 || loser.revision == 0 {
		t.Fatalf("same-UID initial result winner=%+v loser=%+v, want one of each", winner, loser)
	}
	var violation *ConstraintViolationError
	if !errors.As(loser.err, &violation) || violation.Type != ConstraintUnique {
		t.Fatalf("same-UID loser error = %T %v, want UNIQUE ConstraintViolationError", loser.err, loser.err)
	}
	if !errors.Is(loser.err, ErrConflict) {
		t.Fatalf("same-UID loser error = %v, want ErrConflict cause", loser.err)
	}
	if violation.Label != "Function" || len(violation.Properties) != 1 || violation.Properties[0] != "uid" {
		t.Fatalf("same-UID violation target = %s.%v, want Function.[uid]", violation.Label, violation.Properties)
	}

	retry, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction retry: %v", err)
	}
	if err := retry.SetDeferredConstraintValidation(true); err != nil {
		t.Fatalf("SetDeferredConstraintValidation retry: %v", err)
	}
	if err := retry.UpdateNode(&Node{
		ID:         nodeID,
		Labels:     []string{"Function"},
		Properties: map[string]interface{}{"uid": uid, "revision": loser.revision},
	}); err != nil {
		t.Fatalf("UpdateNode retry: %v", err)
	}
	if err := retry.Commit(); err != nil {
		t.Fatalf("Commit retry: %v", err)
	}

	nodes, err := engine.GetNodesByLabel("Function")
	if err != nil {
		t.Fatalf("GetNodesByLabel Function: %v", err)
	}
	if len(nodes) != 1 || nodes[0].ID != nodeID || nodes[0].Properties["uid"] != uid || nodes[0].Properties["revision"] != loser.revision {
		t.Fatalf("same-UID final nodes = %#v, want one node %s at retry revision %d", nodes, nodeID, loser.revision)
	}
	cachedID, found, constrained := schema.LookupUniqueConstraintValue("Function", "uid", uid)
	if !constrained || !found || cachedID != nodeID {
		t.Fatalf("same-UID cache = (%s, %v, %v), want (%s, true, true)", cachedID, found, constrained, nodeID)
	}
}

func BenchmarkBadgerUniqueConstraintCommit_DisjointFunctionBatches(b *testing.B) {
	engine, err := NewBadgerEngineInMemory()
	if err != nil {
		b.Fatalf("NewBadgerEngineInMemory: %v", err)
	}
	b.Cleanup(func() { _ = engine.Close() })
	schema := engine.GetSchemaForNamespace("test")
	if err := schema.AddUniqueConstraint("function_uid", "Function", "uid"); err != nil {
		b.Fatalf("AddUniqueConstraint: %v", err)
	}
	seedBadgerUniqueFunctions(b, engine)
	if err := engine.rebuildUniqueConstraintValues("test", schema); err != nil {
		b.Fatalf("rebuildUniqueConstraintValues: %v", err)
	}
	b.ReportMetric(badgerUniqueLockWriters, "writers")
	b.ReportMetric(badgerUniqueLockKeysPerWriter, "keys/writer")
	b.ResetTimer()

	for iteration := range b.N {
		b.StopTimer()
		transactions := make([]*BadgerTransaction, badgerUniqueLockWriters)
		for writer := range badgerUniqueLockWriters {
			tx, err := engine.BeginTransaction()
			if err != nil {
				b.Fatalf("BeginTransaction writer %d: %v", writer, err)
			}
			if err := tx.SetDeferredConstraintValidation(true); err != nil {
				b.Fatalf("SetDeferredConstraintValidation writer %d: %v", writer, err)
			}
			for key := range badgerUniqueLockKeysPerWriter {
				if err := tx.UpdateNode(badgerUniqueFunctionNode(writer, key, iteration+1)); err != nil {
					b.Fatalf("UpdateNode writer %d key %d: %v", writer, key, err)
				}
			}
			transactions[writer] = tx
		}

		start := make(chan struct{})
		errors := make(chan error, len(transactions))
		var wg sync.WaitGroup
		for _, tx := range transactions {
			tx := tx
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				errors <- tx.Commit()
			}()
		}
		b.StartTimer()
		close(start)
		wg.Wait()
		b.StopTimer()
		close(errors)
		for err := range errors {
			if err != nil {
				b.Fatalf("disjoint Badger commit: %v", err)
			}
		}
	}
}

func seedBadgerUniqueFunctions(t testing.TB, engine *BadgerEngine) {
	t.Helper()
	tx, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction seed: %v", err)
	}
	if err := tx.SetDeferredConstraintValidation(true); err != nil {
		t.Fatalf("SetDeferredConstraintValidation seed: %v", err)
	}
	for writer := range badgerUniqueLockWriters {
		for key := range badgerUniqueLockKeysPerWriter {
			if _, err := tx.CreateNode(badgerUniqueFunctionNode(writer, key, 0)); err != nil {
				t.Fatalf("CreateNode seed writer %d key %d: %v", writer, key, err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit seed: %v", err)
	}
}

func badgerUniqueFunctionNode(writer, key, revision int) *Node {
	return &Node{
		ID:     NodeID(fmt.Sprintf("test:function-%02d-%03d", writer, key)),
		Labels: []string{"Function"},
		Properties: map[string]interface{}{
			"uid":      fmt.Sprintf("content-entity:writer-%02d:key-%03d", writer, key),
			"writer":   writer,
			"key":      key,
			"revision": revision,
		},
	}
}

func assertBadgerUniqueFunctionExactness(t *testing.T, engine *BadgerEngine, schema *SchemaManager, revision int) {
	t.Helper()
	nodes, err := engine.GetNodesByLabel("Function")
	if err != nil {
		t.Fatalf("GetNodesByLabel Function: %v", err)
	}
	wantCount := badgerUniqueLockWriters * badgerUniqueLockKeysPerWriter
	if len(nodes) != wantCount {
		t.Fatalf("Function count = %d, want %d", len(nodes), wantCount)
	}
	seenUIDs := make(map[string]NodeID, len(nodes))
	for _, node := range nodes {
		uid, ok := node.Properties["uid"].(string)
		if !ok || uid == "" {
			t.Fatalf("node %s uid = %#v, want non-empty string", node.ID, node.Properties["uid"])
		}
		if previous, duplicate := seenUIDs[uid]; duplicate {
			t.Fatalf("duplicate Function uid %q on %s and %s", uid, previous, node.ID)
		}
		seenUIDs[uid] = node.ID
		if node.Properties["revision"] != revision {
			t.Fatalf("node %s revision = %#v, want %d", node.ID, node.Properties["revision"], revision)
		}
		cachedID, found, constrained := schema.LookupUniqueConstraintValue("Function", "uid", uid)
		if !constrained || !found || cachedID != node.ID {
			t.Fatalf("Function.uid cache for %q = (%s, %v, %v), want (%s, true, true)", uid, cachedID, found, constrained, node.ID)
		}
	}
}
