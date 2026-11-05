package storage

// NOR-136 cost qualification: native write-behind acknowledgement (ACK) and
// explicit-drain cost on a deterministic, fixed-size update workload.
//
// This file is intended to be copied unchanged onto the paired baseline
// revision. It reuses the package's canonical implicit buffered commit fixture
// (bufferedWrite: BeginTransaction -> SetImplicit -> SetNamespace ->
// UpdateGraphNode -> Commit) as the representative ACK workload. No production
// path, fixture or assertion is reimplemented here.
//
// The graph is fixed. nor136BatchSize nodes are created once, and each timed
// iteration rewrites exactly that disjoint node set with one changed property.
// The payload bytes never change, so SetBytes reports a stable representative
// record size and no iteration grows the node population.
//
// Timed boundaries:
//
//	nodes=<n>/buffered-ack
//	    Commit returns after the write-behind acknowledgement. Buffer retention
//	    is bounded by drains that run with the timer stopped; they are not part
//	    of the measured ACK cost.
//	nodes=<n>/buffered-ack-drain
//	    Commit acknowledgement plus one explicit synchronous
//	    FlushWriteBehind(context.Background()) replay inside the timer. This is
//	    the ACK + physical persistence boundary.
//	nodes=<n>/synchronous
//	    The physical persistence control: identical statements with write-behind disabled, so
//	    Commit itself persists to Badger. Its explicit drain is a no-op.
//
// Correctness is checked untimed after every run: the buffer is empty, the
// physical node count equals the fixed population, and every seeded node
// carries the final version and unchanged payload in its physical body. A cost
// result therefore cannot hide lost, reordered or duplicated writes.

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

const (
	// nor136VersionKey is the only property that changes per iteration.
	nor136VersionKey = "version"
	// nor136PayloadSize keeps every record the same deterministic size.
	nor136PayloadSize = 128
	// nor136RetentionOps bounds the ACK-only buffer between retention drains.
	// It only controls retained memory; the drain is excluded from the timer.
	nor136RetentionOps = 1024
)

// nor136Variant selects one timed boundary.
type nor136Variant struct {
	name     string
	buffered bool
	drain    bool
}

// BenchmarkNOR136WriteBehindCost reports ACK-only, ACK+drain and synchronous
// control cost for the same fixed update batches.
func BenchmarkNOR136WriteBehindCost(b *testing.B) {
	payload := strings.Repeat("x", nor136PayloadSize)
	for _, batchSize := range []int{1, 64} {
		b.Run(fmt.Sprintf("nodes=%d", batchSize), func(b *testing.B) {
			for _, variant := range []nor136Variant{
				{name: "buffered-ack", buffered: true},
				{name: "buffered-ack-drain", buffered: true, drain: true},
				{name: "synchronous"},
			} {
				b.Run(variant.name, func(b *testing.B) {
					nor136Run(b, batchSize, payload, variant)
				})
			}
		})
	}
}

// nor136Run builds one fixed fixture, proves its commit route, measures the
// requested boundary, then verifies the drained physical state outside timing.
func nor136Run(b *testing.B, batchSize int, payload string, variant nor136Variant) {
	b.Helper()

	options := writeBehindTestOptions{manualDrain: true, wal: true}
	if !variant.buffered {
		options.synchronous = true
	}
	engine := newWriteBehindTestEngine(b, time.Hour, options)
	if variant.buffered {
		if engine.writeBehind == nil {
			b.Fatal("buffered variant has no write-behind buffer")
		}
	} else if engine.writeBehind != nil {
		b.Fatal("synchronous control unexpectedly has a write-behind buffer")
	}

	ctx := context.Background()
	ids := make([]NodeID, batchSize)
	for i := range ids {
		ids[i] = NodeID(fmt.Sprintf("test:nor136-%03d", i))
		// Seeding is direct and synchronous on both variants, so both start
		// from the identical physical population.
		if _, err := engine.CreateNode(ctx, &Node{
			ID:     ids[i],
			Labels: []string{"NOR136Cost"},
			Properties: map[string]any{
				"payload":        payload,
				nor136VersionKey: int64(-1),
			},
		}); err != nil {
			b.Fatal(err)
		}
	}

	round := int64(-2)
	update := func(tx *BadgerTransaction) error {
		for _, id := range ids {
			node, err := tx.GetNode(ctx, id)
			if err != nil {
				return fmt.Errorf("read %s: %w", id, err)
			}
			before := CopyNode(node)
			node.Properties[nor136VersionKey] = round
			if err := tx.UpdateGraphNode(ctx, NodeUpdateRequest{Before: before, After: node, Policy: ClientNodeEdits}); err != nil {
				return fmt.Errorf("update %s: %w", id, err)
			}
		}
		return nil
	}

	// Route probe before the timer. The probe writes a distinct sentinel so it
	// can prove the buffered acknowledgement is visible before the drain and
	// physical after it. The timed loop rewrites the sentinel with 0..b.N-1.
	bufferedWrite(b, engine, update)
	if variant.buffered {
		if pending := engine.writeBehind.PendingOps(); pending <= 0 {
			b.Fatalf("probe commit bypassed write-behind (pending=%d)", pending)
		}
		// ACK boundary: the acknowledged image is visible before any drain.
		acknowledged, err := engine.GetNode(ctx, ids[0])
		if err != nil {
			b.Fatalf("read acknowledged image before drain: %v", err)
		}
		if got := fmt.Sprint(acknowledged.Properties[nor136VersionKey]); got != fmt.Sprint(round) {
			b.Fatalf("acknowledged image before drain=%s want=%v", got, round)
		}
		if err := engine.FlushWriteBehind(ctx); err != nil {
			b.Fatalf("probe drain: %v", err)
		}
		if pending := engine.writeBehind.PendingOps(); pending != 0 {
			b.Fatalf("probe drain left pending=%d", pending)
		}
		// Drain boundary: the acknowledged image is now in physical storage.
		drained, err := engine.GetStoredNode(ctx, ids[0])
		if err != nil {
			b.Fatalf("read physical image after drain: %v", err)
		}
		if got := fmt.Sprint(drained.Properties[nor136VersionKey]); got != fmt.Sprint(round) {
			b.Fatalf("physical image after drain=%s want=%v", got, round)
		}
	}
	round = -1

	// One measured operation is one buffered commit of batchSize records. The
	// reported byte count is the deterministic user-property update payload.
	b.SetBytes(int64(batchSize) * int64(nor136PayloadSize+8))
	b.ReportMetric(float64(batchSize), "records/op")
	b.ReportAllocs()
	b.ResetTimer()

	retentionEvery := nor136RetentionOps / batchSize
	if retentionEvery < 1 {
		retentionEvery = 1
	}
	for i := 0; i < b.N; i++ {
		round = int64(i)
		bufferedWrite(b, engine, update)
		switch {
		case variant.drain:
			if err := engine.FlushWriteBehind(ctx); err != nil {
				b.Fatalf("drain iteration %d: %v", i, err)
			}
		case variant.buffered && (i+1)%retentionEvery == 0:
			// Retention control for the ACK-only boundary. The replay bounds
			// retained generations but is explicitly not part of ACK cost.
			b.StopTimer()
			if err := engine.FlushWriteBehind(ctx); err != nil {
				b.Fatalf("retention drain iteration %d: %v", i, err)
			}
			b.StartTimer()
		}
	}
	b.StopTimer()

	// Explicit final drain and untimed truth checks.
	if err := engine.FlushWriteBehind(ctx); err != nil {
		b.Fatalf("final drain: %v", err)
	}
	wantVersion := int64(-2)
	if b.N > 0 {
		wantVersion = int64(b.N) - 1
	}
	nor136Verify(b, engine, ids, payload, wantVersion, variant)
}

// nor136Verify proves the timed workload persisted exactly the expected fixed
// population. It runs after the timer is stopped.
func nor136Verify(b *testing.B, engine *BadgerEngine, ids []NodeID, payload string, wantVersion int64, variant nor136Variant) {
	b.Helper()
	ctx := context.Background()

	if variant.buffered {
		if pending := engine.writeBehind.PendingOps(); pending != 0 {
			b.Fatalf("final drain left pending=%d acknowledged operations", pending)
		}
	}
	count, err := engine.NodeCount(ctx)
	if err != nil {
		b.Fatalf("node count: %v", err)
	}
	if count != int64(len(ids)) {
		b.Fatalf("graph node count=%d want=%d: the timed workload must not grow the graph", count, len(ids))
	}

	// Count the physical node keys directly, so an overlay-only result cannot
	// satisfy the check.
	physical := 0
	if err := engine.db.View(func(txn *badger.Txn) error {
		options := badger.DefaultIteratorOptions
		options.PrefetchValues = false
		iterator := txn.NewIterator(options)
		defer iterator.Close()
		prefix := []byte{prefixNode}
		for iterator.Seek(prefix); iterator.ValidForPrefix(prefix); iterator.Next() {
			physical++
		}
		return nil
	}); err != nil {
		b.Fatalf("physical node scan: %v", err)
	}
	if physical != len(ids) {
		b.Fatalf("physical node count=%d want=%d: acknowledged writes are missing or duplicated", physical, len(ids))
	}

	for _, id := range ids {
		node, err := engine.GetStoredNode(ctx, id)
		if err != nil {
			b.Fatalf("physical read %s: %v", id, err)
		}
		// The stored codec may decode an integer property as any integer type;
		// compare the rendered value, as the package's existing write controls
		// do, instead of coupling correctness to one decoder representation.
		if fmt.Sprint(node.Properties[nor136VersionKey]) != fmt.Sprint(wantVersion) {
			b.Fatalf("%s physical version=%v want=%d: acknowledged write was lost", id, node.Properties[nor136VersionKey], wantVersion)
		}
		if node.Properties["payload"] != payload {
			b.Fatalf("%s physical payload changed", id)
		}
	}
}
