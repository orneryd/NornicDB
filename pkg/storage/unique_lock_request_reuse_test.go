package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// A release callback can outlive its transaction. Calling it again after the
// request buffer is reused must never release a newer owner's locks.
func TestUniqueLockRequestReusePreservesOwnership(t *testing.T) {
	schema := NewSchemaManager()
	keys := make([]uniqueConstraintLockKey, 100)
	for i := range keys {
		keys[i] = uniqueConstraintLockKey{entityKind: 1, entityID: fmt.Sprintf("db:node-%d", i)}
	}
	first, err := schema.acquireUniqueConstraintCommitLocks(context.Background(), "first", keys)
	if err != nil {
		t.Fatal(err)
	}
	first()
	second, err := schema.acquireUniqueConstraintCommitLocks(context.Background(), "second", keys)
	if err != nil {
		t.Fatal(err)
	}
	first() // Must not release any request subsequently acquired by second.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if release, err := schema.acquireUniqueConstraintCommitLocks(ctx, "third", keys[:1]); !errors.Is(err, context.Canceled) {
		if release != nil {
			release()
		}
		second()
		t.Fatalf("third owner acquired an already held key: %v", err)
	}
	second()
	third, err := schema.acquireUniqueConstraintCommitLocks(context.Background(), "third", keys)
	if err != nil {
		t.Fatal(err)
	}
	third()
}

// A canceled acquisition must release every registry reference, including
// requests that were never acquired before the blocked key.
func TestUniqueLockRequestReuseAfterCanceledAcquisition(t *testing.T) {
	schema := NewSchemaManager()
	keys := make([]uniqueConstraintLockKey, 100)
	for i := range keys {
		keys[i] = uniqueConstraintLockKey{entityKind: 1, entityID: fmt.Sprintf("db:node-%d", i)}
	}
	// The first key gets the lower registry order and is reentrant for the
	// canceled owner. The second key blocks, after that first hold is taken.
	prior, err := schema.acquireUniqueConstraintCommitLocks(context.Background(), "canceled", keys[:1])
	if err != nil {
		t.Fatal(err)
	}
	defer prior()
	held, err := schema.acquireUniqueConstraintCommitLocks(context.Background(), "holder", keys[1:2])
	if err != nil {
		t.Fatal(err)
	}
	defer held()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if release, err := schema.acquireUniqueConstraintCommitLocks(ctx, "canceled", keys); !errors.Is(err, context.Canceled) {
		if release != nil {
			release()
		}
		t.Fatalf("canceled owner should fail after the reentrant hold: %v", err)
	}
	if release, err := schema.acquireUniqueConstraintCommitLocks(ctx, "other", keys[:1]); !errors.Is(err, context.Canceled) {
		if release != nil {
			release()
		}
		t.Fatalf("cleanup released the owner's original hold: %v", err)
	}
	other, err := schema.acquireUniqueConstraintCommitLocks(context.Background(), "other", keys[2:])
	if err != nil {
		t.Fatal(err)
	}
	other()
	prior()
	held()
	other, err = schema.acquireUniqueConstraintCommitLocks(context.Background(), "other", keys)
	if err != nil {
		t.Fatal(err)
	}
	other()
}

// Exercise the enlarged pool boundary with the same endpoint-to-edge ratio
// as a large relationship batch, including shared endpoint duplicates.
func TestUniqueLockRequestReuseLargeRelationshipBatch(t *testing.T) {
	const count = 8000
	edges := make([]EdgeID, count)
	endpoints := make([]NodeID, 0, count*2)
	for i := range edges {
		edges[i] = EdgeID(fmt.Sprintf("db:edge-%d", i))
		endpoints = append(endpoints, NodeID(fmt.Sprintf("db:node-%d", i)), "db:shared")
	}
	keys := entityCommitKeys(nil, edges, endpoints)
	schema := NewSchemaManager()
	first, err := schema.acquireUniqueConstraintCommitLocks(context.Background(), "first", keys)
	if err != nil {
		t.Fatal(err)
	}
	first()
	second, err := schema.acquireUniqueConstraintCommitLocks(context.Background(), "second", keys)
	if err != nil {
		t.Fatal(err)
	}
	first()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if release, err := schema.acquireUniqueConstraintCommitLocks(ctx, "third", entityCommitKeys([]NodeID{"db:shared"}, nil, nil)); !errors.Is(err, context.Canceled) {
		if release != nil {
			release()
		}
		second()
		t.Fatalf("shared endpoint was released by an old callback: %v", err)
	}
	second()
}
