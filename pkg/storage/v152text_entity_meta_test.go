package storage

// v1.5.2 verification, built from the text of Plane item N15-50 (NOR-185): the
// access flusher's metadata reader GetEntityMeta returned ErrNotFound for a
// stored node hidden from user reads, because it used a visibility-filtered node
// read. Here the node is hidden by manual suppression (the same user-visibility
// filter; the item's case is decay). Expected: metadata found; ordinary reads
// still hide the node; a missing ID is still not found.

import (
	"context"
	"errors"
	"testing"
)

func TestV152TextEntityMetaForHiddenStoredNode(t *testing.T) {
	b, err := NewBadgerEngine(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ctx := context.Background()
	if _, err := b.CreateNode(ctx, &Node{ID: "test:target", Labels: []string{"Target"}, Properties: map[string]any{"k": "v"}}); err != nil {
		t.Fatal(err)
	}
	if err := b.SetManualSuppression(ctx, StoredEntityNodeKind, "test:target", true); err != nil {
		t.Fatal(err)
	}
	visible, visibleErr := b.GetNode(ctx, "test:target")
	meta, metaErr := b.GetEntityMeta(ctx, "test:target")
	_, missingErr := b.GetEntityMeta(ctx, "test:absent")
	t.Logf("V152TEXT NOR-185 user read node=%v err=%v; GetEntityMeta labels=%v keys=%v err=%v; absent err=%v", visible != nil, visibleErr, meta.Labels, meta.PropertyKeys, metaErr, missingErr)
	if metaErr != nil {
		t.Fatalf("GetEntityMeta did not resolve a stored, hidden node: %v", metaErr)
	}
	if len(meta.Labels) != 1 || meta.Labels[0] != "Target" {
		t.Fatalf("GetEntityMeta labels = %v, want [Target]", meta.Labels)
	}
	if !errors.Is(missingErr, ErrNotFound) {
		t.Fatalf("control: GetEntityMeta for a missing ID returned %v, want ErrNotFound", missingErr)
	}
}
