package storage

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/orneryd/nornicdb/pkg/config"
)

// managedVectorReference names the exact coordinates omitted from one semantic
// UPDATE. Metadata remains complete in the update. Change is its zero-based
// operation index, so references never depend on map iteration order.
type managedVectorReference struct {
	Change uint32
	Digest [sha256.Size]byte
}

// managedVectorFingerprint identifies coordinates by float bits and array
// shape. It does not identify embedding metadata: the semantic record carries
// that metadata in full. Nil and empty arrays, chunk boundaries, signed zero
// and NaN payloads remain distinct. Cancellation is checked between bounded
// blocks, independently of the size of one vector.
func managedVectorFingerprint(ctx context.Context, chunks [][]float32) ([sha256.Size]byte, error) {
	var result [sha256.Size]byte
	if err := ctx.Err(); err != nil {
		return result, err
	}
	digest := sha256.New()
	_, _ = digest.Write([]byte("nornicdb/managed-vector-image/v1\x00"))
	var header [9]byte
	if chunks != nil {
		header[0] = 1
	}
	binary.LittleEndian.PutUint64(header[1:], uint64(len(chunks)))
	_, _ = digest.Write(header[:])
	var block [4096]byte
	for _, chunk := range chunks {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		header[0] = 0
		if chunk != nil {
			header[0] = 1
		}
		binary.LittleEndian.PutUint64(header[1:], uint64(len(chunk)))
		_, _ = digest.Write(header[:])
		for start := 0; start < len(chunk); {
			if err := ctx.Err(); err != nil {
				return result, err
			}
			count := min(len(chunk)-start, len(block)/4)
			for offset, value := range chunk[start : start+count] {
				binary.LittleEndian.PutUint32(block[offset*4:], math.Float32bits(value))
			}
			_, _ = digest.Write(block[:count*4])
			start += count
		}
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	digest.Sum(result[:0])
	return result, nil
}

// walManagedAnchorCache is bounded proof reuse, not recovery authority. A miss
// causes a new complete anchor before any reference is emitted. Hash keys avoid
// retaining caller ID strings. The WAL mutex owns every access; no entity,
// vector slice, property value or variable-size backing array is retained.
const walManagedAnchorCacheSlots = 64

type walManagedAnchorSlot struct {
	identity   [sha256.Size]byte
	digest     [sha256.Size]byte
	generation uint64
	valid      bool
}

type walManagedAnchorCache struct {
	slots [walManagedAnchorCacheSlots]walManagedAnchorSlot
	next  uint8
}

func (cache *walManagedAnchorCache) contains(identity, digest [sha256.Size]byte, generation uint64) bool {
	for index := range cache.slots {
		slot := &cache.slots[index]
		if slot.valid && slot.identity == identity && slot.generation == generation && slot.digest == digest {
			return true
		}
	}
	return false
}

func (cache *walManagedAnchorCache) remember(identity, digest [sha256.Size]byte, generation uint64) {
	for index := range cache.slots {
		if cache.slots[index].valid && cache.slots[index].identity == identity {
			cache.slots[index] = walManagedAnchorSlot{identity: identity, digest: digest, generation: generation, valid: true}
			return
		}
	}
	cache.slots[cache.next] = walManagedAnchorSlot{identity: identity, digest: digest, generation: generation, valid: true}
	cache.next = (cache.next + 1) % walManagedAnchorCacheSlots
}

// ManagedVectorReferences carries validated WAL-local image references from a
// decoded transaction into accepted staging. Its zero value means full images.
// Callers cannot construct references to arbitrary storage state.
type ManagedVectorReferences struct{ entries []managedVectorReference }

// managedReferences journals a durable anchor for every image a transaction
// omits and returns the references its record can carry instead of those
// coordinates.
//
// The anchors of one call go through the anchor batch owner
// (managedVectorAnchorBatch): prepared by the anchor's canonical preparation
// owner, numbered, framed and written by the journal's batch writer in bounded
// batches, and committed before this call returns. A reference is emitted only
// for an image whose anchor this call left durable, so no record ever omits
// coordinates the journal could not restore, and no proof of an anchor reaches
// the journal's cache before the anchor itself is on disk.
func (w *WAL) managedReferences(ctx context.Context, operations []Operation, admission func() error) (ManagedVectorReferences, error) {
	var references ManagedVectorReferences
	if !config.IsWALEnabled() {
		return references, nil
	}
	checkedAdmission := false
	anchors := w.newManagedVectorAnchorBatch()
	for index := range operations {
		operation := &operations[index]
		if operation.Type != OpUpdateNode || !operation.nodeAnchorEligible || operation.Node == nil || operation.OldNode == nil || len(operation.Node.ChunkEmbeddings) == 0 {
			continue
		}
		before, err := managedVectorFingerprint(ctx, operation.OldNode.ChunkEmbeddings)
		if err != nil {
			anchors.discard()
			return ManagedVectorReferences{}, err
		}
		after, err := managedVectorFingerprint(ctx, operation.Node.ChunkEmbeddings)
		if err != nil {
			anchors.discard()
			return ManagedVectorReferences{}, err
		}
		if before != after {
			continue
		}
		// Removing vectors must not skip admission formerly triggered by the
		// full record's size. Validate the complete replay image before even an
		// anchor is appended, through the existing native admission owner.
		if !checkedAdmission {
			if err := admission(); err != nil {
				anchors.discard()
				return ManagedVectorReferences{}, err
			}
			checkedAdmission = true
		}
		anchored, err := anchors.anchored(ctx, operation.OldNode, sha256.Sum256([]byte(operation.Node.ID)), before)
		if err != nil {
			anchors.discard()
			return ManagedVectorReferences{}, err
		}
		if !anchored {
			continue
		}
		references.entries = append(references.entries, managedVectorReference{Change: uint32(index), Digest: before})
	}
	// Every anchor an emitted reference relies on is durable before the
	// reference leaves this owner.
	if err := anchors.flush(ctx); err != nil {
		return ManagedVectorReferences{}, err
	}
	return references, nil
}

// resolve binds omitted coordinates only to the exact predecessor visible in
// this accepted transaction. Receipt observation happens before this call, so a
// duplicate replay never needs an image already replaced by the same command.
func (references ManagedVectorReferences) resolve(ctx context.Context, tx *BadgerTransaction, operations []Operation) ([]Operation, error) {
	if len(references.entries) == 0 {
		return operations, nil
	}
	resolved := append([]Operation(nil), operations...)
	for _, reference := range references.entries {
		if int(reference.Change) >= len(resolved) {
			return nil, ErrInvalidData
		}
		operation := &resolved[reference.Change]
		if operation.Type != OpUpdateNode || operation.Node == nil || operation.Node.ChunkEmbeddings != nil {
			return nil, ErrInvalidData
		}
		predecessor, err := tx.GetNode(ctx, operation.Node.ID)
		if err != nil {
			return nil, fmt.Errorf("WAL vector predecessor %q: %w", operation.Node.ID, err)
		}
		digest, err := managedVectorFingerprint(ctx, predecessor.ChunkEmbeddings)
		if err != nil {
			return nil, err
		}
		if digest != reference.Digest {
			return nil, fmt.Errorf("WAL vector predecessor %q does not match its recorded image: %w", operation.Node.ID, ErrInvalidData)
		}
		image := *operation.Node
		image.ChunkEmbeddings = predecessor.ChunkEmbeddings
		operation.Node = &image
	}
	return resolved, nil
}
