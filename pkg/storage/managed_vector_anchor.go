package storage

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
)

// B30: the managed vector anchor record.
//
// A node's managed embedding state - its chunk vectors and the embedding
// metadata record that describes them - lives in the embedding sidecar key
// space, apart from the node body, and the journal records changes of it as an
// OpUpdateEmbedding audit that names the node but carries no vectors
// (replayEmbeddingOutcome). That audit says only that whatever managed state
// replay restored for the node before it is stale. A store restored from a
// snapshot plus the journal after it therefore cannot restore an embedding the
// embedding worker acknowledged after that snapshot.
//
// The managed vector anchor is the record that carries that acknowledged state.
// It holds the node as the store had committed it when the vectors were
// acknowledged - the complete body the vectors were computed from (ID, labels,
// properties, timestamps, visibility) plus the managed chunk vectors and their
// metadata, with the caller's named vectors left out because replay does not
// restore them - and replay restores only the managed part of it onto the node
// the store holds (#889, #963 validations stay in their existing owner). An
// anchor never creates a node, never rewrites a body, and never regenerates a
// vector: when its recorded source is not the committed node, replay fails and
// changes nothing.
//
// An anchor is not a computed result in progress. It is appended after the
// store acknowledged the vectors, so its record is that already-acknowledged
// state, never a worker's pending work or a client's new image. It is
// therefore outside ordinary transaction admission: OpCreateNode and
// OpUpdateNode keep the standard 16 MiB frame limit, while an anchor's record
// may span frames under the journal's existing spanning owner (NOR-412)
// whatever record policy the WAL was opened with (walRecordSpansFrames).

// opManagedVectorAnchor is the journal record of one node's already
// acknowledged managed embedding state.
const opManagedVectorAnchor OperationType = "managed_vector_anchor"

// managedVectorAnchorVersion identifies the anchor payload layout this build
// writes and the only one it replays.
const managedVectorAnchorVersion uint8 = 1

// managedVectorAnchorBatchBytes bounds the encoded anchor records one batch
// holds before it is committed and released, so a caller that needs many
// anchors at once keeps a bounded working set and pays one journal sync per
// batch rather than one per anchor. One anchor whose own record exceeds the
// bound is still journaled: the bound is checked after a record is queued.
const managedVectorAnchorBatchBytes = 1 << 20

// A count bound also limits proof lookup work when individual images are tiny.
const managedVectorAnchorBatchEntries = 128

// ErrManagedVectorAnchorSource reports an anchor whose recorded source is not
// the node the store has committed: the node is gone, or its body changed
// after the vectors were acknowledged. Replay changes nothing for it - it
// neither creates the node nor restores vectors onto a body they were not
// computed from.
var ErrManagedVectorAnchorSource = errors.New("wal: managed vector anchor source is not the committed node")

// walManagedVectorAnchorPayload is the versioned payload of one anchor record.
// It is encoded through the journal's one recovery representation
// (marshalWALRecoveryPayload), so the source node keeps its stored property
// types, registered temporal types included, instead of being flattened to
// JSON.
type walManagedVectorAnchorPayload struct {
	// Version is the payload layout, managedVectorAnchorVersion.
	Version uint8 `msgpack:"version"`
	// SourceNode is the node as the store had committed it when the managed
	// vectors were acknowledged: the body they were computed from plus those
	// vectors and their metadata. NamedEmbeddings are not recorded because
	// replay does not restore them.
	SourceNode *Node `msgpack:"source_node"`
}

// managedVectorAnchorRecord prepares the one anchor record of before and
// returns it with the database it is journaled under. It is the anchor's one
// preparation owner, used by every writer of the record.
//
// before must be the node as the store has committed it (its ID is
// database-qualified, and its body, chunk vectors and embedding metadata are
// the acknowledged state); vectors still being computed are not an anchor.
func managedVectorAnchorRecord(ctx context.Context, before *Node) (walManagedVectorAnchorPayload, string, error) {
	var payload walManagedVectorAnchorPayload
	if ctx == nil {
		return payload, "", fmt.Errorf("wal: managed vector anchor requires a context: %w", ErrInvalidData)
	}
	if err := ctx.Err(); err != nil {
		return payload, "", err
	}
	if before == nil || before.ID == "" {
		return payload, "", fmt.Errorf("wal: managed vector anchor requires the committed source node: %w", ErrInvalidData)
	}
	// The embedding sidecar owner writes a node's managed state only for a
	// database-qualified ID (BadgerEngine.UpdateNodeEmbeddingSidecar), and
	// replay restores the anchor through that owner. An anchor for an ID it
	// would refuse could never be restored, so it is not journaled either.
	database, _, qualified := ParseDatabasePrefix(string(before.ID))
	if !qualified {
		return payload, "", fmt.Errorf("wal: managed vector anchor requires a database-qualified node ID %q: %w", before.ID, ErrInvalidID)
	}
	payload = walManagedVectorAnchorPayload{
		Version:    managedVectorAnchorVersion,
		SourceNode: managedVectorAnchorSource(database, before),
	}
	return payload, database, nil
}

// managedVectorAnchorSource returns the node image the anchor records: the
// committed body with the managed chunk vectors and metadata it carries, its
// named vectors left out, and its ID stored database-unqualified as every
// journal record stores it (replay supplies the entry's database).
func managedVectorAnchorSource(database string, before *Node) *Node {
	source := *before
	source.ID = NodeID(StripDatabasePrefix(database, string(before.ID)))
	source.NamedEmbeddings = nil
	return &source
}

// managedVectorAnchorProof is the reusable proof of one journaled anchor: the
// hashed node identity and the hash of the managed vector image it recorded.
// The proof is the same value the anchor cache stores, so a batch can hold the
// proofs of the anchors it has queued without holding any entity, vector slice
// or caller ID.
type managedVectorAnchorProof struct {
	identity [sha256.Size]byte
	digest   [sha256.Size]byte
}

// managedVectorAnchorBatch journals proven anchors for one caller through the
// journal's existing batch owner (BatchWriter), so the anchors of one
// transaction are numbered, framed, written and synced by the journal's one
// record order (encodeNextRecordLocked, appendRecordLocked, syncLocked)
// instead of one sync per anchor. Anchors are released in bounded batches
// (managedVectorAnchorBatchBytes), so the pending data stays bounded however
// many anchors the caller needs.
//
// A proof is reused only after its anchor's batch is durable: the proofs of a
// batch become cache entries after CommitWithSeq returned without error, and a
// batch that fails or is cancelled yields neither references nor proofs.
type managedVectorAnchorBatch struct {
	wal     *WAL
	batch   *BatchWriter
	pending []managedVectorAnchorProof
}

// newManagedVectorAnchorBatch starts the anchor batch of one caller.
func (w *WAL) newManagedVectorAnchorBatch() managedVectorAnchorBatch {
	return managedVectorAnchorBatch{wal: w}
}

// anchored reports whether the journal already holds a durable anchor for the
// image (identity, digest), and journals one when it does not. A true result
// means the anchor is durable before the caller returns from flush: an anchor
// this batch queued but has not committed yet is only a promise that flush
// keeps, and its proof is never remembered until then.
func (a *managedVectorAnchorBatch) anchored(ctx context.Context, before *Node, identity, digest [sha256.Size]byte) (bool, error) {
	if a.pendingProof(identity, digest) {
		return true, nil
	}
	// The anchor cache is read under the WAL mutex, its one owner, as before.
	if err := a.wal.lockAdmission(ctx); err != nil {
		return false, err
	}
	known := a.wal.managedVectorAnchors.contains(identity, digest, a.wal.managedVectorGeneration.Load())
	a.wal.mu.Unlock()
	if known {
		return true, nil
	}
	return a.append(ctx, before, identity, digest)
}

// append prepares before's anchor with the anchor's canonical preparation
// owner and queues it in this batch, committing the batch when its encoded
// pending data reaches managedVectorAnchorBatchBytes. The reports whether the
// record was queued: a journal that is not enabled writes nothing, so the
// caller must not emit a reference to an anchor that will never exist.
func (a *managedVectorAnchorBatch) append(ctx context.Context, before *Node, identity, digest [sha256.Size]byte) (bool, error) {
	payload, database, err := managedVectorAnchorRecord(ctx, before)
	if err != nil {
		return false, err
	}
	if a.batch == nil {
		// Allocate only when a missing image actually needs journaling, not
		// for ordinary writes or an entirely warm anchor working set.
		a.batch = &BatchWriter{wal: a.wal}
	}
	queued := a.batch.Len()
	if err := a.batch.append(opManagedVectorAnchor, &payload, database); err != nil {
		return false, fmt.Errorf("wal: failed to journal managed vector anchor: %w", err)
	}
	if a.batch.Len() == queued {
		return false, nil
	}
	a.pending = append(a.pending, managedVectorAnchorProof{identity: identity, digest: digest})
	if len(a.pending) < managedVectorAnchorBatchEntries && a.batch.pendingBytes() < managedVectorAnchorBatchBytes {
		return true, nil
	}
	return true, a.flush(ctx)
}

// flush commits this batch and remembers its proofs once the batch is durable.
// A commit that fails or a cancelled caller discards the batch: the records of
// a failed commit may be incomplete, and the caller's transaction fails, so no
// proof may outlive it. Anchors an earlier flush committed stay durable; they
// record acknowledged managed state whether or not the caller's transaction
// completes.
func (a *managedVectorAnchorBatch) flush(ctx context.Context) error {
	if a.batch == nil || a.batch.Len() == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		a.discard()
		return err
	}
	// An intervening vector audit or journal retirement invalidates reuse.
	// Never stamp an older anchor with the newer provenance epoch.
	generation := a.wal.managedVectorGeneration.Load()
	if _, _, err := a.batch.CommitWithSeq(); err != nil {
		a.discard()
		return err
	}
	proofs := a.pending
	a.pending = a.pending[:0]
	if len(proofs) == 0 {
		return nil
	}
	// The batch is durable: only now may its proofs be reused, at the epoch the
	// journal has reached after the write.
	if err := a.wal.lockAdmission(ctx); err != nil {
		return err
	}
	if a.wal.managedVectorGeneration.Load() != generation {
		a.wal.mu.Unlock()
		return nil
	}
	for _, proof := range proofs {
		a.wal.managedVectorAnchors.remember(proof.identity, proof.digest, generation)
	}
	a.wal.mu.Unlock()
	return nil
}

// discard drops the anchors this batch has not committed and the proofs that
// belong to them.
func (a *managedVectorAnchorBatch) discard() {
	if a.batch != nil {
		a.batch.Rollback()
	}
	a.pending = a.pending[:0]
}

// pendingProof reports whether this batch already queued an anchor for exactly
// this image, whose proof flush will remember once the batch is durable, so the
// same image is anchored once per batch rather than once per operation.
func (a *managedVectorAnchorBatch) pendingProof(identity, digest [sha256.Size]byte) bool {
	for _, proof := range a.pending {
		if proof.identity == identity && proof.digest == digest {
			return true
		}
	}
	return false
}

// replayManagedVectorAnchor restores the managed embedding state an anchor
// recorded, and only that: the node's current body, labels, timestamps, named
// vectors and visibility are all left as the store has them.
//
// The anchor's source body is validated against the committed node first, and
// nothing is written when it does not match: a missing node, different labels,
// properties, timestamps or visibility is an ErrManagedVectorAnchorSource
// failure, never a fabricated node or a regenerated vector. Only after that
// validation does the existing embedding sidecar owner bind the node's current
// runtime embedding identity: it reads the committed node itself, compares the
// embedding source, and derives the revision from that read, so replay cannot
// present a stale physical revision.
func replayManagedVectorAnchor(ctx context.Context, engine Engine, entry WALEntry) error {
	var payload walManagedVectorAnchorPayload
	if err := decodeRecoveryPayload(entry.Data, &payload); err != nil {
		return fmt.Errorf("wal: failed to unmarshal managed vector anchor: %w", err)
	}
	if payload.Version != managedVectorAnchorVersion {
		return fmt.Errorf("%w: managed vector anchor version %d", ErrWALUnsupportedVersion, payload.Version)
	}
	source := payload.SourceNode
	if source == nil || source.ID == "" {
		return fmt.Errorf("wal: managed vector anchor without its source node: %w", ErrInvalidData)
	}
	sidecar, ok := engine.(EmbeddingSidecarUpdater)
	if !ok {
		return fmt.Errorf("storage engine %T does not support embedding sidecar writes", engine)
	}
	stored, err := GetStoredNode(ctx, engine, source.ID)
	if err != nil {
		return fmt.Errorf("%w: node %q: %w", ErrManagedVectorAnchorSource, source.ID, err)
	}
	if err := validateManagedVectorAnchorSource(source, stored); err != nil {
		return err
	}
	// Preserve the current stored node's provenance, never the historical
	// anchor's runtime identity. The sidecar owner validates this source and
	// binds the current embedding revision before publishing its vectors.
	restore := &Node{
		runtime:         stored.runtime,
		ID:              stored.ID,
		Labels:          stored.Labels,
		Properties:      stored.Properties,
		CreatedAt:       stored.CreatedAt,
		UpdatedAt:       stored.UpdatedAt,
		ChunkEmbeddings: source.ChunkEmbeddings,
		EmbedMeta:       source.EmbedMeta,
	}
	if err := sidecar.UpdateNodeEmbeddingSidecar(ctx, restore); err != nil {
		return fmt.Errorf("wal: managed vector anchor restore for node %q: %w", source.ID, err)
	}
	return nil
}

// validateManagedVectorAnchorSource reports nil when source is the body the
// store holds for the anchor's node, and ErrManagedVectorAnchorSource naming
// the part that differs otherwise.
func validateManagedVectorAnchorSource(source, stored *Node) error {
	if !sameLabelSet(source.Labels, stored.Labels) {
		return fmt.Errorf("%w: node %q has other labels", ErrManagedVectorAnchorSource, source.ID)
	}
	if !sameStoredValue(source.Properties, stored.Properties) {
		return fmt.Errorf("%w: node %q has other properties", ErrManagedVectorAnchorSource, source.ID)
	}
	if !source.CreatedAt.Equal(stored.CreatedAt) || !source.UpdatedAt.Equal(stored.UpdatedAt) {
		return fmt.Errorf("%w: node %q has other timestamps", ErrManagedVectorAnchorSource, source.ID)
	}
	if source.RuntimeMetadata().VisibilitySuppressed != stored.RuntimeMetadata().VisibilitySuppressed {
		return fmt.Errorf("%w: node %q has another visibility", ErrManagedVectorAnchorSource, source.ID)
	}
	return nil
}

// advanceManagedVectorGeneration moves the journal's managed vector provenance
// epoch forward. It is called by the owners whose work can invalidate what was
// proven from the journal about a node's managed vectors: an
// OpUpdateEmbedding audit's acceptance or uncertainty, an embedding reset, and
// journal retirement. Callers read it to decide whether an earlier conclusion
// about managed vector state is still current; it only ever increases.
func (w *WAL) advanceManagedVectorGeneration() {
	if w == nil {
		return
	}
	w.managedVectorGeneration.Add(1)
}
