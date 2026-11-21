//go:build closeout_l2_e39

package storage

// Report entry 39: a node with 17,000 chunk vectors of 256 values
// (17,408,000 bytes of vector data) is read through the journal engine, one
// property is added and the node is passed back with
// UpdateNode(NodeUpdateRequest{After: node}), vectors untouched. With the
// standard journal record policy the update was refused (record > one 16 MiB
// frame) and the property not stored; with the large-record policy it
// committed and the journal grew by 21,811,800 bytes. Expected: flag = true is
// stored and the journal grows by about the size of the change.

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"testing"
	"time"
)

const (
	l2E39Chunks = 17000
	l2E39Dims   = 256
	// l2E39VectorBytes is the float32 vector data kept beside the node.
	l2E39VectorBytes = l2E39Chunks * l2E39Dims * 4
	// l2E39GrowthLimit separates the defect from the fix: the defect journals
	// the complete image (>= the 17.4 MB of vectors; 21.8 MB measured), a
	// one-property change journals at most a few KiB. Half the vector data
	// leaves a margin of more than 8 MB on both sides.
	l2E39GrowthLimit = l2E39VectorBytes / 2
)

func l2E39DirSize(t *testing.T, dir string) int64 {
	t.Helper()
	var total int64
	err := filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	if err != nil {
		t.Fatalf("journal size: %v", err)
	}
	return total
}

type l2E39Result struct {
	updateErr error
	growth    int64
	flag      any
	chunks    int
}

// l2E39Run builds the entry's engine, node and vectors under records, then
// performs the one-property update with the request build returns.
func l2E39Run(t *testing.T, records WALRecordPolicy, build func(before, after *Node) NodeUpdateRequest) l2E39Result {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true, synchronous: true, manualDrain: true, records: records})
	b.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Include: []string{"text"}})
	wal := b.bufferedCommitWAL
	w := NewWALEngine(b, wal)
	if _, err := w.CreateNode(ctx, &Node{ID: "test:n", Properties: map[string]any{"text": "alpha"}}); err != nil {
		t.Fatalf("create: %v", err)
	}
	n, err := b.GetNode(ctx, "test:n")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	vectors := make([][]float32, l2E39Chunks)
	for i := range vectors {
		v := make([]float32, l2E39Dims)
		for j := range v {
			v[j] = float32(i*l2E39Dims+j) / 7
		}
		vectors[i] = v
	}
	n.ChunkEmbeddings = vectors
	n.EmbedMeta = map[string]any{"has_embedding": true, "chunk_count": l2E39Chunks}
	if err := w.UpdateNodeEmbeddingSidecar(ctx, n); err != nil {
		t.Fatalf("sidecar: %v", err)
	}
	n, err = w.GetNode(ctx, "test:n")
	if err != nil {
		t.Fatalf("get through journal engine: %v", err)
	}
	if len(n.ChunkEmbeddings) != l2E39Chunks {
		t.Fatalf("precondition: node read through the journal engine has %d chunks, want %d", len(n.ChunkEmbeddings), l2E39Chunks)
	}
	before := copyNode(n)
	n.Properties["flag"] = true
	walDir := filepath.Join(dir, "wal")
	_ = wal.Sync()
	sizeBefore := l2E39DirSize(t, walDir)
	var r l2E39Result
	r.updateErr = w.UpdateNode(ctx, build(before, n))
	_ = wal.Sync()
	r.growth = l2E39DirSize(t, walDir) - sizeBefore
	got, err := b.GetNode(ctx, "test:n")
	if err != nil {
		t.Fatalf("get after update: %v", err)
	}
	r.flag = got.Properties["flag"]
	r.chunks = len(got.ChunkEmbeddings)
	return r
}

func l2E39Blind(_, after *Node) NodeUpdateRequest { return NodeUpdateRequest{After: after} }

func TestL2_e39_Repro(t *testing.T) {
	std := l2E39Run(t, WALRecordsStandard, l2E39Blind)
	large := l2E39Run(t, WALRecordsLarge, l2E39Blind)
	figures := fmt.Sprintf("vectorBytes=%d threshold=%d standard{err=%v flag=%v chunks=%d growth=%d} large{err=%v flag=%v chunks=%d growth=%d}",
		l2E39VectorBytes, l2E39GrowthLimit, std.updateErr, std.flag, std.chunks, std.growth, large.updateErr, large.flag, large.chunks, large.growth)
	var problems []string
	for _, c := range []struct {
		name string
		r    l2E39Result
	}{{"standard", std}, {"large", large}} {
		if c.r.updateErr != nil || c.r.flag != true {
			problems = append(problems, c.name+": flag not stored")
		}
		if c.r.chunks != l2E39Chunks {
			problems = append(problems, fmt.Sprintf("%s: %d chunks after update", c.name, c.r.chunks))
		}
		if c.r.growth >= l2E39GrowthLimit {
			problems = append(problems, fmt.Sprintf("%s: journal grew %d bytes", c.name, c.r.growth))
		}
	}
	if len(problems) > 0 {
		t.Logf("L2RESULT E39 DEFECT %s", figures)
		t.Errorf("one-property UpdateNode: %v", problems)
		return
	}
	t.Logf("L2RESULT E39 OK %s", figures)
}

// Control: the same node without vectors (a one-property update of a plain
// node) is stored and journals little under both policies.
func TestL2_e39_Control(t *testing.T) {
	for _, records := range []WALRecordPolicy{WALRecordsStandard, WALRecordsLarge} {
		ctx := context.Background()
		dir := t.TempDir()
		b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true, synchronous: true, manualDrain: true, records: records})
		b.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Include: []string{"text"}})
		w := NewWALEngine(b, b.bufferedCommitWAL)
		if _, err := w.CreateNode(ctx, &Node{ID: "test:p", Properties: map[string]any{"text": "alpha"}}); err != nil {
			t.Fatalf("create: %v", err)
		}
		n, err := w.GetNode(ctx, "test:p")
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		n.Properties["flag"] = true
		walDir := filepath.Join(dir, "wal")
		_ = b.bufferedCommitWAL.Sync()
		sizeBefore := l2E39DirSize(t, walDir)
		updateErr := w.UpdateNode(ctx, NodeUpdateRequest{After: n})
		_ = b.bufferedCommitWAL.Sync()
		growth := l2E39DirSize(t, walDir) - sizeBefore
		got, err := b.GetNode(ctx, "test:p")
		if err != nil {
			t.Fatalf("get after: %v", err)
		}
		t.Logf("control records=%d err=%v flag=%v growth=%d", records, updateErr, got.Properties["flag"], growth)
		if updateErr != nil || got.Properties["flag"] != true || growth >= 1<<20 {
			t.Errorf("control records=%d: err=%v flag=%v growth=%d", records, updateErr, got.Properties["flag"], growth)
		}
	}
}

// Cause: entry 39 says a request without Before takes the blind route
// (WALEngine.UpdateNode -> engine UpdateNode, wal_mutations.go:212-230 at the
// pin), whose commit journals an entity_commit built from the complete node
// image, while a request with Before (as DB.UpdateNode sends) is routed through
// entity preparation, where managedReferences (managed_vector_reference.go:124)
// can replace unchanged vectors with a reference. This logs the route
// decision for both requests, the size and chunk count of the entity_commit
// record each update journals (large-record policy), and the journal growth.
func TestL2_e39_Cause(t *testing.T) {
	for _, c := range []struct {
		name  string
		build func(before, after *Node) NodeUpdateRequest
	}{
		{"blind (no Before)", l2E39Blind},
		{"observed (Before set)", func(before, after *Node) NodeUpdateRequest {
			return NodeUpdateRequest{Before: before, After: after}
		}},
	} {
		b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{wal: true, synchronous: true, manualDrain: true, records: WALRecordsLarge})
		w := NewWALEngine(b, b.bufferedCommitWAL)
		_, _, _, blindRequired := w.entityPreparationRoute("test:n", false)
		_, _, _, observedRequired := w.entityPreparationRoute("test:n", true)
		t.Logf("route: entityPreparationRoute(observed=false) required=%v, (observed=true) required=%v", blindRequired, observedRequired)

		r := l2E39Run(t, WALRecordsLarge, c.build)
		t.Logf("%s: err=%v flag=%v chunks=%d growth=%d", c.name, r.updateErr, r.flag, r.chunks, r.growth)
	}
	// The last entity_commit record of a blind update, decoded.
	ctx := context.Background()
	dir := t.TempDir()
	b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true, synchronous: true, manualDrain: true, records: WALRecordsLarge})
	b.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Include: []string{"text"}})
	w := NewWALEngine(b, b.bufferedCommitWAL)
	if _, err := w.CreateNode(ctx, &Node{ID: "test:c", Properties: map[string]any{"text": "alpha"}}); err != nil {
		t.Fatalf("create: %v", err)
	}
	n, err := b.GetNode(ctx, "test:c")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	const chunks = 64
	n.ChunkEmbeddings = make([][]float32, chunks)
	for i := range n.ChunkEmbeddings {
		v := make([]float32, l2E39Dims)
		for j := range v {
			v[j] = float32(i*l2E39Dims+j) / 7
		}
		n.ChunkEmbeddings[i] = v
	}
	n.EmbedMeta = map[string]any{"has_embedding": true, "chunk_count": chunks}
	if err := w.UpdateNodeEmbeddingSidecar(ctx, n); err != nil {
		t.Fatalf("sidecar: %v", err)
	}
	n, err = w.GetNode(ctx, "test:c")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	n.Properties["flag"] = true
	if err := w.UpdateNode(ctx, NodeUpdateRequest{After: n}); err != nil {
		t.Logf("blind update: %v", err)
	}
	_ = b.bufferedCommitWAL.Sync()
	entries, err := ReadWALEntriesFromDir(ctx, filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatalf("read journal: %v", err)
	}
	for i := len(entries) - 1; i >= 0; i-- {
		if entries[i].Operation != OpEntityCommit {
			continue
		}
		var data walEntityCommitData
		if err := decodeRecoveryPayload(entries[i].Data, &data); err != nil {
			t.Fatalf("decode: %v", err)
		}
		_, ops, err := DecodeEntityTransaction(data.Payload)
		if err != nil {
			t.Fatalf("decode envelope: %v", err)
		}
		for _, op := range ops {
			chunkCount := 0
			if op.Node != nil {
				chunkCount = len(op.Node.ChunkEmbeddings)
			}
			t.Logf("last entity_commit: %d bytes of data; op %s node chunks=%d (of %d stored)", len(entries[i].Data), op.Type, chunkCount, chunks)
		}
		break
	}
}

// l2E39Records lists the journal records after seq: operation, data size, and
// for an entity_commit the vector references it carries and the chunk count of
// each node image in it. It returns the last sequence seen.
func l2E39Records(t *testing.T, walDir string, after uint64, label string) uint64 {
	t.Helper()
	last := after
	var total int
	err := VisitWALEntriesAfterFromDir(context.Background(), walDir, after, func(e WALEntry) error {
		last = e.Sequence
		total += len(e.Data)
		line := fmt.Sprintf("L2CAUSE E39 %s seq=%d op=%s data=%d B", label, e.Sequence, e.Operation, len(e.Data))
		if e.Operation == OpEntityCommit {
			var data walEntityCommitData
			if err := decodeRecoveryPayload(e.Data, &data); err != nil {
				line += fmt.Sprintf(" decodeErr=%v", err)
			} else if envelope, ops, err := DecodeEntityTransaction(data.Payload); err != nil {
				line += fmt.Sprintf(" envelopeErr=%v", err)
			} else {
				line += fmt.Sprintf(" vectorReferences=%d", len(envelope.VectorReferences.entries))
				for _, op := range ops {
					chunks := 0
					if op.Node != nil {
						chunks = len(op.Node.ChunkEmbeddings)
					}
					line += fmt.Sprintf(" [%s node=%s chunks=%d]", op.Type, op.NodeID, chunks)
				}
			}
		}
		t.Log(line)
		return nil
	})
	if err != nil {
		t.Fatalf("visit journal: %v", err)
	}
	t.Logf("L2CAUSE E39 %s total record data %d B", label, total)
	return last
}

// TestL2_e39_Cause2: the 17,000-chunk node of the Repro (large-record
// policy), updated twice with one property each. For each update it lists the
// records the update appended. Reading (pin): WALEngine.UpdateNode
// (wal_mutations.go:212-230) passes the blind request to
// BadgerEngine.UpdateNode, whose operation is {OpUpdateNode, Node: image,
// OldNode: existingNode (decodeNodeWithEmbeddings, with all vectors),
// nodeAnchorEligible: true} (badger_nodes.go:550, :623); the commit's
// prepareNativeWAL (native_wal_publication.go:40) calls
// WAL.managedReferences (managed_vector_reference.go:124), which, for an
// unchanged vector fingerprint, journals the old image's vectors once as a
// managed_vector_anchor record (managed_vector_anchor.go:161-201) and lets the
// entity_commit carry a reference instead of the vectors. If that holds, the
// first update's growth is the anchor (full vectors, once per node image) and
// the second update journals only a small entity_commit; if instead the
// entity_commit itself carries 17,000 chunks, the image is not replaced.
// It also computes the fingerprints managedReferences compares for the
// stored node and the updated image.
func TestL2_e39_Cause2(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true, synchronous: true, manualDrain: true, records: WALRecordsLarge})
	b.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Include: []string{"text"}})
	wal := b.bufferedCommitWAL
	w := NewWALEngine(b, wal)
	walDir := filepath.Join(dir, "wal")
	if _, err := w.CreateNode(ctx, &Node{ID: "test:n", Properties: map[string]any{"text": "alpha"}}); err != nil {
		t.Fatalf("create: %v", err)
	}
	n, err := b.GetNode(ctx, "test:n")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	vectors := make([][]float32, l2E39Chunks)
	for i := range vectors {
		v := make([]float32, l2E39Dims)
		for j := range v {
			v[j] = float32(i*l2E39Dims+j) / 7
		}
		vectors[i] = v
	}
	n.ChunkEmbeddings = vectors
	n.EmbedMeta = map[string]any{"has_embedding": true, "chunk_count": l2E39Chunks}
	if err := w.UpdateNodeEmbeddingSidecar(ctx, n); err != nil {
		t.Fatalf("sidecar: %v", err)
	}
	_ = wal.Sync()
	seq := l2E39Records(t, walDir, 0, "setup")

	for round, value := range []any{true, "second"} {
		n, err = w.GetNode(ctx, "test:n")
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if len(n.ChunkEmbeddings) != l2E39Chunks {
			t.Fatalf("precondition: %d chunks before update %d", len(n.ChunkEmbeddings), round+1)
		}
		old := copyNode(n)
		n.Properties["flag"] = value
		oldPrint, err1 := managedVectorFingerprint(ctx, old.ChunkEmbeddings)
		newPrint, err2 := managedVectorFingerprint(ctx, n.ChunkEmbeddings)
		t.Logf("L2CAUSE E39 update%d fingerprint stored==image: %v (errs %v %v)", round+1, oldPrint == newPrint && err1 == nil && err2 == nil, err1, err2)
		sizeBefore := l2E39DirSize(t, walDir)
		if err := w.UpdateNode(ctx, NodeUpdateRequest{After: n}); err != nil {
			t.Logf("L2CAUSE E39 update%d error: %v", round+1, err)
		}
		_ = wal.Sync()
		t.Logf("L2CAUSE E39 update%d journal growth %d B (vector data %d B)", round+1, l2E39DirSize(t, walDir)-sizeBefore, l2E39VectorBytes)
		seq = l2E39Records(t, walDir, seq, fmt.Sprintf("update%d", round+1))
	}
}
