package storage

import (
		"fmt"
	"os"
	"path/filepath"
		"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func ei4ind1790Engine(t *testing.T, wal bool) *BadgerEngine {
	t.Helper()
	b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{wal: wal, synchronous: true, manualDrain: true})
	b.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Include: []string{"text"}})
	return b
}

func ei4ind1790Embed(t *testing.T, engine EmbeddingSidecarUpdater, b *BadgerEngine, value float32) {
	t.Helper()
	n, err := b.GetNode(t.Context(), "test:n")
	require.NoError(t, err)
	n = CopyNode(n)
	n.ChunkEmbeddings = [][]float32{{value, 2, 3}}
	n.EmbedMeta = map[string]any{"has_embedding": true, "chunk_count": 1, "embedding_model": "model-B"}
	require.NoError(t, engine.UpdateNodeEmbeddingSidecar(t.Context(), n))
}

func ei4ind1790Prepare(t *testing.T, b *BadgerEngine, id string, edits ...func(*Node)) (*PreparedEntityCommit, []byte) {
	t.Helper()
	tx, err := BeginEntityTransaction(t.Context(), b, CommitModeExplicit)
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	for _, edit := range edits {
		n, err := tx.GetNode(t.Context(), "test:n")
		require.NoError(t, err)
		n = CopyNode(n)
		edit(n)
		require.NoError(t, tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{After: n}))
	}
	p, err := tx.PrepareEntityCommit(t.Context(), EntityCommitRequest{LogicalID: id})
	require.NoError(t, err)
	payload, err := EncodeEntityTransaction(EntityTransactionRequest{Prepared: p})
	require.NoError(t, err)
	return p, payload
}

// A snapshot followed by a worker sidecar write and a user update is an
// ordinary restore sequence. The restored user update must retain the actual
// vectors kept on the source, rather than the snapshot's older or empty set.
func TestEI4Ind1790SnapshotThenWorkerThenUpdate(t *testing.T) {
	for _, before := range []float32{0, 10} {
		t.Run(map[bool]string{true: "snapshot-empty", false: "snapshot-old"}[before == 0], func(t *testing.T) {
			b := ei4ind1790Engine(t, true)
			w := b.bufferedCommitWAL
			journal := NewWALEngine(b, w)
			_, err := journal.CreateNode(t.Context(), &Node{ID: "test:n", Labels: []string{"Doc"}, Properties: map[string]any{"text": "alpha"}})
			require.NoError(t, err)
			if before != 0 {
				ei4ind1790Embed(t, b, b, before)
			}
			snapshot := filepath.Join(t.TempDir(), "snapshot.bin")
			require.Positive(t,w.Sequence())
			require.NoError(t, SaveStreamingSnapshot(t.Context(), b, snapshot, SnapshotOptions{Sequence: w.Sequence()}))
			require.NoError(t,w.ManageSnapshot(t.Context(),w.Sequence(),WALSnapshotTruncate))
			ei4ind1790Embed(t, journal, b, 99)
			p, payload := ei4ind1790Prepare(t, b, "after-worker", func(n *Node) { n.Properties["flag"] = true; n.Labels = append(n.Labels, "Hidden") })
			envelope, _, err := DecodeEntityTransaction(payload)
			require.NoError(t, err)
			receipt, err := journal.SubmitEntityCommit(t.Context(), p, nil)
			require.NoError(t, err)
			require.NoError(t, receipt.Wait(t.Context()))
			want, err := b.GetNode(t.Context(), "test:n")
			require.NoError(t, err)
			require.Equal(t, float32(99), want.ChunkEmbeddings[0][0])
			require.NoError(t, b.Close())
			require.NoError(t, w.Close())
			dest := ei4ind1790Engine(t, false)
			result, _, err := RecoverIntoEngine(t.Context(), dest, w.Config().Dir, snapshot)
			require.NoError(t, err)
			require.Zero(t, result.Failed, "%+v", result.Errors)
			got, err := dest.GetNode(t.Context(), "test:n")
			require.NoError(t, err)
			t.Logf("recordVersion=%d sourceVectors=%v restoredVectors=%v sourceMeta=%v restoredMeta=%v", envelope.Version, want.ChunkEmbeddings, got.ChunkEmbeddings, want.EmbedMeta, got.EmbedMeta)
			require.Equal(t, true, got.Properties["flag"])
			require.Contains(t, got.Labels, "Hidden")
			require.Equal(t, want.ChunkEmbeddings, got.ChunkEmbeddings)
			require.Equal(t, want.EmbedMeta["has_embedding"], got.EmbedMeta["has_embedding"])
			require.Equal(t, want.EmbedMeta["embedding_model"], got.EmbedMeta["embedding_model"])
			require.EqualValues(t, want.EmbedMeta["chunk_count"], got.EmbedMeta["chunk_count"])
		})
	}
}

func TestEI4Ind1790NativeBlindKeptVectorUpdate(t *testing.T) {
 for _,policy:=range []WALRecordPolicy{WALRecordsStandard,WALRecordsLarge}{t.Run(fmt.Sprintf("policy%d",policy),func(t *testing.T){
	b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{wal: true, synchronous: true, manualDrain: true, records: policy})
	b.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Include: []string{"text"}})
	w:=NewWALEngine(b,b.bufferedCommitWAL)
	_,err:=w.CreateNode(t.Context(),&Node{ID:"test:n",Properties:map[string]any{"text":"alpha"}})
	require.NoError(t,err)
	n,err:=b.GetNode(t.Context(),"test:n")
	require.NoError(t,err)
	n.ChunkEmbeddings=make([][]float32,17000)
	for i:=range n.ChunkEmbeddings {
		n.ChunkEmbeddings[i]=make([]float32,256)
		for j:=range n.ChunkEmbeddings[i] {n.ChunkEmbeddings[i][j]=float32(i*256+j)/7}
	}
	n.EmbedMeta=map[string]any{"has_embedding":true,"chunk_count":17000}
	require.NoError(t,w.UpdateNodeEmbeddingSidecar(t.Context(),n))
	n,err=w.GetNode(t.Context(),"test:n")
	require.NoError(t,err)
	require.Len(t,n.ChunkEmbeddings,17000)
	n.Properties["flag"]=true
	dirSize:=func()int64{var total int64;_=filepath.Walk(b.bufferedCommitWAL.Config().Dir,func(_ string,info os.FileInfo,err error)error{if err==nil&&!info.IsDir(){total+=info.Size()};return nil});return total}
	before:=dirSize()
	err=w.UpdateNode(t.Context(),NodeUpdateRequest{After:n})
	t.Logf("NATIVE-BLIND journal bytes written by the property-only update=%d (vector bytes=%d)",dirSize()-before,17000*256*4)
	got,gerr:=b.GetNode(t.Context(),"test:n");require.NoError(t,gerr);t.Logf("NATIVE-BLIND after: flag=%v chunks=%d",got.Properties["flag"],len(got.ChunkEmbeddings))
	t.Logf("NATIVE-BLIND version=%d retainedChunks=%d property-only update=%v",EntityTransactionVersion,len(n.ChunkEmbeddings),err)
	require.NoError(t,err,"an unchanged vector set in a native read/modify/write must not outgrow the journal")
})}
}
