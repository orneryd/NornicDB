package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCommitBuffer_NodeRecreationSupersedesDeletion(t *testing.T) {
	for _, sequence := range []string{"separate-commits", "one-commit", "repeated-recreation"} {
		t.Run(sequence, func(t *testing.T) {
			old := &Node{ID: "test:recreated", Labels: []string{"Person"}, Properties: map[string]any{"version": int64(1)}}
			newest := &Node{ID: old.ID, Labels: []string{"Person"}, Properties: map[string]any{"version": int64(2)}}
			peer := &Node{ID: "test:still-deleted", Labels: []string{"Person"}}
			generation := newCommitBuffer()
			generation.AddCommit(bufferedCommit{ops: []Operation{{Type: OpDeleteNode, NodeID: peer.ID, OldNode: peer}}})
			deleted := Operation{Type: OpDeleteNode, NodeID: old.ID, OldNode: old, DeletedEdgeIDs: []EdgeID{"test:detached"}}
			created := Operation{Type: OpCreateNode, Node: newest}
			if sequence == "one-commit" {
				generation.AddCommit(bufferedCommit{ops: []Operation{deleted, created}})
			} else {
				generation.AddCommit(bufferedCommit{ops: []Operation{deleted}})
				generation.AddCommit(bufferedCommit{ops: []Operation{created}})
			}
			if sequence == "repeated-recreation" {
				generation.AddCommit(bufferedCommit{ops: []Operation{{Type: OpDeleteNode, NodeID: newest.ID, OldNode: newest}}})
				newest = &Node{ID: old.ID, Labels: []string{"Person"}, Properties: map[string]any{"version": int64(3)}}
				generation.AddCommit(bufferedCommit{ops: []Operation{{Type: OpCreateNode, Node: newest}}})
			}
			buffer := &WriteBehindBuffer{active: generation}
			t.Run("point-read", func(t *testing.T) {
				got, found, deleted := generation.getNode(newest.ID)
				require.True(t, found)
				require.False(t, deleted, "a later create supersedes the earlier node tombstone")
				require.Equal(t, newest, got)
			})
			t.Run("overlay", func(t *testing.T) {
				nodes, touched := buffer.LabelOverlay("Person")
				require.True(t, touched[newest.ID])
				require.True(t, touched[peer.ID])
				require.Equal(t, []*Node{newest}, nodes)
			})
			t.Run("label-nodes", func(t *testing.T) {
				require.Equal(t, []*Node{newest}, buffer.LabelNodes("Person"))
			})
			t.Run("tombstones", func(t *testing.T) {
				require.NotContains(t, generation.deletes, string(newest.ID))
				require.NotContains(t, generation.deletedNodeIDs, newest.ID)
				require.Contains(t, generation.deletes, string(peer.ID))
				require.Contains(t, generation.deletedNodeIDs, peer.ID)
				require.Contains(t, generation.deletes, "test:detached", "recreating the node does not revive detached edges")
				require.Contains(t, generation.deletedEdgeIDs, EdgeID("test:detached"))
			})
		})
	}
}

func TestCommitBuffer_NodeFinalDeletionStillWins(t *testing.T) {
	node := &Node{ID: "test:deleted", Labels: []string{"Person"}}
	for _, createFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "delete-only", true: "create-then-delete"}[createFirst], func(t *testing.T) {
			generation := newCommitBuffer()
			if createFirst {
				generation.AddCommit(bufferedCommit{ops: []Operation{{Type: OpCreateNode, Node: node}}})
			}
			generation.AddCommit(bufferedCommit{ops: []Operation{{Type: OpDeleteNode, NodeID: node.ID, OldNode: node}}})
			got, found, deleted := generation.getNode(node.ID)
			require.True(t, found)
			require.True(t, deleted)
			require.Nil(t, got)
			buffer := &WriteBehindBuffer{active: generation}
			nodes, touched := buffer.LabelOverlay("Person")
			require.True(t, touched[node.ID])
			require.Empty(t, nodes)
			require.Empty(t, buffer.LabelNodes("Person"))
		})
	}
}

func TestWriteBehindBuffer_NodeRecreationAcrossGenerations(t *testing.T) {
	for _, sequence := range []string{"newest-create", "newest-delete", "newest-delete-create"} {
		t.Run(sequence, func(t *testing.T) {
			apply := newBlockingApply()
			buffer := NewWriteBehindBuffer(time.Hour, 0, apply.fn)
			t.Cleanup(func() {
				close(apply.release)
				err := buffer.Flush()
				buffer.Close()
				require.NoError(t, err)
			})
			old := &Node{ID: "test:recreated", Labels: []string{"Person"}, Properties: map[string]any{"version": int64(1)}}
			newest := &Node{ID: old.ID, Labels: []string{"Person"}, Properties: map[string]any{"version": int64(2)}}
			deleted := Operation{Type: OpDeleteNode, NodeID: old.ID, OldNode: old}
			if sequence == "newest-delete" {
				buffer.AppendCommit(bufferedCommit{ops: []Operation{{Type: OpCreateNode, Node: old}}})
			} else {
				buffer.AppendCommit(bufferedCommit{ops: []Operation{deleted}})
			}
			buffer.FlushAsync()
			select {
			case <-apply.started:
			case <-time.After(5 * time.Second):
				t.Fatal("older generation did not enter its blocked apply")
			}
			if sequence != "newest-create" {
				buffer.AppendCommit(bufferedCommit{ops: []Operation{deleted}})
			}
			if sequence != "newest-delete" {
				buffer.AppendCommit(bufferedCommit{ops: []Operation{{Type: OpCreateNode, Node: newest}}})
			}
			got, found, isDeleted := buffer.GetNode(old.ID)
			require.True(t, found)
			nodes, touched := buffer.LabelOverlay("Person")
			require.True(t, touched[old.ID])
			if sequence == "newest-delete" {
				require.True(t, isDeleted)
				require.Nil(t, got)
				require.Empty(t, nodes)
			} else {
				require.False(t, isDeleted)
				require.Equal(t, newest, got)
				require.Equal(t, []*Node{newest}, nodes)
			}
		})
	}
}
