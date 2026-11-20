// NOR-312 regression: preserve the original fail-first error assertions.
//
// Every case below exercises a supported public engine read/write whose
// signature already returns an error. A composite statement's participant
// access owner reported that constituent "b" could not be opened, so the
// required outcome (AC2/AC4) is that the failure reaches the caller instead of
// a smaller successful result or ErrNotFound.
//
// These assertions failed on release base e8cd436e5 because constituent-open
// errors were discarded. They must pass by propagating the original failure;
// accepting an incomplete result would hide the defect.
package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompositeConstituentAccessFailureReachesCaller(t *testing.T) {
	_, denied := newConstituentAccessAudit(t)
	ctx := t.Context()

	for _, tc := range []struct {
		name string
		read func() error
	}{
		{"GetNode/qualified", func() error {
			_, err := denied.GetNode(ctx, "4:b:b-2")
			return err
		}},
		{"GetNode/unqualified", func() error {
			_, err := denied.GetNode(ctx, "b-2")
			return err
		}},
		{"GetNodeProjected", func() error {
			_, err := denied.GetNodeProjected(ctx, "4:b:b-2", []string{"value"})
			return err
		}},
		{"GetNodeLatestVisible", func() error {
			_, err := denied.GetNodeLatestVisible(ctx, "4:b:b-2")
			return err
		}},
		{"GetNodesByLabel", func() error {
			_, err := denied.GetNodesByLabel(ctx, "Doc")
			return err
		}},
		{"AllNodes", func() error {
			_, err := denied.AllNodes(ctx)
			return err
		}},
		{"AllEdges", func() error {
			_, err := denied.AllEdges(ctx)
			return err
		}},
		{"NodeCount", func() error {
			_, err := denied.NodeCount(ctx)
			return err
		}},
		{"EdgeCount", func() error {
			_, err := denied.EdgeCount(ctx)
			return err
		}},
		{"EdgeCountByType", func() error {
			_, err := denied.EdgeCountByType(ctx, "R")
			return err
		}},
		{"BatchGetNodes", func() error {
			_, err := denied.BatchGetNodes(ctx, []NodeID{"4:b:b-1"})
			return err
		}},
		{"IterateNodes", func() error {
			return denied.IterateNodes(func(*Node) bool { return true })
		}},
		{"StreamNodes", func() error {
			return denied.StreamNodes(ctx, func(*Node) error { return nil })
		}},
		{"StreamNodeChunks", func() error {
			return denied.StreamNodeChunks(ctx, 1, func([]*Node) error { return nil })
		}},
		{"ForEachNodeIDByLabel", func() error {
			return denied.ForEachNodeIDByLabel(ctx, "Doc", func(NodeID) bool { return true })
		}},
		{"StreamNodesWithOptions", func() error {
			return denied.StreamNodesWithOptions(ctx, StreamNodesOptions{}, func(*Node) error { return nil })
		}},
		{"GetInDegree", func() error {
			_, err := denied.GetInDegree(ctx, "4:b:b-2")
			return err
		}},
		{"GetOutDegree", func() error {
			_, err := denied.GetOutDegree(ctx, "4:b:b-1")
			return err
		}},
		{"GetEdge", func() error {
			_, err := denied.GetEdge(ctx, "5:b:r-b-1")
			return err
		}},
		{"GetEdgeBetween", func() error {
			_, err := denied.GetEdgeBetween(ctx, "4:b:b-1", "4:b:b-2", "R")
			return err
		}},
		{"UpdateNode", func() error {
			return denied.UpdateNode(ctx, NodeUpdateRequest{After: &Node{ID: "4:b:b-2", Labels: []string{"Doc"}, Properties: map[string]any{"value": "changed"}}})
		}},
		{"DeleteNode", func() error {
			return denied.DeleteNode(ctx, "4:b:b-2")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.read()
			var accessErr *constituentAccessError
			require.ErrorAs(t, err, &accessErr, "the participant-open failure must reach the caller, not a smaller success or ErrNotFound")
			require.Equal(t, "b", accessErr.alias)
		})
	}
}
