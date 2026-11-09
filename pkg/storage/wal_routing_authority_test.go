package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type walRoutingAuthority struct {
	Engine
	submissions    int
	mode           EntityCommitMode
	operations     []Operation
	admissionErr   error
	admissionCalls int
}

func (e *walRoutingAuthority) CheckEntityCommitAdmission() error {
	e.admissionCalls++
	return e.admissionErr
}

func (e *walRoutingAuthority) GetInnerEngine() Engine { return e.Engine }
func (e *walRoutingAuthority) SubmitEntityCommit(ctx context.Context, prepared *PreparedEntityCommit, settle func(CommitOutcome)) (*CommitReceipt, error) {
	e.submissions++
	e.mode = prepared.Mode()
	e.operations = prepared.Operations()
	route, ok := EntityCommitSubmitterFor(e.Engine)
	if !ok {
		return nil, ErrInvalidData
	}
	return route.Submitter.SubmitEntityCommit(ctx, prepared, settle)
}

func TestWALTransparentAuthorityOwnsEveryMutation(t *testing.T) {
	for _, scoped := range []bool{false, true} {
		for _, kind := range []string{"create-node", "update-node", "delete-node", "create-edge", "update-edge", "delete-edge", "bulk-create-nodes", "bulk-create-edges", "bulk-delete-nodes", "bulk-delete-edges"} {
			t.Run(fmt.Sprintf("scoped=%t/%s", scoped, kind), func(t *testing.T) {
				var authority *walRoutingAuthority
				native, wal, owner := newWALClientFixture(t, func(engine *MemoryEngine) Engine {
					var inner Engine = engine
					if scoped {
						inner = NewNamespacedEngine(inner, "tenant")
					}
					authority = &walRoutingAuthority{Engine: inner}
					return authority
				})
				namespace := "test"
				if scoped {
					namespace = "tenant"
				}
				physical := func(id string) NodeID { return NodeID(namespace + ":" + id) }
				input := func(id string) NodeID {
					if scoped {
						return NodeID(id)
					}
					return physical(id)
				}
				edgeID := func(id string) EdgeID { return EdgeID(input(id)) }
				require.NoError(t, native.BulkCreateNodes(t.Context(), []*Node{{ID: physical("a")}, {ID: physical("b")}, {ID: physical("doomed")}}))
				require.NoError(t, native.CreateEdge(t.Context(), &Edge{ID: EdgeID(physical("e")), StartNode: physical("a"), EndNode: physical("b"), Type: "R"}))
				edge := func(id string) *Edge {
					return &Edge{ID: edgeID(id), StartNode: input("a"), EndNode: input("b"), Type: "R", Properties: map[string]any{"v": int64(2)}}
				}
				mode := CommitModeDirect
				var err error
				switch kind {
				case "create-node":
					var id NodeID
					id, err = owner.CreateNode(t.Context(), &Node{ID: input("new")})
					require.Equal(t, input("new"), id)
				case "update-node":
					err = owner.UpdateNode(t.Context(), NodeUpdateRequest{After: &Node{ID: input("a"), Properties: map[string]any{"v": 1}}})
				case "delete-node":
					err = owner.DeleteNode(t.Context(), input("doomed"))
				case "create-edge":
					err = owner.CreateEdge(t.Context(), edge("new-edge"))
				case "update-edge":
					err = owner.UpdateEdge(t.Context(), EdgeUpdateRequest{After: edge("e")})
				case "delete-edge":
					err = owner.DeleteEdge(t.Context(), edgeID("e"))
				case "bulk-create-nodes":
					mode = CommitModeImplicit
					err = owner.BulkCreateNodes(t.Context(), []*Node{{ID: input("new")}, {ID: input("next")}})
				case "bulk-create-edges":
					mode = CommitModeImplicit
					err = owner.BulkCreateEdges(t.Context(), []*Edge{edge("new-edge"), edge("next-edge")})
				case "bulk-delete-nodes":
					mode = CommitModeImplicit
					err = owner.BulkDeleteNodes(t.Context(), []NodeID{input("doomed"), input("doomed"), "", input("absent")})
				case "bulk-delete-edges":
					mode = CommitModeImplicit
					err = owner.BulkDeleteEdges(t.Context(), []EdgeID{edgeID("e"), edgeID("e"), "", edgeID("absent")})
				}
				require.NoError(t, err)
				require.Equal(t, 1, authority.submissions)
				require.Equal(t, mode, authority.mode)
				require.NotEmpty(t, authority.operations)
				require.Equal(t, 1, authority.admissionCalls)
				entries := walClientEntries(t, wal)
				require.Len(t, entries, 1)
				require.Equal(t, OpEntityCommit, entries[0].Operation)
				var record walEntityCommitData
				require.NoError(t, decodeRecoveryPayload(entries[0].Data, &record))
				envelope, operations, err := DecodeEntityTransaction(record.Payload)
				require.NoError(t, err)
				require.Equal(t, namespace, envelope.Namespace)
				require.Equal(t, mode, envelope.Mode)
				require.Len(t, operations, len(authority.operations))
				if kind == "bulk-delete-nodes" || kind == "bulk-delete-edges" {
					require.Len(t, operations, 1)
				}
				recovered := newTestMemoryEngine(t)
				require.NoError(t, recovered.BulkCreateNodes(t.Context(), []*Node{{ID: physical("a")}, {ID: physical("b")}, {ID: physical("doomed")}}))
				require.NoError(t, recovered.CreateEdge(t.Context(), &Edge{ID: EdgeID(physical("e")), StartNode: physical("a"), EndNode: physical("b"), Type: "R"}))
				require.NoError(t, ReplayWALEntry(t.Context(), recovered, entries[0]))
				require.NoError(t, ReplayWALEntry(t.Context(), recovered, entries[0]))
				wantNodes, wantEdges := int64(3), int64(1)
				switch kind {
				case "create-node":
					wantNodes = 4
				case "bulk-create-nodes":
					wantNodes = 5
				case "delete-node", "bulk-delete-nodes":
					wantNodes = 2
				case "create-edge":
					wantEdges = 2
				case "bulk-create-edges":
					wantEdges = 3
				case "delete-edge", "bulk-delete-edges":
					wantEdges = 0
				}
				for _, store := range []*MemoryEngine{native, recovered} {
					nodes, err := store.NodeCount(t.Context())
					require.NoError(t, err)
					require.Equal(t, wantNodes, nodes)
					edges, err := store.EdgeCount(t.Context())
					require.NoError(t, err)
					require.Equal(t, wantEdges, edges)
					if kind == "update-node" {
						n, err := store.GetNode(t.Context(), physical("a"))
						require.NoError(t, err)
						require.EqualValues(t, 1, n.Properties["v"])
					}
					if kind == "update-edge" {
						e, err := store.GetEdge(t.Context(), EdgeID(physical("e")))
						require.NoError(t, err)
						require.EqualValues(t, 2, e.Properties["v"])
					}
				}
			})
		}
	}
}

func TestWALTransparentAuthorityRejectsBeforePreparationAndRetainsRetry(t *testing.T) {
	var authority *walRoutingAuthority
	native, wal, owner := newWALClientFixture(t, func(engine *MemoryEngine) Engine {
		authority = &walRoutingAuthority{Engine: engine}
		return authority
	})
	_, err := native.CreateNode(t.Context(), &Node{ID: "test:kept"})
	require.NoError(t, err)
	rejected := errors.New("authoritative admission rejected")
	authority.admissionErr = rejected
	_, err = owner.CreateNode(t.Context(), &Node{ID: "test:rejected"})
	require.ErrorIs(t, err, rejected)
	require.ErrorIs(t, owner.BulkDeleteNodes(t.Context(), []NodeID{"test:kept"}), rejected)
	require.Zero(t, authority.submissions)
	require.Empty(t, walClientEntries(t, wal))
	_, err = native.GetNode(t.Context(), "test:kept")
	require.NoError(t, err)
	_, err = native.GetNode(t.Context(), "test:rejected")
	require.ErrorIs(t, err, ErrNotFound)
	authority.admissionErr = nil
	_, err = owner.CreateNode(t.Context(), &Node{ID: "test:healthy-retry"})
	require.NoError(t, err)
	require.Equal(t, 1, authority.submissions)
	require.Len(t, walClientEntries(t, wal), 1)
}
