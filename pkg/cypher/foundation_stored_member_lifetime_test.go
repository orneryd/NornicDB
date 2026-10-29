package cypher_test

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

// Author: Lane #1644 (Codex). The same real public issuers and actual rollback
// owner used by logical obligations now cross all stored consumers after ending.
func TestFoundationPublicStoredMemberLifetime(t *testing.T) {
	factories := map[string]func(*testing.T) cypher.FoundationPublicFixture{
		"native": func(t *testing.T) cypher.FoundationPublicFixture {
			return cypher.FoundationIssuePublicFixture(t, "transactionStorageWrapper", nil)
		},
		"identity": func(t *testing.T) cypher.FoundationPublicFixture {
			return cypher.FoundationIssuePublicFixture(t, "graphIdentityStorage", nil)
		},
		"relationship": func(t *testing.T) cypher.FoundationPublicFixture {
			return cypher.FoundationIssuePublicFixture(t, "algorithmRelationshipReader", nil)
		},
		"system-empty": func(t *testing.T) cypher.FoundationPublicFixture {
			return cypher.FoundationIssuePublicFixture(t, "systemGraphView", nil)
		},
		"tracing": func(t *testing.T) cypher.FoundationPublicFixture {
			return cypher.FoundationIssuePublicFixture(t, "TracedEngine", func(e storage.Engine) storage.Engine { return storage.NewTracedEngine(e) })
		},
		"wal": func(t *testing.T) cypher.FoundationPublicFixture {
			return cypher.FoundationIssuePublicFixture(t, "WALEngine", func(e storage.Engine) storage.Engine { return storage.NewWALEngine(e, nil) })
		},
		"namespace": func(t *testing.T) cypher.FoundationPublicFixture {
			return cypher.FoundationIssuePublicFixture(t, "NamespacedEngine", func(e storage.Engine) storage.Engine { return storage.NewNamespacedEngine(e, "tenant") })
		},
		"composite-carried-owner": func(t *testing.T) cypher.FoundationPublicFixture {
			f := cypher.FoundationIssuePublicFixture(t, "transactionStorageWrapper", nil)
			binding := storage.MembershipForEngine(f.Engine)
			member, ok := binding.Owner.(storage.MembershipProvider)
			require.True(t, ok)
			f.Engine = storage.NewCompositeEngine(map[string]storage.Engine{"part": f.Committed}, map[string]string{"part": "tenant"}, map[string]string{"part": "read_write"}).WithMembership(member)
			return f
		},
	}
	for kind, issue := range factories {
		t.Run(kind, func(t *testing.T) {
			f := issue(t)
			ctx := context.Background()
			issued := f.Engine
			require.Equal(t, storage.MembershipBound, storage.MembershipForEngine(issued).Status)
			provider, ok := issued.(storage.StoredReadProvider)
			require.True(t, ok)
			for _, mode := range []storage.StoredReadMode{storage.StoredMetadataOnly, storage.StoredSelectedProperties, storage.StoredFullMaterialization} {
				request := storage.StoredReadRequest{Mode: mode, Nodes: []storage.NodeID{f.NodeID}, NodeProperties: []string{}}
				result, err := storage.ReadStored(ctx, issued, request)
				if provider.StoredReadPolicy().Admits(request) {
					require.NoError(t, err)
					require.NotNil(t, result)
				} else {
					require.ErrorIs(t, err, storage.ErrNotImplemented)
				}
			}
			_, err := f.Owner.Execute(ctx, "ROLLBACK", nil)
			require.NoError(t, err)
			require.Equal(t, storage.MembershipEnded, storage.MembershipForEngine(issued).Status, "real rollback setup control")
			for _, mode := range []storage.StoredReadMode{storage.StoredMetadataOnly, storage.StoredSelectedProperties, storage.StoredFullMaterialization} {
				for _, entry := range []string{"operation", "provider"} {
					t.Run(string(rune('0'+mode))+"/"+entry, func(t *testing.T) {
						var work storage.StoredReadWork
						request := storage.StoredReadRequest{Mode: mode, Nodes: []storage.NodeID{f.NodeID}, NodeProperties: []string{}, Work: &work}
						var result storage.StoredReadResult
						var err error
						if entry == "operation" {
							result, err = storage.ReadStored(ctx, issued, request)
						} else {
							result, err = provider.ReadStored(ctx, request)
						}
						assert.Error(t, err, "ended member must not answer mandatory facts or payload")
						assert.Nil(t, result)
						assert.Zero(t, work.Captures, "ended member cannot open a new capture")
					})
				}
			}
		})
	}
}
