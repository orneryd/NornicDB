package storagecensus

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
)

func TestManagedReferenceFieldClassificationsAreExact(t *testing.T) {
	p, _ := ssaFixture(t, `package storage
 type semanticTransactionNode struct { ChunkEmbeddings, unreviewed [][]float32 }
 func (n *semanticTransactionNode) encode() { n.ChunkEmbeddings=nil; n.unreviewed=nil }
 type semanticTransactionNodeLookalike struct { ChunkEmbeddings [][]float32 }
 func (n *semanticTransactionNodeLookalike) encode() { n.ChunkEmbeddings=nil }
 type walManagedAnchorCache struct { next, unreviewed uint8 }
 func (c *walManagedAnchorCache) advance() { c.next++; c.unreviewed++ }
 type walManagedAnchorCacheLookalike struct { next uint8 }
 func (c *walManagedAnchorCacheLookalike) advance() { c.next++ }
 type BadgerEngine struct { nodeCount int }
 func (b *BadgerEngine) unauthorized() { b.nodeCount++ }
 func CensusFixtureUses(n *semanticTransactionNode, look *semanticTransactionNodeLookalike, c *walManagedAnchorCache, other *walManagedAnchorCacheLookalike, b *BadgerEngine) { n.encode(); look.encode(); c.advance(); other.advance(); b.unauthorized() }
 `, "pkg/storage", ssa.SanityCheckFunctions, importableFixture)
	report := &Report{}
	(&analysis{p: p}).checkFields(report)
	violations := strings.Join(report.Violations, "\n")
	expected := map[string]string{
		"pkg/storage.semanticTransactionNode.ChunkEmbeddings": "WAL reference encoding scratch",
		"pkg/storage.walManagedAnchorCache.next":              "WAL proof-cache replacement cursor",
	}
	found := map[string]string{}
	for _, write := range report.FieldWrites {
		if _, approved := expected[write.Field]; approved {
			found[write.Field] = write.Class
		}
	}
	require.Equal(t, expected, found, "both actual writes must be discovered and classified")
	for field := range expected {
		require.NotContains(t, violations, "unclassified field: "+field+" ")
	}
	for _, field := range []string{
		"pkg/storage.semanticTransactionNode.unreviewed",
		"pkg/storage.semanticTransactionNodeLookalike.ChunkEmbeddings",
		"pkg/storage.walManagedAnchorCache.unreviewed",
		"pkg/storage.walManagedAnchorCacheLookalike.next",
	} {
		require.Contains(t, violations, "unclassified field: "+field+" ")
	}
	require.Contains(t, violations, "unowned entity-count change: pkg/storage.BadgerEngine.nodeCount ")
}
