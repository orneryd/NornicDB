package search

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The rebased exact stage introduces node membership and entry scoring
// callbacks. These must keep their separate domains through closure values.
func TestVectorEntryExactStageDomainGuardRejectsBypasses(t *testing.T) {
	g := newVectorDomainGuard(t, `package search
func domainFixtureStageScore(node *stageNode) {
	recordStageScore(node, node.id, 1)
}
func domainFixtureStageOwner(pair hnswBuildPair) *stageNode {
	return &stageNode{id: pair.id}
}
func domainFixtureStageMember(resolver vectorMemberResolver, pair hnswBuildPair) {
	resolver([]string{pair.id})
}
`)
	for _, name := range []string{"domainFixtureStageScore", "domainFixtureStageOwner", "domainFixtureStageMember"} {
		found := false
		for issue := range g.violations {
			if strings.Contains(issue, name) {
				found = true
			}
		}
		require.True(t, found, "guard missed exact-stage bypass %s", name)
	}
	for issue := range g.violations {
		require.Contains(t, issue, "domainFixture", "production violation")
	}
}
