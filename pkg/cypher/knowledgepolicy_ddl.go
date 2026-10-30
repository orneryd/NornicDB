package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// Author: #1663 implementation lane. Command grammar and retained expression
// extents have one owner below both public query entrances.
type CreateDecayProfileBundleCmd = cypherantlr.CreateDecayProfileBundleCmd
type CreateDecayProfileBindingCmd = cypherantlr.CreateDecayProfileBindingCmd
type AlterDecayProfileCmd = cypherantlr.AlterDecayProfileCmd
type AlterDecayProfileBindingCmd = cypherantlr.AlterDecayProfileBindingCmd
type DropDecayProfileCmd = cypherantlr.DropDecayProfileCmd
type ShowDecayProfilesCmd = cypherantlr.ShowDecayProfilesCmd
type CreatePromotionProfileCmd = cypherantlr.CreatePromotionProfileCmd
type AlterPromotionProfileCmd = cypherantlr.AlterPromotionProfileCmd
type DropPromotionProfileCmd = cypherantlr.DropPromotionProfileCmd
type ShowPromotionProfilesCmd = cypherantlr.ShowPromotionProfilesCmd
type CreatePromotionPolicyCmd = cypherantlr.CreatePromotionPolicyCmd
type AlterPromotionPolicyCmd = cypherantlr.AlterPromotionPolicyCmd
type AlterPromotionPolicyDefinitionCmd = cypherantlr.AlterPromotionPolicyDefinitionCmd
type DropPromotionPolicyCmd = cypherantlr.DropPromotionPolicyCmd
type ShowPromotionPoliciesCmd = cypherantlr.ShowPromotionPoliciesCmd

func ParseKnowledgePolicyDDL(ctx context.Context, stmt string, stage SourceStage) (interface{}, bool, error) {
	return cypherantlr.ParseKnowledgePolicyDDL(ctx, stmt, stage, readPolicyKeywordBoundary)
}

func isKnowledgePolicyDDLStatement(statement string) bool {
	return cypherantlr.IsKnowledgePolicyDDLStatement(statement)
}

// Author: #1663 lane. Dependency-inversion adapter only. TJ's contextual
// scanner retains all name/operator/whitespace behavior and ownership.
func readPolicyKeywordBoundary(source *cyphertext.Source, keyword string, from int, boundary cypherantlr.PolicyBoundaryKind) int {
	opts := defaultKeywordScanOpts(braceView{source: source})
	opts.SkipBraces = true
	if boundary == cypherantlr.PolicyWhitespaceBoundary {
		opts.Boundary = keywordBoundaryWhitespace
	}
	return keywordIndexFrom(source.Text, keyword, from, opts)
}
