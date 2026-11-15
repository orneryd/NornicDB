package cypher

import (
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/config"
)

type nodePropertyProofKey struct{ variable, expression string }

// Retain the pure text analysis, not a projection-specific node image or row
// values. The existing bounded, sharded query-analysis owner supplies admission.
// Allocate it only when the experimental path first uses it.
var experimentalNodePropertyProofs = sync.OnceValue(func() *boundedCache[nodePropertyProofKey, []string] {
	return newBoundedCache[nodePropertyProofKey, []string](4096, func(key nodePropertyProofKey) int {
		return boundedCacheStringShardIndex(key.expression)
	})
})

func pipelineNodePredicateProperties(variable, expression string, inline map[string]interface{}) []string {
	if !config.IsProjectedReadsEnabled() {
		return parsePipelineNodePredicateProperties(variable, expression, inline)
	}
	key := nodePropertyProofKey{variable: variable, expression: expression}
	proofs := experimentalNodePropertyProofs()
	properties, found := proofs.get(key)
	if !found {
		// The one canonical parser still decides whole-node use and decoded
		// identities. Inline keys are supplied afresh by each invocation.
		properties = parsePipelineNodePredicateProperties(variable, expression, nil)
		for i, property := range properties {
			properties[i] = strings.Clone(property)
		}
		proofs.put(nodePropertyProofKey{variable: strings.Clone(variable), expression: strings.Clone(expression)}, properties)
	}
	if properties == nil {
		return nil // Whole-node use is an unsafe projection, including on a hit.
	}
	// The caller may sort, append or pass this slice through a storage adapter.
	// Neither that slice nor per-row inline keys may mutate the retained proof.
	out := make([]string, 0, len(properties)+len(inline))
	out = append(out, properties...)
	for property := range inline {
		out = append(out, property)
	}
	sort.Strings(out)
	return slices.Compact(out)
}
