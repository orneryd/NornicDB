package knowledgepolicy

import (
	"log"
	"sort"
	"strings"
)

// Resolver resolves effective decay and promotion configuration for a target.
type Resolver struct {
	bt  *BindingTable
	log *log.Logger
}

// NewResolver creates a Resolver backed by the given BindingTable.
// A nil logger disables runtime conflict warnings.
func NewResolver(bt *BindingTable, logger *log.Logger) *Resolver {
	return &Resolver{bt: bt, log: logger}
}

// ResolveNode returns the CompiledBinding for a node identified by its labels.
// Labels are sorted internally; the caller's slice is not modified.
func (r *Resolver) ResolveNode(labels []string) *CompiledBinding {
	if len(labels) == 0 {
		return r.bt.LookupNode("")
	}

	sorted := make([]string, len(labels))
	copy(sorted, labels)
	sort.Strings(sorted)

	if len(sorted) == 1 {
		return r.bt.LookupNode(sorted[0])
	}

	fullKey := strings.Join(sorted, "\x00")
	if cb := r.bt.lookupNodeExact(fullKey); cb != nil {
		return cb
	}

	for k := len(sorted) - 1; k >= 1; k-- {
		matches := r.collectSubsetMatches(sorted, k)
		if len(matches) == 0 {
			continue
		}
		if len(matches) == 1 {
			return matches[0]
		}
		return r.resolveConflict(matches)
	}

	return r.bt.LookupNode("")
}

// ResolveEdge returns the CompiledBinding for an edge identified by its type.
func (r *Resolver) ResolveEdge(edgeType string) *CompiledBinding {
	return r.bt.LookupEdge(edgeType)
}

// ResolveProperty returns a CompiledBinding for a specific property on a node.
// If the binding has a property-level override, a copy with the override applied is returned.
func (r *Resolver) ResolveProperty(labels []string, propertyPath string) *CompiledBinding {
	cb := r.ResolveNode(labels)
	return resolvePropertyOverride(cb, propertyPath)
}

// ResolveEdgeProperty returns a CompiledBinding for a specific property on an edge.
// If the binding has a property-level override, a copy with the override applied is returned.
func (r *Resolver) ResolveEdgeProperty(edgeType string, propertyPath string) *CompiledBinding {
	cb := r.ResolveEdge(edgeType)
	return resolvePropertyOverride(cb, propertyPath)
}

func resolvePropertyOverride(cb *CompiledBinding, propertyPath string) *CompiledBinding {
	if cb == nil || cb.NoDecay || len(cb.CompiledPropertyRules) == 0 {
		return cb
	}
	override, ok := cb.CompiledPropertyRules[propertyPath]
	if !ok {
		return cb
	}
	out := *cb
	out.NoDecay = override.NoDecay
	if !override.NoDecay {
		out.HalfLifeNanos = override.HalfLifeNanos
		out.ThresholdAgeNanos = override.ThresholdAgeNanos
		out.DecayFloor = override.DecayFloor
		if override.Function != "" {
			out.Function = override.Function
		}
	}
	out.CompiledPropertyRules = nil
	out.HasNoDecayProperty = false
	return &out
}

func (r *Resolver) collectSubsetMatches(sorted []string, k int) []*CompiledBinding {
	var matches []*CompiledBinding
	indices := make([]int, k)
	for i := range indices {
		indices[i] = i
	}

	for {
		parts := make([]string, k)
		for i, idx := range indices {
			parts[i] = sorted[idx]
		}
		key := strings.Join(parts, "\x00")
		if cb := r.bt.lookupNodeExact(key); cb != nil {
			matches = append(matches, cb)
		}

		i := k - 1
		for i >= 0 && indices[i] == i+len(sorted)-k {
			i--
		}
		if i < 0 {
			break
		}
		indices[i]++
		for j := i + 1; j < k; j++ {
			indices[j] = indices[j-1] + 1
		}
	}

	return matches
}

func (r *Resolver) resolveConflict(matches []*CompiledBinding) *CompiledBinding {
	best := matches[0]
	for _, cb := range matches[1:] {
		if cb.DecayBinding != nil && best.DecayBinding != nil {
			if cb.DecayBinding.Order < best.DecayBinding.Order {
				best = cb
			}
		}
	}
	if r.log != nil {
		r.log.Printf("knowledgepolicy: runtime label conflict — %d bindings matched at same specificity, using Order %d",
			len(matches), best.DecayBinding.Order)
	}
	return best
}
