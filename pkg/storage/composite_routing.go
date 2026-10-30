// Package storage provides routing logic for composite engines.
//
// This file implements query analysis and routing for composite databases,
// determining which constituents should be queried based on query patterns.
package storage

import (
	"strings"

	"github.com/orneryd/nornicdb/internal/labelrouting"
)

// QueryAnalyzer analyzes queries to extract routing information.
type QueryAnalyzer struct {
	// Label-based routing: normalized label -> []constituent aliases. The
	// label table is the shared labelrouting owner; this analyzer keeps its
	// current lock-free access because it has never held a lock.
	labelRouting labelrouting.Table

	// Property-based routing: property name -> value -> constituent alias
	propertyRouting map[string]map[interface{}]string

	// Default constituent for property routing when value not found
	propertyDefaults map[string]string
}

// NewQueryAnalyzer creates a new query analyzer for composite routing.
func NewQueryAnalyzer() *QueryAnalyzer {
	return &QueryAnalyzer{
		labelRouting:     labelrouting.New(),
		propertyRouting:  make(map[string]map[interface{}]string),
		propertyDefaults: make(map[string]string),
	}
}

// SetLabelRouting configures label-based routing.
func (a *QueryAnalyzer) SetLabelRouting(label string, constituents []string) {
	a.labelRouting.Set(label, constituents)
}

// SetPropertyRouting configures property-based routing.
func (p *QueryAnalyzer) SetPropertyRouting(propertyName string, value interface{}, constituent string) {
	if p.propertyRouting[propertyName] == nil {
		p.propertyRouting[propertyName] = make(map[interface{}]string)
	}
	p.propertyRouting[propertyName][value] = constituent
}

// SetPropertyDefault sets the default constituent for a property when value not found.
func (p *QueryAnalyzer) SetPropertyDefault(propertyName string, constituent string) {
	p.propertyDefaults[propertyName] = constituent
}

// AnalyzeQuery extracts routing information from a Cypher query string.
// This is a simplified analyzer - a full implementation would parse the AST.
// writes is the statement-effect owner's answer, which storage cannot compute
// itself: cypher.QueryPermissionRequirements(ctx, query, stage).ChangesState.
func (a *QueryAnalyzer) AnalyzeQuery(cypher string, writes bool) *QueryInfo {
	upper := strings.ToUpper(cypher)
	info := &QueryInfo{
		Labels:     []string{},
		Properties: make(map[string]interface{}),
		IsWrite:    writes,
		IsFullScan: false,
	}

	// Extract labels (simplified - looks for :Label patterns)
	// This is a basic implementation - full parser would be more accurate
	words := strings.Fields(upper)
	for i, word := range words {
		if strings.HasPrefix(word, ":") && len(word) > 1 {
			label := strings.TrimPrefix(word, ":")
			label = strings.Trim(label, "()[]{},;")
			if label != "" {
				info.Labels = append(info.Labels, label)
			}
		}
		// Also check for patterns like "MATCH (n:Label)"
		if i > 0 && (words[i-1] == "(" || words[i-1] == ",") && strings.HasPrefix(word, ":") {
			label := strings.TrimPrefix(word, ":")
			label = strings.Trim(label, "()[]{},;")
			if label != "" {
				info.Labels = append(info.Labels, label)
			}
		}
	}

	// If no labels found and it's a read query, it's likely a full scan
	if len(info.Labels) == 0 && !info.IsWrite {
		info.IsFullScan = true
	}

	return info
}

// RouteQuery determines which constituents to query based on query info.
//
// Only a complete nonempty label mapping wins. No labels, any unknown label
// and an all-known mapping whose union is empty all continue into the existing
// property lookup and finally return the caller slice; label routing never
// introduces property fallback of its own.
func (a *QueryAnalyzer) RouteQuery(queryInfo *QueryInfo, allConstituents []string) []string {
	// Try label-based routing first
	if selection := a.labelRouting.Select(queryInfo.Labels); selection.Kind == labelrouting.SelectionMapped {
		return selection.Aliases
	}

	// Try property-based routing
	if len(queryInfo.Properties) > 0 {
		for propName, propMap := range a.propertyRouting {
			if value, exists := queryInfo.Properties[propName]; exists {
				if constituent, found := propMap[value]; found {
					return []string{constituent}
				}
				// Try default
				if defaultConstituent, hasDefault := a.propertyDefaults[propName]; hasDefault {
					return []string{defaultConstituent}
				}
			}
		}
	}

	// No routing rules match - query all constituents
	return allConstituents
}

// QueryInfo contains information extracted from a query for routing.
type QueryInfo struct {
	Labels     []string
	Properties map[string]interface{}
	IsWrite    bool
	IsFullScan bool
}
