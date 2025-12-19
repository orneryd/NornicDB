// Package storage provides routing logic for composite engines.
//
// This file implements query analysis and routing for composite databases,
// determining which constituents should be queried based on query patterns.
package storage

import (
	"strings"
)

// QueryAnalyzer analyzes queries to extract routing information.
type QueryAnalyzer struct {
	// Label-based routing: label -> []constituent aliases
	labelRouting map[string][]string

	// Property-based routing: property name -> value -> constituent alias
	propertyRouting map[string]map[interface{}]string

	// Default constituent for property routing when value not found
	propertyDefaults map[string]string
}

// NewQueryAnalyzer creates a new query analyzer for composite routing.
func NewQueryAnalyzer() *QueryAnalyzer {
	return &QueryAnalyzer{
		labelRouting:     make(map[string][]string),
		propertyRouting:   make(map[string]map[interface{}]string),
		propertyDefaults: make(map[string]string),
	}
}

// SetLabelRouting configures label-based routing.
func (a *QueryAnalyzer) SetLabelRouting(label string, constituents []string) {
	a.labelRouting[strings.ToLower(label)] = constituents
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
func (a *QueryAnalyzer) AnalyzeQuery(cypher string) *QueryInfo {
	upper := strings.ToUpper(cypher)
	info := &QueryInfo{
		Labels:     []string{},
		Properties: make(map[string]interface{}),
		IsWrite:    strings.Contains(upper, "CREATE") || strings.Contains(upper, "MERGE") || strings.Contains(upper, "SET") || strings.Contains(upper, "DELETE"),
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
func (a *QueryAnalyzer) RouteQuery(queryInfo *QueryInfo, allConstituents []string) []string {
	// Try label-based routing first
	if len(queryInfo.Labels) > 0 {
		constituentSet := make(map[string]bool)
		allFound := true

		for _, label := range queryInfo.Labels {
			labelLower := strings.ToLower(label)
			if constituents, exists := a.labelRouting[labelLower]; exists {
				for _, alias := range constituents {
					constituentSet[alias] = true
				}
			} else {
				allFound = false
				break
			}
		}

		if allFound && len(constituentSet) > 0 {
			result := make([]string, 0, len(constituentSet))
			for alias := range constituentSet {
				result = append(result, alias)
			}
			return result
		}
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

