// Package multidb provides routing strategies for composite databases.
//
// Routing determines which constituent databases should be queried for a given operation.
// This enables efficient query execution by only accessing relevant constituents.
package multidb

import (
	"slices"
	"sync"

	"github.com/orneryd/nornicdb/internal/labelrouting"
)

// RoutingStrategy defines how queries are routed to constituent databases.
type RoutingStrategy interface {
	// RouteQuery determines which constituents should be queried for a given operation.
	// Returns list of constituent aliases to query.
	RouteQuery(queryInfo *QueryInfo) []string

	// RouteWrite determines which constituent should receive a write operation.
	// Returns constituent alias, or empty string if routing is ambiguous (full scan).
	RouteWrite(operation string, labels []string, properties map[string]interface{}) string
}

// QueryInfo contains information extracted from a Cypher query for routing decisions.
type QueryInfo struct {
	// Labels referenced in the query
	Labels []string

	// Properties used in WHERE clauses or CREATE/MERGE operations
	Properties map[string]interface{}

	// Property names that might be used for routing
	PropertyNames []string

	// Whether this is a write operation
	IsWrite bool

	// Whether this is a full scan (no specific labels/properties)
	IsFullScan bool
}

// LabelRouting routes queries based on node labels.
// Each label is mapped to one or more constituent aliases.
type LabelRouting struct {
	labelMap labelrouting.Table // label -> []constituent aliases
	mu       sync.RWMutex
}

// NewLabelRouting creates a new label-based routing strategy.
func NewLabelRouting() *LabelRouting {
	return &LabelRouting{
		labelMap: labelrouting.New(),
	}
}

// SetLabelRouting configures which constituents should be queried for a given label.
func (r *LabelRouting) SetLabelRouting(label string, constituents []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.labelMap.Set(label, slices.Clone(constituents))
}

// RouteQuery routes a query based on labels.
func (r *LabelRouting) RouteQuery(queryInfo *QueryInfo) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	selection := r.labelMap.Select(queryInfo.Labels)
	if !selection.Kind.Complete() {
		return nil
	}
	return selection.Aliases
}

// RouteWrite routes a write operation based on labels.
func (r *LabelRouting) RouteWrite(operation string, labels []string, properties map[string]interface{}) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(labels) == 0 {
		return "" // Ambiguous - full scan
	}

	// Use first label for routing
	if constituents, exists := r.labelMap.Lookup(labels[0]); exists && len(constituents) > 0 {
		// Route to first constituent for this label
		return constituents[0]
	}

	return "" // Not found - full scan
}

// PropertyRouting routes queries based on property values.
// Useful for database/shard-based routing (e.g., database_id property).
type PropertyRouting struct {
	propertyName       string                 // Property to route on (e.g., "database_id")
	valueMap           map[interface{}]string // property value -> constituent alias
	defaultConstituent string                 // Default if property not found
	mu                 sync.RWMutex
}

// NewPropertyRouting creates a new property-based routing strategy.
func NewPropertyRouting(propertyName string) *PropertyRouting {
	return &PropertyRouting{
		propertyName: propertyName,
		valueMap:     make(map[interface{}]string),
	}
}

// SetPropertyRouting configures which constituent should be queried for a property value.
func (r *PropertyRouting) SetPropertyRouting(value interface{}, constituent string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.valueMap[value] = constituent
}

// SetDefaultConstituent sets the default constituent for values not in the map.
func (r *PropertyRouting) SetDefaultConstituent(constituent string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.defaultConstituent = constituent
}

// RouteQuery routes a query based on property values.
func (r *PropertyRouting) RouteQuery(queryInfo *QueryInfo) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Check if routing property is in the query
	if queryInfo.Properties == nil {
		return nil // No properties - full scan
	}

	value, exists := queryInfo.Properties[r.propertyName]
	if !exists {
		if r.defaultConstituent != "" {
			return []string{r.defaultConstituent}
		}
		return nil // No property value - full scan
	}

	// Look up constituent for this value
	if constituent, found := r.valueMap[value]; found {
		return []string{constituent}
	}

	// Value not in map - use default or full scan
	if r.defaultConstituent != "" {
		return []string{r.defaultConstituent}
	}

	return nil // Full scan
}

// RouteWrite routes a write operation based on property values.
func (r *PropertyRouting) RouteWrite(operation string, labels []string, properties map[string]interface{}) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if properties == nil {
		return r.defaultConstituent
	}

	value, exists := properties[r.propertyName]
	if !exists {
		return r.defaultConstituent
	}

	if constituent, found := r.valueMap[value]; found {
		return constituent
	}

	return r.defaultConstituent
}

// FullScanRouting routes all queries to all constituents.
// This is the default strategy when no routing rules are configured.
type FullScanRouting struct{}

// NewFullScanRouting creates a new full-scan routing strategy.
func NewFullScanRouting() *FullScanRouting {
	return &FullScanRouting{}
}

// RouteQuery returns nil, indicating all constituents should be queried.
func (r *FullScanRouting) RouteQuery(queryInfo *QueryInfo) []string {
	return nil // nil means all constituents
}

// RouteWrite returns empty string, indicating write should go to first writable constituent.
func (r *FullScanRouting) RouteWrite(operation string, labels []string, properties map[string]interface{}) string {
	return "" // Empty means use default (first writable)
}

// CompositeRouting combines multiple routing strategies.
// Tries each strategy in order until one returns a result.
type CompositeRouting struct {
	strategies []RoutingStrategy
	mu         sync.RWMutex
}

// NewCompositeRouting creates a new composite routing strategy.
func NewCompositeRouting() *CompositeRouting {
	return &CompositeRouting{
		strategies: []RoutingStrategy{},
	}
}

// AddStrategy adds a routing strategy to try in order.
func (c *CompositeRouting) AddStrategy(strategy RoutingStrategy) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.strategies = append(c.strategies, strategy)
}

// RouteQuery tries each strategy until one returns a result.
func (c *CompositeRouting) RouteQuery(queryInfo *QueryInfo) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, strategy := range c.strategies {
		result := strategy.RouteQuery(queryInfo)
		if result != nil {
			return result
		}
	}

	// All strategies returned nil (full scan) - return nil
	return nil
}

// RouteWrite tries each strategy until one returns a result.
func (c *CompositeRouting) RouteWrite(operation string, labels []string, properties map[string]interface{}) string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, strategy := range c.strategies {
		result := strategy.RouteWrite(operation, labels, properties)
		if result != "" {
			return result
		}
	}

	// All strategies returned empty - return empty (use default)
	return ""
}
