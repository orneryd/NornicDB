package knowledgepolicy

import "sync"

// CompiledPropertyOverride is a pre-expanded property-level scoring override.
type CompiledPropertyOverride struct {
	NoDecay           bool
	HalfLifeNanos     int64
	ThresholdAgeNanos int64
	DecayFloor        float64
	Function          DecayFunction
}

type CompiledPromotionRule struct {
	Predicate string
	Profile   *PromotionProfileDef
	Order     int
}

// CompiledBinding is the pre-flattened lookup entry for a label/edge-type.
type CompiledBinding struct {
	DecayProfile           *DecayProfileBundle
	DecayBinding           *DecayProfileBinding
	PromotionPolicy        *PromotionPolicyDef
	VisibilityThreshold    float64
	ScoreFrom              ScoreFromMode
	ScoreFromProperty      string
	Function               DecayFunction
	HalfLifeNanos          int64
	ThresholdAgeNanos      int64
	DecayFloor             float64
	NoDecay                bool
	HasNoDecayProperty     bool
	CompiledPropertyRules  map[string]*CompiledPropertyOverride
	CompiledPromotionRules []CompiledPromotionRule
}

// BindingTable is the compiled lookup for all labels and edge types.
type BindingTable struct {
	mu       sync.RWMutex
	nodes    map[string]*CompiledBinding // key: sorted label set joined by "\x00"
	edges    map[string]*CompiledBinding // key: edge type
	wildNode *CompiledBinding
	wildEdge *CompiledBinding
}

// NewBindingTable creates an empty BindingTable.
func NewBindingTable() *BindingTable {
	return &BindingTable{
		nodes: make(map[string]*CompiledBinding),
		edges: make(map[string]*CompiledBinding),
	}
}

// LookupNode returns the compiled binding for the given sorted label set.
// Returns the wildcard binding if no specific match exists, or nil if neither exists.
func (bt *BindingTable) LookupNode(labelKey string) *CompiledBinding {
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	if cb, ok := bt.nodes[labelKey]; ok {
		return cb
	}
	return bt.wildNode
}

// lookupNodeExact returns the binding for the exact label key without wildcard fallback.
func (bt *BindingTable) lookupNodeExact(labelKey string) *CompiledBinding {
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	return bt.nodes[labelKey]
}

// LookupEdge returns the compiled binding for the given edge type.
// Returns the wildcard binding if no specific match exists, or nil if neither exists.
func (bt *BindingTable) LookupEdge(edgeType string) *CompiledBinding {
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	if cb, ok := bt.edges[edgeType]; ok {
		return cb
	}
	return bt.wildEdge
}

// SetNode sets a compiled binding for a label key.
func (bt *BindingTable) SetNode(labelKey string, cb *CompiledBinding) {
	bt.mu.Lock()
	defer bt.mu.Unlock()
	bt.nodes[labelKey] = cb
}

// SetEdge sets a compiled binding for an edge type.
func (bt *BindingTable) SetEdge(edgeType string, cb *CompiledBinding) {
	bt.mu.Lock()
	defer bt.mu.Unlock()
	bt.edges[edgeType] = cb
}

// SetWildNode sets the wildcard node binding.
func (bt *BindingTable) SetWildNode(cb *CompiledBinding) {
	bt.mu.Lock()
	defer bt.mu.Unlock()
	bt.wildNode = cb
}

// SetWildEdge sets the wildcard edge binding.
func (bt *BindingTable) SetWildEdge(cb *CompiledBinding) {
	bt.mu.Lock()
	defer bt.mu.Unlock()
	bt.wildEdge = cb
}

// NodeCount returns the number of specific node bindings.
func (bt *BindingTable) NodeCount() int {
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	return len(bt.nodes)
}

// EdgeCount returns the number of specific edge bindings.
func (bt *BindingTable) EdgeCount() int {
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	return len(bt.edges)
}

// HasWildNode returns whether a wildcard node binding is set.
func (bt *BindingTable) HasWildNode() bool {
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	return bt.wildNode != nil
}

// HasWildEdge returns whether a wildcard edge binding is set.
func (bt *BindingTable) HasWildEdge() bool {
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	return bt.wildEdge != nil
}
