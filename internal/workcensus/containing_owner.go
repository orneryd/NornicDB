package workcensus

import (
	"go/token"
	"strings"
)

// ScopeWitness requires an observation to use one exact typed value of its
// containing owner (or lexical ancestor). Fields is a direct field path from
// that binding. Receiver chooses the observation's receiver instead of Argument.
// This proves binding identity, not alias provenance or control-flow dominance.
type ScopeWitness struct {
	OwnerID  string   `json:"owner_id,omitempty"`
	Value    string   `json:"value"`
	Fields   []string `json:"fields,omitempty"`
	Type     string   `json:"type"`
	Argument int      `json:"argument,omitempty"`
	Receiver bool     `json:"receiver,omitempty"`
}

// ContainingOwnerWitness classifies work INSIDE an actual observing function.
// It never delegates enclosing enumeration to a nested call target. The named
// layer must contain this source owner, and the selected observation must be in
// this very body, with the required scope and a source-removal control.
type ContainingOwnerWitness struct {
	OwnerID     string       `json:"owner_id"`
	OwnerDigest string       `json:"owner_digest"`
	Observation CallWitness  `json:"observation"`
	Scope       ScopeWitness `json:"scope"`
}

func containingOwnerFailures(m, a Member, p Policy, members map[string]Member, definitions map[string]Definition, unsafeBindings map[string]bool) []string {
	w := a.ContainingOwner
	if w == nil {
		return []string{"NO CONTAINING OWNER WITNESS " + m.ID}
	}
	owner, found := definitions[m.OwnerID]
	if !found || w.OwnerID != m.OwnerID || !contains([]string{"function", "method", "closure"}, owner.Kind) {
		return []string{"CONTAINING OWNER ID " + m.ID}
	}
	var failures []string
	if w.OwnerDigest == "" || w.OwnerDigest != owner.Digest || owner.Digest != m.OwnerDigest {
		failures = append(failures, "CONTAINING OWNER CHANGED "+m.ID)
	}
	contained := false
	for _, layer := range p.Layers {
		if layer.Name != a.OwnerLayer {
			continue
		}
		for _, body := range layer.Members {
			identity := body.OwnerID == owner.ID || (body.OwnerID == "" && owner.Symbol != "" && body.Symbol == owner.Symbol)
			if identity && body.Digest == owner.Digest && (body.Symbol == "" || body.Symbol == owner.Symbol) {
				contained = true
			}
		}
	}
	if strings.TrimSpace(a.OwnerLayer) == "" || !contained {
		failures = append(failures, "OWNER LAYER CONTAINING BODY "+m.ID)
	}
	observation := w.Observation
	controlFound := false
	for _, control := range p.Controls {
		if control.ID == observation.RemovalControl && control.ID != "" && control.MemberID == observation.MemberID && control.Replacement != "" {
			controlFound = true
		}
	}
	if !controlFound {
		failures = append(failures, "NO OWNER OBSERVATION REMOVAL CONTROL "+m.ID)
	}
	site, found := members[observation.MemberID]
	if !found {
		return append(failures, "OWNER OBSERVATION MISSING "+m.ID+" "+observation.MemberID)
	}
	if site.UnresolvedTargets {
		failures = append(failures, "OWNER OBSERVATION TARGET UNKNOWN "+m.ID+" "+site.ID)
	}
	if site.Node != "call" || site.OwnerID != owner.ID || site.OwnerDigest != owner.Digest {
		failures = append(failures, "OWNER OBSERVATION OWNER "+m.ID+" "+site.ID)
	}
	if observation.Digest == "" || observation.Digest != site.Digest || observation.Callee == "" || !contains(site.Callees, observation.Callee) {
		failures = append(failures, "OWNER OBSERVATION CHANGED "+m.ID+" "+site.ID)
	}
	if !matchesOwnerScope(w.Scope, owner, site, definitions, unsafeBindings) {
		failures = append(failures, "OWNER OBSERVATION SCOPE "+m.ID+" "+site.ID)
	}
	return failures
}

func matchesOwnerScope(scope ScopeWitness, owner Definition, observation Member, definitions map[string]Definition, unsafeBindings map[string]bool) bool {
	if scope.Value == "" || scope.Type == "" || !observation.Typed || scope.Argument < 0 {
		return false
	}
	scopeOwnerID := scope.OwnerID
	if scopeOwnerID == "" {
		scopeOwnerID = owner.ID
	}
	// A closure may capture its ancestor's binding; an unrelated owner cannot
	// lend a same-spelled variable or its observation to this body.
	seen := map[string]bool{}
	for owner.ID != scopeOwnerID {
		if seen[owner.ID] || owner.ParentID == "" {
			return false
		}
		seen[owner.ID] = true
		var found bool
		owner, found = definitions[owner.ParentID]
		if !found {
			return false
		}
	}
	valueFound := false
	for _, value := range owner.Values {
		if value.Reference == scope.Value && (len(scope.Fields) > 0 || value.Type == scope.Type) {
			valueFound = true
		}
	}
	if !valueFound {
		return false
	}
	reference := scope.Value
	for _, field := range scope.Fields {
		if !token.IsIdentifier(field) || field == "_" {
			return false
		}
		reference += "." + field
	}
	for slot := reference; ; {
		if unsafeBindings[slot] {
			return false
		}
		end := strings.LastIndexByte(slot, '.')
		// Binding IDs themselves contain package dots. Only field selectors
		// following the final /role#ordinal component are scope prefixes.
		if end < strings.LastIndexByte(scope.Value, '/') || len(slot) <= len(scope.Value) {
			break
		}
		slot = slot[:end]
	}
	if scope.Receiver {
		return scope.Argument == 0 && observation.ReceiverReference == reference && observation.ReceiverType == scope.Type
	}
	return scope.Argument < len(observation.ArgReferences) && scope.Argument < len(observation.ArgTypes) && observation.ArgReferences[scope.Argument] == reference && observation.ArgTypes[scope.Argument] == scope.Type
}
