package workcensus

import "strings"

// LayerMember pins a callee body present in the extracted source composition.
type LayerMember struct {
	OwnerID      string        `json:"owner_id,omitempty"`
	Observations []CallWitness `json:"observations,omitempty"`
	Symbol       string        `json:"symbol"`
	Digest       string        `json:"digest"`
}

// OwnerLayer is client ownership data checked against actual source definitions.
type OwnerLayer struct {
	Name    string        `json:"name"`
	Members []LayerMember `json:"members"`
}

// Definition is an engine fact, independent of selectors and dispositions.
type Definition struct {
	ID       string            `json:"id,omitempty"`
	ParentID string            `json:"parent_id,omitempty"`
	Kind     string            `json:"kind,omitempty"`
	Values   []ValueDefinition `json:"values,omitempty"`
	Writes   []string          `json:"writes,omitempty"`
	Escapes  []string          `json:"escapes,omitempty"`
	Symbol   string            `json:"symbol"`
	Package  string            `json:"package"`
	File     string            `json:"file"`
	Digest   string            `json:"digest"`
}

func classKinds(p Policy) (map[string]string, []string) {
	rules := map[string]string{
		"contextual statement owner": "statement", "non-statement boundary": "non-statement",
		"bounded": "bounded", "owned by another layer": "delegated",
	}
	var failures []string
	for class, kind := range p.ClassKinds {
		if !contains([]string{"statement", "non-statement", "bounded", "delegated", "observing-owner", "review"}, kind) {
			failures = append(failures, "UNKNOWN CLASS KIND "+class)
		} else if fixed, ok := rules[class]; ok && fixed != kind {
			failures = append(failures, "CLASS KIND CONTRADICTION "+class)
		} else {
			rules[class] = kind
		}
	}
	for _, class := range p.BoundedClasses {
		if prior := rules[class]; prior != "" && prior != "bounded" {
			failures = append(failures, "CLASS KIND CONTRADICTION "+class)
		} else {
			rules[class] = "bounded"
		}
	}
	for _, class := range p.Classes {
		if rules[class] == "" {
			failures = append(failures, "MISSING CLASS KIND "+class)
		}
	}
	return rules, failures
}

func dispositionFailures(m, approval Member, kind string, definitions map[string]Definition, layers map[string]map[string]string) []string {
	var failures []string
	switch kind {
	case "non-statement":
		if m.Reachable {
			failures = append(failures, "REACHABLE NON-STATEMENT "+m.ID)
		}
	case "statement", "observing-owner":
		if !m.Reachable {
			failures = append(failures, "UNREACHABLE STATEMENT "+m.ID)
		}
	case "bounded":
		if !m.BoundKnown || m.Bound < 0 || m.BoundReason == "" {
			failures = append(failures, "UNSOUND BOUND "+m.ID)
		}
	case "delegated":
		if m.UnresolvedTargets {
			failures = append(failures, "UNRESOLVED DELEGATED CALLEE "+m.ID)
		}
		layer, known := layers[approval.OwnerLayer]
		if strings.TrimSpace(approval.OwnerLayer) == "" || !known {
			failures = append(failures, "MISSING OWNER LAYER "+m.ID)
			break
		}
		// Delegating a nested call does not delegate its enclosing enumeration.
		if m.Node != "call" || len(m.Callees) == 0 {
			failures = append(failures, "NO DELEGATABLE CALLEE "+m.ID)
			break
		}
		for _, callee := range m.Callees {
			definition, visible := definitions[callee]
			if digest, contained := layer[callee]; !contained || !visible || digest == "" || digest != definition.Digest {
				failures = append(failures, "OWNER LAYER CALLEE "+m.ID+" "+callee)
			}
		}
	}
	return failures
}
