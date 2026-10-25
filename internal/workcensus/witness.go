package workcensus

import (
	"fmt"
	"path/filepath"
	"strings"
)

// CallWitness pins an observation call selected by client data inside the exact
// delegated callee. It proves source presence, not execution on every path.
type CallWitness struct {
	MemberID       string `json:"member_id"`
	Digest         string `json:"digest"`
	Callee         string `json:"callee"`
	RemovalControl string `json:"removal_control"`
}

// SourceControl replaces one engine-owned call span before running the SAME
// Extract path again. Clients supply a type-correct neutral replacement, such as
// error(nil). A compile failure is an invalid control, not a successful rejection.
type SourceControl struct {
	ID          string `json:"id"`
	MemberID    string `json:"member_id"`
	Replacement string `json:"replacement"`
}

// ExtractWithControl performs source mutation, never inventory mutation. The
// returned candidate must go through the client's ordinary Validate call; a
// removal control requires a delegation-observation failure for its calling row.
func ExtractWithControl(c Config, control SourceControl) (Inventory, error) {
	baseline, err := Extract(c)
	if err != nil {
		return Inventory{}, err
	}
	if strings.TrimSpace(control.ID) == "" || control.Replacement == "" {
		return Inventory{}, fmt.Errorf("invalid source control")
	}
	var site *Member
	for i := range baseline.Members {
		if baseline.Members[i].ID == control.MemberID {
			if site != nil {
				return Inventory{}, fmt.Errorf("ambiguous control member %s", control.MemberID)
			}
			site = &baseline.Members[i]
		}
	}
	if site == nil || site.Node != "call" || site.File == "" {
		return Inventory{}, fmt.Errorf("control requires a selected source call: %s", control.MemberID)
	}
	root, err := filepath.Abs(c.Dir)
	if err != nil {
		return Inventory{}, err
	}
	path := filepath.Join(root, filepath.FromSlash(site.File))
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return Inventory{}, fmt.Errorf("control source outside configured root")
	}
	raw, err := readSource(c, path)
	if err != nil {
		return Inventory{}, err
	}
	for _, source := range baseline.Sources {
		if source.File == site.File && source.Digest != fingerprint(raw) {
			return Inventory{}, fmt.Errorf("control source changed after extraction")
		}
	}
	if site.Offset < 0 || site.EndOffset <= site.Offset || site.EndOffset > len(raw) {
		return Inventory{}, fmt.Errorf("invalid control span")
	}
	mutated := append([]byte(nil), raw[:site.Offset]...)
	mutated = append(mutated, control.Replacement...)
	mutated = append(mutated, raw[site.EndOffset:]...)
	overlay := map[string][]byte{}
	for path, body := range c.Overlay {
		overlay[path] = body
	}
	overlay[path] = mutated
	c.Overlay = overlay
	return Extract(c)
}

func delegationWitnessFailures(m, a Member, p Policy, actual map[string]Member) []string {
	if !p.RequireDelegationWitnesses {
		return nil
	}
	var failures []string
	controls := map[string]SourceControl{}
	for _, control := range p.Controls {
		controls[control.ID] = control
	}
	for _, callee := range m.Callees {
		var witnesses []CallWitness
		for _, layer := range p.Layers {
			if layer.Name == a.OwnerLayer {
				for _, member := range layer.Members {
					if member.Symbol == callee {
						witnesses = member.Observations
					}
				}
			}
		}
		if len(witnesses) == 0 {
			failures = append(failures, "NO DELEGATION OBSERVATION "+m.ID+" "+callee)
			continue
		}
		for _, witness := range witnesses {
			control, declared := controls[witness.RemovalControl]
			if !declared || control.MemberID != witness.MemberID || control.Replacement == "" {
				failures = append(failures, "NO OBSERVATION REMOVAL CONTROL "+m.ID+" "+witness.MemberID)
			}
			site, found := actual[witness.MemberID]
			if !found {
				failures = append(failures, "DELEGATION OBSERVATION MISSING "+m.ID+" "+witness.MemberID)
				continue
			}
			if site.Node != "call" || site.OwnerSymbol != callee {
				failures = append(failures, "DELEGATION OBSERVATION OWNER "+m.ID+" "+witness.MemberID)
			}
			if witness.Digest == "" || site.Digest != witness.Digest || witness.Callee == "" || !contains(site.Callees, witness.Callee) {
				failures = append(failures, "DELEGATION OBSERVATION CHANGED "+m.ID+" "+witness.MemberID)
			}
		}
	}
	return failures
}
