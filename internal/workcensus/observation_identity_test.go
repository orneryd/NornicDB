package workcensus

// Author: #1667 implementation lane (Codex). These controls exercise the same
// source mutation/extraction/Validate path as the clients, with repeated calls.

import (
	"encoding/json"
	"sort"
	"strings"
	"testing"
)

// JSON keeps this behavioral regression executable on the older public API.
// A source-site field, when supplied by the engine, is consumed as engine DATA;
// the fixture never manufactures an observation identity.
func repeatedCallWitness(t *testing.T, member Member, control string) CallWitness {
	t.Helper()
	raw, err := json.Marshal(member)
	if err != nil {
		t.Fatal(err)
	}
	var identity struct {
		SiteID string `json:"site_id"`
	}
	if err := json.Unmarshal(raw, &identity); err != nil {
		t.Fatal(err)
	}
	raw, err = json.Marshal(map[string]string{"member_id": member.ID, "site_id": identity.SiteID, "digest": member.Digest, "callee": member.Callees[0], "removal_control": control})
	if err != nil {
		t.Fatal(err)
	}
	var witness CallWitness
	if err := json.Unmarshal(raw, &witness); err != nil {
		t.Fatal(err)
	}
	return witness
}

func TestRepeatedObservationRemovalIdentity(t *testing.T) {
	for _, delegated := range []bool{false, true} {
		name := "containing-owner"
		if delegated {
			name = "delegated-call"
		}
		t.Run(name, func(t *testing.T) {
			c := containingFixtureConfig(t, strings.Replace(containingFixture, "_ = lowerObserve(scope); lowerEvent(scope)", "_ = lowerObserve(scope); _ = lowerObserve(scope); lowerEvent(scope)", 1))
			c.Selectors = append(c.Selectors, Selector{Kind: "delegated call", Node: "call", Names: []string{"lowerInside"}, Packages: []string{"census.fixture"}})
			baseline := mustExtract(t, c)
			var observations []Member
			for _, member := range baseline.Members {
				if member.Kind == "observation" && strings.Contains(member.Owner, "lowerInside") {
					observations = append(observations, member)
				}
			}
			sort.Slice(observations, func(i, j int) bool { return observations[i].Offset < observations[j].Offset })
			if len(observations) != 2 || observations[0].Digest != observations[1].Digest {
				t.Fatal("fixture needs two identical observation calls", observations)
			}
			for selected := range observations {
				for removed := range observations {
					t.Run(string(rune('0'+selected))+"/remove"+string(rune('0'+removed)), func(t *testing.T) {
						approval, policy, loop, _ := containingApproval(t, baseline)
						control := SourceControl{ID: "remove selected witness", MemberID: observations[selected].ID, Replacement: "0"}
						witness := repeatedCallWitness(t, observations[selected], control.ID)
						policy.Controls = []SourceControl{control}
						var workID string
						if delegated {
							policy.RequireDelegationWitnesses = true
							policy.Classes = append(policy.Classes, "delegated")
							policy.ClassKinds["delegated"] = "delegated"
							policy.Layers[0].Members[0].Observations = []CallWitness{witness}
						}
						for i := range approval.Members {
							m := &approval.Members[i]
							if m.ContainingOwner != nil {
								m.ContainingOwner.Observation = witness
								if delegated {
									m.Class, m.ContainingOwner = "reviewed", nil
								}
							}
							if delegated && m.Kind == "delegated call" {
								m.Class, m.OwnerLayer = "delegated", policy.Layers[0].Name
								workID = m.ID
							}
						}
						if !delegated {
							workID = loop.ID
						}
						if failures := Validate(baseline, approval, policy); len(failures) != 0 {
							t.Fatal("baseline witness must be valid", failures)
						}
						mutation := SourceControl{ID: "remove actual occurrence", MemberID: observations[removed].ID, Replacement: "0"}
						actual, err := ExtractWithControl(c, mutation)
						if err != nil {
							t.Fatal("source control must type-load", err)
						}
						// Refresh ordinary membership/body pins so only the actual
						// observation contract can reject its own removal.
						remaining := reviewed(actual)
						for i := range policy.Layers[0].Members {
							for _, definition := range actual.Definitions {
								if definition.ID == loop.OwnerID {
									policy.Layers[0].Members[i].Digest = definition.Digest
								}
							}
						}
						for i := range remaining.Members {
							for _, original := range approval.Members {
								if original.ID == remaining.Members[i].ID && (original.ID == workID || !delegated && original.ContainingOwner != nil) {
									remaining.Members[i].Class, remaining.Members[i].OwnerLayer = original.Class, original.OwnerLayer
									if original.ContainingOwner != nil {
										copy := *original.ContainingOwner
										copy.OwnerDigest = remaining.Members[i].OwnerDigest
										remaining.Members[i].ContainingOwner = &copy
									}
								}
							}
						}
						failures := Validate(actual, remaining, policy)
						if removed != selected {
							if len(failures) != 0 {
								t.Fatal("removing another identical occurrence changed the retained witness", failures)
							}
							return
						}
						prefix := "OWNER OBSERVATION MISSING "
						if delegated {
							prefix = "DELEGATION OBSERVATION MISSING "
						}
						if !strings.Contains(strings.Join(failures, "\n"), prefix+workID+" "+observations[selected].ID) {
							t.Fatal("removed observation borrowed an identical later call", failures)
						}
					})
				}
			}
		})
	}
}
