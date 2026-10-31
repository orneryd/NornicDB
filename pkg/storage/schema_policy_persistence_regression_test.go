package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"testing"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
)

// errPolicyPersistenceRejected is the persister failure injected by these
// tests. A rejected persistence must publish nothing: no readable schema, no
// recompiled binding table and no change callback.
var errPolicyPersistenceRejected = errors.New("policy persistence rejected")

// policyPersistenceState is every reader-visible projection of the
// knowledge-policy schema family: the exported definition the persister
// receives, the SHOW projections the Cypher and procedure surfaces return, and
// the compiled binding table the scorer reads.
type policyPersistenceState struct {
	Definition string
	Bundles    string
	Bindings   string
	Profiles   string
	Policies   string
	Table      string
}

// policyPersistencePointer matches the pointer values fmt prints, so the
// compiled binding table is compared by content. Allocations alone are checked
// separately, because rebuilding a table from unchanged state allocates new
// memory while the value pointers it prints may be reused between entries.
var policyPersistencePointer = regexp.MustCompile(`0x[0-9a-f]+`)

func policyPersistenceTableContent(table *knowledgepolicy.BindingTable) string {
	return policyPersistencePointer.ReplaceAllString(fmt.Sprintf("%#v", table), "0x0")
}

// policyPersistenceSnapshot captures the projections a rejected mutation must
// leave exactly as they were.
func policyPersistenceSnapshot(t *testing.T, sm *SchemaManager) policyPersistenceState {
	t.Helper()
	blob, err := json.Marshal(sm.ExportDefinition())
	require.NoError(t, err)
	bundles, bindings := sm.ShowDecayProfiles()
	return policyPersistenceState{
		Definition: string(blob),
		Bundles:    fmt.Sprintf("%#v", bundles),
		Bindings:   fmt.Sprintf("%#v", bindings),
		Profiles:   fmt.Sprintf("%#v", sm.ShowPromotionProfiles()),
		Policies:   fmt.Sprintf("%#v", sm.ShowPromotionPolicies()),
		Table:      policyPersistenceTableContent(sm.GetBindingTable()),
	}
}

// seedPolicyPersistenceLayout seeds the layout every mutator form starts from:
// one decay profile bundle and binding plus one promotion profile and policy,
// so the exported definition, every SHOW projection and the compiled binding
// table are non-empty and a rejected mutation has published state it could
// wrongly disturb.
func seedPolicyPersistenceLayout(t *testing.T, sm *SchemaManager) {
	t.Helper()
	require.NoError(t, sm.CreateDecayProfileBundle(validBundle("base_bundle")))
	require.NoError(t, sm.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{
		Name:         "base_binding",
		TargetLabels: []string{"BaseLabel"},
		ProfileRef:   "base_bundle",
	}))
	require.NoError(t, sm.CreatePromotionProfile(validPromoProfile("base_profile")))
	require.NoError(t, sm.CreatePromotionPolicy(knowledgepolicy.PromotionPolicyDef{
		Name:         "base_policy",
		TargetLabels: []string{"BaseLabel"},
		Enabled:      true,
	}))
}

func policyPersistencePolicy(name, label string) knowledgepolicy.PromotionPolicyDef {
	return knowledgepolicy.PromotionPolicyDef{Name: name, TargetLabels: []string{label}, Enabled: true}
}

func policyPersistenceBinding(name, bundle, label string) knowledgepolicy.DecayProfileBinding {
	return knowledgepolicy.DecayProfileBinding{Name: name, TargetLabels: []string{label}, ProfileRef: bundle}
}

// policyPersistenceMutator is one form of the knowledge-policy mutator family
// with the state it needs and, for the CREATE forms, its IF NOT EXISTS form.
type policyPersistenceMutator struct {
	name   string
	seed   func(*SchemaManager) error
	mutate func(*SchemaManager) error
	noOp   func(*SchemaManager) error // nil when the form has no IF NOT EXISTS form
	// noOpSeed creates the state the IF NOT EXISTS no-op form must find.
	noOpSeed func(*SchemaManager) error
}

// policyPersistenceMutators is the complete mutator family: thirteen forms
// across the twelve exported mutators (DropDecayProfile removes either a bundle
// or a binding), each carrying a real change of its own state.
func policyPersistenceMutators() []policyPersistenceMutator {
	const (
		bundleName  = "audit_bundle"
		bindingName = "audit_binding"
		profileName = "audit_profile"
		policyName  = "audit_policy"
		label       = "AuditLabel"
	)

	createBundle := func(sm *SchemaManager) error {
		return sm.CreateDecayProfileBundle(validBundle(bundleName))
	}
	createBinding := func(sm *SchemaManager) error {
		return sm.CreateDecayProfileBinding(policyPersistenceBinding(bindingName, bundleName, label))
	}
	createProfile := func(sm *SchemaManager) error {
		return sm.CreatePromotionProfile(validPromoProfile(profileName))
	}
	createPolicy := func(sm *SchemaManager) error {
		return sm.CreatePromotionPolicy(policyPersistencePolicy(policyName, label))
	}
	createBundleAndBinding := func(sm *SchemaManager) error {
		if err := createBundle(sm); err != nil {
			return err
		}
		return createBinding(sm)
	}

	return []policyPersistenceMutator{
		{
			name:     "create_decay_bundle",
			mutate:   createBundle,
			noOp:     func(sm *SchemaManager) error { return sm.CreateDecayProfileBundle(validBundle(bundleName), true) },
			noOpSeed: createBundle,
		},
		{
			name:   "create_decay_binding",
			seed:   createBundle,
			mutate: createBinding,
			noOp: func(sm *SchemaManager) error {
				return sm.CreateDecayProfileBinding(policyPersistenceBinding(bindingName, bundleName, label), true)
			},
			noOpSeed: createBinding,
		},
		{
			name:     "create_promotion_profile",
			mutate:   createProfile,
			noOp:     func(sm *SchemaManager) error { return sm.CreatePromotionProfile(validPromoProfile(profileName), true) },
			noOpSeed: createProfile,
		},
		{
			name:   "create_promotion_policy",
			mutate: createPolicy,
			noOp: func(sm *SchemaManager) error {
				return sm.CreatePromotionPolicy(policyPersistencePolicy(policyName, label), true)
			},
			noOpSeed: createPolicy,
		},
		{
			name: "alter_decay_bundle",
			seed: createBundle,
			mutate: func(sm *SchemaManager) error {
				return sm.AlterDecayProfile(bundleName, map[string]interface{}{"halfLifeSeconds": int64(60)})
			},
		},
		{
			name: "alter_decay_binding",
			seed: createBundleAndBinding,
			mutate: func(sm *SchemaManager) error {
				return sm.AlterDecayProfileBinding(bindingName, policyPersistenceBinding(bindingName, bundleName, "AuditOther"))
			},
		},
		{
			name:   "drop_decay_bundle",
			seed:   createBundle,
			mutate: func(sm *SchemaManager) error { return sm.DropDecayProfile(bundleName) },
		},
		{
			name:   "drop_decay_binding",
			seed:   createBundleAndBinding,
			mutate: func(sm *SchemaManager) error { return sm.DropDecayProfile(bindingName) },
		},
		{
			name: "alter_promotion_profile",
			seed: createProfile,
			mutate: func(sm *SchemaManager) error {
				return sm.AlterPromotionProfile(profileName, map[string]interface{}{"multiplier": 2.0})
			},
		},
		{
			name:   "drop_promotion_profile",
			seed:   createProfile,
			mutate: func(sm *SchemaManager) error { return sm.DropPromotionProfile(profileName) },
		},
		{
			name: "alter_promotion_policy",
			seed: createPolicy,
			mutate: func(sm *SchemaManager) error {
				return sm.AlterPromotionPolicy(policyName, map[string]interface{}{"enabled": false})
			},
		},
		{
			name: "alter_promotion_policy_definition",
			seed: createPolicy,
			mutate: func(sm *SchemaManager) error {
				return sm.AlterPromotionPolicyDefinition(policyName, policyPersistencePolicy(policyName, "AuditOther"))
			},
		},
		{
			name:   "drop_promotion_policy",
			seed:   createPolicy,
			mutate: func(sm *SchemaManager) error { return sm.DropPromotionPolicy(policyName) },
		},
	}
}

// newPolicyPersistenceSeeded builds a schema holding the shared layout plus the
// mutator's own seed, with no persister, so the seeds are admitted normally.
func newPolicyPersistenceSeeded(t *testing.T, c policyPersistenceMutator) *SchemaManager {
	t.Helper()
	sm := NewSchemaManager()
	seedPolicyPersistenceLayout(t, sm)
	if c.seed != nil {
		require.NoError(t, c.seed(sm), "seed %s", c.name)
	}
	return sm
}

// newPolicyPersistenceFixture seeds the shared layout, the mutator seed and the
// optional extra seeds, then returns the manager together with the projections
// and the compiled binding-table allocation a rejected mutation must leave
// untouched.
func newPolicyPersistenceFixture(t *testing.T, c policyPersistenceMutator, extra ...func(*SchemaManager) error) (*SchemaManager, policyPersistenceState, *knowledgepolicy.BindingTable) {
	t.Helper()
	sm := newPolicyPersistenceSeeded(t, c)
	for _, seed := range extra {
		require.NoError(t, seed(sm), "seed %s", c.name)
	}
	before := policyPersistenceSnapshot(t, sm)
	table := sm.GetBindingTable()
	require.NotNil(t, table, "the fixture must compile a binding table so a rejected persist can be checked against it")
	return sm, before, table
}

// TestKnowledgePolicyRejectedPersistenceKeepsPublishedSchema covers every
// knowledge-policy mutator form. A rejected persistence must stage exactly one
// changed definition, publish nothing (the exported schema definition, every
// SHOW projection and the compiled binding table), notify nobody, and leave no
// ghost edit that a later accepted write could make durable.
func TestKnowledgePolicyRejectedPersistenceKeepsPublishedSchema(t *testing.T) {
	mutators := policyPersistenceMutators()
	require.Len(t, mutators, 13, "every knowledge-policy mutator form must be covered")

	t.Run("rejected mutation keeps the published schema", func(t *testing.T) {
		for _, c := range mutators {
			t.Run(c.name, func(t *testing.T) {
				sm, before, table := newPolicyPersistenceFixture(t, c)

				persisted := 0
				var staged *SchemaDefinition
				sm.SetPersister(func(def *SchemaDefinition) error {
					persisted++
					staged = def
					return errPolicyPersistenceRejected
				})
				changed := 0
				sm.SetKnowledgePolicyChangedHook(func() { changed++ })

				require.ErrorIs(t, c.mutate(sm), errPolicyPersistenceRejected)
				require.Equal(t, 1, persisted, "the mutation must reach persistence exactly once")
				require.NotNil(t, staged, "the rejected mutation must stage a definition")

				stagedBlob, err := json.Marshal(staged)
				require.NoError(t, err)
				require.NotEqual(t, before.Definition, string(stagedBlob),
					"the rejected mutation must actually change the definition it stages, or this case is vacuous")

				require.Zero(t, changed, "no change callback after a rejected persistence")
				require.Equal(t, before, policyPersistenceSnapshot(t, sm), "a rejected change must not become the readable schema")
				require.Same(t, table, sm.GetBindingTable(), "a rejected change must not replace the compiled binding table")
			})
		}
	})

	t.Run("IF NOT EXISTS on an existing name persists nothing", func(t *testing.T) {
		noOps := 0
		for _, c := range mutators {
			if c.noOp == nil {
				continue
			}
			noOps++
			t.Run(c.name, func(t *testing.T) {
				sm, before, table := newPolicyPersistenceFixture(t, c, c.noOpSeed)

				persisted := 0
				sm.SetPersister(func(*SchemaDefinition) error {
					persisted++
					return errPolicyPersistenceRejected
				})
				changed := 0
				sm.SetKnowledgePolicyChangedHook(func() { changed++ })

				require.NoError(t, c.noOp(sm))
				require.Zero(t, persisted, "IF NOT EXISTS on an existing name must not persist")
				require.Zero(t, changed, "IF NOT EXISTS on an existing name must not notify")
				require.Equal(t, before, policyPersistenceSnapshot(t, sm))
				require.Same(t, table, sm.GetBindingTable(), "the no-op must not replace the compiled binding table")
			})
		}
		require.Equal(t, 4, noOps, "each of the four CREATE forms must have its IF NOT EXISTS no-op covered")
	})

	t.Run("rejected mutation does not ride a later accepted write", func(t *testing.T) {
		for _, c := range mutators {
			t.Run(c.name, func(t *testing.T) {
				sm, before, _ := newPolicyPersistenceFixture(t, c)

				rejecting := true
				var durable []string
				sm.SetPersister(func(def *SchemaDefinition) error {
					if rejecting {
						return errPolicyPersistenceRejected
					}
					blob, err := json.Marshal(def)
					if err != nil {
						return err
					}
					durable = append(durable, string(blob))
					return nil
				})

				require.ErrorIs(t, c.mutate(sm), errPolicyPersistenceRejected)

				rejecting = false
				accepted := policyPersistencePolicy("accepted_policy", "AcceptedLabel")
				require.NoError(t, sm.CreatePromotionPolicy(accepted))
				require.Len(t, durable, 1, "the accepted write must persist exactly once")

				// A schema that only ever received the fixture seeds and the accepted
				// change is exactly what the accepted write must make durable, so a
				// ghost edit retained from the rejected mutation cannot match it.
				witness := newPolicyPersistenceSeeded(t, c)
				require.NoError(t, witness.CreatePromotionPolicy(accepted))
				witnessBlob, err := json.Marshal(witness.ExportDefinition())
				require.NoError(t, err)

				require.Equal(t, string(witnessBlob), durable[0], "the accepted persist must contain no ghost edit from the rejected mutation")
				require.Equal(t, policyPersistenceSnapshot(t, sm).Definition, durable[0], "the accepted write must make the published definition durable")
				require.NotEqual(t, before.Definition, durable[0], "the accepted change must really reach the durable definition")
			})
		}
	})
}
