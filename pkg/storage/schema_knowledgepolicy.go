package storage

import (
	"fmt"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// createKnowledgeProfile is the shared create path for knowledge-policy
// profiles (decay bundles, promotion profiles): ensure the map, reject a
// duplicate name (honoring IF NOT EXISTS), validate, store a copy and finish
// the mutation.
func createKnowledgeProfile[T any](
	sm *SchemaManager,
	profiles *map[string]*T,
	name string,
	value T,
	alreadyExists func(string) localization.Message,
	validate func(*T) error,
	ifNotExists bool,
) error {
	sm.mu.Lock()
	if *profiles == nil {
		*profiles = make(map[string]*T)
	}
	if _, exists := (*profiles)[name]; exists {
		sm.mu.Unlock()
		if ifNotExists {
			return nil
		}
		return localizedError(alreadyExists(name), nil)
	}
	if err := validate(&value); err != nil {
		sm.mu.Unlock()
		return err
	}
	(*profiles)[name] = &value
	return sm.finishKnowledgePolicyMutationLocked()
}

// alterKnowledgeProfile is the shared update path for knowledge-policy
// profiles: find the named profile, apply the option updates to a copy,
// validate the copy and finish the mutation.
func alterKnowledgeProfile[T any](
	sm *SchemaManager,
	profiles map[string]*T,
	notFound func(string) localization.Message,
	apply func(*T, map[string]interface{}) error,
	validate func(*T) error,
	name string,
	updates map[string]interface{},
) error {
	sm.mu.Lock()
	if profiles == nil {
		sm.mu.Unlock()
		return localizedError(notFound(name), nil)
	}
	profile, ok := profiles[name]
	if !ok {
		sm.mu.Unlock()
		return localizedError(notFound(name), nil)
	}
	updated := *profile
	if err := apply(&updated, updates); err != nil {
		sm.mu.Unlock()
		return err
	}
	if err := validate(&updated); err != nil {
		sm.mu.Unlock()
		return err
	}
	profiles[name] = &updated
	return sm.finishKnowledgePolicyMutationLocked()
}

// CreateDecayProfileBundle adds a decay profile bundle to the schema.
func (sm *SchemaManager) CreateDecayProfileBundle(bundle knowledgepolicy.DecayProfileBundle, ifNotExists ...bool) error {
	return createKnowledgeProfile(sm, &sm.decayProfileBundles, bundle.Name, bundle, localization.StorageSchemaDecayProfileBundleAlreadyExists, validateDecayProfileBundle, len(ifNotExists) > 0 && ifNotExists[0])
}

// CreateDecayProfileBinding adds a decay profile binding to the schema.
func (sm *SchemaManager) CreateDecayProfileBinding(binding knowledgepolicy.DecayProfileBinding, ifNotExists ...bool) error {
	sm.mu.Lock()

	if sm.decayProfileBindings == nil {
		sm.decayProfileBindings = make(map[string]*knowledgepolicy.DecayProfileBinding)
	}

	if _, exists := sm.decayProfileBindings[binding.Name]; exists {
		if len(ifNotExists) > 0 && ifNotExists[0] {
			sm.mu.Unlock()
			return nil
		}
		sm.mu.Unlock()
		return localizedError(localization.StorageSchemaDecayProfileBindingAlreadyExists(binding.Name), nil)
	}

	if binding.ProfileRef != "" && sm.decayProfileBundles != nil {
		if _, ok := sm.decayProfileBundles[binding.ProfileRef]; !ok {
			sm.mu.Unlock()
			return localizedError(localization.StorageSchemaDecayProfileBundleNotFound(binding.ProfileRef), nil)
		}
	}

	if err := sm.validateBindingTarget(&binding); err != nil {
		sm.mu.Unlock()
		return err
	}

	sort.Strings(binding.TargetLabels)
	b := binding
	sm.decayProfileBindings[b.Name] = &b
	return sm.finishKnowledgePolicyMutationLocked()
}

// DropDecayProfile removes a decay profile bundle or binding by name.
func (sm *SchemaManager) DropDecayProfile(name string, ifExists ...bool) error {
	sm.mu.Lock()

	if sm.decayProfileBundles != nil {
		if _, exists := sm.decayProfileBundles[name]; exists {
			if sm.isBundleReferenced(name) {
				sm.mu.Unlock()
				return localizedError(localization.StorageSchemaDecayProfileBundleReferenced(name), nil)
			}
			delete(sm.decayProfileBundles, name)
			return sm.finishKnowledgePolicyMutationLocked()
		}
	}

	if sm.decayProfileBindings != nil {
		if _, exists := sm.decayProfileBindings[name]; exists {
			delete(sm.decayProfileBindings, name)
			return sm.finishKnowledgePolicyMutationLocked()
		}
	}

	if len(ifExists) > 0 && ifExists[0] {
		sm.mu.Unlock()
		return nil
	}
	sm.mu.Unlock()
	return localizedError(localization.StorageSchemaDecayProfileNotFound(name), nil)
}

// AlterDecayProfile updates options on an existing decay profile bundle.
func (sm *SchemaManager) AlterDecayProfile(name string, updates map[string]interface{}) error {
	return alterKnowledgeProfile(sm, sm.decayProfileBundles, localization.StorageSchemaDecayProfileBundleNotFound, applyBundleUpdates, validateDecayProfileBundle, name, updates)
}

// AlterDecayProfileBinding replaces the target and APPLY definition of an existing binding.
func (sm *SchemaManager) AlterDecayProfileBinding(name string, replacement knowledgepolicy.DecayProfileBinding) error {
	sm.mu.Lock()

	existing, ok := sm.decayProfileBindings[name]
	if !ok {
		sm.mu.Unlock()
		return localizedError(localization.StorageSchemaDecayProfileNotFound(name), nil)
	}

	replacement.Name = name
	replacement.Order = existing.Order
	if replacement.ProfileRef != "" {
		if _, ok := sm.decayProfileBundles[replacement.ProfileRef]; !ok {
			sm.mu.Unlock()
			return localizedError(localization.StorageSchemaDecayProfileBundleNotFound(replacement.ProfileRef), nil)
		}
	}
	if err := sm.validateBindingTarget(&replacement); err != nil {
		sm.mu.Unlock()
		return err
	}

	sort.Strings(replacement.TargetLabels)
	sm.decayProfileBindings[name] = &replacement
	return sm.finishKnowledgePolicyMutationLocked()
}

// ShowDecayProfiles returns all stored decay profile bundles and bindings.
func (sm *SchemaManager) ShowDecayProfiles() ([]knowledgepolicy.DecayProfileBundle, []knowledgepolicy.DecayProfileBinding) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	bundles := make([]knowledgepolicy.DecayProfileBundle, 0, len(sm.decayProfileBundles))
	for _, b := range sm.decayProfileBundles {
		bundles = append(bundles, *b)
	}
	sort.Slice(bundles, func(i, j int) bool { return bundles[i].Name < bundles[j].Name })

	bindings := make([]knowledgepolicy.DecayProfileBinding, 0, len(sm.decayProfileBindings))
	for _, b := range sm.decayProfileBindings {
		bindings = append(bindings, *b)
	}
	sort.Slice(bindings, func(i, j int) bool { return bindings[i].Name < bindings[j].Name })

	return bundles, bindings
}

// CreatePromotionProfile adds a promotion profile to the schema.
func (sm *SchemaManager) CreatePromotionProfile(profile knowledgepolicy.PromotionProfileDef, ifNotExists ...bool) error {
	return createKnowledgeProfile(sm, &sm.promotionProfiles, profile.Name, profile, localization.StorageSchemaPromotionProfileAlreadyExists, validatePromotionProfile, len(ifNotExists) > 0 && ifNotExists[0])
}

// DropPromotionProfile removes a promotion profile by name.
func (sm *SchemaManager) DropPromotionProfile(name string, ifExists ...bool) error {
	sm.mu.Lock()

	if sm.promotionProfiles == nil {
		if len(ifExists) > 0 && ifExists[0] {
			sm.mu.Unlock()
			return nil
		}
		sm.mu.Unlock()
		return localizedError(localization.StorageSchemaPromotionProfileNotFound(name), nil)
	}

	if _, exists := sm.promotionProfiles[name]; !exists {
		if len(ifExists) > 0 && ifExists[0] {
			sm.mu.Unlock()
			return nil
		}
		sm.mu.Unlock()
		return localizedError(localization.StorageSchemaPromotionProfileNotFound(name), nil)
	}

	if sm.isPromotionProfileReferenced(name) {
		sm.mu.Unlock()
		return localizedError(localization.StorageSchemaPromotionProfileReferenced(name), nil)
	}

	delete(sm.promotionProfiles, name)
	return sm.finishKnowledgePolicyMutationLocked()
}

// AlterPromotionProfile updates options on an existing promotion profile.
func (sm *SchemaManager) AlterPromotionProfile(name string, updates map[string]interface{}) error {
	return alterKnowledgeProfile(sm, sm.promotionProfiles, localization.StorageSchemaPromotionProfileNotFound, applyPromotionProfileUpdates, validatePromotionProfile, name, updates)
}

// ShowPromotionProfiles returns all stored promotion profiles.
func (sm *SchemaManager) ShowPromotionProfiles() []knowledgepolicy.PromotionProfileDef {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	profiles := make([]knowledgepolicy.PromotionProfileDef, 0, len(sm.promotionProfiles))
	for _, p := range sm.promotionProfiles {
		profiles = append(profiles, *p)
	}
	sort.Slice(profiles, func(i, j int) bool { return profiles[i].Name < profiles[j].Name })
	return profiles
}

// CreatePromotionPolicy adds a promotion policy to the schema.
func (sm *SchemaManager) CreatePromotionPolicy(policy knowledgepolicy.PromotionPolicyDef, ifNotExists ...bool) error {
	sm.mu.Lock()

	if sm.promotionPolicies == nil {
		sm.promotionPolicies = make(map[string]*knowledgepolicy.PromotionPolicyDef)
	}

	if _, exists := sm.promotionPolicies[policy.Name]; exists {
		sm.mu.Unlock()
		if len(ifNotExists) > 0 && ifNotExists[0] {
			return nil
		}
		return localizedError(localization.StorageSchemaPromotionPolicyAlreadyExists(policy.Name), nil)
	}

	for _, wc := range policy.WhenClauses {
		if wc.ProfileRef != "" {
			if sm.promotionProfiles == nil {
				sm.mu.Unlock()
				return localizedError(localization.StorageSchemaPromotionProfileWhenClauseNotFound(wc.ProfileRef), nil)
			}
			if _, ok := sm.promotionProfiles[wc.ProfileRef]; !ok {
				sm.mu.Unlock()
				return localizedError(localization.StorageSchemaPromotionProfileWhenClauseNotFound(wc.ProfileRef), nil)
			}
		}
	}

	sort.Strings(policy.TargetLabels)
	p := policy
	sm.promotionPolicies[p.Name] = &p
	return sm.finishKnowledgePolicyMutationLocked()
}

// DropPromotionPolicy removes a promotion policy by name.
func (sm *SchemaManager) DropPromotionPolicy(name string, ifExists ...bool) error {
	sm.mu.Lock()

	if sm.promotionPolicies == nil {
		if len(ifExists) > 0 && ifExists[0] {
			sm.mu.Unlock()
			return nil
		}
		sm.mu.Unlock()
		return localizedError(localization.StorageSchemaPromotionPolicyNotFound(name), nil)
	}

	if _, exists := sm.promotionPolicies[name]; !exists {
		if len(ifExists) > 0 && ifExists[0] {
			sm.mu.Unlock()
			return nil
		}
		sm.mu.Unlock()
		return localizedError(localization.StorageSchemaPromotionPolicyNotFound(name), nil)
	}

	delete(sm.promotionPolicies, name)
	return sm.finishKnowledgePolicyMutationLocked()
}

// ValidatePromotionPolicyOptions accepts only a boolean enabled option.
// For example, ValidatePromotionPolicyOptions(map[string]interface{}{"enabled": false})
// succeeds, while unknown options and non-boolean enabled values return errors.
func ValidatePromotionPolicyOptions(updates map[string]interface{}) error {
	for key, value := range updates {
		if key != "enabled" {
			return localizedError(localization.StorageSchemaUnknownOption(key), nil)
		}
		if _, ok := value.(bool); !ok {
			return localizedError(localization.StorageSchemaOptionBooleanRequired(key), nil)
		}
	}
	return nil
}

// AlterPromotionPolicy updates an existing promotion policy.
// Invalid options are rejected before changing the policy or persisting schema.
func (sm *SchemaManager) AlterPromotionPolicy(name string, updates map[string]interface{}) error {
	sm.mu.Lock()

	if sm.promotionPolicies == nil {
		sm.mu.Unlock()
		return localizedError(localization.StorageSchemaPromotionPolicyNotFound(name), nil)
	}
	if _, ok := sm.promotionPolicies[name]; !ok {
		sm.mu.Unlock()
		return localizedError(localization.StorageSchemaPromotionPolicyNotFound(name), nil)
	}

	policy := sm.promotionPolicies[name]
	if err := ValidatePromotionPolicyOptions(updates); err != nil {
		sm.mu.Unlock()
		return err
	}
	if v, ok := updates["enabled"]; ok {
		if b, ok := v.(bool); ok {
			policy.Enabled = b
		}
	}
	return sm.finishKnowledgePolicyMutationLocked()
}

// AlterPromotionPolicyDefinition replaces the target and APPLY definition of an existing policy.
func (sm *SchemaManager) AlterPromotionPolicyDefinition(name string, replacement knowledgepolicy.PromotionPolicyDef) error {
	sm.mu.Lock()

	existing, ok := sm.promotionPolicies[name]
	if !ok {
		sm.mu.Unlock()
		return localizedError(localization.StorageSchemaPromotionPolicyNotFound(name), nil)
	}

	for _, clause := range replacement.WhenClauses {
		if clause.ProfileRef != "" {
			if _, ok := sm.promotionProfiles[clause.ProfileRef]; !ok {
				sm.mu.Unlock()
				return localizedError(localization.StorageSchemaPromotionProfileWhenClauseNotFound(clause.ProfileRef), nil)
			}
		}
	}

	replacement.Name = name
	replacement.Enabled = existing.Enabled
	sort.Strings(replacement.TargetLabels)
	sm.promotionPolicies[name] = &replacement
	return sm.finishKnowledgePolicyMutationLocked()
}

func (sm *SchemaManager) finishKnowledgePolicyMutationLocked() error {
	sm.rebuildBindingTableLocked()
	persist := sm.persist
	var def *SchemaDefinition
	if persist != nil {
		def = sm.exportDefinitionLocked()
	}
	onChanged := sm.knowledgePolicyChanged
	sm.mu.Unlock()
	if persist != nil {
		if err := persist(def); err != nil {
			return err
		}
	}
	if onChanged != nil {
		onChanged()
	}
	return nil
}

// ShowPromotionPolicies returns all stored promotion policies.
func (sm *SchemaManager) ShowPromotionPolicies() []knowledgepolicy.PromotionPolicyDef {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	policies := make([]knowledgepolicy.PromotionPolicyDef, 0, len(sm.promotionPolicies))
	for _, p := range sm.promotionPolicies {
		policies = append(policies, *p)
	}
	sort.Slice(policies, func(i, j int) bool { return policies[i].Name < policies[j].Name })
	return policies
}

// GetBindingTable returns the current compiled binding table, or nil if not built.
func (sm *SchemaManager) GetBindingTable() *knowledgepolicy.BindingTable {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.bindingTable
}

// SetBindingTable replaces the compiled binding table.
func (sm *SchemaManager) SetBindingTable(bt *knowledgepolicy.BindingTable) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.bindingTable = bt
}

// rebuildBindingTableLocked recompiles the BindingTable from current schema
// state. Called under sm.mu.Lock() after every mutating DDL operation so the
// scorer always sees a consistent, up-to-date table.
func (sm *SchemaManager) rebuildBindingTableLocked() {
	bt, err := knowledgepolicy.BuildBindingTable(
		sm.decayProfileBundles,
		sm.decayProfileBindings,
		sm.promotionProfiles,
		sm.promotionPolicies,
	)
	if err == nil {
		sm.bindingTable = bt
	}
}

// persistIfSet calls the persist hook if one is set.
func (sm *SchemaManager) persistIfSet() error {
	if sm.persist == nil {
		return nil
	}
	return sm.persist(sm.exportDefinitionLocked())
}

func (sm *SchemaManager) isBundleReferenced(name string) bool {
	for _, b := range sm.decayProfileBindings {
		if b.ProfileRef == name {
			return true
		}
	}
	return false
}

func (sm *SchemaManager) isPromotionProfileReferenced(name string) bool {
	for _, p := range sm.promotionPolicies {
		for _, wc := range p.WhenClauses {
			if wc.ProfileRef == name {
				return true
			}
		}
	}
	return false
}

func (sm *SchemaManager) validateBindingTarget(binding *knowledgepolicy.DecayProfileBinding) error {
	if binding.IsEdge {
		for _, existingBinding := range sm.decayProfileBindings {
			if existingBinding.IsEdge && existingBinding.TargetEdgeType == binding.TargetEdgeType && existingBinding.Name != binding.Name {
				return localizedError(localization.StorageSchemaDecayEdgeBindingConflict(binding.TargetEdgeType, existingBinding.Name), nil)
			}
		}
	} else if !binding.IsWildcard && len(binding.TargetLabels) > 0 {
		targetKey := sortedLabelKey(binding.TargetLabels)
		for _, existingBinding := range sm.decayProfileBindings {
			if !existingBinding.IsEdge && !existingBinding.IsWildcard && sortedLabelKey(existingBinding.TargetLabels) == targetKey && existingBinding.Name != binding.Name {
				return localizedError(localization.StorageSchemaDecayLabelBindingConflict(binding.TargetLabels, existingBinding.Name), nil)
			}
		}
	}

	for _, rule := range binding.PropertyRules {
		if sm.isPropertyInStructuralIndex(binding.TargetLabels, rule.PropertyPath) {
			return localizedError(localization.StorageSchemaDecayStructuralIndexConflict(rule.PropertyPath), nil)
		}
	}

	return nil
}

func (sm *SchemaManager) isPropertyInStructuralIndex(labels []string, property string) bool {
	for _, label := range labels {
		key := label + ":" + property
		if _, ok := sm.propertyIndexes[key]; ok {
			return true
		}
		if _, ok := sm.rangeIndexes[key]; ok {
			return true
		}
	}
	for _, idx := range sm.compositeIndexes {
		for _, label := range labels {
			if idx.Label == label {
				for _, prop := range idx.Properties {
					if prop == property {
						return true
					}
				}
			}
		}
	}
	return false
}

func sortedLabelKey(labels []string) string {
	sorted := make([]string, len(labels))
	copy(sorted, labels)
	sort.Strings(sorted)
	return strings.Join(sorted, "\x00")
}

func validateDecayProfileBundle(b *knowledgepolicy.DecayProfileBundle) error {
	if b.Name == "" {
		return localizedError(localization.StorageSchemaDecayProfileBundleNameRequired(), nil)
	}
	if !knowledgepolicy.ValidDecayFunctions[b.Function] {
		return localizedError(localization.StorageSchemaInvalidDecayFunction(b.Function), nil)
	}
	if !knowledgepolicy.ValidScoreFromModes[b.ScoreFrom] {
		return localizedError(localization.StorageSchemaInvalidScoreFromMode(b.ScoreFrom), nil)
	}
	if !knowledgepolicy.ValidScopeTypes[b.Scope] {
		return localizedError(localization.StorageSchemaInvalidScopeType(b.Scope), nil)
	}
	if b.ScoreFrom == knowledgepolicy.ScoreFromCustom && b.ScoreFromProperty == "" {
		return localizedError(localization.StorageSchemaScoreFromPropertyRequired(), nil)
	}
	if b.VisibilityThreshold < 0 || b.VisibilityThreshold > 1 {
		return localizedError(localization.StorageSchemaVisibilityThresholdOutOfRange(b.VisibilityThreshold), nil)
	}
	if b.ScoreFloor < 0 || b.ScoreFloor > 1 {
		return localizedError(localization.StorageSchemaScoreFloorOutOfRange(b.ScoreFloor), nil)
	}
	return nil
}

func validatePromotionProfile(p *knowledgepolicy.PromotionProfileDef) error {
	if p.Name == "" {
		return localizedError(localization.StorageSchemaPromotionProfileNameRequired(), nil)
	}
	if !knowledgepolicy.ValidScopeTypes[p.Scope] {
		return localizedError(localization.StorageSchemaInvalidScopeType(p.Scope), nil)
	}
	if p.Multiplier < 0 {
		return localizedError(localization.StorageSchemaMultiplierNonNegative(p.Multiplier), nil)
	}
	if p.ScoreCap < 0 || p.ScoreCap > 1 {
		return localizedError(localization.StorageSchemaScoreCapOutOfRange(p.ScoreCap), nil)
	}
	if p.ScoreFloor < 0 || p.ScoreFloor > 1 {
		return localizedError(localization.StorageSchemaScoreFloorOutOfRange(p.ScoreFloor), nil)
	}
	return nil
}

// ApplyDecayProfileOptions returns a validated updated bundle without modifying
// the input. Invalid options return an error and the original bundle.
// For example, updated, err := ApplyDecayProfileOptions(bundle,
// map[string]interface{}{"halfLifeSeconds": int64(3600)}) changes its half life.
func ApplyDecayProfileOptions(bundle knowledgepolicy.DecayProfileBundle, updates map[string]interface{}) (knowledgepolicy.DecayProfileBundle, error) {
	updated := bundle
	if err := applyBundleUpdates(&updated, updates); err != nil {
		return bundle, err
	}
	if err := validateDecayProfileBundle(&updated); err != nil {
		return bundle, err
	}
	return updated, nil
}

func applyBundleUpdates(bundle *knowledgepolicy.DecayProfileBundle, updates map[string]interface{}) error {
	for k, v := range updates {
		switch k {
		case "scope":
			scope, ok := v.(string)
			if !ok {
				return localizedError(localization.StorageSchemaInvalidScopeType(v), nil)
			}
			bundle.Scope = knowledgepolicy.ScopeType(scope)
		case "halfLifeSeconds":
			if n, ok := toInt64(v); ok {
				bundle.HalfLifeSeconds = n
			} else {
				return localizedError(localization.CypherKnowledgePolicyInvalidValue(k, fmt.Sprint(v), false), nil)
			}
		case "visibilityThreshold":
			if f, ok := toFloat64(v); ok {
				bundle.VisibilityThreshold = f
			} else {
				return localizedError(localization.CypherKnowledgePolicyInvalidValue(k, fmt.Sprint(v), false), nil)
			}
		case "scoreFloor":
			if f, ok := toFloat64(v); ok {
				bundle.ScoreFloor = f
			} else {
				return localizedError(localization.CypherKnowledgePolicyInvalidValue(k, fmt.Sprint(v), false), nil)
			}
		case "function":
			if s, ok := v.(string); ok {
				fn := knowledgepolicy.DecayFunction(s)
				if !knowledgepolicy.ValidDecayFunctions[fn] {
					return localizedError(localization.StorageSchemaInvalidDecayFunction(s), nil)
				}
				bundle.Function = fn
			} else {
				return localizedError(localization.StorageSchemaInvalidDecayFunction(v), nil)
			}
		case "decayEnabled":
			if b, ok := v.(bool); ok {
				bundle.DecayEnabled = b
			} else {
				return localizedError(localization.StorageSchemaOptionBooleanRequired(k), nil)
			}
		case "enabled":
			if b, ok := v.(bool); ok {
				bundle.Enabled = b
			} else {
				return localizedError(localization.StorageSchemaOptionBooleanRequired(k), nil)
			}
		case "scoreFrom":
			if s, ok := v.(string); ok {
				mode := knowledgepolicy.ScoreFromMode(s)
				if !knowledgepolicy.ValidScoreFromModes[mode] {
					return localizedError(localization.StorageSchemaInvalidScoreFromMode(s), nil)
				}
				bundle.ScoreFrom = mode
			} else {
				return localizedError(localization.StorageSchemaInvalidScoreFromMode(v), nil)
			}
		case "scoreFromProperty":
			if s, ok := v.(string); ok {
				bundle.ScoreFromProperty = s
			} else {
				return localizedError(localization.CypherKnowledgePolicyInvalidValue(k, fmt.Sprint(v), false), nil)
			}
		default:
			return localizedError(localization.StorageSchemaUnknownOption(k), nil)
		}
	}
	return nil
}

// ApplyPromotionProfileOptions returns a validated updated profile without
// modifying the input. Invalid options return an error and the input profile.
//
// Example:
//
//	updated, err := ApplyPromotionProfileOptions(profile, map[string]interface{}{"scoreFloor": 0.5})
func ApplyPromotionProfileOptions(profile knowledgepolicy.PromotionProfileDef, updates map[string]interface{}) (knowledgepolicy.PromotionProfileDef, error) {
	updated := profile
	if err := applyPromotionProfileUpdates(&updated, updates); err != nil {
		return profile, err
	}
	if err := validatePromotionProfile(&updated); err != nil {
		return profile, err
	}
	return updated, nil
}

func applyPromotionProfileUpdates(profile *knowledgepolicy.PromotionProfileDef, updates map[string]interface{}) error {
	for k, v := range updates {
		switch k {
		case "scope":
			scope, ok := v.(string)
			if !ok {
				return localizedError(localization.StorageSchemaInvalidScopeType(v), nil)
			}
			profile.Scope = knowledgepolicy.ScopeType(scope)
		case "multiplier":
			if f, ok := toFloat64(v); ok {
				profile.Multiplier = f
			} else {
				return localizedError(localization.CypherKnowledgePolicyInvalidValue(k, fmt.Sprint(v), false), nil)
			}
		case "scoreFloor":
			if f, ok := toFloat64(v); ok {
				profile.ScoreFloor = f
			} else {
				return localizedError(localization.CypherKnowledgePolicyInvalidValue(k, fmt.Sprint(v), false), nil)
			}
		case "scoreCap":
			if f, ok := toFloat64(v); ok {
				profile.ScoreCap = f
			} else {
				return localizedError(localization.CypherKnowledgePolicyInvalidValue(k, fmt.Sprint(v), false), nil)
			}
		case "enabled":
			if b, ok := v.(bool); ok {
				profile.Enabled = b
			} else {
				return localizedError(localization.StorageSchemaOptionBooleanRequired(k), nil)
			}
		default:
			return localizedError(localization.StorageSchemaUnknownOption(k), nil)
		}
	}
	return nil
}

func toInt64(v interface{}) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case float64:
		return int64(n), true
	default:
		return 0, false
	}
}

func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int64:
		return float64(n), true
	case int:
		return float64(n), true
	default:
		return 0, false
	}
}
