package auth

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"
)

// FineGrainedAuthRule grants its roles to authenticated users for whom an
// enabled condition evaluates to true.
type FineGrainedAuthRule struct {
	Name      string   `json:"name"`
	Condition string   `json:"condition"`
	Enabled   bool     `json:"enabled"`
	Roles     []string `json:"roles,omitempty"`
}

// FineGrainedAuthRuleInput is the identity information visible to a rule
// condition. Claims come from a verified identity provider at admission. Nil
// collections denote unavailable providers; empty collections denote available
// providers that supplied no tags or claims.
type FineGrainedAuthRuleInput struct {
	Context    context.Context
	NativeTags []string
	Claims     map[string]any
	Now        time.Time
}

// FineGrainedAuthRuleCondition evaluates a compiled, graph-independent rule.
type FineGrainedAuthRuleCondition func(FineGrainedAuthRuleInput) bool

// FineGrainedAuthRuleCompiler validates and compiles an authorization condition.
type FineGrainedAuthRuleCompiler func(string) (FineGrainedAuthRuleCondition, error)

type compiledFineGrainedAuthRule struct {
	roles     []string
	condition FineGrainedAuthRuleCondition
}

func validateFineGrainedAuthRules(rules []FineGrainedAuthRule) error {
	if len(rules) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(rules))
	for _, rule := range rules {
		if strings.TrimSpace(rule.Name) == "" || strings.TrimSpace(rule.Condition) == "" {
			return fmt.Errorf("%w: authorization rules require a name and condition", ErrInvalidFineGrainedPolicy)
		}
		if _, exists := seen[rule.Name]; exists {
			return fmt.Errorf("%w: duplicate authorization rule", ErrInvalidFineGrainedPolicy)
		}
		seen[rule.Name] = struct{}{}
		for _, role := range rule.Roles {
			if NormalizeRoleName(role) == "" || NormalizeRoleName(role) == FineGrainedPublicRole {
				return fmt.Errorf("%w: invalid authorization rule role", ErrInvalidFineGrainedPolicy)
			}
		}
	}
	return nil
}

// SetAuthRuleCompiler binds the Cypher condition compiler before serving.
func (runtime *FineGrainedRuntime) SetAuthRuleCompiler(compiler FineGrainedAuthRuleCompiler) {
	runtime.authRuleCompiler = compiler
}

func (runtime *FineGrainedRuntime) compileAuthRules(rules []FineGrainedAuthRule) ([]compiledFineGrainedAuthRule, error) {
	var compiled []compiledFineGrainedAuthRule
	for _, rule := range rules {
		if runtime.authRuleCompiler == nil {
			return nil, fmt.Errorf("%w: authorization rule compiler unavailable", ErrInvalidFineGrainedPolicy)
		}
		condition, err := runtime.authRuleCompiler(rule.Condition)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidFineGrainedPolicy, err)
		}
		if rule.Enabled && len(rule.Roles) > 0 {
			compiled = append(compiled, compiledFineGrainedAuthRule{roles: slices.Clone(rule.Roles), condition: condition})
		}
	}
	return compiled, nil
}

func matchingFineGrainedAuthRuleRoles(rules []compiledFineGrainedAuthRule, base []string, input FineGrainedAuthRuleInput) []string {
	for _, rule := range rules {
		if rule.condition(input) {
			base = append(base, rule.roles...)
		}
	}
	return base
}
