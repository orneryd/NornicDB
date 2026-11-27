package storage

import (
	"context"
	"slices"
	"sync"
)

type constraintAdmissionKey struct{}

type constraintAdmissionScope struct {
	kind  ConstraintEntityType
	label string
}

type constraintAdmission struct {
	mu       sync.Mutex
	denied   error
	observed map[constraintAdmissionScope][]Constraint
}

type constraintAdmissionError struct{ denied error }

func (e *constraintAdmissionError) Error() string { return e.denied.Error() }
func (e *constraintAdmissionError) Unwrap() error { return e.denied }

// WithConstraintAdmission binds a fine-grained write's schema observation to
// the storage validation of that same write. If a new uniqueness rule appears
// between authorization and validation, storage refuses before comparing any
// values. The caller supplies its own authorization error classification.
func WithConstraintAdmission(ctx context.Context, denied error) context.Context {
	return context.WithValue(ctx, constraintAdmissionKey{}, &constraintAdmission{
		denied: denied, observed: make(map[constraintAdmissionScope][]Constraint),
	})
}

// RecordConstraintAdmission records the first schema view checked for each
// label/type in this write. A later precheck in a bulk write cannot widen that
// view for an earlier entity after an administrator adds a constraint.
func RecordConstraintAdmission(ctx context.Context, kind ConstraintEntityType, labels []string, constraints []Constraint) {
	admission, _ := ctx.Value(constraintAdmissionKey{}).(*constraintAdmission)
	if admission == nil {
		return
	}
	admission.mu.Lock()
	defer admission.mu.Unlock()
	for _, label := range labels {
		scope := constraintAdmissionScope{kind: kind, label: label}
		if _, seen := admission.observed[scope]; seen {
			continue
		}
		var observed []Constraint
		for _, constraint := range constraints {
			if constraint.EffectiveEntityType() != kind || constraint.Label != label {
				continue
			}
			if constraint.Type != ConstraintUnique && constraint.Type != ConstraintNodeKey && constraint.Type != ConstraintRelationshipKey {
				continue
			}
			constraint.Properties = slices.Clone(constraint.Properties)
			observed = append(observed, constraint)
		}
		admission.observed[scope] = observed
	}
}

func validateConstraintAdmission(ctx context.Context, constraint Constraint, properties map[string]any) error {
	if constraint.Type != ConstraintUnique && constraint.Type != ConstraintNodeKey && constraint.Type != ConstraintRelationshipKey {
		return nil
	}
	for _, key := range constraint.Properties {
		if properties[key] == nil {
			return nil // No complete key can produce a uniqueness outcome.
		}
	}
	for _, admission := range constraintAdmissionsFromContext(ctx) {
		if err := admission.check(constraint); err != nil {
			return err
		}
	}
	return nil
}

func constraintAdmissionsFromContext(ctx context.Context) []*constraintAdmission {
	switch value := ctx.Value(constraintAdmissionKey{}).(type) {
	case *constraintAdmission:
		return []*constraintAdmission{value}
	case []*constraintAdmission:
		return value
	default:
		return nil
	}
}

func (a *constraintAdmission) check(constraint Constraint) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.observedConstraintLocked(constraint) {
		return &constraintAdmissionError{denied: a.denied}
	}
	return nil
}

func (a *constraintAdmission) observedConstraintLocked(constraint Constraint) bool {
	observed, seen := a.observed[constraintAdmissionScope{kind: constraint.EffectiveEntityType(), label: constraint.Label}]
	if !seen {
		return true // This write did not need a precheck for this label.
	}
	for _, prior := range observed {
		if prior.Name == constraint.Name && prior.Type == constraint.Type &&
			prior.Label == constraint.Label && slices.Equal(prior.Properties, constraint.Properties) {
			return true
		}
	}
	return false
}

// rememberConstraintAdmissionLocked is called under tx.mu by every staged
// node/relationship writer. The per-statement context is not the commit
// context for an explicit or deferred transaction.
func (tx *BadgerTransaction) rememberConstraintAdmissionLocked(ctx context.Context) {
	admission, _ := ctx.Value(constraintAdmissionKey{}).(*constraintAdmission)
	if admission != nil && !slices.Contains(tx.constraintAdmissions, admission) {
		tx.constraintAdmissions = append(tx.constraintAdmissions, admission)
	}
}

func (tx *BadgerTransaction) constraintAdmissionContext(ctx context.Context) context.Context {
	if len(tx.constraintAdmissions) == 0 {
		return ctx
	}
	return context.WithValue(ctx, constraintAdmissionKey{}, tx.constraintAdmissions)
}

// A commit checks all schema observations before any uniqueness comparison.
// Validation also uses these observations to cover schema publication after
// this first check but before a validator reads its current constraint list.
func (tx *BadgerTransaction) validateRecordedConstraintAdmissions(ctx context.Context) error {
	if len(tx.constraintAdmissions) == 0 {
		return nil
	}
	schema := tx.engine.GetSchemaForNamespace(tx.namespace)
	if schema == nil {
		return nil
	}
	for _, admission := range tx.constraintAdmissions {
		admission.mu.Lock()
		scopes := make([]constraintAdmissionScope, 0, len(admission.observed))
		for scope := range admission.observed {
			scopes = append(scopes, scope)
		}
		admission.mu.Unlock()
		for _, scope := range scopes {
			current, err := schema.GetConstraintsForLabels(ctx, []string{scope.label}, ConstraintUnique, ConstraintNodeKey, ConstraintRelationshipKey)
			if err != nil {
				return err
			}
			for _, constraint := range current {
				if constraint.EffectiveEntityType() != scope.kind || constraint.Label != scope.label {
					continue
				}
				if err := admission.check(constraint); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
