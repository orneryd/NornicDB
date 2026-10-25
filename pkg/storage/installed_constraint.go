package storage

import (
	"encoding/json"
	"iter"
	"strings"
)

// installedConstraintDefinition owns one primitive definition. Consumers copy
// this value and read scalar facts; only snapshot publishes mutable data.
// The collection storage is reachable solely inside the constructor's closures.
type installedConstraintDefinition struct {
	Name          string
	Type          ConstraintType
	EntityType    ConstraintEntityType
	Label         string
	OwnedIndex    string
	MaxCount      int
	Direction     string
	SourceLabel   string
	TargetLabel   string
	PolicyMode    string
	Properties    constraintPropertyNames
	AllowedValues constraintDomainValues
}

// constraintDefinitionSource keeps public admission values and installed
// read-only values on the same validation path. Installed reads share immutable
// value handles without copying their collection storage.
type constraintDefinitionSource interface {
	installedConstraint() installedConstraintDefinition
}

func (c Constraint) installedConstraint() installedConstraintDefinition {
	return installConstraintDefinition(c)
}

func (c installedConstraintDefinition) installedConstraint() installedConstraintDefinition { return c }

func installConstraintDefinition(input Constraint) installedConstraintDefinition {
	owned := input.detached()
	names := constraintPropertyNamesOf(owned.Properties)
	encodedNames, _ := json.Marshal(owned.Properties)
	names.key = string(encodedNames)
	return installedConstraintDefinition{
		Name: owned.Name, Type: owned.Type, EntityType: owned.EntityType,
		Label: owned.Label, OwnedIndex: owned.OwnedIndex, MaxCount: owned.MaxCount,
		Direction: owned.Direction, SourceLabel: owned.SourceLabel,
		TargetLabel: owned.TargetLabel, PolicyMode: owned.PolicyMode,
		Properties: names,
		AllowedValues: constraintDomainValues{count: len(owned.AllowedValues),
			allows: func(value any) bool { return isValueInAllowedList(value, owned.AllowedValues) },
			copy:   func() []any { return copyContractValue(owned.AllowedValues).([]any) }},
	}
}

func (c installedConstraintDefinition) EffectiveEntityType() ConstraintEntityType {
	if c.EntityType == "" {
		return ConstraintEntityNode
	}
	return c.EntityType
}

func (c installedConstraintDefinition) snapshot() Constraint {
	return Constraint{Name: c.Name, Type: c.Type, EntityType: c.EntityType,
		Label: c.Label, OwnedIndex: c.OwnedIndex, MaxCount: c.MaxCount,
		Direction: c.Direction, SourceLabel: c.SourceLabel, TargetLabel: c.TargetLabel,
		PolicyMode: c.PolicyMode, Properties: c.Properties.snapshot(),
		AllowedValues: c.AllowedValues.snapshot()}
}

type constraintPropertyNames struct {
	count   int
	offset  int
	nilList bool
	key     string
	at      func(int) string
}

func (names constraintPropertyNames) Len() int            { return names.count }
func (names constraintPropertyNames) At(index int) string { return names.at(names.offset + index) }
func (names constraintPropertyNames) All() iter.Seq2[int, string] {
	return func(yield func(int, string) bool) {
		for index := 0; index < names.count; index++ {
			if !yield(index, names.At(index)) {
				return
			}
		}
	}
}
func (names constraintPropertyNames) snapshot() []string {
	if names.nilList || names.at == nil {
		return nil
	}
	out := make([]string, names.count)
	for index := range out {
		out[index] = names.At(index)
	}
	return out
}

// constraintPropertyNamesOf adapts private index-owned property lists and the
// installed definition to one read-only iterator. The borrowed index list is
// never returned or changed by constraint readers.
func constraintPropertyNamesOf(source any) constraintPropertyNames {
	switch names := source.(type) {
	case nil:
		return constraintPropertyNames{nilList: true}
	case constraintPropertyNames:
		return names
	case []string:
		return constraintPropertyNames{count: len(names), nilList: names == nil,
			at: func(index int) string { return names[index] }}
	default:
		panic("invalid constraint property-name source")
	}
}

func (names constraintPropertyNames) Slice(low, high int) constraintPropertyNames {
	names.offset += low
	names.count = high - low
	names.key = ""
	return names
}

func (names constraintPropertyNames) Join(separator string) string {
	var out strings.Builder
	if names.count == 0 {
		return ""
	}
	if names.count == 1 {
		return names.At(0)
	}
	size := (names.count - 1) * len(separator)
	for _, name := range names.All() {
		size += len(name)
	}
	out.Grow(size)
	for index, name := range names.All() {
		if index > 0 {
			out.WriteString(separator)
		}
		out.WriteString(name)
	}
	return out.String()
}

type constraintDomainValues struct {
	count  int
	allows func(any) bool
	copy   func() []any
}

func (values constraintDomainValues) Len() int { return values.count }
func (values constraintDomainValues) Allows(value any) bool {
	return values.allows != nil && values.allows(value)
}
func (values constraintDomainValues) snapshot() []any {
	if values.copy == nil {
		return nil
	}
	return values.copy()
}

func sameConstraintPropertyOrderSlice(a []string, b constraintPropertyNames) bool {
	if len(a) != b.Len() {
		return false
	}
	for index, name := range a {
		if name != b.At(index) {
			return false
		}
	}
	return true
}
