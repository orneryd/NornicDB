package storage

import (
	"errors"
	"github.com/stretchr/testify/require"
	"go/types"
	"sort"
	"testing"
	"time"
)

type installedKindProbe struct {
	constraint           Constraint
	seed, invalid, valid map[string]any
	edge, newAnchor      bool
}

func installedKindProbes() map[ConstraintType]installedKindProbe {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	window := func(from, to int) map[string]any {
		return map[string]any{"key": "k", "from": start.Add(time.Duration(from) * time.Hour), "to": start.Add(time.Duration(to) * time.Hour)}
	}
	return map[ConstraintType]installedKindProbe{
		ConstraintUnique:          {constraint: Constraint{Properties: []string{"p"}}, seed: map[string]any{"p": int64(1)}, invalid: map[string]any{"p": int64(1)}, valid: map[string]any{"p": int64(2)}},
		ConstraintNodeKey:         {constraint: Constraint{Properties: []string{"p"}}, invalid: map[string]any{}, valid: map[string]any{"p": int64(2)}},
		ConstraintExists:          {constraint: Constraint{Properties: []string{"p"}}, invalid: map[string]any{}, valid: map[string]any{"p": int64(2)}},
		ConstraintPropertyType:    {invalid: map[string]any{"p": "wrong"}, valid: map[string]any{"p": int64(2)}},
		ConstraintTemporal:        {constraint: Constraint{Properties: []string{"key", "from", "to"}}, seed: window(0, 4), invalid: window(2, 5), valid: window(4, 6)},
		ConstraintRelationshipKey: {constraint: Constraint{Properties: []string{"p"}}, seed: map[string]any{"p": int64(1)}, invalid: map[string]any{"p": int64(1)}, valid: map[string]any{"p": int64(2)}, edge: true},
		ConstraintDomain:          {constraint: Constraint{Properties: []string{"p"}, AllowedValues: []any{"yes"}}, invalid: map[string]any{"p": "no"}, valid: map[string]any{"p": "yes"}},
		ConstraintCardinality:     {constraint: Constraint{Direction: "OUTGOING", MaxCount: 1}, seed: map[string]any{}, invalid: map[string]any{}, valid: map[string]any{}, edge: true, newAnchor: true},
		ConstraintPolicy:          {constraint: Constraint{SourceLabel: "Blocked", TargetLabel: "End", PolicyMode: "DISALLOWED"}, invalid: map[string]any{}, valid: map[string]any{}, edge: true, newAnchor: true},
	}
}

func TestInstalledConstraintKindsAndPublicRejections(t *testing.T) {
	checked := checkStoragePackage(t, "")
	kindType := checked.pkg.Scope().Lookup("ConstraintType").Type()
	var actual, covered []string
	for _, name := range checked.pkg.Scope().Names() {
		if value, ok := checked.pkg.Scope().Lookup(name).(*types.Const); ok && types.Identical(value.Type(), kindType) {
			actual = append(actual, value.Val().ExactString())
		}
	}
	probes := installedKindProbes()
	for kind := range probes {
		covered = append(covered, "\""+string(kind)+"\"")
	}
	sort.Strings(actual)
	sort.Strings(covered)
	require.NotEmpty(t, actual)
	require.Equal(t, actual, covered, "every constraint kind needs a public rejection/control")
	for kind, probe := range probes {
		for _, route := range []string{"direct", "transaction"} {
			t.Run(string(kind)+"/"+route, func(t *testing.T) {
				engine, err := NewBadgerEngineInMemory()
				require.NoError(t, err)
				defer engine.Close()
				if probe.edge {
					for id, label := range map[string]string{"a": "Blocked", "b": "End", "c": "Okay"} {
						_, err = engine.CreateNode(&Node{ID: NodeID("test:" + id), Labels: []string{label}})
						require.NoError(t, err)
					}
				}
				schema := engine.GetSchemaForNamespace("test")
				constraint := probe.constraint
				constraint.Name = "probe"
				constraint.Type = kind
				constraint.Label = "Probe"
				if probe.edge {
					constraint.EntityType = ConstraintEntityRelationship
				}
				if kind == ConstraintPropertyType {
					require.NoError(t, schema.AddPropertyTypeConstraint("typed", "Probe", "p", PropertyTypeInteger))
				} else {
					require.NoError(t, schema.AddConstraint(constraint))
				}
				write := func(id string, props map[string]any, newAnchor bool) error {
					anchor := NodeID("test:a")
					if newAnchor {
						anchor = "test:c"
					}
					node := &Node{ID: NodeID("test:" + id), Labels: []string{"Probe"}, Properties: props}
					edge := &Edge{ID: EdgeID("test:" + id), StartNode: anchor, EndNode: "test:b", Type: "Probe", Properties: props}
					if route == "direct" {
						if probe.edge {
							return engine.CreateEdge(edge)
						}
						_, err := engine.CreateNode(node)
						return err
					}
					tx, err := engine.BeginTransaction()
					if err != nil {
						return err
					}
					defer tx.Rollback()
					if err = tx.SetNamespace("test"); err != nil {
						return err
					}
					if probe.edge {
						err = tx.CreateEdge(edge)
					} else {
						_, err = tx.CreateNode(node)
					}
					if err != nil {
						return err
					}
					return tx.Commit()
				}
				if probe.seed != nil {
					require.NoError(t, write("seed", probe.seed, false))
				}
				before := schema.GetAllConstraints()
				first := write("rejected", probe.invalid, false)
				require.Error(t, first)
				var violation *ConstraintViolationError
				require.True(t, errors.As(first, &violation))
				require.Equal(t, kind, violation.Type)
				violation.Type = "changed"
				violation.Label = "changed"
				if len(violation.Properties) > 0 {
					violation.Properties[0] = "changed"
				}
				second := write("rejected-again", probe.invalid, false)
				require.Error(t, second)
				var next *ConstraintViolationError
				require.True(t, errors.As(second, &next))
				require.Equal(t, kind, next.Type)
				require.Equal(t, before, schema.GetAllConstraints(), "a public violation changed the installed definition")
				require.NoError(t, write("valid", probe.valid, probe.newAnchor))
			})
		}
	}
}
