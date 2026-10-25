package adminimport

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/testutil/cyphernames"
	"github.com/orneryd/nornicdb/internal/testutil/emittercheck"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRelationshipEmitterProperty(t *testing.T) {
	for _, name := range cyphernames.Names() {
		t.Run(name, func(t *testing.T) {
			texts := []string{cypherPattern(storage.ConstraintEntityRelationship, name), cypherRelationshipPattern(name, "OUTGOING", name, name), cypherRelationshipPattern(name, "INCOMING", name, name)}
			for _, kind := range []storage.ConstraintType{storage.ConstraintUnique, storage.ConstraintExists, storage.ConstraintRelationshipKey, storage.ConstraintTemporal, storage.ConstraintDomain, storage.ConstraintCardinality, storage.ConstraintPolicy} {
				constraint := storage.Constraint{Name: "c", Type: kind, EntityType: storage.ConstraintEntityRelationship, Label: name, Properties: []string{name}, SourceLabel: name, TargetLabel: name, PolicyMode: "ALLOWED", MaxCount: 2}
				if kind == storage.ConstraintTemporal {
					constraint.Properties = []string{name, "valid`from", "valid`to"}
				}
				if kind == storage.ConstraintDomain {
					constraint.AllowedValues = []any{int64(1)}
				}
				text, ok := renderConstraintStatement(constraintStatementRequest{general: constraint})
				require.True(t, ok, kind)
				texts = append(texts, text)
			}
			text, ok := renderConstraintStatement(constraintStatementRequest{typeConstraint: &storage.PropertyTypeConstraint{Name: "pt", EntityType: storage.ConstraintEntityRelationship, Label: name, Property: name, ExpectedType: "INTEGER"}})
			require.True(t, ok)
			texts = append(texts, text)
			for _, kind := range []string{"RANGE", "FULLTEXT", "VECTOR"} {
				text, ok := renderIndexStatement(map[string]any{"name": "idx", "type": kind, "entityType": "RELATIONSHIP", "label": name, "relationshipTypes": []string{name}, "properties": []string{name}, "property": name, "dimensions": 3})
				require.True(t, ok, kind)
				texts = append(texts, text)
			}
			for _, text := range texts {
				emittercheck.Check(t, text, name, nil, nil)
				if strings.HasPrefix(text, "CREATE ") {
					keys := []string{name}
					if strings.Contains(text, "TEMPORAL NO OVERLAP") {
						keys = []string{name, "valid`from", "valid`to"}
					}
					if strings.Contains(text, "MAX COUNT") || strings.HasSuffix(text, "ALLOWED") {
						keys = nil
					}
					emittercheck.CheckSchema(t, text, name, keys)
				}

				got, err := cyphernames.ReadType(text)
				require.NoError(t, err, text)
				require.Equal(t, name, got, text)

			}
		})
	}
}

func TestNameEmissionNodeRenderers(t *testing.T) {
	for _, name := range []string{"two words", "space ", "a.b", "a,b", "tick`name"} {
		t.Run(name, func(t *testing.T) {
			check := func(text string) {
				e := emittercheck.Executor(t)
				require.NoError(t, cyphernames.SchemaRoundTrip(text, name, []string{name}, func(query string) ([][]any, error) {
					r, err := e.Execute(context.Background(), query, nil)
					if err != nil {
						return nil, err
					}
					return r.Rows, nil
				}))
			}
			for _, kind := range []storage.ConstraintType{storage.ConstraintUnique, storage.ConstraintExists, storage.ConstraintNodeKey, storage.ConstraintDomain} {
				text, ok := renderConstraintStatement(constraintStatementRequest{general: storage.Constraint{Name: name, Type: kind, EntityType: storage.ConstraintEntityNode, Label: name, Properties: []string{name}, AllowedValues: []any{int64(1)}}})
				require.True(t, ok)
				check(text)
			}
			text, ok := renderConstraintStatement(constraintStatementRequest{typeConstraint: &storage.PropertyTypeConstraint{Name: name, EntityType: storage.ConstraintEntityNode, Label: name, Property: name, ExpectedType: "INTEGER"}})
			require.True(t, ok)
			check(text)
			for _, kind := range []string{"PROPERTY", "COMPOSITE", "RANGE", "FULLTEXT", "VECTOR"} {
				text, ok := renderIndexStatement(map[string]any{"name": name, "type": kind, "entityType": "NODE", "label": name, "labels": []string{name}, "properties": []string{name}, "property": name, "dimensions": 3})
				require.True(t, ok, kind)
				check(text)
			}
		})
	}
}
