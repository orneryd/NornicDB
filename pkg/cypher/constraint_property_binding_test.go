package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Public constraint declarations bind property references to their FOR variable
// and retain quoted punctuation as data. Rejection must preserve schema, indexes
// and stored values. The additional consumer cases check the same exact dotted
// identity through existence, type, domain and relationship-key enforcement.
func TestConstraintPropertyQualifierMismatchIsRejected(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		for _, tc := range []struct {
			name     string
			label    string
			property string
			ddl      string
		}{
			{
				name:     "backticked dotted property",
				label:    "QualifierQuoted",
				property: "external.id",
				ddl:      "CREATE CONSTRAINT qualifier_quoted FOR (n:QualifierQuoted) REQUIRE other.`external.id` IS UNIQUE",
			},
			{
				name: "existence qualifier", label: "QualifierExists", property: "external.id",
				ddl: "CREATE CONSTRAINT qualifier_exists FOR (n:QualifierExists) REQUIRE other.`external.id` IS NOT NULL",
			},
			{
				name: "type qualifier", label: "QualifierType", property: "external.id",
				ddl: "CREATE CONSTRAINT qualifier_type FOR (n:QualifierType) REQUIRE other.`external.id` IS :: STRING",
			},
			{
				name: "domain qualifier", label: "QualifierDomain", property: "external.id",
				ddl: "CREATE CONSTRAINT qualifier_domain FOR (n:QualifierDomain) REQUIRE other.`external.id` IN ['first', 'second']",
			},
			{
				name: "quoted declared variable", label: "QualifierAlias", property: "external.id",
				ddl: "CREATE CONSTRAINT qualifier_alias FOR (`n.x`:QualifierAlias) REQUIRE other.`external.id` IS UNIQUE",
			},
			{
				name:     "plain property",
				label:    "QualifierPlain",
				property: "code",
				ddl:      "CREATE CONSTRAINT qualifier_plain FOR (n:QualifierPlain) REQUIRE other.code IS UNIQUE",
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				exec, _ := newCypherTestExecutor(t, "constraint_qualifier_mismatch")
				ctx := context.Background()

				// Distinct values: if the qualifier were ignored, a uniqueness
				// constraint bound to tc.property would be admitted over this
				// data. Backfill cannot be the reason the statement fails.
				for _, create := range []string{
					"CREATE (:" + tc.label + " {`" + tc.property + "`: 'first'})",
					"CREATE (:" + tc.label + " {`" + tc.property + "`: 'second'})",
				} {
					_, err := exec.Execute(ctx, create, nil)
					require.NoError(t, err, create)
				}

				schema := exec.storage.GetSchema()
				constraintsBefore := len(schema.GetAllConstraints())
				indexesBefore := len(schema.GetIndexes())
				ownedIndexesBefore := len(schema.GetCompositeIndexesForLabel(tc.label))
				require.Zero(t, constraintsBefore)

				_, err := exec.Execute(ctx, tc.ddl, nil)
				if assert.Error(t, err, "%s: a property reference qualified by another variable names no property of the FOR pattern, so the statement must be rejected", tc.ddl) {
					requireErrorStatus(t, err, "Neo.ClientError.Statement.SyntaxError", tc.ddl)
				}
				assert.Len(t, schema.GetAllConstraints(), constraintsBefore,
					"rejection leaves the constraint schema unchanged")
				assert.Len(t, schema.GetIndexes(), indexesBefore,
					"rejection leaves the schema indexes unchanged")
				assert.Len(t, schema.GetCompositeIndexesForLabel(tc.label), ownedIndexesBefore,
					"rejection leaves the label's equality indexes unchanged")
				assert.False(t, schema.MaintainsPropertyIndex(tc.label, tc.property),
					"rejection leaves no property index for the rejected reference")
				assert.EqualValues(t, 2, countOf(t, exec, ctx, "MATCH (n:"+tc.label+") RETURN count(n) AS c"),
					"rejection leaves the stored data unchanged")

				result, err := exec.Execute(ctx, "MATCH (n:"+tc.label+") RETURN properties(n) AS p", nil)
				require.NoError(t, err)
				values := make(map[interface{}]bool, len(result.Rows))
				for _, row := range result.Rows {
					properties, ok := row[0].(map[string]interface{})
					require.True(t, ok, "properties(n) projects the stored property map")
					values[properties[tc.property]] = true
				}
				assert.Equal(t, map[interface{}]bool{"first": true, "second": true}, values,
					"rejection leaves the existing property values unchanged")
			})
		}
	})
}

func TestConstraintCompositeQuotedCommaPropertyKeepsIdentity(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		exec, _ := newCypherTestExecutor(t, "constraint_composite_quoted_comma")
		ctx := context.Background()
		const label = "CompositeQuotedComma"
		const ddl = "CREATE CONSTRAINT composite_quoted_comma FOR (n:CompositeQuotedComma) REQUIRE (n.`external,id`, n.other) IS UNIQUE"

		schema := exec.storage.GetSchema()
		_, err := exec.Execute(ctx, ddl, nil)
		require.NoError(t, err, "%s: a comma inside backticks is part of the property name", ddl)
		require.Len(t, schema.GetAllConstraints(), 1)
		constraint := constraintNamedForBindingTest(t, schema, "composite_quoted_comma")
		assert.Equal(t, storage.ConstraintUnique, constraint.Type)
		assert.Equal(t, storage.ConstraintEntityNode, constraint.EffectiveEntityType())
		assert.Equal(t, []string{"external,id", "other"}, constraint.Properties,
			"the quoted comma is one property name, so the constraint holds exactly two properties")
		ownedIndexes := schema.GetCompositeIndexesForLabel(label)
		require.Len(t, ownedIndexes, 1, "composite uniqueness owns one equality index over the pair")
		assert.Equal(t, []string{"external,id", "other"}, ownedIndexes[0].Properties,
			"the owned index keeps the same two property names")

		// The write path must use the same identity as the schema: the quoted
		// name denotes one property, and the pair decides uniqueness.
		create := "CREATE (:" + label + " {`external,id`: 'alpha', other: 'beta'})"
		_, err = exec.Execute(ctx, create, nil)
		require.NoError(t, err, create)
		_, err = exec.Execute(ctx, create, nil)
		if assert.Error(t, err, "a second node with the same property pair must be rejected") {
			requireErrorStatus(t, err, "Neo.ClientError.Schema.ConstraintValidationFailed", create)
		}
		assert.EqualValues(t, 1, countOf(t, exec, ctx, "MATCH (n:"+label+") RETURN count(n) AS c"),
			"the rejected duplicate pair stores nothing")
		assert.Len(t, schema.GetAllConstraints(), 1,
			"the rejected write leaves the constraint schema unchanged")

		for _, differing := range []string{
			"CREATE (:" + label + " {`external,id`: 'alpha', other: 'gamma'})",
			"CREATE (:" + label + " {`external,id`: 'delta', other: 'beta'})",
		} {
			_, err = exec.Execute(ctx, differing, nil)
			require.NoError(t, err, "%s differs in one member of the pair", differing)
		}
		assert.EqualValues(t, 3, countOf(t, exec, ctx, "MATCH (n:"+label+") RETURN count(n) AS c"),
			"each pair that differs in either member is admitted")

		// The stored pairs are read back by the exact property names the
		// constraint registered, so the comma identity is checked on data
		// rather than through a plan that may seek the owned index.
		type pair struct{ externalID, other string }
		result, err := exec.Execute(ctx, "MATCH (n:"+label+") RETURN properties(n) AS p", nil)
		require.NoError(t, err)
		pairs := make(map[pair]bool, len(result.Rows))
		for _, row := range result.Rows {
			properties, ok := row[0].(map[string]interface{})
			require.True(t, ok, "properties(n) projects the stored property map")
			externalID, _ := properties["external,id"].(string)
			other, _ := properties["other"].(string)
			pairs[pair{externalID: externalID, other: other}] = true
		}
		assert.Equal(t, map[pair]bool{
			{externalID: "alpha", other: "beta"}:  true,
			{externalID: "alpha", other: "gamma"}: true,
			{externalID: "delta", other: "beta"}:  true,
		}, pairs, "the quoted comma names one stored property in every admitted pair")
	})
}

func TestConstraintQuotedDottedPropertyConsumers(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		t.Run("existence IS NOT NULL", func(t *testing.T) {
			exec, _ := newCypherTestExecutor(t, "quoted_dotted_exists")
			ctx := context.Background()
			const ddl = "CREATE CONSTRAINT quoted_exists FOR (n:QuotedExists) REQUIRE n.`external.id` IS NOT NULL"
			_, err := exec.Execute(ctx, ddl, nil)
			require.NoError(t, err, ddl)

			constraint := constraintNamedForBindingTest(t, exec.storage.GetSchema(), "quoted_exists")
			assert.Equal(t, storage.ConstraintExists, constraint.Type)
			assert.Equal(t, storage.ConstraintEntityNode, constraint.EffectiveEntityType())
			assert.Equal(t, []string{"external.id"}, constraint.Properties)

			_, err = exec.Execute(ctx, "CREATE (:QuotedExists {other: 1})", nil)
			if assert.Error(t, err, "a node without the constrained property must be rejected") {
				requireErrorStatus(t, err, "Neo.ClientError.Schema.ConstraintValidationFailed",
					"CREATE (:QuotedExists {other: 1})")
			}
			_, err = exec.Execute(ctx, "CREATE (:QuotedExists {`external.id`: 'present'})", nil)
			require.NoError(t, err)
			assert.EqualValues(t, 1, countOf(t, exec, ctx, "MATCH (n:QuotedExists) RETURN count(n) AS c"))
		})

		t.Run("property type IS STRING", func(t *testing.T) {
			exec, _ := newCypherTestExecutor(t, "quoted_dotted_type")
			ctx := context.Background()
			const ddl = "CREATE CONSTRAINT quoted_type FOR (n:QuotedType) REQUIRE n.`external.id` IS :: STRING"
			_, err := exec.Execute(ctx, ddl, nil)
			require.NoError(t, err, ddl)

			var found bool
			for _, constraint := range exec.storage.GetSchema().GetAllPropertyTypeConstraints() {
				if constraint.Name != "quoted_type" {
					continue
				}
				found = true
				assert.Equal(t, "external.id", constraint.Property)
				assert.Equal(t, storage.PropertyTypeString, constraint.ExpectedType)
			}
			require.True(t, found, "the property type constraint is registered under its quoted name")

			_, err = exec.Execute(ctx, "CREATE (:QuotedType {`external.id`: 'text'})", nil)
			require.NoError(t, err)
			_, err = exec.Execute(ctx, "CREATE (:QuotedType {`external.id`: 7})", nil)
			if assert.Error(t, err, "a non-string value on the constrained property must be rejected") {
				requireErrorStatus(t, err, "Neo.ClientError.Schema.ConstraintValidationFailed",
					"CREATE (:QuotedType {`external.id`: 7})")
			}
			assert.EqualValues(t, 1, countOf(t, exec, ctx, "MATCH (n:QuotedType) RETURN count(n) AS c"))
		})

		t.Run("domain IN list", func(t *testing.T) {
			exec, _ := newCypherTestExecutor(t, "quoted_dotted_domain")
			ctx := context.Background()
			const ddl = "CREATE CONSTRAINT quoted_domain FOR (n:QuotedDomain) REQUIRE n.`external.id` IN ['active', 'paused']"
			_, err := exec.Execute(ctx, ddl, nil)
			require.NoError(t, err, ddl)

			constraint := constraintNamedForBindingTest(t, exec.storage.GetSchema(), "quoted_domain")
			assert.Equal(t, storage.ConstraintDomain, constraint.Type)
			assert.Equal(t, storage.ConstraintEntityNode, constraint.EffectiveEntityType())
			assert.Equal(t, []string{"external.id"}, constraint.Properties)
			assert.Equal(t, []interface{}{"active", "paused"}, constraint.AllowedValues)

			_, err = exec.Execute(ctx, "CREATE (:QuotedDomain {`external.id`: 'active'})", nil)
			require.NoError(t, err)
			_, err = exec.Execute(ctx, "CREATE (:QuotedDomain {`external.id`: 'retired'})", nil)
			if assert.Error(t, err, "a value outside the domain must be rejected") {
				requireErrorStatus(t, err, "Neo.ClientError.Schema.ConstraintValidationFailed",
					"CREATE (:QuotedDomain {`external.id`: 'retired'})")
			}
			assert.EqualValues(t, 1, countOf(t, exec, ctx, "MATCH (n:QuotedDomain) RETURN count(n) AS c"))
		})

		t.Run("relationship key single property", func(t *testing.T) {
			exec, _ := newCypherTestExecutor(t, "quoted_dotted_rel_key")
			ctx := context.Background()
			const ddl = "CREATE CONSTRAINT quoted_rel_key FOR ()-[r:QuotedRelKey]-() REQUIRE r.`external.id` IS RELATIONSHIP KEY"
			_, err := exec.Execute(ctx, ddl, nil)
			require.NoError(t, err, ddl)

			constraint := constraintNamedForBindingTest(t, exec.storage.GetSchema(), "quoted_rel_key")
			assert.Equal(t, storage.ConstraintRelationshipKey, constraint.Type)
			assert.Equal(t, storage.ConstraintEntityRelationship, constraint.EffectiveEntityType())
			assert.Equal(t, []string{"external.id"}, constraint.Properties)

			create := "CREATE (:QuotedRelKeyA {k: 1})-[:QuotedRelKey {`external.id`: 'same'}]->(:QuotedRelKeyB {k: 1})"
			_, err = exec.Execute(ctx, create, nil)
			require.NoError(t, err, create)
			_, err = exec.Execute(ctx, create, nil)
			if assert.Error(t, err, "a second relationship with the same key must be rejected") {
				requireErrorStatus(t, err, "Neo.ClientError.Schema.ConstraintValidationFailed", create)
			}
			assert.EqualValues(t, 1, countOf(t, exec, ctx, "MATCH ()-[r:QuotedRelKey]->() RETURN count(r) AS c"))

			distinct := "CREATE (:QuotedRelKeyA {k: 2})-[:QuotedRelKey {`external.id`: 'other'}]->(:QuotedRelKeyB {k: 2})"
			_, err = exec.Execute(ctx, distinct, nil)
			require.NoError(t, err, distinct)
			assert.EqualValues(t, 2, countOf(t, exec, ctx, "MATCH ()-[r:QuotedRelKey]->() RETURN count(r) AS c"))
		})

		t.Run("relationship key composite", func(t *testing.T) {
			exec, _ := newCypherTestExecutor(t, "quoted_dotted_rel_key_pair")
			ctx := context.Background()
			const ddl = "CREATE CONSTRAINT quoted_rel_key_pair FOR ()-[r:QuotedRelKeyPair]-() REQUIRE (r.`external.id`, r.other) IS RELATIONSHIP KEY"
			_, err := exec.Execute(ctx, ddl, nil)
			require.NoError(t, err, ddl)

			constraint := constraintNamedForBindingTest(t, exec.storage.GetSchema(), "quoted_rel_key_pair")
			assert.Equal(t, storage.ConstraintRelationshipKey, constraint.Type)
			assert.Equal(t, storage.ConstraintEntityRelationship, constraint.EffectiveEntityType())
			assert.Equal(t, []string{"external.id", "other"}, constraint.Properties,
				"the quoted dotted name stays one property next to the plain one")

			create := "CREATE (:QuotedRelKeyPairA {k: 1})-[:QuotedRelKeyPair {`external.id`: 'alpha', other: 'beta'}]->(:QuotedRelKeyPairB {k: 1})"
			_, err = exec.Execute(ctx, create, nil)
			require.NoError(t, err, create)
			_, err = exec.Execute(ctx, create, nil)
			if assert.Error(t, err, "a second relationship with the same key pair must be rejected") {
				requireErrorStatus(t, err, "Neo.ClientError.Schema.ConstraintValidationFailed", create)
			}
			assert.EqualValues(t, 1, countOf(t, exec, ctx, "MATCH ()-[r:QuotedRelKeyPair]->() RETURN count(r) AS c"))

			differing := "CREATE (:QuotedRelKeyPairA {k: 2})-[:QuotedRelKeyPair {`external.id`: 'alpha', other: 'gamma'}]->(:QuotedRelKeyPairB {k: 2})"
			_, err = exec.Execute(ctx, differing, nil)
			require.NoError(t, err, differing)
			assert.EqualValues(t, 2, countOf(t, exec, ctx, "MATCH ()-[r:QuotedRelKeyPair]->() RETURN count(r) AS c"))
		})
	})
}

// constraintNamedForBindingTest returns the constraint registered under name.
// Callers keep their own assertions about what that constraint must hold.
func constraintNamedForBindingTest(t *testing.T, schema *storage.SchemaManager, name string) storage.Constraint {
	t.Helper()
	for _, constraint := range schema.GetAllConstraints() {
		if constraint.Name == name {
			return constraint
		}
	}
	require.FailNowf(t, "constraint is not registered",
		"constraint %q absent from %+v", name, schema.GetAllConstraints())
	return storage.Constraint{}
}
