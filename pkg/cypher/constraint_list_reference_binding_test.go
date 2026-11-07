package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The constraint property-list forms — NODE KEY, TEMPORAL and RELATIONSHIP KEY —
// name each of their properties through the FOR variable exactly as the scalar
// forms do, both in a native declaration and in a contract block.
//
// Every negative case declares its list through another variable while the
// label's stored data already satisfies the constraint the misbound declaration
// would build, so a data check cannot be the reason the statement fails; the
// same declaration written with the FOR variable is the positive control over
// that same data. A rejected declaration must leave the constraint schema, its
// indexes, its contracts and the stored values as they were.
func TestConstraintPropertyListReferencesBindToTheForVariable(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		nodeKeyData := []string{
			"CREATE (:NodeKeyBind {code: 'first'})",
			"CREATE (:NodeKeyBind {code: 'second'})",
		}
		temporalData := []string{
			"CREATE (:TemporalBind {account: 'first', valid_from: datetime('2024-01-01T00:00:00Z'), valid_to: datetime('2024-02-01T00:00:00Z')})",
			"CREATE (:TemporalBind {account: 'second', valid_from: datetime('2024-03-01T00:00:00Z'), valid_to: datetime('2024-04-01T00:00:00Z')})",
		}
		// Distinct identifiers and distinct (external.id, extra) pairs: a
		// RELATIONSHIP KEY over either misbound list is admissible for this data.
		relationshipKeyData := []string{
			"CREATE (:RelKeyBindA {k: 1})-[:RelKeyBind {`external.id`: 'first', extra: 'one'}]->(:RelKeyBindB {k: 1})",
			"CREATE (:RelKeyBindA {k: 2})-[:RelKeyBind {`external.id`: 'second', extra: 'two'}]->(:RelKeyBindB {k: 2})",
		}

		for _, tc := range []struct {
			name      string
			namespace string
			data      []string
			// rejected is the declaration whose list names another variable.
			rejected string
			// accepted is the same declaration written with the FOR variable:
			// the positive control over the same data.
			accepted string
			// contractName is the contract name accepted declares; empty for a
			// native declaration.
			contractName string
			// entity is the entity type the accepted declaration constrains.
			entity storage.ConstraintEntityType
			// properties is the exact property list the accepted declaration
			// must register.
			properties []string
			// label is the label or relationship type the accepted declaration
			// constrains.
			label string
			// count projects the stored population.
			count string
			// duplicate stores a value that collides with the first stored
			// member; the accepted declaration must reject it.
			duplicate string
		}{
			{
				name:       "node key list",
				namespace:  "constraint_node_key_list_binding",
				data:       nodeKeyData,
				rejected:   "CREATE CONSTRAINT node_key_misbound FOR (n:NodeKeyBind) REQUIRE (other.code) IS NODE KEY",
				accepted:   "CREATE CONSTRAINT node_key_bound FOR (n:NodeKeyBind) REQUIRE (n.code) IS NODE KEY",
				entity:     storage.ConstraintEntityNode,
				properties: []string{"code"},
				label:      "NodeKeyBind",
				count:      "MATCH (n:NodeKeyBind) RETURN count(n) AS c",
				duplicate:  "CREATE (:NodeKeyBind {code: 'first'})",
			},
			{
				name:       "node key list of a quoted variable and dotted name",
				namespace:  "constraint_node_key_quoted_list_binding",
				data:       []string{"CREATE (:QuotedKeyBind {`external.id`: 'first'})", "CREATE (:QuotedKeyBind {`external.id`: 'second'})"},
				rejected:   "CREATE CONSTRAINT quoted_key_misbound FOR (`n.x`:QuotedKeyBind) REQUIRE (other.`external.id`) IS NODE KEY",
				accepted:   "CREATE CONSTRAINT quoted_key_bound FOR (`n.x`:QuotedKeyBind) REQUIRE (`n.x`.`external.id`) IS NODE KEY",
				entity:     storage.ConstraintEntityNode,
				properties: []string{"external.id"},
				label:      "QuotedKeyBind",
				count:      "MATCH (n:QuotedKeyBind) RETURN count(n) AS c",
				duplicate:  "CREATE (:QuotedKeyBind {`external.id`: 'first'})",
			},
			{
				name:       "temporal list",
				namespace:  "constraint_temporal_list_binding",
				data:       temporalData,
				rejected:   "CREATE CONSTRAINT temporal_misbound FOR (n:TemporalBind) REQUIRE (other.account, other.valid_from, other.valid_to) IS TEMPORAL NO OVERLAP",
				accepted:   "CREATE CONSTRAINT temporal_bound FOR (n:TemporalBind) REQUIRE (n.account, n.valid_from, n.valid_to) IS TEMPORAL NO OVERLAP",
				entity:     storage.ConstraintEntityNode,
				properties: []string{"account", "valid_from", "valid_to"},
				label:      "TemporalBind",
				count:      "MATCH (n:TemporalBind) RETURN count(n) AS c",
				duplicate:  "CREATE (:TemporalBind {account: 'first', valid_from: datetime('2024-01-15T00:00:00Z'), valid_to: datetime('2024-01-20T00:00:00Z')})",
			},
			{
				name:         "contract node key list",
				namespace:    "constraint_contract_node_key_list_binding",
				data:         nodeKeyData,
				rejected:     "CREATE CONSTRAINT contract_node_key_misbound FOR (n:NodeKeyBind) REQUIRE { (other.code) IS NODE KEY }",
				accepted:     "CREATE CONSTRAINT contract_node_key_bound FOR (n:NodeKeyBind) REQUIRE { (n.code) IS NODE KEY }",
				contractName: "contract_node_key_bound",
				entity:       storage.ConstraintEntityNode,
				properties:   []string{"code"},
				label:        "NodeKeyBind",
				count:        "MATCH (n:NodeKeyBind) RETURN count(n) AS c",
				duplicate:    "CREATE (:NodeKeyBind {code: 'first'})",
			},
			{
				name:         "contract temporal list",
				namespace:    "constraint_contract_temporal_list_binding",
				data:         temporalData,
				rejected:     "CREATE CONSTRAINT contract_temporal_misbound FOR (n:TemporalBind) REQUIRE { (other.account, other.valid_from, other.valid_to) IS TEMPORAL NO OVERLAP }",
				accepted:     "CREATE CONSTRAINT contract_temporal_bound FOR (n:TemporalBind) REQUIRE { (n.account, n.valid_from, n.valid_to) IS TEMPORAL NO OVERLAP }",
				contractName: "contract_temporal_bound",
				entity:       storage.ConstraintEntityNode,
				properties:   []string{"account", "valid_from", "valid_to"},
				label:        "TemporalBind",
				count:        "MATCH (n:TemporalBind) RETURN count(n) AS c",
				duplicate:    "CREATE (:TemporalBind {account: 'first', valid_from: datetime('2024-01-15T00:00:00Z'), valid_to: datetime('2024-01-20T00:00:00Z')})",
			},
			{
				name:         "contract relationship key list",
				namespace:    "constraint_contract_rel_key_list_binding",
				data:         relationshipKeyData,
				rejected:     "CREATE CONSTRAINT contract_rel_key_misbound FOR ()-[r:RelKeyBind]-() REQUIRE { (other.`external.id`) IS RELATIONSHIP KEY }",
				accepted:     "CREATE CONSTRAINT contract_rel_key_bound FOR ()-[r:RelKeyBind]-() REQUIRE { (r.`external.id`) IS RELATIONSHIP KEY }",
				contractName: "contract_rel_key_bound",
				entity:       storage.ConstraintEntityRelationship,
				properties:   []string{"external.id"},
				label:        "RelKeyBind",
				count:        "MATCH ()-[r:RelKeyBind]->() RETURN count(r) AS c",
				duplicate:    "CREATE (:RelKeyBindA {k: 3})-[:RelKeyBind {`external.id`: 'first', extra: 'three'}]->(:RelKeyBindB {k: 3})",
			},
			{
				name:         "contract relationship key pair",
				namespace:    "constraint_contract_rel_key_pair_binding",
				data:         relationshipKeyData,
				rejected:     "CREATE CONSTRAINT contract_rel_key_pair_misbound FOR ()-[r:RelKeyBind]-() REQUIRE { (other.`external.id`, other.extra) IS RELATIONSHIP KEY }",
				accepted:     "CREATE CONSTRAINT contract_rel_key_pair_bound FOR ()-[r:RelKeyBind]-() REQUIRE { (r.`external.id`, r.extra) IS RELATIONSHIP KEY }",
				contractName: "contract_rel_key_pair_bound",
				entity:       storage.ConstraintEntityRelationship,
				properties:   []string{"external.id", "extra"},
				label:        "RelKeyBind",
				count:        "MATCH ()-[r:RelKeyBind]->() RETURN count(r) AS c",
				duplicate:    "CREATE (:RelKeyBindA {k: 3})-[:RelKeyBind {`external.id`: 'first', extra: 'one'}]->(:RelKeyBindB {k: 3})",
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ctx := context.Background()

				rejectedExec, _ := newCypherTestExecutor(t, tc.namespace)
				for _, create := range tc.data {
					_, err := rejectedExec.Execute(ctx, create, nil)
					require.NoError(t, err, create)
				}
				schema := rejectedExec.storage.GetSchema()
				constraintsBefore := len(schema.GetAllConstraints())
				indexesBefore := len(schema.GetIndexes())
				typesBefore := len(schema.GetAllPropertyTypeConstraints())
				ownedBefore := len(schema.GetCompositeIndexesForLabel(tc.label))

				_, err := rejectedExec.Execute(ctx, tc.rejected, nil)
				if assert.Error(t, err,
					"%s: a property reference qualified by another variable names no property of the FOR pattern, so the declaration must be rejected", tc.rejected) {
					requireErrorStatus(t, err, "Neo.ClientError.Statement.SyntaxError", tc.rejected)
				}
				assert.Len(t, schema.GetAllConstraints(), constraintsBefore,
					"rejection leaves the constraint schema unchanged")
				assert.Len(t, schema.GetIndexes(), indexesBefore,
					"rejection leaves the schema indexes unchanged")
				assert.Len(t, schema.GetAllPropertyTypeConstraints(), typesBefore,
					"rejection leaves the property type schema unchanged")
				assert.Len(t, schema.GetCompositeIndexesForLabel(tc.label), ownedBefore,
					"rejection leaves the label's equality indexes unchanged")
				contracts, err := schema.GetAllConstraintContracts(ctx)
				require.NoError(t, err)
				assert.Empty(t, contracts, "rejection registers no constraint contract")
				for _, property := range tc.properties {
					assert.False(t, schema.MaintainsPropertyIndex(tc.label, property),
						"rejection leaves no property index for %s", property)
				}
				assert.EqualValues(t, len(tc.data), countOf(t, rejectedExec, ctx, tc.count),
					"rejection leaves the stored data unchanged")

				acceptedExec, _ := newCypherTestExecutor(t, tc.namespace+"_control")
				for _, create := range tc.data {
					_, err := acceptedExec.Execute(ctx, create, nil)
					require.NoError(t, err, create)
				}
				_, err = acceptedExec.Execute(ctx, tc.accepted, nil)
				require.NoError(t, err,
					"the same declaration over the same data with its FOR variable remains supported: %s", tc.accepted)
				registered := tc.contractName
				if registered != "" {
					registered = storage.ConstraintContractEntryName(registered, 0)
				} else {
					registered = constraintNameOfBindingStatement(tc.accepted)
				}
				accepted := constraintNamedForBindingTest(t, acceptedExec.storage.GetSchema(), registered)
				assert.Equal(t, tc.entity, accepted.EffectiveEntityType(), tc.accepted)
				assert.Equal(t, tc.properties, accepted.Properties,
					"%s: every reference binds to the property the FOR variable names", tc.accepted)

				assert.EqualValues(t, len(tc.data), countOf(t, acceptedExec, ctx, tc.count),
					"the accepted declaration stores nothing by itself")
				_, err = acceptedExec.Execute(ctx, tc.duplicate, nil)
				if assert.Error(t, err, "the accepted declaration rejects a duplicate: %s", tc.duplicate) {
					requireErrorStatus(t, err, "Neo.ClientError.Schema.ConstraintValidationFailed", tc.duplicate)
				}
				assert.EqualValues(t, len(tc.data), countOf(t, acceptedExec, ctx, tc.count),
					"the rejected duplicate stores nothing")
			})
		}

	})
}

// constraintNameOfBindingStatement returns the constraint name a native
// CREATE CONSTRAINT statement declares: the word after CREATE CONSTRAINT.
func constraintNameOfBindingStatement(statement string) string {
	fields := strings.Fields(statement)
	for index, field := range fields {
		if field == "CONSTRAINT" && index+1 < len(fields) {
			return fields[index+1]
		}
	}
	return ""
}

// The scalar forms keep binding through the same requirement reader: a
// reference to another variable is rejected before any schema change, while the
// FOR variable's own reference stays supported.
func TestConstraintScalarReferencesBindToTheForVariable(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		ctx := context.Background()
		for _, tc := range []struct {
			name     string
			rejected string
			accepted string
		}{
			{
				name:     "uniqueness",
				rejected: "CREATE CONSTRAINT scalar_unique_misbound FOR (n:ScalarBind) REQUIRE other.code IS UNIQUE",
				accepted: "CREATE CONSTRAINT scalar_unique_bound FOR (n:ScalarBind) REQUIRE n.code IS UNIQUE",
			},
			{
				name:     "existence",
				rejected: "CREATE CONSTRAINT scalar_exists_misbound FOR (n:ScalarBind) REQUIRE other.code IS NOT NULL",
				accepted: "CREATE CONSTRAINT scalar_exists_bound FOR (n:ScalarBind) REQUIRE n.code IS NOT NULL",
			},
			{
				name:     "property type",
				rejected: "CREATE CONSTRAINT scalar_type_misbound FOR (n:ScalarBind) REQUIRE other.code IS :: STRING",
				accepted: "CREATE CONSTRAINT scalar_type_bound FOR (n:ScalarBind) REQUIRE n.code IS :: STRING",
			},
			{
				name:     "domain",
				rejected: "CREATE CONSTRAINT scalar_domain_misbound FOR (n:ScalarBind) REQUIRE other.code IN ['first', 'second']",
				accepted: "CREATE CONSTRAINT scalar_domain_bound FOR (n:ScalarBind) REQUIRE n.code IN ['first', 'second']",
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				exec, _ := newCypherTestExecutor(t, "constraint_scalar_binding")
				schema := exec.storage.GetSchema()
				_, err := exec.Execute(ctx, tc.rejected, nil)
				if assert.Error(t, err, tc.rejected) {
					requireErrorStatus(t, err, "Neo.ClientError.Statement.SyntaxError", tc.rejected)
				}
				assert.Empty(t, schema.GetAllConstraints(), tc.rejected)
				assert.Empty(t, schema.GetAllPropertyTypeConstraints(), tc.rejected)

				control, _ := newCypherTestExecutor(t, "constraint_scalar_binding_control")
				_, err = control.Execute(ctx, tc.accepted, nil)
				require.NoError(t, err, tc.accepted)
			})
		}
	})
}

// A contract entry that is not one of the compiled primitives keeps compiling
// as the boolean expression it is: a comparison and a value list are evaluated
// by the contract itself, so the shared requirement reader must leave them to
// that owner instead of compiling a native constraint from them.
func TestConstraintContractBooleanExpressionStaysBoolean(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		ctx := context.Background()
		for _, tc := range []struct{ name, expression string }{
			{name: "comparison", expression: "n.points > 0"},
			{name: "value list", expression: "n.status IN ['open', 'done']"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				exec, _ := newCypherTestExecutor(t, "constraint_contract_boolean_entry")
				ddl := "CREATE CONSTRAINT contract_boolean FOR (n:BooleanEntry) REQUIRE { " + tc.expression + " }"
				_, err := exec.Execute(ctx, ddl, nil)
				require.NoError(t, err, ddl)

				contracts, err := exec.storage.GetSchema().GetAllConstraintContracts(ctx)
				require.NoError(t, err)
				require.Len(t, contracts, 1)
				require.Len(t, contracts[0].Entries, 1)
				entry := contracts[0].Entries[0]
				assert.Equal(t, storage.ConstraintContractKindBooleanNode, entry.Kind,
					"%s keeps the contract's boolean expression kind", tc.expression)
				assert.Equal(t, tc.expression, entry.Expression)
				assert.Empty(t, exec.storage.GetSchema().GetAllConstraints(),
					"%s compiles no native constraint", tc.expression)

				// The contract's own expression, not a compiled constraint, rejects the
				// write; the autocommit wrapper reports it as the failed transaction.
				_, err = exec.Execute(ctx, "CREATE (:BooleanEntry {points: -1, status: 'closed'})", nil)
				require.ErrorContains(t, err, tc.expression,
					"the contract evaluates %s itself", tc.expression)
			})
		}
	})
}
