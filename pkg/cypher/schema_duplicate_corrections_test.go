package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestSchemaConstraintNamesArePreservedOnSupportedFORRequireShapes records the
// name every supported shape keeps: the CREATE CONSTRAINT prefix names the
// constraint it creates, for the node key, property type, relationship key and
// single-property shapes, both at the parser boundary and through Execute.
func TestSchemaConstraintNamesArePreservedOnSupportedFORRequireShapes(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()

	t.Run("node key", func(t *testing.T) {
		declaration, recognized, err := exec.parseCreateConstraintPropertyDDL(ctx,
			"CREATE CONSTRAINT named_node_key FOR (n:Person) REQUIRE (n.a, n.b) IS NODE KEY")
		require.NoError(t, err)
		require.True(t, recognized)
		require.Equal(t, "named_node_key", declaration.name)
		require.Equal(t, "Person", declaration.label)
		require.Equal(t, constraintRequirementNodeKey, declaration.requirement.kind)
		require.Equal(t, []string{"a", "b"}, declaration.requirement.properties)
	})

	t.Run("property type", func(t *testing.T) {
		declaration, recognized, err := exec.parseCreateConstraintPropertyDDL(ctx,
			"CREATE CONSTRAINT named_type FOR (n:Person) REQUIRE n.age IS :: INTEGER")
		require.NoError(t, err)
		require.True(t, recognized)
		require.Equal(t, "named_type", declaration.name)
		require.Equal(t, "Person", declaration.label)
		require.Equal(t, "age", declaration.requirement.property)
	})

	t.Run("single property", func(t *testing.T) {
		declaration, recognized, err := exec.parseCreateConstraintPropertyDDL(ctx,
			"CREATE CONSTRAINT named_unique FOR (n:Person) REQUIRE n.email IS UNIQUE")
		require.NoError(t, err)
		require.True(t, recognized)
		require.Equal(t, "named_unique", declaration.name)
		require.Equal(t, "Person", declaration.label)
		require.Equal(t, "email", declaration.requirement.property)
		require.Equal(t, constraintRequirementUnique, declaration.requirement.kind)
	})

	t.Run("relationship key", func(t *testing.T) {
		declaration, recognized, err := exec.parseCreateConstraintPropertyDDL(ctx,
			"CREATE CONSTRAINT named_rel_key FOR ()-[r:KNOWS]-() REQUIRE (r.since) IS RELATIONSHIP KEY")
		require.NoError(t, err)
		require.True(t, recognized)
		require.Equal(t, "named_rel_key", declaration.name)
		require.Equal(t, []string{"since"}, declaration.requirement.properties)
	})

	t.Run("through Execute", func(t *testing.T) {
		_, err := exec.Execute(ctx,
			"CREATE CONSTRAINT created_unique FOR (n:Created) REQUIRE n.email IS UNIQUE", nil)
		require.NoError(t, err)
		require.Contains(t, readSchemaNames(t, ctx, exec, "SHOW CONSTRAINTS YIELD name RETURN name ORDER BY name"), "created_unique")
	})
}

// TestSchemaLegacyAssertShapeIsRejected records the current contract for the
// Neo4j 4.x ON ... ASSERT shape: it is unsupported syntax. The one property
// declaration owner rejects the shape before reading its predicate, and the
// public entry keeps its own rejection, so no statement of this shape reaches a
// predicate reader.
func TestSchemaLegacyAssertShapeIsRejected(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()

	statements := []string{
		"CREATE CONSTRAINT legacy_node_key ON (n:Legacy) ASSERT (n.a, n.b) IS NODE KEY",
		"CREATE CONSTRAINT legacy_type ON (n:Legacy) ASSERT n.age IS TYPED INTEGER",
		"CREATE CONSTRAINT legacy_unique ON (n:Legacy) ASSERT n.email IS UNIQUE",
	}
	for _, statement := range statements {
		declaration, recognized, err := exec.parseCreateConstraintPropertyDDL(ctx, statement)
		require.False(t, recognized, statement)
		require.EqualError(t, err, "unsupported CREATE CONSTRAINT shape", statement)
		require.Equal(t, parsedConstraintPropertyDeclaration{}, declaration, statement)
	}

	for _, statement := range statements {
		_, err := exec.Execute(ctx, statement, nil)
		require.EqualError(t, err, "invalid CREATE CONSTRAINT syntax", statement)
		requireErrorStatus(t, err, "Neo.ClientError.Statement.SyntaxError", statement)
	}
}

// TestSchemaPropertiesSegmentOwnsIndexAndFulltextONClauses pins the one ON-clause
// owner both index families now read: the same property expression, OPTIONS tail
// and errors as before, with EACH and a bracketed list admitted only by the
// fulltext form.
func TestSchemaPropertiesSegmentOwnsIndexAndFulltextONClauses(t *testing.T) {
	ctx := context.Background()

	indexCases := []struct {
		onClause string
		props    string
		tail     string
		errText  string
	}{
		{onClause: "(n.name, n.`full name`) OPTIONS {indexProvider: 'range-1.0'}", props: "n.name, n.`full name`", tail: "OPTIONS {indexProvider: 'range-1.0'}"},
		{onClause: "n.name OPTIONS {indexProvider: 'range-1.0'}", props: "n.name", tail: "OPTIONS {indexProvider: 'range-1.0'}"},
		{onClause: "n.name", props: "n.name"},
		{onClause: "[n.name]", props: "[n.name]"},
		{onClause: "EACH [n.name]", props: "EACH [n.name]"},
		{onClause: "", errText: "empty ON clause"},
		{onClause: "(n.name", errText: "invalid ON clause"},
	}
	for _, testCase := range indexCases {
		props, tail, err := propertiesSegment(ctx, testCase.onClause, propertySyntaxOrdinary)
		if testCase.errText != "" {
			require.EqualError(t, err, testCase.errText, testCase.onClause)
			continue
		}
		require.NoError(t, err, testCase.onClause)
		require.Equal(t, testCase.props, props, testCase.onClause)
		require.Equal(t, testCase.tail, tail, testCase.onClause)
	}

	fulltextCases := []struct {
		onClause string
		props    string
		tail     string
		errText  string
	}{
		{onClause: "EACH [n.title, n.body] OPTIONS {indexConfig: {}}", props: "n.title, n.body", tail: "OPTIONS {indexConfig: {}}"},
		{onClause: "[n.title]", props: "n.title"},
		{onClause: "(n.title)", props: "n.title"},
		{onClause: "n.title", props: "n.title"},
		{onClause: "EACH", errText: "empty ON clause"},
		{onClause: "", errText: "empty ON clause"},
		{onClause: "EACH (n.title", errText: "invalid ON clause"},
		{onClause: "EACH [n.title", errText: "invalid ON EACH clause"},
	}
	for _, testCase := range fulltextCases {
		props, tail, err := propertiesSegment(ctx, testCase.onClause, propertySyntaxFulltext)
		if testCase.errText != "" {
			require.EqualError(t, err, testCase.errText, testCase.onClause)
			continue
		}
		require.NoError(t, err, testCase.onClause)
		require.Equal(t, testCase.props, props, testCase.onClause)
		require.Equal(t, testCase.tail, tail, testCase.onClause)
	}
}

// TestSchemaIndexONClauseAdmissionUnchanged checks the same statements through
// Execute: the shared ON-clause owner keeps the accepted and rejected index
// forms exactly as the two families admitted them.
func TestSchemaIndexONClauseAdmissionUnchanged(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		exec, _ := newTestExecutor(t)
		ctx := context.Background()

		accepted := []string{
			"CREATE RANGE INDEX seg_range FOR (n:Seg) ON (n.p) OPTIONS {}",
			"CREATE RANGE INDEX seg_range_plain FOR (n:Seg) ON n.q",
			"CREATE FULLTEXT INDEX seg_full FOR (n:Seg) ON EACH [n.title] OPTIONS {}",
		}
		for _, statement := range accepted {
			_, err := exec.Execute(ctx, statement, nil)
			require.NoError(t, err, statement)
		}

		// Parenthesized fulltext ON is a native extension, not ANTLR grammar.
		// Both outcomes predate this consolidation (38bd71f65); neither parser
		// may silently publish a schema object when it rejects the statement.
		_, extensionErr := exec.Execute(ctx, "CREATE FULLTEXT INDEX seg_full_paren FOR (n:Seg) ON (n.body)", nil)
		if config.GetParserType() == config.ParserTypeANTLR {
			requireErrorStatus(t, extensionErr, "Neo.ClientError.Statement.SyntaxError", "parenthesized fulltext extension")
			require.NotContains(t, readSchemaNames(t, ctx, exec, "SHOW INDEXES YIELD name RETURN name"), "seg_full_paren")
		} else {
			require.NoError(t, extensionErr)
			require.Contains(t, readSchemaNames(t, ctx, exec, "SHOW INDEXES YIELD name RETURN name"), "seg_full_paren")
		}

		rejected := []string{
			// CREATE [RANGE|VECTOR] INDEX admits neither EACH nor a list.
			"CREATE RANGE INDEX seg_each FOR (n:Seg) ON EACH [n.p]",
			"CREATE RANGE INDEX seg_bracket FOR (n:Seg) ON [n.p]",
			// A bare OPTIONS keyword has no payload, in either family.
			"CREATE RANGE INDEX seg_bare FOR (n:Seg) ON (n.p) OPTIONS",
			"CREATE FULLTEXT INDEX seg_full_bare FOR (n:Seg) ON EACH [n.t] OPTIONS",
		}
		for _, statement := range rejected {
			_, err := exec.Execute(ctx, statement, nil)
			requireErrorStatus(t, err, "Neo.ClientError.Statement.SyntaxError", statement)
		}
	})
}

// TestSchemaForPatternReaderOwnsTheForClause pins the one FOR pattern reading:
// node and relationship patterns, their variable and their label or type, and
// the rejection of every shape neither the index parser nor the contract admits.
func TestSchemaForPatternReaderOwnsTheForClause(t *testing.T) {
	cases := []struct {
		clause         string
		isRelationship bool
		variableSource string
		labelOrType    string
		direction      string
		ok             bool
	}{
		{clause: "(n:Person)", variableSource: "n", labelOrType: "Person", ok: true},
		{clause: "  (n:Person)  ", variableSource: "n", labelOrType: "Person", ok: true},
		{clause: "(n:`Weird Label`)", variableSource: "n", labelOrType: "Weird Label", ok: true},
		{clause: "()-[r:KNOWS]-()", isRelationship: true, variableSource: "r", labelOrType: "KNOWS", ok: true},
		{clause: "(a)-[`r x`:`KNOWS TYPE`]-(b)", isRelationship: true, variableSource: "`r x`", labelOrType: "KNOWS TYPE", ok: true},
		{clause: "()-[r:KNOWS]->()", isRelationship: true, variableSource: "r", labelOrType: "KNOWS", direction: "OUTGOING", ok: true},
		{clause: "()<-[r:KNOWS]-()", isRelationship: true, variableSource: "r", labelOrType: "KNOWS", direction: "INCOMING", ok: true},
		{clause: "(:User)-[:FOLLOWS]->(:User)", isRelationship: true, labelOrType: "FOLLOWS", direction: "OUTGOING", ok: true},
		{clause: "(:User)<-[:FOLLOWS]-(:User)", isRelationship: true, labelOrType: "FOLLOWS", direction: "INCOMING", ok: true},
		{clause: "(:User)-[:FOLLOWS]-(:User)", isRelationship: true, labelOrType: "FOLLOWS", ok: true},
		{clause: "(a)<-[r:TYPE]->(b)", isRelationship: true, variableSource: "r", labelOrType: "TYPE", direction: "OUTGOING", ok: true},
		{clause: "()-[r:`a b|c `]->()", isRelationship: true, variableSource: "r", labelOrType: "a b|c ", direction: "OUTGOING", ok: true},
		{clause: "(a)-[`r x`:`KNOWS TYPE`]->(b)", isRelationship: true, variableSource: "`r x`", labelOrType: "KNOWS TYPE", direction: "OUTGOING", ok: true},
		{clause: ""},
		{clause: "(n)"},
		{clause: ":Person"},
		{clause: "()-[r]-()"},
		{clause: "[r:KNOWS]"},
		{clause: "(n:Person) trailing"},
	}
	for _, testCase := range cases {
		target, ok := parseForPatternTarget(testCase.clause)
		require.Equal(t, testCase.ok, ok, testCase.clause)
		require.Equal(t, testCase.isRelationship, target.isRelationship, testCase.clause)
		require.Equal(t, testCase.variableSource, target.variableSource, testCase.clause)
		require.Equal(t, testCase.labelOrType, target.name, testCase.clause)
		require.Equal(t, testCase.direction, target.direction, testCase.clause)
	}
}

// TestSchemaForPatternCallersKeepTheirOwnErrors checks the two callers that map
// the shared reading to their own subjects: the index parser names the FOR
// clause it read, the contract names its entity.
func TestSchemaForPatternCallersKeepTheirOwnErrors(t *testing.T) {
	label, relType, isRelationship, err := parseCreateIndexForPattern("(n:Person)")
	require.NoError(t, err)
	require.Equal(t, "Person", label)
	require.Empty(t, relType)
	require.False(t, isRelationship)

	label, relType, isRelationship, err = parseCreateIndexForPattern("()-[r:KNOWS]-()")
	require.NoError(t, err)
	require.Empty(t, label)
	require.Equal(t, "KNOWS", relType)
	require.True(t, isRelationship)

	_, _, _, err = parseCreateIndexForPattern("")
	require.EqualError(t, err, "empty FOR clause")

	_, _, _, err = parseCreateIndexForPattern("(n)")
	require.EqualError(t, err, "invalid node FOR clause")

	_, _, _, err = parseCreateIndexForPattern("()-[r]-()")
	require.EqualError(t, err, "invalid relationship FOR clause")

	entityType, variable, labelOrType, ok := parseConstraintContractForClause("()-[r:KNOWS]-()")
	require.True(t, ok)
	require.Equal(t, storage.ConstraintEntityRelationship, entityType)
	require.Equal(t, "r", variable)
	require.Equal(t, "KNOWS", labelOrType)

	entityType, variable, labelOrType, ok = parseConstraintContractForClause("(n:Person)")
	require.True(t, ok)
	require.Equal(t, storage.ConstraintEntityNode, entityType)
	require.Equal(t, "n", variable)
	require.Equal(t, "Person", labelOrType)

	_, _, _, ok = parseConstraintContractForClause("(n)")
	require.False(t, ok)
}

// TestSchemaRelationshipKeyDDLUsesTheForPatternVariable checks the relationship
// key declaration at that owner: it takes the pattern's variable from the shared
// FOR reading and still rejects property references qualified by another
// variable.
func TestSchemaRelationshipKeyDDLUsesTheForPatternVariable(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()

	declaration, recognized, err := exec.parseCreateConstraintPropertyDDL(ctx,
		"CREATE CONSTRAINT rk FOR ()-[r:KNOWS]-() REQUIRE (r.since, r.reason) IS RELATIONSHIP KEY")
	require.NoError(t, err)
	require.True(t, recognized)
	require.Equal(t, "rk", declaration.name)
	require.Equal(t, "KNOWS", declaration.label)
	require.True(t, declaration.isRelationship)
	require.Equal(t, constraintRequirementRelationshipKey, declaration.requirement.kind)
	require.Equal(t, []string{"since", "reason"}, declaration.requirement.properties)

	_, recognized, err = exec.parseCreateConstraintPropertyDDL(ctx,
		"CREATE CONSTRAINT rk_mismatch FOR ()-[r:KNOWS]-() REQUIRE (s.since) IS RELATIONSHIP KEY")
	require.True(t, recognized)
	require.EqualError(t, err, "unsupported constraint property references predicate")
}

// TestSchemaRequireHeadOwnsTheOptionsSplit records that the shared FOR ...
// REQUIRE head still strips and admits the one OPTIONS tail, and that the
// contract statement reaches only the prefix owner, so it keeps rejecting the
// block's OPTIONS tail with the error it already produced.
func TestSchemaRequireHeadOwnsTheOptionsSplit(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()

	name, patternSpan, requireExpr, options, err := exec.parseCreateConstraintForRequireHead(ctx,
		"CREATE CONSTRAINT head_opts FOR (n:Head) REQUIRE n.p IS UNIQUE OPTIONS {indexProvider: 'range-1.0'}")
	require.NoError(t, err)
	require.Equal(t, "head_opts", name)
	require.Equal(t, " (n:Head) ", patternSpan)
	require.Equal(t, "n.p IS UNIQUE", requireExpr)
	require.Equal(t, "{indexProvider: 'range-1.0'}", options.literal)
	require.Empty(t, options.parameter)

	_, _, _, _, err = exec.parseCreateConstraintForRequireHead(ctx,
		"CREATE CONSTRAINT head_bare FOR (n:Head) REQUIRE n.p IS UNIQUE OPTIONS")
	require.EqualError(t, err, "invalid OPTIONS clause")
}

// TestSchemaContractRequireBlockStaysClosedToOptions proves the contract
// statement admits no OPTIONS after its REQUIRE block: a well-formed tail, a
// bare keyword and trailing syntax all stay the REQUIRE-block error, and the
// block-only statement still creates the contract.
func TestSchemaContractRequireBlockStaysClosedToOptions(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		exec, _ := newTestExecutor(t)
		ctx := context.Background()

		block := "CREATE CONSTRAINT contract_opts FOR (n:ContractOpts) REQUIRE { n.id IS UNIQUE }"
		rejected := []string{
			block + " OPTIONS {}",
			block + " OPTIONS {indexProvider: 'range-1.0'}",
			block + " OPTIONS",
			block + " trailing",
		}
		for _, statement := range rejected {
			// The contract parser owns the decision and its REQUIRE-block error.
			_, err := exec.parseCreateConstraintContractDDL(ctx, statement)
			require.EqualError(t, err, "invalid REQUIRE block clause", statement)

			// Both public parsers reject the tail with SyntaxError; ANTLR reports
			// its grammar position, while the native router reports the DDL error.
			_, err = exec.Execute(ctx, statement, nil)
			requireErrorStatus(t, err, "Neo.ClientError.Statement.SyntaxError", statement)
			if config.GetParserType() != config.ParserTypeANTLR {
				require.EqualError(t, err, "invalid CREATE CONSTRAINT syntax", statement)
			}
			contracts, readErr := exec.storage.GetSchema().GetConstraintContractsForTarget(ctx, storage.ConstraintEntityNode, "ContractOpts")
			require.NoError(t, readErr)
			require.Empty(t, contracts)
		}

		_, err := exec.Execute(ctx, block, nil)
		require.NoError(t, err)
		require.Contains(t, readSchemaNames(t, ctx, exec, "SHOW CONSTRAINT CONTRACTS"), "contract_opts")

		// The block's delimiter reading is the shared one: a nested brace inside an
		// entry stays inside the block.
		parsed, err := exec.parseCreateConstraintContractDDL(ctx,
			"CREATE CONSTRAINT nested_brace FOR (n:BraceOwner) REQUIRE { n.id IS UNIQUE; COUNT { (n)-[:R]->(:X) } <= 1 }")
		require.NoError(t, err)
		require.Contains(t, parsed.body, "COUNT { (n)-[:R]->(:X) } <= 1")

		_, err = exec.parseCreateConstraintContractDDL(ctx,
			"CREATE CONSTRAINT unnamed_default FOR (n:BraceDefault) REQUIRE { n.id IS UNIQUE }")
		require.NoError(t, err)
	})
}

// TestSchemaContractRelationshipKeyEntryStaysParenthesized proves the contract's
// relationship-key admission is unchanged: the parenthesized list still compiles
// to the RELATIONSHIP KEY primitive, and the unparenthesized spelling the shared
// predicate would accept stays a boolean relationship expression.
func TestSchemaContractRelationshipKeyEntryStaysParenthesized(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()

	entry, compiled, propertyType, err := exec.parseConstraintContractEntry(ctx,
		"(r.since, r.reason) IS RELATIONSHIP KEY", "r",
		storage.ConstraintEntityRelationship, "KNOWS", "contract_entry", 0)
	require.NoError(t, err)
	require.Nil(t, propertyType)
	require.NotNil(t, compiled)
	require.Equal(t, storage.ConstraintRelationshipKey, compiled.Type)
	require.Equal(t, []string{"since", "reason"}, compiled.Properties)
	require.Equal(t, storage.ConstraintContractKindPrimitiveRelationship, entry.Kind)

	entry, compiled, propertyType, err = exec.parseConstraintContractEntry(ctx,
		"r.since IS RELATIONSHIP KEY", "r",
		storage.ConstraintEntityRelationship, "KNOWS", "contract_entry", 1)
	require.NoError(t, err)
	require.Nil(t, propertyType)
	require.Nil(t, compiled)
	require.Equal(t, storage.ConstraintContractKindBooleanRelationship, entry.Kind)
	require.Equal(t, "r.since IS RELATIONSHIP KEY", entry.Expression)
}

// TestSchemaContractRelationshipKeyAdmissionUnchanged checks the same two entries
// through Execute: only the parenthesized entry compiles a constraint.
func TestSchemaContractRelationshipKeyAdmissionUnchanged(t *testing.T) {
	parenthesized, _ := newTestExecutor(t)
	ctx := context.Background()

	_, err := parenthesized.Execute(ctx,
		"CREATE CONSTRAINT rel_guard_paren FOR ()-[r:ParenRel]-() REQUIRE { (r.since, r.reason) IS RELATIONSHIP KEY }", nil)
	require.NoError(t, err)
	constraints, err := parenthesized.Execute(ctx, "SHOW CONSTRAINTS", nil)
	require.NoError(t, err)
	require.Len(t, constraints.Rows, 1)
	require.Equal(t, storage.ConstraintContractEntryName("rel_guard_paren", 0), constraints.Rows[0][1])

	// The unparenthesized spelling is not a compiled primitive, so the contract
	// stores it as a boolean relationship entry and the contract's own validation
	// reads it as an expression; no RELATIONSHIP KEY constraint is created, which
	// is what a widened admission would have produced instead.
	unparenthesized, _ := newTestExecutor(t)
	_, err = unparenthesized.Execute(ctx,
		"CREATE CONSTRAINT rel_guard_bool FOR ()-[r:BoolRel]-() REQUIRE { r.since IS RELATIONSHIP KEY }", nil)
	require.ErrorContains(t, err, "unsupported relationship predicate")
	constraints, err = unparenthesized.Execute(ctx, "SHOW CONSTRAINTS", nil)
	require.NoError(t, err)
	require.Empty(t, constraints.Rows)
}

// TestSchemaCardinalityDDLUsesTheSharedForPatternReading records the cardinality
// contract's reading of that target: the type and direction are the FOR
// pattern's own, the directional admission still rejects the node and
// undirected shapes with the error it always produced, and the MAX COUNT
// admission is unchanged.
func TestSchemaCardinalityDDLUsesTheSharedForPatternReading(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()

	outgoing, err := exec.parseCreateConstraintCardinalityDDL(ctx,
		"CREATE CONSTRAINT card_out FOR ()-[r:KNOWS]->() REQUIRE MAX COUNT 2")
	require.NoError(t, err)
	require.Equal(t, "card_out", outgoing.name)
	require.Equal(t, "KNOWS", outgoing.relType)
	require.Equal(t, "OUTGOING", outgoing.direction)
	require.Equal(t, 2, outgoing.maxCount)

	incoming, err := exec.parseCreateConstraintCardinalityDDL(ctx,
		"CREATE CONSTRAINT card_in FOR ()<-[r:`a b|c `]-() REQUIRE MAX COUNT 3")
	require.NoError(t, err)
	require.Equal(t, "card_in", incoming.name)
	require.Equal(t, "a b|c ", incoming.relType)
	require.Equal(t, "INCOMING", incoming.direction)
	require.Equal(t, 3, incoming.maxCount)

	for _, statement := range []string{
		"CREATE CONSTRAINT card_undirected FOR ()-[r:KNOWS]-() REQUIRE MAX COUNT 2",
		"CREATE CONSTRAINT card_node FOR (n:Person) REQUIRE MAX COUNT 2",
		"CREATE CONSTRAINT card_untyped FOR ()-[r]->() REQUIRE MAX COUNT 2",
	} {
		_, err := exec.parseCreateConstraintCardinalityDDL(ctx, statement)
		require.EqualError(t, err, "invalid cardinality FOR pattern", statement)
	}

	_, err = exec.parseCreateConstraintCardinalityDDL(ctx,
		"CREATE CONSTRAINT card_zero FOR ()-[r:KNOWS]->() REQUIRE MAX COUNT 0")
	require.ErrorContains(t, err, "MAX COUNT must be a positive integer")
}

// TestSchemaCardinalityDirectionOnPublicDDL proves the public CREATE CONSTRAINT
// path stores the direction and type the shared reading derived - outgoing and
// incoming, a quoted pipe and space type kept exactly - and that a rejected
// directional statement publishes no schema object.
func TestSchemaCardinalityDirectionOnPublicDDL(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		exec, store := newTestExecutor(t)
		ctx := context.Background()

		_, err := exec.Execute(ctx, "CREATE CONSTRAINT card_out FOR ()-[r:KNOWS]->() REQUIRE MAX COUNT 2", nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, "CREATE CONSTRAINT card_in FOR ()<-[r:`a b|c `]-() REQUIRE MAX COUNT 3", nil)
		require.NoError(t, err)

		stored := map[string]storage.Constraint{}
		for _, constraint := range store.GetSchema().GetAllConstraints() {
			stored[constraint.Name] = constraint
		}
		require.Len(t, stored, 2)
		require.Equal(t, storage.ConstraintCardinality, stored["card_out"].Type)
		require.Equal(t, storage.ConstraintEntityRelationship, stored["card_out"].EntityType)
		require.Equal(t, "KNOWS", stored["card_out"].Label)
		require.Equal(t, "OUTGOING", stored["card_out"].Direction)
		require.Equal(t, 2, stored["card_out"].MaxCount)
		require.Equal(t, "a b|c ", stored["card_in"].Label)
		require.Equal(t, "INCOMING", stored["card_in"].Direction)
		require.Equal(t, 3, stored["card_in"].MaxCount)

		reported, err := exec.Execute(ctx, "SHOW CONSTRAINTS YIELD name, labelsOrTypes, createStatement", nil)
		require.NoError(t, err)
		statements := map[string]string{}
		for _, row := range reported.Rows {
			require.Len(t, row, 3)
			name := row[0].(string)
			statements[name] = row[2].(string)
			if name == "card_in" {
				require.Equal(t, []string{"a b|c "}, row[1])
			}
		}
		require.Equal(t, "CREATE CONSTRAINT `card_out` FOR ()-[r:`KNOWS`]->() REQUIRE MAX COUNT 2", statements["card_out"])
		require.Equal(t, "CREATE CONSTRAINT `card_in` FOR ()<-[r:`a b|c `]-() REQUIRE MAX COUNT 3", statements["card_in"])

		// The undirected pattern the retired reader rejected stays rejected on
		// the public path, keeps its error, and publishes nothing.
		before := readSchemaNames(t, ctx, exec, "SHOW CONSTRAINTS YIELD name RETURN name ORDER BY name")
		require.Equal(t, []string{"card_in", "card_out"}, before)
		_, err = exec.Execute(ctx, "CREATE CONSTRAINT card_undirected FOR ()-[r:KNOWS]-() REQUIRE MAX COUNT 2", nil)
		requireErrorStatus(t, err, "Neo.ClientError.Statement.SyntaxError", "undirected cardinality")
		require.EqualError(t, err, "invalid cardinality FOR pattern")
		require.Equal(t, before, readSchemaNames(t, ctx, exec, "SHOW CONSTRAINTS YIELD name RETURN name ORDER BY name"))
	})
}
