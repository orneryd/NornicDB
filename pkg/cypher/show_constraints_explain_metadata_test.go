package cypher

import (
	"context"
	"fmt"
	"slices"
	"testing"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// NOR-300: a supported SHOW [kind] CONSTRAINTS listing has one column
// declaration, in the typed SHOW owner that also builds the listing. EXPLAIN
// and PROFILE answer with that declaration instead of declining the statement,
// and their header is the listing's own header in both Cypher versions.

// showConstraintsExplainFixture creates one constraint of each kind the listing
// variants below select.
func showConstraintsExplainFixture(t *testing.T) *StorageExecutor {
	t.Helper()
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	for _, ddl := range []string{
		"CREATE CONSTRAINT nor300_explain_unique FOR (n:Person) REQUIRE n.email IS UNIQUE",
		"CREATE CONSTRAINT nor300_explain_key FOR (n:Book) REQUIRE (n.isbn, n.edition) IS NODE KEY",
		"CREATE CONSTRAINT nor300_explain_exists FOR (n:Person) REQUIRE n.name IS NOT NULL",
		"CREATE CONSTRAINT nor300_explain_rel_exists FOR ()-[r:Follows]-() REQUIRE r.since IS NOT NULL",
		"CREATE CONSTRAINT nor300_explain_type FOR (n:Person) REQUIRE n.age IS :: INTEGER",
	} {
		if _, err := exec.Execute(t.Context(), ddl, nil); err != nil {
			t.Fatalf("create fixture constraint %q: %v", ddl, err)
		}
	}
	return exec
}

// showConstraintsExplainColumns are the default columns a listing returns, per
// statement version, as Neo4j 5.26 and 2026.09 report them.
var showConstraintsExplainColumns = map[string][]string{
	"5":  {"id", "name", "type", "entityType", "labelsOrTypes", "properties", "ownedIndex", "propertyType"},
	"25": {"id", "name", "type", "entityType", "labelsOrTypes", "properties", "enforcedLabel", "ownedIndex", "propertyType"},
}

func TestShowConstraintsExplainHeaderMatchesListing(t *testing.T) {
	exec := showConstraintsExplainFixture(t)
	modes := []struct {
		name, version, prefix string
		ctx                   context.Context
	}{
		{name: "cypher5", version: "5", ctx: t.Context()},
		{name: "cypher25-context", version: "25", ctx: context.WithValue(t.Context(), cypherVersionKey{}, "25")},
		{name: "cypher25-grammar", version: "25", prefix: "CYPHER 25 ", ctx: t.Context()},
	}
	statements := []string{
		"SHOW CONSTRAINTS",
		"SHOW ALL CONSTRAINTS",
		"SHOW UNIQUE CONSTRAINTS",
		"SHOW NODE UNIQUE CONSTRAINTS",
		"SHOW NODE KEY CONSTRAINTS",
		"SHOW NODE PROPERTY EXISTENCE CONSTRAINTS",
		"SHOW REL PROPERTY EXISTENCE CONSTRAINTS",
		"SHOW PROPERTY TYPE CONSTRAINTS",
	}
	for _, mode := range modes {
		for _, statement := range statements {
			t.Run(mode.name+"/"+statement, func(t *testing.T) {
				want := showConstraintsExplainColumns[mode.version]
				query := mode.prefix + statement
				listed, err := exec.Execute(mode.ctx, query, nil)
				if err != nil {
					t.Fatalf("listing rejected: %v", err)
				}
				if !slices.Equal(listed.Columns, want) {
					t.Fatalf("listing columns = %v, want %v", listed.Columns, want)
				}
				// The header comes from the metadata owner, which compiles the
				// declaration; the listing never runs to establish it.
				columns, err := exec.StatementResultColumnsContext(mode.ctx, query, RawSource)
				if err != nil {
					t.Fatalf("metadata owner failed: %v", err)
				}
				if !slices.Equal(columns, want) {
					t.Fatalf("metadata owner columns = %v, want %v", columns, want)
				}
				explained, err := exec.Execute(mode.ctx, mode.prefix+"EXPLAIN "+statement, nil)
				if err != nil {
					t.Fatalf("EXPLAIN rejected: %v", err)
				}
				if !slices.Equal(explained.Columns, want) {
					t.Fatalf("EXPLAIN columns = %v, want %v", explained.Columns, want)
				}
				if len(explained.Rows) != 0 {
					t.Fatalf("EXPLAIN returned %d rows, want none", len(explained.Rows))
				}
				if explained.Metadata["plan"] == nil {
					t.Fatal("EXPLAIN returned no plan")
				}
				profiled, err := exec.Execute(mode.ctx, mode.prefix+"PROFILE "+statement, nil)
				if err != nil {
					t.Fatalf("PROFILE rejected the listing's own header: %v", err)
				}
				if !slices.Equal(profiled.Columns, want) {
					t.Fatalf("PROFILE columns = %v, want %v", profiled.Columns, want)
				}
				if len(profiled.Rows) != len(listed.Rows) {
					t.Fatalf("PROFILE rows = %d, listing rows = %d", len(profiled.Rows), len(listed.Rows))
				}
			})
		}
	}
}

// The declaration is the listing's own, so the unfiltered listing still reports
// every fixture constraint and YIELD * still reaches the full column set.
func TestShowConstraintsListingRetainsRowsAndFullColumns(t *testing.T) {
	exec := showConstraintsExplainFixture(t)
	listed, err := exec.Execute(t.Context(), "SHOW CONSTRAINTS", nil)
	if err != nil {
		t.Fatalf("SHOW CONSTRAINTS: %v", err)
	}
	if len(listed.Rows) != 5 {
		t.Fatalf("SHOW CONSTRAINTS rows = %d, want 5", len(listed.Rows))
	}
	full, err := exec.Execute(t.Context(), "SHOW CONSTRAINTS YIELD *", nil)
	if err != nil {
		t.Fatalf("SHOW CONSTRAINTS YIELD *: %v", err)
	}
	columns := showConstraintResult(false).columns
	if !slices.Equal(full.Columns, columns) {
		t.Fatalf("YIELD * columns = %v, want %v", full.Columns, columns)
	}
	if len(full.Rows) != len(listed.Rows) {
		t.Fatalf("YIELD * rows = %d, want %d", len(full.Rows), len(listed.Rows))
	}
}

// EXPLAIN and PROFILE read the declaration without listing or mutating the
// schema: an EXPLAIN carries no rows, and the constraint and index sets are
// identical before and after.
func TestShowConstraintsExplainLeavesSchemaUnchanged(t *testing.T) {
	exec := showConstraintsExplainFixture(t)
	schema := exec.storage.GetSchema()
	before := showConstraintsSchemaSnapshot(schema)
	modes := []struct {
		name string
		ctx  context.Context
	}{
		{name: "cypher5", ctx: t.Context()},
		{name: "cypher25", ctx: context.WithValue(t.Context(), cypherVersionKey{}, "25")},
	}
	statements := []string{"SHOW CONSTRAINTS", "SHOW NODE KEY CONSTRAINTS", "SHOW PROPERTY TYPE CONSTRAINTS"}
	for _, mode := range modes {
		for _, statement := range statements {
			for _, prefix := range []string{"EXPLAIN ", "PROFILE "} {
				query := prefix + statement
				if _, err := exec.Execute(mode.ctx, query, nil); err != nil {
					t.Fatalf("%s/%s: %v", mode.name, query, err)
				}
			}
		}
	}
	if after := showConstraintsSchemaSnapshot(schema); !slices.Equal(before, after) {
		t.Fatalf("schema changed\nbefore: %v\nafter:  %v", before, after)
	}
}

// The header is a declaration, not an observation: an empty schema answers
// EXPLAIN with the same columns as a populated one.
func TestShowConstraintsExplainHeaderIsDeclaredOnEmptySchema(t *testing.T) {
	populated, err := showConstraintsExplainFixture(t).Execute(t.Context(), "EXPLAIN SHOW CONSTRAINTS", nil)
	if err != nil {
		t.Fatalf("EXPLAIN on a populated schema: %v", err)
	}
	empty := NewStorageExecutor(newTestMemoryEngine(t))
	declared, err := empty.Execute(t.Context(), "EXPLAIN SHOW CONSTRAINTS", nil)
	if err != nil {
		t.Fatalf("EXPLAIN on an empty schema: %v", err)
	}
	if !slices.Equal(declared.Columns, populated.Columns) {
		t.Fatalf("empty schema columns = %v, populated = %v", declared.Columns, populated.Columns)
	}
}

// The declaration owner answers the commands whose listing schema it declares
// and declines the others: a command with a listing of its own keeps the
// contract it had. The command's kind selects rows, never columns.
func TestShowCommandDeclaredResultScope(t *testing.T) {
	declared := func(cypher string, cypher25 bool) (showResult, bool, error) {
		syntax, err := cypherantlr.ParseSyntax(t.Context(), cypher, cypherantlr.StatementEntry, cypher25, cypherantlr.AcceptedSource)
		if err != nil {
			t.Fatalf("%q: %v", cypher, err)
		}
		show := statementSyntaxQuery(syntax).ChildRule(cypherantlr.CypherParserRULE_showCommand)
		if !show.Valid() {
			t.Fatalf("%q is not a SHOW command", cypher)
		}
		return showCommandResult(t.Context(), show, cypher25)
	}
	for _, cypher := range []string{"SHOW PROCEDURES", "SHOW FUNCTIONS", "SHOW SETTINGS", "SHOW ALIASES FOR DATABASES"} {
		if _, known, err := declared(cypher, false); known || err != nil {
			t.Fatalf("%q answered by a declaration this owner does not hold: known=%v err=%v", cypher, known, err)
		}
	}
	for _, statement := range []struct {
		cypher string
		want   []string
	}{
		{"SHOW NODE KEY CONSTRAINTS", append(append([]string(nil), showConstraintsExplainColumns["5"]...), "options", "createStatement", "direction", "maxCount", "sourceLabel", "targetLabel", "policyMode")},
		{"SHOW CONSTRAINT CONTRACTS", showConstraintContractsResult.columns},
		{"SHOW USERS", showUsersResult.columns},
		{"SHOW INDEXES", showIndexesResult.columns},
	} {
		result, known, err := declared(statement.cypher, false)
		if !known || err != nil {
			t.Fatalf("%q not answered: known=%v err=%v", statement.cypher, known, err)
		}
		if !slices.Equal(result.columns, statement.want) {
			t.Fatalf("%q columns = %v, want %v", statement.cypher, result.columns, statement.want)
		}
	}
	constraints, _, err := declared("SHOW CONSTRAINTS", false)
	if err != nil {
		t.Fatalf("SHOW CONSTRAINTS: %v", err)
	}
	if !slices.Equal(constraints.defaults, showConstraintsExplainColumns["5"]) {
		t.Fatalf("constraint defaults = %v, want %v", constraints.defaults, showConstraintsExplainColumns["5"])
	}
	if _, known, err := declared("SHOW PROPERTY UNIQUE CONSTRAINTS", false); !known || err == nil {
		t.Fatalf("Cypher 5 must reject PROPERTY UNIQUE: known=%v err=%v", known, err)
	}
	if _, known, err := declared("SHOW PROPERTY UNIQUE CONSTRAINTS", true); !known || err != nil {
		t.Fatalf("Cypher 25 lists PROPERTY UNIQUE: known=%v err=%v", known, err)
	}
}

// showConstraintsSchemaSnapshot records every constraint and index the schema
// holds, so a reader that listed or changed them is visible.
func showConstraintsSchemaSnapshot(schema *storage.SchemaManager) []string {
	if schema == nil {
		return nil
	}
	snapshot := make([]string, 0, len(schema.GetAllConstraints())+len(schema.GetIndexes()))
	for _, constraint := range schema.GetAllConstraints() {
		snapshot = append(snapshot, fmt.Sprintf("constraint %s %s %s %s %v", constraint.Name, constraint.Type,
			constraint.EffectiveEntityType(), constraint.Label, constraint.Properties))
	}
	for _, constraint := range schema.GetAllPropertyTypeConstraints() {
		snapshot = append(snapshot, fmt.Sprintf("type constraint %s %s %s %s %s", constraint.Name,
			constraint.EffectiveEntityType(), constraint.Label, constraint.Property, constraint.ExpectedType))
	}
	for _, index := range schema.GetIndexes() {
		snapshot = append(snapshot, fmt.Sprintf("index %v", index))
	}
	slices.Sort(snapshot)
	return snapshot
}
