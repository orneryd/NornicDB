package cypher

import (
	"context"
	"fmt"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A property type constraint reads its type as a type predicate does and
// stores it as Neo4j normalizes it. Normalized types and rejections were
// recorded on Neo4j 5.26.30 and 2026.09 (their Community Edition reports the
// normalized type before refusing the Enterprise constraint).
func TestPropertyTypeConstraintTypes(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "property_type_constraint_types"))
	ctx := context.Background()
	normalized := []struct{ written, stored string }{
		{"INTEGER | INTEGER", "INTEGER"},
		{"FLOAT | INTEGER | STRING", "STRING | INTEGER | FLOAT"},
		{"ANY<INTEGER | STRING>", "STRING | INTEGER"},
		{"LOCAL TIME", "LOCAL TIME"},
		{"ZONED TIME", "ZONED TIME"},
		{"TIME WITH TIME ZONE", "ZONED TIME"},
		{"TIME WITHOUT TIME ZONE", "LOCAL TIME"},
		{"TIMESTAMP WITH TIME ZONE", "ZONED DATETIME"},
		{"DURATION", "DURATION"},
		{"POINT", "POINT"},
		{"VARCHAR", "STRING"},
		{"SIGNED INTEGER", "INTEGER"},
		{"BOOL", "BOOLEAN"},
		{"ARRAY<DATE NOT NULL>", "LIST<DATE NOT NULL>"},
		{"LIST<POINT NOT NULL> | LIST<DURATION NOT NULL> | DATE", "DATE | LIST<DURATION NOT NULL> | LIST<POINT NOT NULL>"},
		{"LIST<INTEGER NOT NULL> | STRING", "STRING | LIST<INTEGER NOT NULL>"},
		// NornicDB's kept spellings.
		{"DATETIME", "ZONED DATETIME"},
		{"LOCALDATETIME | ZONEDDATETIME", "LOCAL DATETIME | ZONED DATETIME"},
		{"LIST<STRING>", "LIST<STRING NOT NULL>"},
	}
	for index, tc := range normalized {
		label := fmt.Sprintf("N%d", index)
		_, err := exec.Execute(ctx, fmt.Sprintf("CREATE CONSTRAINT n%d FOR (n:%s) REQUIRE n.p IS :: %s", index, label, tc.written), nil)
		require.NoError(t, err, tc.written)
		result, err := exec.Execute(ctx, fmt.Sprintf("SHOW CONSTRAINTS YIELD name, propertyType WHERE name = 'n%d' RETURN propertyType", index), nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{tc.stored}}, result.Rows, tc.written)
	}
	for index, written := range []string{
		"STRING NOT NULL", "LIST<LIST<STRING NOT NULL> NOT NULL>", "LIST<INTEGER | STRING>", "LIST<ANY<INTEGER | STRING> NOT NULL>",
		"ANY<INTEGER | FLOAT> NOT NULL", "NODE", "MAP", "ANY", "NULL", "NOTHING", "PROPERTY VALUE",
		"LIST<STRING NOT NULL> NOT NULL", "INTEGER NOT NULL | FLOAT",
	} {
		_, err := exec.Execute(ctx, fmt.Sprintf("CREATE CONSTRAINT r%d FOR (n:R%d) REQUIRE n.p IS :: %s", index, index, written), nil)
		require.Error(t, err, written)
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, written)
	}

	// Values: a union takes any member's values; the new types take theirs.
	for _, statement := range []string{
		"CREATE CONSTRAINT u FOR (n:U) REQUIRE n.p IS :: INTEGER | STRING",
		"CREATE CONSTRAINT lt FOR (n:LT) REQUIRE n.p IS :: LOCAL TIME",
		"CREATE CONSTRAINT zt FOR (n:ZT) REQUIRE n.p IS :: ZONED TIME",
		"CREATE CONSTRAINT du FOR (n:DU) REQUIRE n.p IS :: DURATION",
		"CREATE CONSTRAINT pt FOR (n:PT) REQUIRE n.p IS :: POINT",
		"CREATE CONSTRAINT ul FOR (n:UL) REQUIRE n.p IS :: DATE | LIST<DURATION NOT NULL>",
	} {
		_, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}
	for _, accepted := range []string{
		"CREATE (:U {p: 1})", "CREATE (:U {p: 'x'})", "CREATE (:U)",
		"CREATE (:LT {p: localtime('12:00')})", "CREATE (:ZT {p: time('12:00+01:00')})",
		"CREATE (:DU {p: duration('P1D')})", "CREATE (:PT {p: point({x: 1, y: 2})})",
		"CREATE (:UL {p: date('2020-01-01')})", "CREATE (:UL {p: [duration('P1D')]})",
	} {
		_, err := exec.Execute(ctx, accepted, nil)
		require.NoError(t, err, accepted)
	}
	for _, rejected := range []string{
		"CREATE (:U {p: 1.5})", "CREATE (:U {p: true})",
		"CREATE (:LT {p: time('12:00+01:00')})", "CREATE (:ZT {p: localtime('12:00')})",
		"CREATE (:DU {p: 'P1D'})", "CREATE (:PT {p: [1, 2]})",
		"CREATE (:UL {p: [date('2020-01-01')]})", "CREATE (:UL {p: duration('P1D')})",
	} {
		_, err := exec.Execute(ctx, rejected, nil)
		require.Error(t, err, rejected)
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Schema.ConstraintValidationFailed", code, rejected)
	}
	// Existing values are checked when the constraint is created.
	_, err := exec.Execute(ctx, "CREATE (:E {p: 1.5})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE CONSTRAINT e FOR (n:E) REQUIRE n.p IS :: INTEGER | STRING", nil)
	require.Error(t, err)
	_, err = exec.Execute(ctx, "CREATE CONSTRAINT e FOR (n:E) REQUIRE n.p IS :: INTEGER | FLOAT", nil)
	require.NoError(t, err)
}
