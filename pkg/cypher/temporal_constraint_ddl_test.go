package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A temporal no-overlap constraint is declared under its own head, which names
// the validity window in one call and the grouping key after GROUP BY:
//
//	CREATE CONSTRAINT name FOR (n:Account)
//	REQUIRE temporal.epoch(n.starts_at, n.ends_at) GROUP BY n.accountId
//
// It compiles to the definition the tuple spelling already produced — the
// grouping key followed by the validity window in the stored constraint's
// properties — so every reader and every enforcement path is the existing one.
// The tuple spelling ((n.accountId, n.starts_at, n.ends_at) IS TEMPORAL NO
// OVERLAP) is deprecated, stays accepted under CREATE CONSTRAINT, and declares
// the same constraint.

// withParserTypes runs one body under each executor parser mode, as the other
// public parser-mode tests do: the native reader and the ANTLR grammar both
// admit the statements the body declares and refuse the ones it rejects.
func withParserTypes(t *testing.T, body func(t *testing.T)) {
	t.Helper()
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			require.Equal(t, parser, config.GetParserType())
			body(t)
		})
	}
}

// temporalConstraintNamed returns the stored constraint with the given name.
func temporalConstraintNamed(t *testing.T, engine storage.Engine, name string) storage.Constraint {
	t.Helper()
	for _, constraint := range engine.GetSchema().GetAllConstraints() {
		if constraint.Name == name {
			return constraint
		}
	}
	t.Fatalf("constraint %q is not in the schema", name)
	return storage.Constraint{}
}

// constraintCreateStatements lists the createStatement SHOW CONSTRAINTS
// reports for every constraint, by name.
func constraintCreateStatements(t *testing.T, exec *StorageExecutor) map[string]string {
	t.Helper()
	result, err := exec.Execute(context.Background(), "SHOW CONSTRAINTS YIELD *", nil)
	require.NoError(t, err)
	statements := make(map[string]string, len(result.Rows))
	for _, row := range result.Rows {
		values := make(map[string]interface{}, len(result.Columns))
		for i, column := range result.Columns {
			values[column] = row[i]
		}
		name, _ := values["name"].(string)
		statement, _ := values["createStatement"].(string)
		statements[name] = statement
	}
	return statements
}

func TestCreateNoOverlapConstraint(t *testing.T) {
	withParserTypes(t, testCreateNoOverlapConstraint)
}

func testCreateNoOverlapConstraint(t *testing.T) {
	ctx := context.Background()

	t.Run("node grouping key", func(t *testing.T) {
		exec, engine := newConstraintGapExecutor(t)
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT acct_no_overlap FOR (n:Account) REQUIRE temporal.epoch(n.starts_at, n.ends_at) GROUP BY n.accountId`, nil)
		require.NoError(t, err)

		constraint := temporalConstraintNamed(t, engine, "acct_no_overlap")
		require.Equal(t, storage.ConstraintTemporal, constraint.Type)
		require.Equal(t, "Account", constraint.Label)
		require.Equal(t, storage.ConstraintEntityNode, constraint.EffectiveEntityType())
		require.Equal(t, []string{"accountId", "starts_at", "ends_at"}, constraint.Properties)

		// The declared constraint is the enforced one: one window per account.
		run := func(query string) error {
			_, err := exec.Execute(ctx, query, nil)
			return err
		}
		require.NoError(t, run(`CREATE (:Account {accountId: 'a1', starts_at: datetime('2024-01-01T00:00:00Z'), ends_at: datetime('2024-02-01T00:00:00Z')})`))
		require.Error(t, run(`CREATE (:Account {accountId: 'a1', starts_at: datetime('2024-01-15T00:00:00Z'), ends_at: datetime('2024-03-01T00:00:00Z')})`),
			"an overlapping window for the same account must be rejected")
		require.NoError(t, run(`CREATE (:Account {accountId: 'a2', starts_at: datetime('2024-01-15T00:00:00Z'), ends_at: datetime('2024-03-01T00:00:00Z')})`),
			"another account is another group")
		require.NoError(t, run(`CREATE (:Account {accountId: 'a1', starts_at: datetime('2024-02-01T00:00:00Z'), ends_at: datetime('2024-03-01T00:00:00Z')})`),
			"a window starting where the previous one ends does not overlap")
	})

	t.Run("composite grouping key", func(t *testing.T) {
		exec, engine := newConstraintGapExecutor(t)
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT acct_tenant_no_overlap FOR (n:Account) REQUIRE temporal.epoch(n.starts_at, n.ends_at) GROUP BY n.tenant, n.accountId`, nil)
		require.NoError(t, err)

		constraint := temporalConstraintNamed(t, engine, "acct_tenant_no_overlap")
		require.Equal(t, storage.ConstraintTemporal, constraint.Type)
		require.Equal(t, []string{"tenant", "accountId", "starts_at", "ends_at"}, constraint.Properties)

		run := func(query string) error {
			_, err := exec.Execute(ctx, query, nil)
			return err
		}
		require.NoError(t, run(`CREATE (:Account {tenant: 't1', accountId: 'a1', starts_at: datetime('2024-01-01T00:00:00Z'), ends_at: datetime('2024-02-01T00:00:00Z')})`))
		require.Error(t, run(`CREATE (:Account {tenant: 't1', accountId: 'a1', starts_at: datetime('2024-01-15T00:00:00Z'), ends_at: datetime('2024-03-01T00:00:00Z')})`),
			"every key property matches, so the windows collide")
		require.NoError(t, run(`CREATE (:Account {tenant: 't2', accountId: 'a1', starts_at: datetime('2024-01-15T00:00:00Z'), ends_at: datetime('2024-03-01T00:00:00Z')})`),
			"a different tenant is another group")
		require.NoError(t, run(`CREATE (:Account {tenant: 't1', accountId: 'a2', starts_at: datetime('2024-01-15T00:00:00Z'), ends_at: datetime('2024-03-01T00:00:00Z')})`),
			"a different account is another group")
	})

	t.Run("relationship grouping key", func(t *testing.T) {
		exec, engine := newConstraintGapExecutor(t)
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT owns_no_overlap FOR ()-[r:OWNS]-() REQUIRE temporal.epoch(r.starts_at, r.ends_at) GROUP BY r.accountId`, nil)
		require.NoError(t, err)

		constraint := temporalConstraintNamed(t, engine, "owns_no_overlap")
		require.Equal(t, storage.ConstraintTemporal, constraint.Type)
		require.Equal(t, "OWNS", constraint.Label)
		require.Equal(t, storage.ConstraintEntityRelationship, constraint.EffectiveEntityType())
		require.Equal(t, []string{"accountId", "starts_at", "ends_at"}, constraint.Properties)

		run := func(query string) error {
			_, err := exec.Execute(ctx, query, nil)
			return err
		}
		require.NoError(t, run(`CREATE (:Owner {name: 'Alice'}), (:Company {name: 'Acme'})`))
		require.NoError(t, run(`MATCH (o:Owner {name: 'Alice'}), (c:Company {name: 'Acme'}) CREATE (o)-[:OWNS {accountId: 'a1', starts_at: datetime('2024-01-01T00:00:00Z'), ends_at: datetime('2024-02-01T00:00:00Z')}]->(c)`))
		require.Error(t, run(`MATCH (o:Owner {name: 'Alice'}), (c:Company {name: 'Acme'}) CREATE (o)-[:OWNS {accountId: 'a1', starts_at: datetime('2024-01-15T00:00:00Z'), ends_at: datetime('2024-03-01T00:00:00Z')}]->(c)`),
			"an overlapping ownership window for the same account must be rejected")
		require.NoError(t, run(`MATCH (o:Owner {name: 'Alice'}), (c:Company {name: 'Acme'}) CREATE (o)-[:OWNS {accountId: 'a2', starts_at: datetime('2024-01-15T00:00:00Z'), ends_at: datetime('2024-03-01T00:00:00Z')}]->(c)`),
			"another account is another group")
	})

	t.Run("IF NOT EXISTS and OPTIONS", func(t *testing.T) {
		exec, engine := newConstraintGapExecutor(t)
		ddl := `CREATE CONSTRAINT acct_no_overlap IF NOT EXISTS FOR (n:Account) REQUIRE temporal.epoch(n.starts_at, n.ends_at) GROUP BY n.accountId OPTIONS {}`
		_, err := exec.Execute(ctx, ddl, nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, ddl, nil)
		require.NoError(t, err, "a repeated IF NOT EXISTS declaration is admitted")

		constraints := engine.GetSchema().GetAllConstraints()
		require.Len(t, constraints, 1)
		require.Equal(t, "acct_no_overlap", constraints[0].Name)
		require.Equal(t, []string{"accountId", "starts_at", "ends_at"}, constraints[0].Properties)
	})

	t.Run("window and grouping key must name the FOR variable", func(t *testing.T) {
		for name, ddl := range map[string]string{
			"grouping key": `CREATE CONSTRAINT misbound FOR (n:Account) REQUIRE temporal.epoch(n.starts_at, n.ends_at) GROUP BY other.accountId`,
			"window":       `CREATE CONSTRAINT misbound FOR (n:Account) REQUIRE temporal.epoch(other.starts_at, other.ends_at) GROUP BY n.accountId`,
		} {
			exec, engine := newConstraintGapExecutor(t)
			_, err := exec.Execute(ctx, ddl, nil)
			require.Error(t, err, name)
			require.Empty(t, engine.GetSchema().GetAllConstraints(), name)
		}
	})

	t.Run("no grouping key is refused", func(t *testing.T) {
		exec, engine := newConstraintGapExecutor(t)
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT no_key FOR (n:Account) REQUIRE temporal.epoch(n.starts_at, n.ends_at)`, nil)
		require.ErrorContains(t, err, "at least 3 properties")
		require.Empty(t, engine.GetSchema().GetAllConstraints())
	})
}

// Reject unshipped spellings and malformed epoch declarations before schema effects.
func TestCreateNoOverlapConstraintRejectsOtherRequirements(t *testing.T) {
	withParserTypes(t, func(t *testing.T) {
		ctx := context.Background()
		for name, ddl := range map[string]string{
			"unshipped head":                `CREATE NO OVERLAP CONSTRAINT refused FOR (n:Account) REQUIRE temporal.epoch(n.starts_at,n.ends_at) GROUP BY n.accountId`,
			"unshipped interval":            `CREATE CONSTRAINT refused FOR (n:Account) REQUIRE temporal.interval(n.starts_at,n.ends_at) GROUP BY n.accountId`,
			"redundant suffix":              `CREATE CONSTRAINT refused FOR (n:Account) REQUIRE temporal.epoch(n.starts_at,n.ends_at) IS NO OVERLAP GROUP BY n.accountId`,
			"window names another variable": `CREATE CONSTRAINT refused FOR (n:Account) REQUIRE temporal.epoch(other.starts_at,n.ends_at) GROUP BY n.accountId`,
			"key names another variable":    `CREATE CONSTRAINT refused FOR (n:Account) REQUIRE temporal.epoch(n.starts_at,n.ends_at) GROUP BY other.accountId`,
		} {
			t.Run(name, func(t *testing.T) {
				exec, engine := newConstraintGapExecutor(t)
				_, err := exec.Execute(ctx, ddl, nil)
				require.Error(t, err, ddl)
				require.Empty(t, engine.GetSchema().GetAllConstraints(),
					"a refused declaration registers nothing")
			})
		}
	})
}

// Both spellings of the requirement declare one constraint: same type, entity,
// label, properties and default name, the same enforcement, and the same
// createStatement from SHOW CONSTRAINTS.
func TestTemporalConstraintSpellingsCompileToOneDefinition(t *testing.T) {
	withParserTypes(t, testTemporalConstraintSpellingsCompileToOneDefinition)
}

func testTemporalConstraintSpellingsCompileToOneDefinition(t *testing.T) {
	ctx := context.Background()
	const name = "acct_tenant_no_overlap"
	intervalDDL := `CREATE CONSTRAINT acct_tenant_no_overlap FOR (n:Account) REQUIRE temporal.epoch(n.starts_at, n.ends_at) GROUP BY n.tenant, n.accountId`
	tupleDDL := `CREATE CONSTRAINT acct_tenant_no_overlap FOR (n:Account) REQUIRE (n.tenant, n.accountId, n.starts_at, n.ends_at) IS TEMPORAL NO OVERLAP`

	intervalExec, intervalEngine := newConstraintGapExecutor(t)
	_, err := intervalExec.Execute(ctx, intervalDDL, nil)
	require.NoError(t, err)
	tupleExec, tupleEngine := newConstraintGapExecutor(t)
	_, err = tupleExec.Execute(ctx, tupleDDL, nil)
	require.NoError(t, err)

	intervalConstraint := temporalConstraintNamed(t, intervalEngine, name)
	tupleConstraint := temporalConstraintNamed(t, tupleEngine, name)
	require.Equal(t, tupleConstraint.Type, intervalConstraint.Type)
	require.Equal(t, tupleConstraint.EffectiveEntityType(), intervalConstraint.EffectiveEntityType())
	require.Equal(t, tupleConstraint.Label, intervalConstraint.Label)
	require.Equal(t, tupleConstraint.Properties, intervalConstraint.Properties)
	require.Equal(t, []string{"tenant", "accountId", "starts_at", "ends_at"}, intervalConstraint.Properties)

	// The unnamed declaration names both constraints the same way.
	unnamed := `CREATE CONSTRAINT FOR (n:Account) REQUIRE temporal.epoch(n.starts_at, n.ends_at) GROUP BY n.tenant, n.accountId`
	unnamedTuple := `CREATE CONSTRAINT FOR (n:Account) REQUIRE (n.tenant, n.accountId, n.starts_at, n.ends_at) IS TEMPORAL NO OVERLAP`
	unnamedIntervalExec, unnamedIntervalEngine := newConstraintGapExecutor(t)
	_, err = unnamedIntervalExec.Execute(ctx, unnamed, nil)
	require.NoError(t, err)
	unnamedTupleExec, unnamedTupleEngine := newConstraintGapExecutor(t)
	_, err = unnamedTupleExec.Execute(ctx, unnamedTuple, nil)
	require.NoError(t, err)
	intervalConstraints := unnamedIntervalEngine.GetSchema().GetAllConstraints()
	tupleConstraints := unnamedTupleEngine.GetSchema().GetAllConstraints()
	require.Len(t, intervalConstraints, 1)
	require.Len(t, tupleConstraints, 1)
	require.Equal(t, tupleConstraints[0].Name, intervalConstraints[0].Name)
	require.NotEmpty(t, intervalConstraints[0].Name)

	// One declaration is what SHOW CONSTRAINTS reports, and it re-creates the
	// constraint it describes.
	intervalStatement := constraintCreateStatements(t, intervalExec)[name]
	require.Equal(t,
		"CREATE CONSTRAINT `acct_tenant_no_overlap` FOR (n:`Account`) REQUIRE temporal.epoch(n.`starts_at`, n.`ends_at`) GROUP BY n.`tenant`, n.`accountId`",
		intervalStatement)
	require.Equal(t, intervalStatement, constraintCreateStatements(t, tupleExec)[name])

	recreated, recreatedEngine := newConstraintGapExecutor(t)
	_, err = recreated.Execute(ctx, intervalStatement, nil)
	require.NoError(t, err)
	require.Equal(t, intervalConstraint, temporalConstraintNamed(t, recreatedEngine, name))

	// Both definitions enforce the same windows.
	for _, exec := range []*StorageExecutor{intervalExec, tupleExec} {
		run := func(query string) error {
			_, err := exec.Execute(ctx, query, nil)
			return err
		}
		require.NoError(t, run(`CREATE (:Account {tenant: 't1', accountId: 'a1', starts_at: datetime('2024-01-01T00:00:00Z'), ends_at: datetime('2024-02-01T00:00:00Z')})`))
		require.Error(t, run(`CREATE (:Account {tenant: 't1', accountId: 'a1', starts_at: datetime('2024-01-15T00:00:00Z'), ends_at: datetime('2024-03-01T00:00:00Z')})`))
		require.NoError(t, run(`CREATE (:Account {tenant: 't1', accountId: 'a2', starts_at: datetime('2024-01-15T00:00:00Z'), ends_at: datetime('2024-03-01T00:00:00Z')})`))
	}
}

// The tuple spelling is the deprecated form of the definition the interval
// spelling states, and it stays usable: a definition stored under it is
// enforced as it always was, and SHOW CONSTRAINTS writes it back in the current
// spelling.
func TestDeprecatedTemporalTupleSpellingStaysAccepted(t *testing.T) {
	withParserTypes(t, testDeprecatedTemporalTupleSpellingStaysAccepted)
}

func testDeprecatedTemporalTupleSpellingStaysAccepted(t *testing.T) {
	ctx := context.Background()
	exec, engine := newConstraintGapExecutor(t)
	_, err := exec.Execute(ctx, `CREATE CONSTRAINT legacy_no_overlap FOR ()-[r:OWNS]-() REQUIRE (r.accountId, r.starts_at, r.ends_at) IS TEMPORAL NO OVERLAP`, nil)
	require.NoError(t, err)

	constraint := temporalConstraintNamed(t, engine, "legacy_no_overlap")
	require.Equal(t, storage.ConstraintTemporal, constraint.Type)
	require.Equal(t, []string{"accountId", "starts_at", "ends_at"}, constraint.Properties)
	require.Equal(t,
		"CREATE CONSTRAINT `legacy_no_overlap` FOR ()-[r:`OWNS`]-() REQUIRE temporal.epoch(r.`starts_at`, r.`ends_at`) GROUP BY r.`accountId`",
		constraintCreateStatements(t, exec)["legacy_no_overlap"])

	run := func(query string) error {
		_, err := exec.Execute(ctx, query, nil)
		return err
	}
	require.NoError(t, run(`CREATE (:Owner {name: 'Alice'}), (:Company {name: 'Acme'})`))
	require.NoError(t, run(`MATCH (o:Owner {name: 'Alice'}), (c:Company {name: 'Acme'}) CREATE (o)-[:OWNS {accountId: 'a1', starts_at: datetime('2024-01-01T00:00:00Z'), ends_at: datetime('2024-02-01T00:00:00Z')}]->(c)`))
	require.Error(t, run(`MATCH (o:Owner {name: 'Alice'}), (c:Company {name: 'Acme'}) CREATE (o)-[:OWNS {accountId: 'a1', starts_at: datetime('2024-01-15T00:00:00Z'), ends_at: datetime('2024-03-01T00:00:00Z')}]->(c)`))
}
