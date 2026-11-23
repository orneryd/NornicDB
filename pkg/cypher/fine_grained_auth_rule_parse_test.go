package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// authRuleParseBool is the *bool of an expected SET ENABLED value.
func authRuleParseBool(value bool) *bool {
	return &value
}

// requireFineGrainedAuthRule parses statement as an authorization-rule command
// and compares the whole product.
func requireFineGrainedAuthRule(t *testing.T, statement string, want fineGrainedAuthRuleCommand) {
	t.Helper()
	command, ok, err := parseFineGrainedAuthRuleCommand(t.Context(), statement)
	require.True(t, ok, statement)
	require.NoError(t, err, statement)
	require.Equal(t, want, command, statement)
}

func TestFineGrainedAuthRuleParseCreate(t *testing.T) {
	for name, test := range map[string]struct {
		statement string
		want      fineGrainedAuthRuleCommand
	}{
		"condition only": {
			statement: "CREATE AUTH RULE salesRule SET CONDITION abac.oidc.user_attribute('department') = 'sales'",
			want: fineGrainedAuthRuleCommand{
				Verb: fineGrainedAuthRuleCreateVerb, Name: "salesRule",
				Condition: "abac.oidc.user_attribute('department') = 'sales'",
			},
		},
		"condition with a top level AND": {
			statement: "CREATE AUTH RULE engineeringUKRule SET CONDITION abac.oidc.user_attribute('department') = 'engineering' AND abac.oidc.user_attribute('location') = 'UK'",
			want: fineGrainedAuthRuleCommand{
				Verb: fineGrainedAuthRuleCreateVerb, Name: "engineeringUKRule",
				Condition: "abac.oidc.user_attribute('department') = 'engineering' AND abac.oidc.user_attribute('location') = 'UK'",
			},
		},
		"condition and enabled": {
			statement: "CREATE AUTH RULE engineeringUKRule SET CONDITION abac.oidc.user_attribute('department') = 'engineering' AND abac.oidc.user_attribute('location') = 'UK' SET ENABLED true",
			want: fineGrainedAuthRuleCommand{
				Verb: fineGrainedAuthRuleCreateVerb, Name: "engineeringUKRule",
				Condition: "abac.oidc.user_attribute('department') = 'engineering' AND abac.oidc.user_attribute('location') = 'UK'",
				Enabled:   authRuleParseBool(true),
			},
		},
		"or replace": {
			statement: "CREATE OR REPLACE AUTH RULE salesRule SET CONDITION true SET ENABLED true",
			want: fineGrainedAuthRuleCommand{
				Verb: fineGrainedAuthRuleCreateVerb, Name: "salesRule", Replace: true,
				Condition: "true", Enabled: authRuleParseBool(true),
			},
		},
		"if not exists": {
			statement: "CREATE AUTH RULE temporary_reader IF NOT EXISTS SET CONDITION abac.oidc.user_attribute('jobTitle') = 'developer' SET ENABLED false",
			want: fineGrainedAuthRuleCommand{
				Verb: fineGrainedAuthRuleCreateVerb, Name: "temporary_reader", IfNotExists: true,
				Condition: "abac.oidc.user_attribute('jobTitle') = 'developer'",
				Enabled:   authRuleParseBool(false),
			},
		},
		"nested WHERE over a list": {
			statement: "CREATE AUTH RULE ruleset_countries SET CONDITION any(country IN abac.oidc.user_attribute('citizenshipCountries') WHERE country IN ['c1', 'c5']) SET ENABLED true",
			want: fineGrainedAuthRuleCommand{
				Verb: fineGrainedAuthRuleCreateVerb, Name: "ruleset_countries",
				Condition: "any(country IN abac.oidc.user_attribute('citizenshipCountries') WHERE country IN ['c1', 'c5'])",
				Enabled:   authRuleParseBool(true),
			},
		},
		"line break and indentation are kept": {
			statement: "CREATE AUTH RULE onCallRule\nSET CONDITION 'on-call' IN abac.native.user_tags()\n   AND (time.transaction('UTC').hour < 9 OR time.transaction('UTC').hour >= 17) SET ENABLED true",
			want: fineGrainedAuthRuleCommand{
				Verb: fineGrainedAuthRuleCreateVerb, Name: "onCallRule",
				Condition: "'on-call' IN abac.native.user_tags()\n   AND (time.transaction('UTC').hour < 9 OR time.transaction('UTC').hour >= 17)",
				Enabled:   authRuleParseBool(true),
			},
		},
		"nested SET WHERE and AND in quoted text": {
			statement: "CREATE AUTH RULE quoted SET CONDITION n.title = 'SET ENABLED true AND n.x IS NOT NULL WHERE 1 = 1' SET ENABLED false",
			want: fineGrainedAuthRuleCommand{
				Verb: fineGrainedAuthRuleCreateVerb, Name: "quoted",
				Condition: "n.title = 'SET ENABLED true AND n.x IS NOT NULL WHERE 1 = 1'",
				Enabled:   authRuleParseBool(false),
			},
		},
		"nested group holding SET WHERE and AND": {
			statement: "CREATE AUTH RULE nested SET CONDITION size([x IN range(1, 10) WHERE x.set > 2 AND x IN collect({set: 1})]) > 0 SET ENABLED true",
			want: fineGrainedAuthRuleCommand{
				Verb: fineGrainedAuthRuleCreateVerb, Name: "nested",
				Condition: "size([x IN range(1, 10) WHERE x.set > 2 AND x IN collect({set: 1})]) > 0",
				Enabled:   authRuleParseBool(true),
			},
		},
		"quoted rule name": {
			statement: "CREATE AUTH RULE `sales rule` SET CONDITION true",
			want: fineGrainedAuthRuleCommand{
				Verb: fineGrainedAuthRuleCreateVerb, Name: "sales rule", Condition: "true",
			},
		},
		"lower case words and terminator": {
			statement: "create auth rule salesrule set condition 1 = 1 set enabled true;",
			want: fineGrainedAuthRuleCommand{
				Verb: fineGrainedAuthRuleCreateVerb, Name: "salesrule",
				Condition: "1 = 1", Enabled: authRuleParseBool(true),
			},
		},
		"comment before the set clause": {
			statement: "CREATE AUTH RULE r // SET ENABLED true\nSET CONDITION 1 = 1",
			want: fineGrainedAuthRuleCommand{
				Verb: fineGrainedAuthRuleCreateVerb, Name: "r", Condition: "1 = 1",
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			requireFineGrainedAuthRule(t, test.statement, test.want)
		})
	}
}

func TestFineGrainedAuthRuleParseAlter(t *testing.T) {
	for name, test := range map[string]struct {
		statement string
		want      fineGrainedAuthRuleCommand
	}{
		"condition": {
			statement: "ALTER AUTH RULE ruleset_countries SET CONDITION any(country IN abac.oidc.user_attribute('citizenshipCountries') WHERE country IN ['c1', 'c5', 'c6'])",
			want: fineGrainedAuthRuleCommand{
				Verb: fineGrainedAuthRuleAlterVerb, Name: "ruleset_countries",
				Condition: "any(country IN abac.oidc.user_attribute('citizenshipCountries') WHERE country IN ['c1', 'c5', 'c6'])",
			},
		},
		"enabled only": {
			statement: "ALTER AUTH RULE ruleset_countries SET ENABLED false",
			want: fineGrainedAuthRuleCommand{
				Verb: fineGrainedAuthRuleAlterVerb, Name: "ruleset_countries",
				Enabled: authRuleParseBool(false),
			},
		},
		"condition and enabled with if exists": {
			statement: "ALTER AUTH RULES `ruleset countries` IF EXISTS SET CONDITION n.set = 1 SET ENABLED true;",
			want: fineGrainedAuthRuleCommand{
				Verb: fineGrainedAuthRuleAlterVerb, Name: "ruleset countries", IfExists: true,
				Condition: "n.set = 1", Enabled: authRuleParseBool(true),
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			requireFineGrainedAuthRule(t, test.statement, test.want)
		})
	}
}

func TestFineGrainedAuthRuleParseRename(t *testing.T) {
	for name, test := range map[string]struct {
		statement string
		want      fineGrainedAuthRuleCommand
	}{
		"rename": {
			statement: "RENAME AUTH RULE ruleset_countries TO disabled_ruleset_countries",
			want: fineGrainedAuthRuleCommand{
				Verb: fineGrainedAuthRuleRenameVerb, Name: "ruleset_countries",
				NewName: "disabled_ruleset_countries",
			},
		},
		"if exists with quoted names": {
			statement: "RENAME AUTH RULES `old name` IF EXISTS TO `new name`;",
			want: fineGrainedAuthRuleCommand{
				Verb: fineGrainedAuthRuleRenameVerb, Name: "old name", IfExists: true,
				NewName: "new name",
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			requireFineGrainedAuthRule(t, test.statement, test.want)
		})
	}
}

func TestFineGrainedAuthRuleParseDrop(t *testing.T) {
	for name, test := range map[string]struct {
		statement string
		want      fineGrainedAuthRuleCommand
	}{
		"drop": {
			statement: "DROP AUTH RULE temporary_reader",
			want:      fineGrainedAuthRuleCommand{Verb: fineGrainedAuthRuleDropVerb, Name: "temporary_reader"},
		},
		"if exists": {
			statement: "DROP AUTH RULES `temporary reader` IF EXISTS;",
			want: fineGrainedAuthRuleCommand{
				Verb: fineGrainedAuthRuleDropVerb, Name: "temporary reader", IfExists: true,
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			requireFineGrainedAuthRule(t, test.statement, test.want)
		})
	}
}

func TestFineGrainedAuthRuleParseRoleAssignment(t *testing.T) {
	for name, test := range map[string]struct {
		statement string
		want      fineGrainedAuthRuleCommand
	}{
		"grant one role to one rule": {
			statement: "GRANT ROLE salesTeam TO AUTH RULE salesRule",
			want: fineGrainedAuthRuleCommand{
				Verb:  fineGrainedAuthRuleGrantVerb,
				Roles: []string{"salesTeam"}, Names: []string{"salesRule"},
			},
		},
		"grant roles to rules": {
			statement: "GRANT ROLES reader, `finance audit` TO AUTH RULES financeApproverRule, `on call rule`;",
			want: fineGrainedAuthRuleCommand{
				Verb:  fineGrainedAuthRuleGrantVerb,
				Roles: []string{"reader", "finance audit"},
				Names: []string{"financeApproverRule", "on call rule"},
			},
		},
		"revoke one role from one rule": {
			statement: "REVOKE ROLE reader FROM AUTH RULE temporary_reader",
			want: fineGrainedAuthRuleCommand{
				Verb:  fineGrainedAuthRuleRevokeVerb,
				Roles: []string{"reader"}, Names: []string{"temporary_reader"},
			},
		},
		"revoke roles from rules": {
			statement: "revoke roles a, b from auth rules x, y",
			want: fineGrainedAuthRuleCommand{
				Verb:  fineGrainedAuthRuleRevokeVerb,
				Roles: []string{"a", "b"}, Names: []string{"x", "y"},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			requireFineGrainedAuthRule(t, test.statement, test.want)
		})
	}
}

func TestFineGrainedAuthRuleParseShow(t *testing.T) {
	for name, test := range map[string]struct {
		statement string
		want      fineGrainedAuthRuleCommand
	}{
		"show auth rules": {
			statement: "SHOW AUTH RULES",
			want:      fineGrainedAuthRuleCommand{Verb: fineGrainedAuthRuleShowVerb},
		},
		"show auth rule as command": {
			statement: "SHOW AUTH RULE AS COMMAND",
			want:      fineGrainedAuthRuleCommand{Verb: fineGrainedAuthRuleShowVerb, AsCommands: true},
		},
		"show auth rules as commands with terminator": {
			statement: "SHOW AUTH RULES AS COMMANDS;",
			want:      fineGrainedAuthRuleCommand{Verb: fineGrainedAuthRuleShowVerb, AsCommands: true},
		},
		"show roles with auth rules": {
			statement: "SHOW ROLES WITH AUTH RULES",
			want:      fineGrainedAuthRuleCommand{Verb: fineGrainedAuthRuleShowVerb, WithRoles: true},
		},
		"show all roles with auth rules as commands": {
			statement: "SHOW ALL ROLES WITH AUTH RULES AS COMMANDS",
			want: fineGrainedAuthRuleCommand{
				Verb: fineGrainedAuthRuleShowVerb, WithRoles: true, AsCommands: true,
			},
		},
		"show populated roles with auth rules": {
			statement: "SHOW POPULATED ROLES WITH AUTH RULE",
			want:      fineGrainedAuthRuleCommand{Verb: fineGrainedAuthRuleShowVerb, WithRoles: true},
		},
	} {
		t.Run(name, func(t *testing.T) {
			requireFineGrainedAuthRule(t, test.statement, test.want)
		})
	}
}

// A SHOW statement's tail belongs to the SHOW command, so this reader reads the
// head and the AS COMMANDS marker and leaves the rest of the statement alone.
func TestFineGrainedAuthRuleParseLeavesShowTail(t *testing.T) {
	for name, test := range map[string]struct {
		statement string
		want      fineGrainedAuthRuleCommand
	}{
		"yield": {
			statement: "SHOW AUTH RULES YIELD name, roles",
			want:      fineGrainedAuthRuleCommand{Verb: fineGrainedAuthRuleShowVerb},
		},
		"yield where order by and limit": {
			statement: "SHOW AUTH RULES YIELD name, enabled WHERE enabled = true ORDER BY name LIMIT 10",
			want:      fineGrainedAuthRuleCommand{Verb: fineGrainedAuthRuleShowVerb},
		},
		"as commands then yield": {
			statement: "SHOW ROLES WITH AUTH RULES AS COMMANDS YIELD name",
			want: fineGrainedAuthRuleCommand{
				Verb: fineGrainedAuthRuleShowVerb, WithRoles: true, AsCommands: true,
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			requireFineGrainedAuthRule(t, test.statement, test.want)
		})
	}
}

func TestFineGrainedAuthRuleParseSyntaxErrors(t *testing.T) {
	for name, test := range map[string]struct {
		statement string
		detail    string
	}{
		"create without a rule name": {
			statement: "CREATE AUTH RULE",
			detail:    "expected an auth rule name",
		},
		"create without a set clause": {
			statement: "CREATE AUTH RULE r",
			detail:    "expected SET CONDITION",
		},
		"create with enabled before condition": {
			statement: "CREATE AUTH RULE r SET ENABLED true",
			detail:    "expected SET CONDITION",
		},
		"create with an empty condition": {
			statement: "CREATE AUTH RULE r SET CONDITION",
			detail:    "expected a condition expression",
		},
		"create or replace with an empty condition": {
			statement: "CREATE OR REPLACE AUTH RULE r SET CONDITION   ",
			detail:    "expected a condition expression",
		},
		"create with a missing enabled value": {
			statement: "CREATE AUTH RULE r SET CONDITION 1 = 1 SET ENABLED",
			detail:    "expected true or false after SET ENABLED",
		},
		"create with a non boolean enabled value": {
			statement: "CREATE AUTH RULE r SET CONDITION 1 = 1 SET ENABLED 1",
			detail:    "expected true or false after SET ENABLED",
		},
		"create with two conditions": {
			statement: "CREATE AUTH RULE r SET CONDITION 1 = 1 SET CONDITION 2 = 2",
			detail:    "unexpected text after the auth rule command",
		},
		"drop with text after the statement": {
			statement: "DROP AUTH RULE r extra",
			detail:    "unexpected text after the auth rule command",
		},
		"alter without a set clause": {
			statement: "ALTER AUTH RULE r",
			detail:    "expected SET CONDITION or SET ENABLED",
		},
		"alter with a non boolean enabled value": {
			statement: "ALTER AUTH RULE r IF EXISTS SET ENABLED maybe",
			detail:    "expected true or false after SET ENABLED",
		},
		"alter with an empty condition": {
			statement: "ALTER AUTH RULE r SET CONDITION",
			detail:    "expected a condition expression",
		},
		"rename without TO": {
			statement: "RENAME AUTH RULE r",
			detail:    "expected TO followed by a new auth rule name",
		},
		"rename without a new name": {
			statement: "RENAME AUTH RULE r TO",
			detail:    "expected a new auth rule name",
		},
		"rename with text after the new name": {
			statement: "RENAME AUTH RULE r TO s extra",
			detail:    "unexpected text after the auth rule command",
		},
		"drop without a rule name": {
			statement: "DROP AUTH RULE",
			detail:    "expected an auth rule name",
		},
		"drop with an unread IF EXISTS": {
			statement: "DROP AUTH RULE r IF EXIST",
			detail:    "unexpected text after the auth rule command",
		},
		"grant without a rule name": {
			statement: "GRANT ROLE reader TO AUTH RULE",
			detail:    "expected an auth rule name",
		},
		"grant with a trailing comma": {
			statement: "GRANT ROLE reader TO AUTH RULES x,",
			detail:    "expected an auth rule name",
		},
		"grant with a rule wildcard": {
			statement: "GRANT ROLE reader TO AUTH RULE *",
			detail:    "expected an auth rule name",
		},
		"grant with text after the rules": {
			statement: "GRANT ROLE reader TO AUTH RULE x extra",
			detail:    "unexpected text after the auth rules",
		},
		"revoke without a rule name": {
			statement: "REVOKE ROLE reader FROM AUTH RULE",
			detail:    "expected an auth rule name",
		},
		"revoke with TO instead of FROM": {
			statement: "REVOKE ROLE reader TO AUTH RULE x",
		},
	} {
		t.Run(name, func(t *testing.T) {
			command, ok, err := parseFineGrainedAuthRuleCommand(t.Context(), test.statement)
			if test.detail == "" {
				// REVOKE … TO … is not an authorization-rule statement: the role
				// list is followed by the keyword this verb does not take, so the
				// statement keeps its own reader.
				require.False(t, ok, test.statement)
				require.NoError(t, err, test.statement)
				require.Equal(t, fineGrainedAuthRuleCommand{}, command, test.statement)
				return
			}
			require.True(t, ok, test.statement)
			require.ErrorContains(t, err, test.detail, test.statement)
			var semantic *SemanticError
			require.ErrorAs(t, err, &semantic, test.statement)
			require.Equal(t, "Neo.ClientError.Statement.SyntaxError", semantic.Code, test.statement)
		})
	}
}

// The role assignment of an account and a role, the privilege statements and
// every other Cypher statement belong to their own readers: this parser must not
// claim them.
func TestFineGrainedAuthRuleParseLeavesOtherCommands(t *testing.T) {
	for _, statement := range []string{
		"GRANT ROLE admin TO bob",
		"GRANT ROLES a, b TO bob",
		"GRANT ROLE admin TO auth",
		"REVOKE ROLE admin FROM bob",
		"REVOKE ROLES a, b FROM bob",
		"GRANT ROLE",
		"GRANT TRAVERSE ON GRAPH * TO r",
		"GRANT ACCESS ON DATABASE neo4j TO r",
		"GRANT EXECUTE PROCEDURE db.x ON DBMS TO r",
		"CREATE ROLE security",
		"CREATE OR REPLACE DATABASE neo4j",
		"CREATE (n:Person {name: 'a'})",
		"ALTER USER bob SET PASSWORD 'secret'",
		"DROP INDEX idx",
		"RENAME USER a TO b",
		"SHOW ROLES",
		"SHOW ALL ROLES",
		"SHOW ROLES WITH USERS",
		"SHOW PRIVILEGES",
		"SHOW ROLE admin PRIVILEGES",
		"SHOW AUTH",
		"MATCH (n) RETURN n",
		"",
		"   ",
	} {
		command, ok, err := parseFineGrainedAuthRuleCommand(t.Context(), statement)
		require.NoError(t, err, statement)
		require.False(t, ok, statement)
		require.Equal(t, fineGrainedAuthRuleCommand{}, command, statement)
	}
}

// The authorization-rule reader and the fine-grained privilege reader own
// disjoint statements in both directions.
func TestFineGrainedAuthRuleParseDoesNotCollideWithPrivileges(t *testing.T) {
	ctx := t.Context()
	authRules := []string{
		"CREATE AUTH RULE r SET CONDITION true SET ENABLED true",
		"CREATE OR REPLACE AUTH RULE r SET CONDITION true",
		"ALTER AUTH RULE r SET ENABLED false",
		"RENAME AUTH RULE a TO b",
		"DROP AUTH RULE r",
		"GRANT ROLE reader TO AUTH RULE r",
		"REVOKE ROLE reader FROM AUTH RULE r",
		"SHOW AUTH RULES AS COMMANDS",
		"SHOW ALL ROLES WITH AUTH RULES",
	}
	for _, statement := range authRules {
		_, ok, err := parseFineGrainedAuthRuleCommand(ctx, statement)
		require.True(t, ok, statement)
		require.NoError(t, err, statement)
		_, claimed, claimErr := parseFineGrainedPrivilegeCommand(ctx, statement)
		require.NoError(t, claimErr, statement)
		require.False(t, claimed, statement)
	}
	privileges := []string{
		"GRANT TRAVERSE ON GRAPH * NODES Post TO r",
		"DENY MATCH {*} ON GRAPH * TO r",
		"REVOKE GRANT READ {*} ON GRAPH neo4j FROM r",
		"GRANT ACCESS ON DATABASE neo4j TO r",
		"GRANT CREATE ROLE ON DBMS TO r",
		"GRANT ALL DBMS PRIVILEGES ON DBMS TO r",
	}
	for _, statement := range privileges {
		_, claimed, claimErr := parseFineGrainedPrivilegeCommand(ctx, statement)
		require.True(t, claimed, statement)
		require.NoError(t, claimErr, statement)
		command, ok, err := parseFineGrainedAuthRuleCommand(ctx, statement)
		require.NoError(t, err, statement)
		require.False(t, ok, statement)
		require.Equal(t, fineGrainedAuthRuleCommand{}, command, statement)
	}
}

func TestFineGrainedAuthRuleParseHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	command, ok, err := parseFineGrainedAuthRuleCommand(ctx, "CREATE AUTH RULE r SET CONDITION true")
	require.Equal(t, fineGrainedAuthRuleCommand{}, command)
	require.False(t, ok)
	require.ErrorIs(t, err, context.Canceled)
}
