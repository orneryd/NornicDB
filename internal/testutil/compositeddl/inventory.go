// Package compositeddl inventories the native composite/alias administration
// contract. It is shared by grammar and public-protocol regression tests.
package compositeddl

import (
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/observability"
)

const Password = "synthetic-composite-ddl-password-741928"
const BarePassword = "SyntheticCredential741928"

type Case struct {
	Name           string
	Setup          []string
	Statement      string
	Parameters     map[string]any
	Reject         bool
	DatabaseSuffix string
	ReplyAlias     string
	// Grammar marks native-supported syntax that the ANTLR grammar must model.
	Grammar                                                bool
	Inspect                                                string
	Alias, Database, Type, Access, URI, Auth, User, Secret string
}

// Cases follows the two callers of parseConstituentFromTokens and the standalone
// alias readers. Unsupported Neo4j forms are boundary controls, not new features.
func Cases() []Case {
	local := "CREATE COMPOSITE DATABASE $db ALIAS local FOR DATABASE nornic"
	cases := []Case{
		{Name: "local", Statement: local, Grammar: true, Inspect: "constituents", Alias: "local", Database: "nornic", Type: "local", Access: "read_write"},
		{Name: "local_options", Statement: local + " TYPE local ACCESS read", Grammar: true, Inspect: "constituents", Alias: "local", Database: "nornic", Type: "local", Access: "read"},
		{Name: "local_alias", Statement: "CREATE ALIAS $db FOR DATABASE nornic", Grammar: true, Inspect: "aliases", Alias: "$db", Database: "nornic"},
		// The current standalone owner does not support idempotent alias creation.
		// Reject the modifier instead of compacting it into another catalog name.
		{Name: "local_alias_if_not_exists_rejected", Statement: "CREATE ALIAS $db IF NOT EXISTS FOR DATABASE nornic", Grammar: true, Reject: true, Inspect: "aliases"},
		{Name: "drop_alias", Setup: []string{"CREATE ALIAS $db FOR DATABASE nornic"}, Statement: "DROP ALIAS $db", Grammar: true, Inspect: "aliases"},
		{Name: "drop_alias_if_exists", Statement: "DROP ALIAS $db IF EXISTS", Grammar: true, Inspect: "aliases"},
		{Name: "drop_alias_existing_if_exists", Setup: []string{"CREATE ALIAS $db FOR DATABASE nornic"}, Statement: "DROP ALIAS $db IF EXISTS", Grammar: true, Inspect: "aliases"},
		{Name: "drop_composite", Setup: []string{local}, Statement: "DROP COMPOSITE DATABASE $db", Grammar: true, Inspect: "databases"},
		{Name: "alter_drop", Setup: []string{local}, Statement: "ALTER COMPOSITE DATABASE $db DROP ALIAS local", Grammar: true, Inspect: "empty"},
		// Invalid trailing options must be rejected before either DROP mutates
		// catalog state. Keep the original malformed inputs as negative controls.
		{Name: "alter_drop_rejects_options", Setup: []string{local}, Statement: "ALTER COMPOSITE DATABASE $db DROP ALIAS local AT 'http://127.0.0.1:1' USER admin PASSWORD '" + Password + "' DRIVER {} PROPERTIES {} WAIT", Reject: true, Inspect: "constituents", Alias: "local", Database: "nornic", Type: "local", Access: "read_write"},
		{Name: "drop_alias_rejects_options", Setup: []string{"CREATE ALIAS $db FOR DATABASE nornic"}, Statement: "DROP ALIAS $db IF EXISTS FOR DATABASE AT 'http://127.0.0.1:1' USER admin PASSWORD '" + Password + "' DRIVER {} PROPERTIES {} WAIT", Reject: true, Inspect: "aliases", Alias: "$db", Database: "nornic"},
	}
	for _, scheme := range []string{"http", "bolt"} {
		for _, mode := range []string{"none", "explicit", "uri", "both"} {
			uri := scheme + "://127.0.0.1:1"
			if mode == "uri" || mode == "both" {
				uri = scheme + "://admin:" + Password + "@127.0.0.1:1"
			}
			options := " AT '" + uri + "'"
			auth, user := "oidc_forwarding", ""
			if mode == "explicit" || mode == "both" {
				options += " USER admin PASSWORD '" + Password + "'"
				auth, user = "user_password", "admin"
			}
			if mode == "uri" {
				// URI userinfo remains credential material; only an explicit USER
				// clause is a public catalog identity.
				user = observability.RedactedPlaceholder()
			}
			options += " TYPE remote ACCESS read"
			for _, operation := range []string{"create", "if_not_exists", "add"} {
				c := Case{Name: operation + "_" + scheme + "_" + mode, Grammar: true, Inspect: "constituents", Alias: "r", Database: "nornic", Type: "remote", Access: "read", URI: scheme + "://127.0.0.1:1", Auth: auth, User: user}
				c.Statement = "CREATE COMPOSITE DATABASE $db ALIAS r FOR DATABASE nornic" + options
				if operation == "if_not_exists" {
					c.Statement = strings.Replace(c.Statement, "$db", "$db IF NOT EXISTS", 1)
				}
				if operation == "add" {
					c.Setup = []string{local}
					c.Statement = "ALTER COMPOSITE DATABASE $db ADD ALIAS r FOR DATABASE nornic" + options
				}
				cases = append(cases, c)
			}
		}
	}
	for _, options := range []struct{ name, text, access, auth, user, secret string }{
		{"defaults", " AT 'http://127.0.0.1:1'", "read_write", "oidc_forwarding", "", ""},
		{"write", " AT 'http://127.0.0.1:1' ACCESS write", "write", "oidc_forwarding", "", ""},
		{"oidc", " AT 'http://127.0.0.1:1' OIDC CREDENTIAL FORWARDING", "read_write", "oidc_forwarding", "", ""},
		{"secret", " AT 'http://127.0.0.1:1' SECRET REF 'vault/path'", "read_write", "oidc_forwarding", "", "vault/path"},
		{"quoted", " AT \"http://127.0.0.1:1\" USER `admin` PASSWORD '" + Password + "' TYPE 'remote' ACCESS 'read_write'", "read_write", "user_password", "admin", ""},
		{"reordered", " ACCESS read USER admin PASSWORD '" + Password + "' TYPE remote AT 'http://127.0.0.1:1' SECRET REF 'vault/path'", "read", "user_password", "admin", "vault/path"},
		{"repeated", " AT 'http://127.0.0.1:1' ACCESS write ACCESS read", "read", "oidc_forwarding", "", ""},
		{"numeric_password", " AT 'http://127.0.0.1:1' USER admin PASSWORD 123456789", "read_write", "user_password", "admin", ""},
		{"bare_password", " AT 'http://127.0.0.1:1' USER admin PASSWORD " + BarePassword, "read_write", "user_password", "admin", ""},
	} {
		for _, operation := range []string{"create", "add"} {
			c := Case{Name: operation + "_" + options.name, Grammar: true, Inspect: "constituents", Alias: "r", Database: "nornic", Type: "remote", URI: "http://127.0.0.1:1", Access: options.access, Auth: options.auth, User: options.user, Secret: options.secret}
			c.Statement = "CREATE COMPOSITE DATABASE $db ALIAS r FOR DATABASE nornic" + options.text
			if operation == "add" {
				c.Setup = []string{local}
				c.Statement = "ALTER COMPOSITE DATABASE $db ADD ALIAS r FOR DATABASE nornic" + options.text
			}
			cases = append(cases, c)
		}
	}
	cases = append(cases,
		// The native administration reader consumes these tokens literally;
		// parameter binding here would be a separate native behavior change.
		Case{Name: "literal_parameter_tokens", Statement: "CREATE COMPOSITE DATABASE $db ALIAS $alias FOR DATABASE $target AT $uri USER $user PASSWORD $password ACCESS read", Parameters: map[string]any{"alias": "r", "target": "nornic", "uri": "http://127.0.0.1:1", "user": "admin", "password": Password}, Grammar: true, Inspect: "constituents", Alias: "$alias", Database: "$target", Type: "remote", URI: "$uri", Access: "read", Auth: "user_password", User: "$user"},
		Case{Name: "multiple", Statement: local + " ALIAS r FOR DATABASE nornic AT 'http://127.0.0.1:1' ACCESS read", Grammar: true, Inspect: "constituents", Alias: "r", Database: "nornic", Type: "remote", URI: "http://127.0.0.1:1", Access: "read", Auth: "oidc_forwarding"},
		Case{Name: "if_not_exists_existing", Setup: []string{local}, Statement: "CREATE COMPOSITE DATABASE $db IF NOT EXISTS ALIAS r FOR DATABASE nornic AT 'http://127.0.0.1:1' USER admin PASSWORD '" + Password + "'", Grammar: true, Inspect: "constituents", Alias: "local", Database: "nornic", Type: "local", Access: "read_write"},
		Case{Name: "if_not_exists_standard", Setup: []string{"CREATE DATABASE $db"}, Statement: "CREATE COMPOSITE DATABASE $db IF NOT EXISTS ALIAS r FOR DATABASE nornic AT 'http://127.0.0.1:1'", Grammar: true, Inspect: "databases"},
	)
	for _, suffix := range []string{"AT", "AT ''", "AT 'http://127.0.0.1:1' USER", "AT 'http://127.0.0.1:1' PASSWORD", "AT 'http://127.0.0.1:1' USER admin", "AT 'http://127.0.0.1:1' PASSWORD '" + Password + "'", "AT 'http://127.0.0.1:1' TYPE local", "TYPE other", "TYPE", "ACCESS other", "ACCESS", "OIDC", "OIDC CREDENTIAL", "SECRET", "SECRET REF", "USER admin PASSWORD '" + Password + "'", "AT 'http://127.0.0.1:1' USER admin PASSWORD '" + Password + "' OIDC CREDENTIAL FORWARDING", "AT 'http://127.0.0.1:1' DRIVER {}", "AT 'http://127.0.0.1:1' PROPERTIES {}", "AT 'http://127.0.0.1:1' WAIT", "AT 'http://127.0.0.1:1' NOWAIT"} {
		for _, operation := range []string{"create", "add"} {
			c := Case{Name: operation + "_invalid_" + strconv.Itoa(len(cases)), Reject: true, Inspect: "databases"}
			c.Statement = "CREATE COMPOSITE DATABASE $db ALIAS r FOR DATABASE nornic " + suffix
			if operation == "add" {
				c.Setup = []string{local}
				c.Statement = "ALTER COMPOSITE DATABASE $db ADD ALIAS r FOR DATABASE nornic " + suffix
				c.Inspect, c.Alias, c.Database, c.Type, c.Access = "constituents", "local", "nornic", "local", "read_write"
			}
			cases = append(cases, c)
		}
	}
	for _, statement := range []string{
		"CREATE COMPOSITE DATABASE $db",
		"CREATE OR REPLACE COMPOSITE DATABASE $db ALIAS r FOR DATABASE nornic AT 'http://127.0.0.1:1'",
		"CREATE COMPOSITE DATABASE $db WAIT",
		"CREATE COMPOSITE DATABASE $db WAIT 1 SEC",
		"CREATE COMPOSITE DATABASE $db NOWAIT",
		"DROP COMPOSITE DATABASE $db IF EXISTS",
		"DROP COMPOSITE DATABASE $db WAIT",
		"DROP COMPOSITE DATABASE $db NOWAIT",
		"CREATE ALIAS $db FOR DATABASE nornic AT 'http://127.0.0.1:1' USER admin PASSWORD '" + Password + "'",
		"CREATE ALIAS $db FOR DATABASE nornic PROPERTIES {}",
		"CREATE ALIAS $db FOR DATABASE nornic AT 'http://127.0.0.1:1' DRIVER {}",
		"DROP ALIAS $db FOR DATABASE",
		"CREATE OR REPLACE ALIAS $db FOR DATABASE nornic",
		"ALTER ALIAS $db SET DATABASE TARGET nornic AT 'http://127.0.0.1:1' USER admin PASSWORD '" + Password + "' DRIVER {} PROPERTIES {}",
		"ALTER ALIAS $db IF EXISTS SET DATABASE TARGET nornic",
	} {
		cases = append(cases, Case{Name: "unsupported_" + strconv.Itoa(len(cases)), Statement: statement, Reject: true, Inspect: "databases"})
	}
	// Native administration names and every listing consumer share the same
	// executor contract. Include names that ordinary identifier grammar splits.
	for _, suffix := range []string{"-target", ".target"} {
		setup := []string{local, "CREATE ALIAS $db_alias FOR DATABASE $db"}
		for i, statement := range []string{
			"SHOW ALIASES FOR DATABASE $db",
			"SHOW ALIASES FOR DATABASE $db WHERE alias = '$db_alias'",
			"SHOW ALIASES FOR DATABASE $db YIELD alias, database WHERE alias = '$db_alias'",
			"SHOW ALIASES FOR DATABASE $db YIELD * ORDER BY alias LIMIT 1 RETURN alias, database",
		} {
			cases = append(cases, Case{Name: "show_aliases_" + suffix + strconv.Itoa(i), DatabaseSuffix: suffix, Setup: setup, Statement: statement, Grammar: true, Inspect: "aliases", Alias: "$db_alias", Database: "$db", ReplyAlias: "$db_alias"})
		}
		for _, c := range []Case{
			{Name: "create_named", Statement: local, Inspect: "constituents", Alias: "local", Database: "nornic", Type: "local", Access: "read_write"},
			{Name: "add_named", Setup: []string{local}, Statement: "ALTER COMPOSITE DATABASE $db ADD ALIAS remote_name FOR DATABASE nornic AT 'http://127.0.0.1:1' ACCESS read", Inspect: "constituents", Alias: "remote_name", Database: "nornic", Type: "remote", URI: "http://127.0.0.1:1", Access: "read", Auth: "oidc_forwarding"},
			{Name: "drop_named", Setup: []string{local}, Statement: "DROP COMPOSITE DATABASE $db", Inspect: "databases"},
			{Name: "alter_drop_named", Setup: []string{local}, Statement: "ALTER COMPOSITE DATABASE $db DROP ALIAS local", Inspect: "empty"},
			{Name: "create_alias_named", Setup: []string{local}, Statement: "CREATE ALIAS $db_alias FOR DATABASE $db", Inspect: "aliases", Alias: "$db_alias", Database: "$db"},
			{Name: "drop_alias_named", Setup: setup, Statement: "DROP ALIAS $db_alias", Inspect: "aliases"},
			{Name: "show_constituents_named", Setup: []string{local}, Statement: "SHOW CONSTITUENTS FOR COMPOSITE DATABASE $db YIELD alias, database WHERE alias = 'local'", Inspect: "constituents", Alias: "local", Database: "nornic", Type: "local", Access: "read_write"},
			{Name: "show_composites_named", Setup: []string{local}, Statement: "SHOW COMPOSITE DATABASES YIELD name WHERE name = '$db'", Inspect: "constituents", Alias: "local", Database: "nornic", Type: "local", Access: "read_write"},
		} {
			c.Name += suffix
			c.DatabaseSuffix, c.Grammar = suffix, true
			cases = append(cases, c)
		}
	}
	// A native-extension dispatch must not turn an unparsed listing suffix into
	// a successful, unfiltered catalog read. All public routes share these cases.
	for i, statement := range []string{
		"SHOW ALIASES garbage",
		"SHOW ALIASES garbage WHERE alias = '$db_alias'",
		"SHOW ALIASES garbage YIELD alias, database",
		"SHOW ALIASES garbage FOR DATABASE $db",
		"SHOW ALIASES FOR",
		"SHOW ALIASES FOR DATABASE",
		"SHOW ALIASES FOR DATABASE $db garbage",
		"SHOW ALIASES FOR DATABASE $db garbage WHERE alias = '$db_alias'",
		"SHOW ALIASES FOR DATABASE $db garbage YIELD alias, database",
		"SHOW ALIASES FOR DATABASES garbage",
		"SHOW ALIASES FOR DATABASE $db,garbage",
		"SHOW ALIASES FOR DATABASE $db@garbage",
		"SHOW ALIASES FOR DATABASE $db;garbage",
	} {
		cases = append(cases, Case{Name: "show_aliases_invalid_" + strconv.Itoa(i), Setup: []string{local, "CREATE ALIAS $db_alias FOR DATABASE $db"}, Statement: statement, Reject: true, Inspect: "aliases"})
	}
	for i, statement := range []string{
		"SHOW ALIASES WHERE alias = '$db_alias'",
		"SHOW ALIASES YIELD alias, database WHERE alias = '$db_alias'",
		"SHOW ALIASES FOR DATABASES WHERE alias = '$db_alias'",
	} {
		cases = append(cases, Case{Name: "show_aliases_tail_" + strconv.Itoa(i), Setup: []string{local, "CREATE ALIAS $db_alias FOR DATABASE $db"}, Statement: statement, Grammar: true, Inspect: "aliases", Alias: "$db_alias", Database: "$db", ReplyAlias: "$db_alias"})
	}
	for i, suffix := range []string{"-1", ".1"} {
		// The lexer reads .1 as a numeric literal. Qualified catalog-name
		// grammar must consume that component without changing its identity.
		cases = append(cases, Case{Name: "show_aliases_numeric_" + strconv.Itoa(i), DatabaseSuffix: suffix, Setup: []string{local, "CREATE ALIAS numeric_$db FOR DATABASE $db"}, Statement: "SHOW ALIASES FOR DATABASE $db WHERE alias = 'numeric_$db'", Grammar: true, Inspect: "aliases", Alias: "numeric_$db", Database: "$db", ReplyAlias: "numeric_$db"})
	}
	// The shared composite reader reports malformed trailing literals through
	// the native command/error owner even when ANTLR selects the extension.
	for _, alter := range []bool{false, true} {
		for _, quote := range []string{"'", "\""} {
			for _, repeated := range []bool{false, true} {
				name := "composite_error_create"
				statement := "CREATE COMPOSITE DATABASE $db ALIAS r FOR DATABASE nornic"
				var setup []string
				if alter {
					name, statement, setup = "composite_error_alter", "ALTER COMPOSITE DATABASE $db ADD ALIAS r FOR DATABASE nornic", []string{local}
				}
				literal := quote + Password + quote
				statement += " AT 'http://127.0.0.1:1' USER admin PASSWORD "
				if repeated {
					name, statement = name+"_keyword", statement+"PASSWORD "+literal
				} else {
					name, statement = name+"_trailing", statement+literal+" "+literal
				}
				if quote == "'" {
					name += "_single"
				} else {
					name += "_double"
				}
				cases = append(cases, Case{Name: name, Setup: setup, Statement: statement, Reject: true, Inspect: "databases"})
			}
		}
	}
	return cases
}
