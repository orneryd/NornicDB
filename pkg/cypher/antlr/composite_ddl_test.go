package antlr

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/testutil/compositeddl"
)

func TestANTLRCompositeDDLInventory(t *testing.T) {
	for _, c := range compositeddl.Cases() {
		if !c.Grammar {
			continue
		}
		t.Run(c.Name, func(t *testing.T) {
			query := strings.ReplaceAll(c.Statement, "$db", "ddl_inventory"+c.DatabaseSuffix)
			if err := Validate(query); err != nil {
				t.Error(strings.ReplaceAll(err.Error(), compositeddl.Password, "<password>"))
			}
		})
	}
	if err := Validate("SHOW CONSTITUENTS FOR COMPOSITE DATABASE ddl_inventory"); err != nil {
		t.Error(err)
	}
	if err := Validate("CREATE COMPOSITE DATABASE ddl ALIAS r FOR DATABASE nornic AT $uri USER $user PASSWORD $password"); err != nil {
		t.Error(err)
	}
}

// ANTLR diagnostics themselves must never quote a password literal, even if
// parsing stops before a credential clause or a malformed value is the token.
func TestANTLRCredentialSyntaxDiagnostics(t *testing.T) {
	for _, query := range []string{
		"CREATE ALIAS r FOR DATABASE nornic '" + compositeddl.Password + "'",
		"CREATE COMPOSITE DATABASE ddl ALIAS r FOR DATABASE nornic AT 'http://127.0.0.1:1' PASSWORD '" + compositeddl.Password + "' '" + compositeddl.Password + "'",
		"CREATE COMPOSITE DATABASE ddl ALIAS r FOR DATABASE nornic PASSWORD '" + compositeddl.Password,
	} {
		if err := Validate(query); err == nil {
			t.Error("malformed DDL accepted")
		} else if strings.Contains(err.Error(), compositeddl.Password) {
			t.Error("Validate syntax error disclosed password")
		}
		result, err := Parse(query)
		if err == nil {
			t.Error("malformed DDL accepted by Parse")
		} else if strings.Contains(err.Error(), compositeddl.Password) {
			t.Error("Parse syntax error disclosed password")
		}
		if result != nil {
			for _, detail := range result.Errors {
				if strings.Contains(detail, compositeddl.Password) {
					t.Error("ParseResult disclosed password")
				}
			}
		}
	}
}
