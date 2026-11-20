package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// providerBoolPtr returns a pointer to value for a declared account field.
func providerBoolPtr(value bool) *bool {
	return &value
}

// requireUserProviderStatement parses statement as a fine-grained security
// directory command and compares the whole declared command.
func requireUserProviderStatement(t *testing.T, ctx context.Context, statement string, want securityDirectoryStatement) {
	t.Helper()
	command, err := parseSecurityDirectoryStatement(ctx, statement)
	require.NoError(t, err, statement)
	require.Equal(t, want, command, statement)
}

// TestParseUserProviderCreateClauses proves the CREATE USER grammar of named
// auth providers: SET AUTH [PROVIDER] 'provider' { SET ID 'subject' } declares
// one external provider's subject identifier, the native provider keeps using
// the account's native password clauses, and the legacy standalone clauses are
// unchanged.
func TestParseUserProviderCreateClauses(t *testing.T) {
	for name, test := range map[string]struct {
		statement string
		params    map[string]interface{}
		want      securityDirectoryStatement
	}{
		"external provider beside the native password": {
			statement: "CREATE USER alice SET PASSWORD 'first-password' SET AUTH 'oidc-mainsso' { SET ID 'alice@example.com' }",
			want: securityDirectoryStatement{
				action: "dbms.user.create", name: "alice",
				account: securityUserMutation{
					password:          strPtr("first-password"),
					externalProviders: map[string]string{"oidc-mainsso": "alice@example.com"},
				},
			},
		},
		"external provider without a native clause": {
			statement: "CREATE USER alice SET AUTH PROVIDER 'oidc-mainsso' {SET ID 'alice@example.com'}",
			want: securityDirectoryStatement{
				action: "dbms.user.create", name: "alice",
				account: securityUserMutation{
					externalProviders: map[string]string{"oidc-mainsso": "alice@example.com"},
				},
			},
		},
		"repeated distinct providers": {
			statement: "CREATE USER alice SET AUTH 'oidc-mainsso' { SET ID 'alice@example.com' } SET AUTH 'ldap-corp' { SET ID 'alice' }",
			want: securityDirectoryStatement{
				action: "dbms.user.create", name: "alice",
				account: securityUserMutation{
					externalProviders: map[string]string{"oidc-mainsso": "alice@example.com", "ldap-corp": "alice"},
				},
			},
		},
		"native provider declares the native password": {
			statement: "CREATE USER alice SET AUTH 'native' { SET PASSWORD 'first-password' SET PASSWORD CHANGE NOT REQUIRED }",
			want: securityDirectoryStatement{
				action: "dbms.user.create", name: "alice",
				account: securityUserMutation{
					password:               strPtr("first-password"),
					nativeAuthProvider:     true,
					passwordChangeRequired: providerBoolPtr(false),
				},
			},
		},
		"native provider declares a plaintext password": {
			statement: "CREATE USER alice SET AUTH 'native' {SET PLAINTEXT PASSWORD 'first-password'}",
			want: securityDirectoryStatement{
				action: "dbms.user.create", name: "alice",
				account: securityUserMutation{password: strPtr("first-password"), nativeAuthProvider: true},
			},
		},
		"native and external providers stay distinct": {
			statement: "CREATE USER alice SET AUTH 'native' {SET PASSWORD 'first-password'} SET AUTH 'ldap-corp' {SET ID 'alice'}",
			want: securityDirectoryStatement{
				action: "dbms.user.create", name: "alice",
				account: securityUserMutation{
					password:           strPtr("first-password"),
					nativeAuthProvider: true,
					externalProviders:  map[string]string{"ldap-corp": "alice"},
				},
			},
		},
		"provider name and subject from parameters": {
			statement: "CREATE USER alice SET AUTH $provider { SET ID $subject }",
			params:    map[string]interface{}{"provider": "oidc-mainsso", "subject": "alice@example.com"},
			want: securityDirectoryStatement{
				action: "dbms.user.create", name: "alice",
				account: securityUserMutation{externalProviders: map[string]string{"oidc-mainsso": "alice@example.com"}},
			},
		},
		"legacy standalone clauses are unchanged": {
			statement: "CREATE USER alice SET PASSWORD 'first-password' CHANGE REQUIRED SET STATUS SUSPENDED SET HOME DATABASE homedb",
			want: securityDirectoryStatement{
				action: "dbms.user.create", name: "alice",
				account: securityUserMutation{
					password:               strPtr("first-password"),
					passwordChangeRequired: providerBoolPtr(true),
					suspended:              providerBoolPtr(true),
					homeDatabase:           strPtr("homedb"),
				},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			if test.params != nil {
				ctx = withParams(ctx, test.params)
			}
			requireUserProviderStatement(t, ctx, test.statement, test.want)
		})
	}
}

// TestParseUserProviderAlterClauses proves the ALTER USER grammar of named auth
// providers: REMOVE AUTH [PROVIDER[S]] takes one name, a comma list or a
// bracketed list, REMOVE ALL AUTH [PROVIDER[S]] removes every provider, and
// SET AUTH declares the same providers CREATE USER declares.
func TestParseUserProviderAlterClauses(t *testing.T) {
	for name, test := range map[string]struct {
		statement string
		params    map[string]interface{}
		want      securityDirectoryStatement
	}{
		"single provider": {
			statement: "ALTER USER alice REMOVE AUTH 'native'",
			want: securityDirectoryStatement{
				action: "dbms.user.alter", name: "alice",
				account: securityUserMutation{removeAuthProviders: []string{"native"}},
			},
		},
		"single provider with the provider keyword": {
			statement: "ALTER USER alice REMOVE AUTH PROVIDER 'oidc-mainsso'",
			want: securityDirectoryStatement{
				action: "dbms.user.alter", name: "alice",
				account: securityUserMutation{removeAuthProviders: []string{"oidc-mainsso"}},
			},
		},
		"comma list of providers": {
			statement: "ALTER USER alice REMOVE AUTH PROVIDERS 'oidc-mainsso', 'ldap-corp'",
			want: securityDirectoryStatement{
				action: "dbms.user.alter", name: "alice",
				account: securityUserMutation{removeAuthProviders: []string{"oidc-mainsso", "ldap-corp"}},
			},
		},
		"bracketed list of providers": {
			statement: "ALTER USER alice REMOVE AUTH ['oidc-mainsso', 'ldap-corp']",
			want: securityDirectoryStatement{
				action: "dbms.user.alter", name: "alice",
				account: securityUserMutation{removeAuthProviders: []string{"oidc-mainsso", "ldap-corp"}},
			},
		},
		"provider from a parameter": {
			statement: "ALTER USER alice REMOVE AUTH $provider",
			params:    map[string]interface{}{"provider": "oidc-mainsso"},
			want: securityDirectoryStatement{
				action: "dbms.user.alter", name: "alice",
				account: securityUserMutation{removeAuthProviders: []string{"oidc-mainsso"}},
			},
		},
		"provider list mixing literal and parameter": {
			statement: "ALTER USER alice REMOVE AUTH PROVIDERS ['oidc-mainsso', $provider]",
			params:    map[string]interface{}{"provider": "ldap-corp"},
			want: securityDirectoryStatement{
				action: "dbms.user.alter", name: "alice",
				account: securityUserMutation{removeAuthProviders: []string{"oidc-mainsso", "ldap-corp"}},
			},
		},
		"all providers": {
			statement: "ALTER USER alice REMOVE ALL AUTH",
			want: securityDirectoryStatement{
				action: "dbms.user.alter", name: "alice",
				account: securityUserMutation{removeAllAuthProviders: true},
			},
		},
		"all providers with the provider keyword": {
			statement: "ALTER USER alice REMOVE ALL AUTH PROVIDERS",
			want: securityDirectoryStatement{
				action: "dbms.user.alter", name: "alice",
				account: securityUserMutation{removeAllAuthProviders: true},
			},
		},
		"remove all and set an external provider": {
			statement: "ALTER USER alice REMOVE ALL AUTH SET AUTH 'ldap-corp' {SET ID 'alice'}",
			want: securityDirectoryStatement{
				action: "dbms.user.alter", name: "alice",
				account: securityUserMutation{
					removeAllAuthProviders: true,
					externalProviders:      map[string]string{"ldap-corp": "alice"},
				},
			},
		},
		"remove the native provider and set a native password": {
			statement: "ALTER USER alice REMOVE AUTH 'native' SET AUTH 'native' { SET PASSWORD 'second-password' SET PASSWORD CHANGE NOT REQUIRED }",
			want: securityDirectoryStatement{
				action: "dbms.user.alter", name: "alice",
				account: securityUserMutation{
					nativeAuthProvider:     true,
					removeAuthProviders:    []string{"native"},
					password:               strPtr("second-password"),
					passwordChangeRequired: providerBoolPtr(false),
				},
			},
		},
		"external provider beside a status clause": {
			statement: "ALTER USER alice SET AUTH 'ldap-corp' { SET ID 'alice' } SET STATUS SUSPENDED",
			want: securityDirectoryStatement{
				action: "dbms.user.alter", name: "alice",
				account: securityUserMutation{
					externalProviders: map[string]string{"ldap-corp": "alice"},
					suspended:         providerBoolPtr(true),
				},
			},
		},
		"legacy standalone clauses are unchanged": {
			statement: "ALTER USER alice REMOVE HOME DATABASE SET STATUS ACTIVE SET PASSWORD 'second-password' CHANGE REQUIRED",
			want: securityDirectoryStatement{
				action: "dbms.user.alter", name: "alice",
				account: securityUserMutation{
					homeDatabase:           strPtr(""),
					suspended:              providerBoolPtr(false),
					password:               strPtr("second-password"),
					passwordChangeRequired: providerBoolPtr(true),
				},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			if test.params != nil {
				ctx = withParams(ctx, test.params)
			}
			requireUserProviderStatement(t, ctx, test.statement, test.want)
		})
	}
}

// TestParseUserProviderInvalidClauses proves the grammar rejects a repeated
// provider key, a repeated subject key, an empty provider name, an empty
// subject and every malformed provider clause with the statement syntax error.
func TestParseUserProviderInvalidClauses(t *testing.T) {
	for name, statement := range map[string]string{
		"duplicate provider key":                        "CREATE USER alice SET AUTH 'ldap-corp' { SET ID 'alice' } SET AUTH 'ldap-corp' { SET ID 'alice-two' }",
		"duplicate subject key":                         "CREATE USER alice SET AUTH 'ldap-corp' { SET ID 'alice' SET ID 'alice-two' }",
		"empty provider name":                           "CREATE USER alice SET AUTH '' { SET ID 'alice' }",
		"empty subject":                                 "CREATE USER alice SET AUTH 'ldap-corp' { SET ID '' }",
		"provider without account keys":                 "CREATE USER alice SET AUTH 'ldap-corp'",
		"empty account keys":                            "CREATE USER alice SET AUTH 'ldap-corp' { }",
		"unterminated account keys":                     "CREATE USER alice SET AUTH 'ldap-corp' { SET ID 'alice'",
		"unsupported external key":                      "CREATE USER alice SET AUTH 'ldap-corp' { SET NAME 'alice' }",
		"external key without a value":                  "CREATE USER alice SET AUTH 'ldap-corp' { SET }",
		"native block without a native clause":          "CREATE USER alice SET AUTH 'native' { SET PASSWORD CHANGE REQUIRED }",
		"native block with an external subject":         "CREATE USER alice SET AUTH 'native' { SET ID 'alice' }",
		"empty native block":                            "CREATE USER alice SET AUTH 'native' { }",
		"plural providers after SET AUTH":               "CREATE USER alice SET AUTH PROVIDERS 'ldap-corp' { SET ID 'alice' }",
		"create without an auth provider":               "CREATE USER alice SET STATUS ACTIVE",
		"remove auth without provider names":            "ALTER USER alice REMOVE AUTH",
		"remove empty provider list":                    "ALTER USER alice REMOVE AUTH []",
		"remove empty provider name":                    "ALTER USER alice REMOVE AUTH PROVIDERS 'ldap-corp', ''",
		"remove unterminated provider list":             "ALTER USER alice REMOVE AUTH ['ldap-corp'",
		"remove provider list with a trailing comma":    "ALTER USER alice REMOVE AUTH PROVIDERS 'ldap-corp',",
		"remove an unsupported account part":            "ALTER USER alice REMOVE PASSWORD 'x'",
		"create cannot remove a provider":               "CREATE USER alice SET PASSWORD 'first-password' REMOVE AUTH 'ldap-corp'",
		"unexpected input after an all-provider clause": "ALTER USER alice REMOVE ALL AUTH PROVIDERS 'ldap-corp'",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parseSecurityDirectoryStatement(t.Context(), statement)
			requireSecurityDirectoryErrorCode(t, err, "Neo.ClientError.Statement.SyntaxError")
		})
	}
}

// TestParseUserProviderParameterValues proves the provider name and subject
// positions keep the existing string parameter contract: a missing parameter
// and a non-string parameter are refused with their own error codes.
func TestParseUserProviderParameterValues(t *testing.T) {
	statement := "CREATE USER alice SET AUTH 'ldap-corp' { SET ID $subject }"
	_, err := parseSecurityDirectoryStatement(t.Context(), statement)
	requireSecurityDirectoryErrorCode(t, err, "Neo.ClientError.Statement.ParameterMissing")

	_, err = parseSecurityDirectoryStatement(withParams(t.Context(), map[string]interface{}{"subject": int64(7)}), statement)
	requireSecurityDirectoryErrorCode(t, err, "Neo.ClientError.Statement.TypeError")
}
