package cypher

import (
	"context"
	"errors"
	"slices"
	"strings"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/auth"
)

// securityUserMutation is the clause set one CREATE USER or ALTER USER
// statement declares. A nil or empty field is a clause the statement did not
// write; a statement the clauses leave out never changes that part of the
// account.
//
// The native provider's account fields and the external providers' account
// keys are separate owners: password and passwordChangeRequired hold the
// account's native password clauses, and externalProviders holds one subject
// identifier per named external provider. A native clause never writes an
// external provider and an external provider never writes a native field.
type securityUserMutation struct {
	password               *string
	passwordChangeRequired *bool
	suspended              *bool
	homeDatabase           *string
	externalProviders      map[string]string
	removeAuthProviders    []string
	removeAllAuthProviders bool
	nativeAuthProvider     bool
}

type securityDirectoryStatement struct {
	action    string
	name      string
	other     string
	users     []string
	roles     []string
	ifPresent bool
	copyOf    *string
	account   securityUserMutation
}

func isFineGrainedRoleListing(ctx context.Context, query string) bool {
	return auth.FineGrainedRequestFromContext(ctx) != nil &&
		(startsWithKeywords(ctx, query, "SHOW", "ROLES") || startsWithKeywords(ctx, query, "SHOW", "ALL ROLES"))
}

func isFineGrainedDirectoryStatement(ctx context.Context, query string) bool {
	if auth.FineGrainedRequestFromContext(ctx) == nil {
		return false
	}
	return startsWithKeywords(ctx, query, "CREATE", "ROLE") ||
		startsWithKeywords(ctx, query, "CREATE", "USER") ||
		startsWithKeywords(ctx, query, "DROP", "ROLE") ||
		startsWithKeywords(ctx, query, "DROP", "USER") ||
		startsWithKeywords(ctx, query, "RENAME", "ROLE") ||
		startsWithKeywords(ctx, query, "RENAME", "USER") ||
		isFineGrainedOwnPasswordChange(ctx, query) ||
		startsWithKeywords(ctx, query, "ALTER", "USER") ||
		startsWithKeywords(ctx, query, "GRANT", "ROLE") ||
		startsWithKeywords(ctx, query, "GRANT", "ROLES") ||
		startsWithKeywords(ctx, query, "REVOKE", "ROLE") ||
		startsWithKeywords(ctx, query, "REVOKE", "ROLES")
}

func parseSecurityDirectoryStatement(ctx context.Context, text string) (securityDirectoryStatement, error) {
	var command securityDirectoryStatement
	cursor := fineGrainedPrivilegeCursor{text: text, params: getParamsFromContext(ctx)}
	cursor.skipGap()
	verb, ok := cursor.readName()
	if !ok {
		return command, fineGrainedPrivilegeSyntaxError("expected a security administration verb")
	}
	cursor.skipGap()
	if strings.EqualFold(verb, "GRANT") || strings.EqualFold(verb, "REVOKE") {
		if !cursor.takeKeyword("ROLES") && !cursor.takeKeyword("ROLE") {
			return command, fineGrainedPrivilegeSyntaxError("expected ROLE or ROLES")
		}
		roles, err := cursor.readSecurityNameList()
		if err != nil {
			return command, err
		}
		preposition := "TO"
		command.action = "dbms.role.assign"
		if strings.EqualFold(verb, "REVOKE") {
			preposition, command.action = "FROM", "dbms.role.remove"
		}
		if !cursor.takeKeyword(preposition) {
			return command, fineGrainedPrivilegeSyntaxError("expected " + preposition + " followed by users")
		}
		command.roles = roles
		command.users, err = cursor.readSecurityNameList()
		if err != nil {
			return command, err
		}
		if cursor.at != len(text) {
			return command, fineGrainedPrivilegeSyntaxError("unexpected input after user names")
		}
		return command, nil
	}
	if strings.EqualFold(verb, "ALTER") && cursor.takeKeyword("CURRENT") {
		if !cursor.takeKeyword("USER") || !cursor.takeKeyword("SET") || !cursor.takeKeyword("PASSWORD") || !cursor.takeKeyword("FROM") {
			return command, fineGrainedPrivilegeSyntaxError("expected ALTER CURRENT USER SET PASSWORD FROM")
		}
		current, err := cursor.readSecurityString("expected a password string")
		if err != nil {
			return command, err
		}
		if !cursor.takeKeyword("TO") {
			return command, fineGrainedPrivilegeSyntaxError("expected TO followed by a password")
		}
		password, err := cursor.readSecurityString("expected a password string")
		if err != nil {
			return command, err
		}
		if cursor.at != len(text) {
			return command, fineGrainedPrivilegeSyntaxError("unexpected input after new password")
		}
		return securityDirectoryStatement{action: "self.password", other: current, account: securityUserMutation{password: &password}}, nil
	}
	kind, ok := cursor.readName()
	if !ok {
		return command, fineGrainedPrivilegeSyntaxError("expected a security directory object")
	}
	command.action = "dbms." + lowerASCII(kind) + "." + lowerASCII(verb)
	cursor.skipGap()
	name, err := cursor.readSecurityName()
	if err != nil {
		return command, err
	}
	command.name = name
	cursor.skipGap()
	if cursor.takeKeyword("IF") {
		expect := "EXISTS"
		if strings.EqualFold(verb, "CREATE") && !cursor.takeKeyword("NOT") {
			return command, fineGrainedPrivilegeSyntaxError("expected IF NOT EXISTS")
		}
		if !cursor.takeKeyword(expect) {
			return command, fineGrainedPrivilegeSyntaxError("expected EXISTS")
		}
		command.ifPresent = true
	}
	switch command.action {
	case "dbms.role.rename", "dbms.user.rename":
		if !cursor.takeKeyword("TO") {
			return command, fineGrainedPrivilegeSyntaxError("expected TO followed by a new name")
		}
		next, err := cursor.readSecurityName()
		if err != nil {
			return command, err
		}
		command.other = next
		cursor.skipGap()
	case "dbms.role.create":
		if cursor.takeKeyword("AS") {
			if !cursor.takeKeyword("COPY") || !cursor.takeKeyword("OF") {
				return command, fineGrainedPrivilegeSyntaxError("expected AS COPY OF followed by a role name")
			}
			source, err := cursor.readSecurityName()
			if err != nil {
				return command, err
			}
			command.copyOf = &source
			cursor.skipGap()
		}
	case "dbms.user.alter":
		if err := cursor.readSecurityUserClauses(&command.account, false); err != nil {
			return command, err
		}
	case "dbms.user.create":
		if err := cursor.readSecurityUserClauses(&command.account, true); err != nil {
			return command, err
		}
		if command.account.password == nil && len(command.account.externalProviders) == 0 {
			return command, fineGrainedPrivilegeSyntaxError("expected SET PASSWORD followed by a string, or SET AUTH followed by an auth provider")
		}
	}
	if cursor.at != len(text) {
		return command, fineGrainedPrivilegeSyntaxError("unexpected input after security administration command")
	}
	return command, nil
}

// readSecurityUserClauses reads one CREATE USER or ALTER USER statement's SET
// and REMOVE clauses in any order. CREATE USER must declare a native password
// or at least one external auth provider, because every NornicDB account
// needs an auth provider.
func (cursor *fineGrainedPrivilegeCursor) readSecurityUserClauses(mutation *securityUserMutation, creating bool) error {
	written := 0
	for {
		cursor.skipGap()
		if cursor.at == len(cursor.text) {
			break
		}
		if err := cursor.readSecurityUserClause(mutation, creating); err != nil {
			return err
		}
		written++
	}
	if written == 0 {
		if creating {
			return fineGrainedPrivilegeSyntaxError("expected SET PASSWORD followed by a string, or SET AUTH followed by an auth provider")
		}
		return fineGrainedPrivilegeSyntaxError("expected SET or REMOVE followed by a user clause")
	}
	return nil
}

func (cursor *fineGrainedPrivilegeCursor) readSecurityUserClause(mutation *securityUserMutation, creating bool) error {
	if !creating && cursor.takeKeyword("REMOVE") {
		if cursor.takeKeyword("HOME") {
			if !cursor.takeKeyword("DATABASE") {
				return fineGrainedPrivilegeSyntaxError("expected REMOVE HOME DATABASE")
			}
			home := ""
			mutation.homeDatabase = &home
			return nil
		}
		return cursor.readSecurityRemoveAuthClause(mutation)
	}
	if !cursor.takeKeyword("SET") {
		if creating {
			return fineGrainedPrivilegeSyntaxError("expected SET followed by a user clause")
		}
		return fineGrainedPrivilegeSyntaxError("expected SET or REMOVE followed by a user clause")
	}
	switch {
	case cursor.takeKeyword("PLAINTEXT"):
		if !cursor.takeKeyword("PASSWORD") {
			return fineGrainedPrivilegeSyntaxError("expected PLAINTEXT PASSWORD followed by a string")
		}
		return cursor.readSecurityPasswordClause(mutation)
	case cursor.takeKeyword("PASSWORD"):
		if cursor.wordAt("CHANGE") {
			cursor.takeKeyword("CHANGE")
			return cursor.readSecurityChangeRequirement(mutation)
		}
		return cursor.readSecurityPasswordClause(mutation)
	case cursor.takeKeyword("CHANGE"):
		return cursor.readSecurityChangeRequirement(mutation)
	case cursor.takeKeyword("STATUS"):
		switch {
		case cursor.takeKeyword("SUSPENDED"):
			suspended := true
			mutation.suspended = &suspended
		case cursor.takeKeyword("ACTIVE"):
			suspended := false
			mutation.suspended = &suspended
		default:
			return fineGrainedPrivilegeSyntaxError("expected SET STATUS ACTIVE or SUSPENDED")
		}
		return nil
	case cursor.takeKeyword("HOME"):
		if !cursor.takeKeyword("DATABASE") {
			return fineGrainedPrivilegeSyntaxError("expected SET HOME DATABASE followed by a database name")
		}
		database, err := cursor.readSecurityName()
		if err != nil {
			return err
		}
		cursor.skipGap()
		mutation.homeDatabase = &database
		return nil
	case cursor.takeKeyword("AUTH"):
		return cursor.readSecuritySetAuthClause(mutation)
	}
	return fineGrainedPrivilegeSyntaxError("expected a supported user clause")
}

func (cursor *fineGrainedPrivilegeCursor) readSecurityPasswordClause(mutation *securityUserMutation) error {
	password, err := cursor.readSecurityString("expected a password string")
	if err != nil {
		return err
	}
	mutation.password = &password
	if cursor.takeKeyword("CHANGE") {
		return cursor.readSecurityChangeRequirement(mutation)
	}
	return nil
}

func (cursor *fineGrainedPrivilegeCursor) readSecurityChangeRequirement(mutation *securityUserMutation) error {
	required := !cursor.takeKeyword("NOT")
	if !cursor.takeKeyword("REQUIRED") {
		return fineGrainedPrivilegeSyntaxError("expected CHANGE [NOT] REQUIRED")
	}
	mutation.passwordChangeRequired = &required
	return nil
}

// fineGrainedNativeAuthProvider is the auth provider name Neo4j reserves for
// the account's own native password.
const fineGrainedNativeAuthProvider = "native"

// readSecuritySetAuthClause reads SET AUTH [PROVIDER] 'provider' { keys }: one
// named auth provider's account keys for the user. The native provider owns
// the account's native password clauses; every other name is an external
// provider that owns one subject identifier. Each provider a statement sets is
// distinct, because the statement declares that provider's account keys once.
func (cursor *fineGrainedPrivilegeCursor) readSecuritySetAuthClause(mutation *securityUserMutation) error {
	cursor.takeKeyword("PROVIDER")
	provider, err := cursor.readSecurityString("expected an auth provider name string")
	if err != nil {
		return err
	}
	if provider == "" {
		return fineGrainedPrivilegeSyntaxError("an auth provider name cannot be empty")
	}
	if !cursor.openSecurityAuthKeys() {
		return fineGrainedPrivilegeSyntaxError("expected { followed by the auth provider keys")
	}
	if provider == fineGrainedNativeAuthProvider {
		if mutation.nativeAuthProvider {
			return fineGrainedPrivilegeSyntaxError("the native provider is set more than once")
		}
		mutation.nativeAuthProvider = true
		return cursor.readSecurityNativeAuthKeys(mutation)
	}
	if _, written := mutation.externalProviders[provider]; written {
		return fineGrainedPrivilegeSyntaxError("auth provider " + provider + " is set more than once")
	}
	subject, err := cursor.readSecurityExternalAuthKeys(provider)
	if err != nil {
		return err
	}
	if mutation.externalProviders == nil {
		mutation.externalProviders = map[string]string{}
	}
	mutation.externalProviders[provider] = subject
	return nil
}

// openSecurityAuthKeys consumes the { that opens an auth provider's account
// keys and reports whether it stood there.
func (cursor *fineGrainedPrivilegeCursor) openSecurityAuthKeys() bool {
	cursor.skipGap()
	if cursor.at >= len(cursor.text) || cursor.text[cursor.at] != '{' {
		return false
	}
	cursor.at++
	cursor.skipGap()
	return true
}

// takeSecurityAuthKeysEnd consumes the } that closes an auth provider's
// account keys and reports whether it stood there.
func (cursor *fineGrainedPrivilegeCursor) takeSecurityAuthKeysEnd() bool {
	cursor.skipGap()
	if cursor.at >= len(cursor.text) || cursor.text[cursor.at] != '}' {
		return false
	}
	cursor.at++
	cursor.skipGap()
	return true
}

// readSecurityNativeAuthKeys reads the native provider's account keys with the
// account's existing native password clause parsers: SET [PLAINTEXT] PASSWORD
// 'password' and SET PASSWORD CHANGE [NOT] REQUIRED.
func (cursor *fineGrainedPrivilegeCursor) readSecurityNativeAuthKeys(mutation *securityUserMutation) error {
	written := 0
	for !cursor.takeSecurityAuthKeysEnd() {
		if cursor.at == len(cursor.text) {
			return fineGrainedPrivilegeSyntaxError("expected } after the native auth provider keys")
		}
		if !cursor.takeKeyword("SET") {
			return fineGrainedPrivilegeSyntaxError("expected SET followed by a native auth provider key")
		}
		if err := cursor.readSecurityNativeAuthKey(mutation); err != nil {
			return err
		}
		written++
	}
	if written == 0 {
		return fineGrainedPrivilegeSyntaxError("expected SET PASSWORD or SET PASSWORD CHANGE in the native auth provider keys")
	}
	return nil
}

// readSecurityNativeAuthKey reads one SET key of the native provider's account
// keys. The native provider's keys are the account's native password clauses.
func (cursor *fineGrainedPrivilegeCursor) readSecurityNativeAuthKey(mutation *securityUserMutation) error {
	switch {
	case cursor.takeKeyword("PLAINTEXT"):
		if !cursor.takeKeyword("PASSWORD") {
			return fineGrainedPrivilegeSyntaxError("expected PLAINTEXT PASSWORD followed by a string")
		}
		return cursor.readSecurityPasswordClause(mutation)
	case cursor.takeKeyword("PASSWORD"):
		if cursor.wordAt("CHANGE") {
			cursor.takeKeyword("CHANGE")
			return cursor.readSecurityChangeRequirement(mutation)
		}
		return cursor.readSecurityPasswordClause(mutation)
	case cursor.takeKeyword("CHANGE"):
		return cursor.readSecurityChangeRequirement(mutation)
	}
	return fineGrainedPrivilegeSyntaxError("expected SET PASSWORD, SET PLAINTEXT PASSWORD or SET PASSWORD CHANGE [NOT] REQUIRED in the native auth provider keys")
}

// readSecurityExternalAuthKeys reads the account keys of the external provider
// named provider: exactly one SET ID 'subject'. The subject is the provider's
// identifier for the account, so the block declares it once and it cannot be
// empty.
func (cursor *fineGrainedPrivilegeCursor) readSecurityExternalAuthKeys(provider string) (string, error) {
	subject := ""
	written := 0
	for !cursor.takeSecurityAuthKeysEnd() {
		if cursor.at == len(cursor.text) {
			return "", fineGrainedPrivilegeSyntaxError("expected } after the auth provider keys of " + provider)
		}
		if !cursor.takeKeyword("SET") || !cursor.takeKeyword("ID") {
			return "", fineGrainedPrivilegeSyntaxError("expected SET ID followed by a string in the auth provider keys of " + provider)
		}
		if written > 0 {
			return "", fineGrainedPrivilegeSyntaxError("the subject of auth provider " + provider + " is set more than once")
		}
		id, err := cursor.readSecurityString("expected an auth provider ID string")
		if err != nil {
			return "", err
		}
		if id == "" {
			return "", fineGrainedPrivilegeSyntaxError("the subject of auth provider " + provider + " cannot be empty")
		}
		subject = id
		written++
	}
	if written == 0 {
		return "", fineGrainedPrivilegeSyntaxError("expected SET ID followed by a string in the auth provider keys of " + provider)
	}
	return subject, nil
}

// readSecurityRemoveAuthClause reads REMOVE AUTH [PROVIDER[S]] providers or
// REMOVE ALL AUTH [PROVIDER[S]] after REMOVE has been consumed.
func (cursor *fineGrainedPrivilegeCursor) readSecurityRemoveAuthClause(mutation *securityUserMutation) error {
	all := cursor.takeKeyword("ALL")
	if !cursor.takeKeyword("AUTH") {
		return fineGrainedPrivilegeSyntaxError("expected REMOVE HOME DATABASE, REMOVE AUTH or REMOVE ALL AUTH")
	}
	if !cursor.takeKeyword("PROVIDERS") {
		cursor.takeKeyword("PROVIDER")
	}
	if all {
		mutation.removeAllAuthProviders = true
		return nil
	}
	providers, err := cursor.readSecurityAuthProviderList()
	if err != nil {
		return err
	}
	mutation.removeAuthProviders = append(mutation.removeAuthProviders, providers...)
	return nil
}

// readSecurityAuthProviderList reads the provider names of REMOVE AUTH: one
// quoted name, a comma-separated list of names, or a bracketed list of names.
// Each name is a string literal or a string parameter bound to one.
func (cursor *fineGrainedPrivilegeCursor) readSecurityAuthProviderList() ([]string, error) {
	cursor.skipGap()
	bracketed := cursor.at < len(cursor.text) && cursor.text[cursor.at] == '['
	if bracketed {
		cursor.at++
	}
	var providers []string
	for {
		cursor.skipGap()
		provider, err := cursor.readSecurityString("expected an auth provider name string")
		if err != nil {
			return nil, err
		}
		if provider == "" {
			return nil, fineGrainedPrivilegeSyntaxError("an auth provider name cannot be empty")
		}
		providers = append(providers, provider)
		if cursor.at < len(cursor.text) && cursor.text[cursor.at] == ',' {
			cursor.at++
			continue
		}
		break
	}
	if bracketed {
		cursor.skipGap()
		if cursor.at >= len(cursor.text) || cursor.text[cursor.at] != ']' {
			return nil, fineGrainedPrivilegeSyntaxError("expected ] after the auth provider names")
		}
		cursor.at++
		cursor.skipGap()
	}
	return providers, nil
}

func isFineGrainedOwnPasswordChange(ctx context.Context, query string) bool {
	return startsWithKeywords(ctx, query, "ALTER", "CURRENT USER")
}

// readSecurityName reads one security directory object name: a symbolic name,
// or one parameter reference bound to a string.
func (cursor *fineGrainedPrivilegeCursor) readSecurityName() (string, error) {
	if cursor.at < len(cursor.text) && cursor.text[cursor.at] == '$' {
		name, end, ok := schemaOptionsParameter(cursor.text[cursor.at:])
		if !ok {
			return "", fineGrainedPrivilegeSyntaxError("expected a name")
		}
		cursor.at += end
		return cursor.securityStringParameter(name)
	}
	name, ok := cursor.readName()
	if !ok {
		return "", fineGrainedPrivilegeSyntaxError("expected a name")
	}
	return name, nil
}

// readSecurityNameList reads a comma-separated list of security directory
// object names. Each member may be a symbolic name or a parameter reference.
func (cursor *fineGrainedPrivilegeCursor) readSecurityNameList() ([]string, error) {
	cursor.skipGap()
	var names []string
	for {
		name, err := cursor.readSecurityName()
		if err != nil {
			return nil, err
		}
		names = append(names, name)
		cursor.skipGap()
		if cursor.at < len(cursor.text) && cursor.text[cursor.at] == ',' {
			cursor.at++
			cursor.skipGap()
			continue
		}
		return names, nil
	}
}

// readSecurityString reads one security directory string: a string literal or
// one parameter reference bound to a string.
func (cursor *fineGrainedPrivilegeCursor) readSecurityString(missing string) (string, error) {
	if cursor.at < len(cursor.text) && cursor.text[cursor.at] == '$' {
		name, end, ok := schemaOptionsParameter(cursor.text[cursor.at:])
		if !ok {
			return "", fineGrainedPrivilegeSyntaxError(missing)
		}
		cursor.at += end
		cursor.skipGap()
		return cursor.securityStringParameter(name)
	}
	end, _, found := cyphertext.ScanStringToken(cursor.text, cursor.at)
	if !found {
		return "", fineGrainedPrivilegeSyntaxError(missing)
	}
	value, ok := cyphertext.DecodeString(cursor.text[cursor.at:end], cyphertext.AcceptedSource)
	if !ok {
		return "", fineGrainedPrivilegeSyntaxError(missing)
	}
	cursor.at = end
	cursor.skipGap()
	return value, nil
}

// securityStringParameter returns the string one parameter reference binds to.
// A name or credential position takes a string value and nothing else.
func (cursor *fineGrainedPrivilegeCursor) securityStringParameter(name string) (string, error) {
	value, supplied := cursor.params[name]
	if !supplied {
		return "", parameterMissingError([]string{name})
	}
	text, isString := value.(string)
	if !isString {
		return "", newSemanticError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType",
			"Parameter $"+name+" must be a String")
	}
	return text, nil
}

// securityDirectoryActions is the DBMS privilege each declared operation
// requires. An ALTER USER statement that writes several kinds of change needs
// the privilege for every one of them.
func securityDirectoryActions(command securityDirectoryStatement) []string {
	if command.action != "dbms.user.alter" {
		return []string{command.action}
	}
	var actions []string
	account := command.account
	if account.nativeAuthProvider || account.providerChange() != nil {
		actions = append(actions, "dbms.user.auth.set")
	}
	if !account.nativeAuthProvider && (account.password != nil || account.passwordChangeRequired != nil) {
		actions = append(actions, "dbms.user.password.set")
	}
	if command.account.suspended != nil {
		actions = append(actions, "dbms.user.status.set")
	}
	if command.account.homeDatabase != nil {
		actions = append(actions, "dbms.user.home.set")
	}
	return actions
}

func (account securityUserMutation) providerChange() *auth.FineGrainedProviderChange {
	if len(account.externalProviders) == 0 && len(account.removeAuthProviders) == 0 && !account.removeAllAuthProviders {
		return nil
	}
	return &auth.FineGrainedProviderChange{
		Set: account.externalProviders, Remove: account.removeAuthProviders, RemoveAll: account.removeAllAuthProviders,
	}
}

// securityDirectoryHomeDatabase resolves a declared home database through the
// database catalog, so the stored account names the canonical database.
func (e *StorageExecutor) securityDirectoryHomeDatabase(request *auth.FineGrainedRequest, home *string) (*string, error) {
	if home == nil {
		return nil, nil
	}
	name := *home
	if name != "" {
		target := resolveGraphSelection(e.dbManager, name)
		if target.err != nil || target.remote || request.DatabaseID(target.target) == "" {
			return nil, newSemanticError("Neo.ClientError.Database.DatabaseNotFound", "DatabaseNotFound", "Unknown home database")
		}
		name = target.target
	}
	return &name, nil
}

func securityDirectoryHomeValue(home *string) string {
	if home == nil {
		return ""
	}
	return *home
}

func (e *StorageExecutor) executeSecurityDirectoryStatement(ctx context.Context, request *auth.FineGrainedRequest, query string) (*ExecuteResult, error) {
	command, err := parseSecurityDirectoryStatement(ctx, query)
	if err != nil {
		return nil, err
	}
	if command.action == "self.password" {
		current, signedIn := authenticatedUserFromContext(ctx)
		if !signedIn {
			return nil, auth.ErrFineGrainedForbidden
		}
		return &ExecuteResult{}, request.Runtime.ChangeOwnPassword(ctx, current.Name, command.other, *command.account.password)
	}
	for _, action := range securityDirectoryActions(command) {
		if !request.AllowsDBMS(action, "") {
			return nil, auth.ErrFineGrainedForbidden
		}
	}
	runtime := request.Runtime
	home := command.account.homeDatabase
	if command.action == "dbms.user.alter" || command.action == "dbms.user.create" {
		resolved, err := e.securityDirectoryHomeDatabase(request, home)
		if err != nil {
			return nil, err
		}
		home = resolved
	}
	switch command.action {
	case "dbms.role.create":
		if command.copyOf != nil {
			err = runtime.CreateRoleCopy(ctx, command.name, *command.copyOf)
		} else {
			err = runtime.CreateRole(ctx, command.name)
		}
	case "dbms.role.rename":
		err = runtime.RenameRole(ctx, command.name, command.other)
	case "dbms.user.rename":
		err = runtime.RenameUser(ctx, command.name, command.other)
	case "dbms.role.drop":
		err = runtime.DropRole(ctx, command.name)
	case "dbms.role.assign", "dbms.role.remove":
		err = runtime.SetRoleMembership(ctx, command.users, command.roles, command.action == "dbms.role.assign")
	case "dbms.user.alter":
		err = runtime.AlterUser(ctx, command.name, auth.FineGrainedUserAlteration{
			Password:               command.account.password,
			PasswordChangeRequired: command.account.passwordChangeRequired,
			Suspended:              command.account.suspended,
			HomeDatabase:           home,
			Providers:              command.account.providerChange(),
		})
	case "dbms.user.create":
		account := command.account
		var password string
		if account.password != nil {
			password = *account.password
		}
		err = runtime.CreateUserAccount(ctx, auth.FineGrainedAccount{
			Name:                   command.name,
			Password:               password,
			PasswordChangeRequired: account.password != nil && (account.passwordChangeRequired == nil || *account.passwordChangeRequired),
			Providers:              account.externalProviders,
			Suspended:              command.account.suspended != nil && *command.account.suspended,
			HomeDatabase:           securityDirectoryHomeValue(home),
		})
	case "dbms.user.drop":
		err = runtime.DeleteUser(ctx, command.name)
	default:
		return nil, unsupportedAdministrationCommandError(ctx, query)
	}
	if command.ifPresent {
		// IF NOT EXISTS makes an existing target a no-op. A CREATE ROLE AS
		// COPY OF statement still reports a source role that does not exist.
		if command.action == "dbms.role.create" && command.copyOf != nil {
			if errors.Is(err, auth.ErrRoleExists) {
				err = nil
			}
		} else if errors.Is(err, auth.ErrRoleExists) || errors.Is(err, auth.ErrRoleNotFound) ||
			errors.Is(err, auth.ErrUserExists) || errors.Is(err, auth.ErrUserNotFound) {
			err = nil
		}
	}
	return &ExecuteResult{}, err
}

func (e *StorageExecutor) executeShowFineGrainedRoles(ctx context.Context, head string) (*ExecuteResult, error) {
	request := auth.FineGrainedRequestFromContext(ctx)
	if !request.AllowsDBMS("dbms.role.show", "") {
		return nil, auth.ErrFineGrainedForbidden
	}
	result := &ExecuteResult{Columns: []string{"role"}}
	roles := request.Runtime.RoleNames()
	if findMultiWordKeywordIndex(ctx, head, "WITH", "USERS") < 0 {
		for _, name := range roles {
			result.Rows = append(result.Rows, []any{name})
		}
		return result, nil
	}
	if !request.AllowsDBMS("dbms.user.show", "") {
		return nil, auth.ErrFineGrainedForbidden
	}
	result.Columns = append(result.Columns, "member")
	users := request.Runtime.Users()
	for _, role := range roles {
		matched := false
		for _, user := range users {
			if slices.Contains(user.Roles, auth.Role(role)) {
				matched = true
				result.Rows = append(result.Rows, []any{role, user.Username})
			}
		}
		if !matched {
			result.Rows = append(result.Rows, []any{role, nil})
		}
	}
	return result, nil
}
