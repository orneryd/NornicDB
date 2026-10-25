package cypher

import (
	"context"
	"errors"
	"strings"

	"github.com/orneryd/nornicdb/pkg/auth"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/multidb"
)

// PermissionRequirements describes the entitlements required to execute a query.
// Procedure requirements come from the registered procedure contract, while query
// requirements preserve the established top-level Cypher classification.
type PermissionRequirements struct {
	Read   bool
	Write  bool
	Schema bool
	Admin  bool
	// BoostedProcedures name the procedures the query calls when they alone
	// make the query need administrator rights: a role holding EXECUTE
	// BOOSTED PROCEDURE for every one of them may run the query without
	// them.
	BoostedProcedures []string
}

// AdminGrantedBy reports whether a role's procedure grants satisfy the
// query's administrator requirement: it needs admin only for the procedures
// it calls, and allowsBoosted grants every one of them boosted.
func (r PermissionRequirements) AdminGrantedBy(allowsBoosted func(procedure string) bool) bool {
	if !r.Admin || len(r.BoostedProcedures) == 0 || allowsBoosted == nil {
		return false
	}
	for _, procedure := range r.BoostedProcedures {
		if !allowsBoosted(procedure) {
			return false
		}
	}
	return true
}

// QueryPermissionRequirements returns the permissions required by a query and
// a registered procedure invoked by that query, when present.
func QueryPermissionRequirements(ctx context.Context, query string) PermissionRequirements {
	query = classificationText(query)
	requirements := PermissionRequirements{
		Read:   true,
		Schema: isSchemaPermissionQuery(query),
		Admin:  isAdminPermissionQuery(query),
	}
	if !requirements.Schema && !requirements.Admin {
		_, body := parseExecutionMode(query)
		contract, _ := readStatementExecutionContract(ctx, body)
		requirements.Write = contract.writes
	}

	// Every procedure the query calls counts, not just the first: a later
	// CALL (chained after YIELD, or inside a subquery) needs its own mode's
	// permission too. A procedure needs admin exactly when its Admin flag
	// says so (the flag SHOW PROCEDURES lists, as in Neo4j); its mode says
	// only whether it reads, writes or changes the schema.
	// An administration command needs admin whatever it calls; otherwise
	// the admin procedures the query calls are listed for procedure grants.
	adminCommand := requirements.Admin
	for _, procedure := range RegisteredProceduresForCalls(ctx, query) {
		switch procedure.Mode {
		case ProcedureModeWrite:
			requirements.Write = true
		case ProcedureModeSchema:
			requirements.Schema = true
		}
		if procedure.Admin {
			if !adminCommand {
				requirements.BoostedProcedures = append(requirements.BoostedProcedures, procedure.Name)
			}
			requirements.Admin = true
		}
	}
	// A write that reads nothing of the graph (CREATE, and SET / DELETE of
	// what it created) needs write but not read, as Neo4j lets a principal
	// with WRITE but no READ privilege run CREATE. Any other statement reads.
	if requirements.Write && !requirements.Schema && !requirements.Admin && !statementReadsGraphText(query) {
		requirements.Read = false
	}
	return requirements
}

// RegisteredProceduresForCalls returns the registered procedures named by every
// grammar-owned CALL clause, including nested subqueries. Strings and comments
// do not add calls; quoted name parts use the same registry identity as execution.
// For example, RegisteredProceduresForCalls(ctx, "CALL db.labels()") resolves
// the declaration listed by SHOW PROCEDURES without exposing its handler.
func RegisteredProceduresForCalls(ctx context.Context, query string) []ProcedureSpec {
	structure, err := cypherantlr.ParseSyntax(ctx, query, cypherantlr.FragmentEntry, cypherVersionFromContext(ctx) == "25")
	if err != nil {
		return nil
	}
	var procedures []ProcedureSpec
	visitStatementProcedureSyntax(structure, nil, func(procedure ProcedureSpec, _ string) bool {
		procedures = append(procedures, procedure)
		return false
	})
	return procedures
}

// visitStatementProcedureSyntax is the single grammar walk for structural
// statement admission and live registered-procedure resolution. inspect observes
// every syntax node; visit observes resolved declarations and may stop the walk.
func visitStatementProcedureSyntax(structure cypherantlr.StatementSyntax, inspect func(cypherantlr.SyntaxNode), visit func(ProcedureSpec, string) bool) bool {
	ensureBuiltInProceduresRegistered()
	stopped := false
	structure.Walk(func(node cypherantlr.SyntaxNode) func() {
		if stopped {
			return nil
		}
		if inspect != nil {
			inspect(node)
		}
		if node.Rule() != cypherantlr.CypherParserRULE_queryCallSt && node.Rule() != cypherantlr.CypherParserRULE_standaloneCall {
			return nil
		}
		name := node.ChildRule(cypherantlr.CypherParserRULE_invocationName).QualifiedName()
		if procedure, found := globalProcedureRegistry.Get(name); found {
			stopped = visit(procedure.Spec, node.Text())
			return nil
		}
		for _, procedure := range globalProcedureRegistry.List() {
			if strings.EqualFold(name, procedure.Name) && visit(procedure, node.Text()) {
				stopped = true
				break
			}
		}
		return nil
	})
	return stopped
}

// procedureNameAt parses the dotted procedure name after a CALL keyword
// ending at offset: segments of identifier characters or backticked text,
// separated by dots, with whitespace allowed around them.
func procedureNameAt(query string, offset int) (string, bool) {
	i := offset
	skipSpace := func() {
		for i < len(query) && isASCIISpace(query[i]) {
			i++
		}
	}
	// Keep the ordinary dotted name as a source slice. Only quoted parts
	// or spaces around separators need normalization, shared by auth and CALL.
	var normalized strings.Builder
	normalizing := false
	startName, endName, components := offset, offset, 0
	expectComponent := false
	for {
		skipSpace()
		if i >= len(query) {
			break
		}
		partStart := i
		part, next, ok := scanIdentifierToken(query, i)
		if !ok {
			if expectComponent {
				return "", false
			}
			break
		}
		expectComponent = false
		quoted := isCypherBacktickByte(query[i])
		i = next
		if components == 0 {
			startName = partStart
		}
		if !normalizing && (quoted || components > 0 && partStart != endName+1) {
			if components > 0 {
				normalized.WriteString(query[startName:endName])
			}
			normalizing = true
		}
		if normalizing {
			if components > 0 {
				normalized.WriteByte('.')
			}
			normalized.WriteString(part)
		}
		endName = i
		components++
		skipSpace()
		if i < len(query) && query[i] == '.' {
			i++
			expectComponent = true
			continue
		}
		break
	}
	if components == 0 {
		return "", false
	}
	if normalizing {
		return normalized.String(), true
	}
	return query[startName:endName], true
}

func isSchemaPermissionQuery(query string) bool {
	commandOffset := firstExecutableCypherOffset(query)
	for _, command := range [][2]string{
		{"CREATE", "INDEX"},
		{"CREATE", "RANGE INDEX"},
		{"CREATE", "FULLTEXT INDEX"},
		{"CREATE", "VECTOR INDEX"},
		{"DROP", "INDEX"},
		{"CREATE", "CONSTRAINT"},
		{"DROP", "CONSTRAINT"},
	} {
		if findMultiWordKeywordIndex(query, command[0], command[1]) == commandOffset {
			return true
		}
	}
	return false
}

func isAdminPermissionQuery(query string) bool {
	commandOffset := firstExecutableCypherOffset(query)
	// GRANT, REVOKE, DENY and SHOW … PRIVILEGES administer privileges.
	if commandOffset >= 0 && commandOffset < len(query) && isPrivilegeAdministrationCommand(query[commandOffset:]) {
		return true
	}
	// CREATE OR REPLACE DATABASE: the pair scan below misses the OR REPLACE
	// between CREATE and DATABASE.
	if isCreateOrReplaceDatabaseQuery(query) {
		return true
	}
	for _, command := range [][2]string{
		{"SHOW", "USERS"},
		{"SHOW", "TRANSACTIONS"},
		{"SHOW", "TRANSACTION"},
		{"TERMINATE", "TRANSACTIONS"},
		{"TERMINATE", "TRANSACTION"},
	} {
		if findMultiWordKeywordIndex(query, command[0], command[1]) == commandOffset {
			return true
		}
	}
	for _, command := range [][2]string{
		{"CREATE", "DATABASE"},
		{"DROP", "DATABASE"},
		{"ALTER", "DATABASE"},
		{"CREATE", "COMPOSITE DATABASE"},
		{"DROP", "COMPOSITE DATABASE"},
		{"ALTER", "COMPOSITE DATABASE"},
		{"CREATE", "ALIAS"},
		{"DROP", "ALIAS"},
		{"CREATE", "DECAY PROFILE"},
		{"ALTER", "DECAY PROFILE"},
		{"DROP", "DECAY PROFILE"},
		{"CREATE", "PROMOTION PROFILE"},
		{"ALTER", "PROMOTION PROFILE"},
		{"DROP", "PROMOTION PROFILE"},
		{"CREATE", "PROMOTION POLICY"},
		{"ALTER", "PROMOTION POLICY"},
		{"DROP", "PROMOTION POLICY"},
	} {
		if findMultiWordKeywordIndex(query, command[0], command[1]) == commandOffset {
			return true
		}
	}
	return isCreateProcedureCommand(query) || isDropProcedureCommand(query)
}

// isCreateOrReplaceDatabaseQuery reports whether query is
// CREATE OR REPLACE DATABASE <name>.
func isCreateOrReplaceDatabaseQuery(query string) bool {
	commandOffset := firstExecutableCypherOffset(query)
	opts := defaultKeywordScanOpts()
	create := keywordIndexFrom(query, "CREATE", commandOffset, opts)
	if create != commandOffset {
		return false
	}
	orReplace := keywordIndexFrom(query, "OR REPLACE", create+len("CREATE"), opts)
	if orReplace < 0 {
		return false
	}
	database := keywordIndexFrom(query, "DATABASE", orReplace+len("OR REPLACE"), opts)
	return database >= 0
}

func firstExecutableCypherOffset(query string) int {
	index := 0
	if strings.HasPrefix(query, "\xef\xbb\xbf") {
		index = 3
	}
	for {
		for index < len(query) && isASCIISpace(query[index]) {
			index++
		}
		switch {
		case index+1 < len(query) && query[index] == '/' && query[index+1] == '/':
			index += 2
			for index < len(query) && query[index] != '\n' {
				index++
			}
		case index+1 < len(query) && query[index] == '/' && query[index+1] == '*':
			index += 2
			for index+1 < len(query) && (query[index] != '*' || query[index+1] != '/') {
				index++
			}
			if index+1 >= len(query) {
				return len(query)
			}
			index += 2
		default:
			return index
		}
	}
}

func queryKeywords(query string) map[string]bool {
	keywords := make(map[string]bool)
	for index := 0; index < len(query); {
		if end, quoted := skipCypherLexicalToken(query, index); quoted {
			index = end
			continue
		}

		if !isIdentStartByte(query[index]) {
			index++
			continue
		}
		start := index
		index++
		for index < len(query) && isIdentByte(query[index]) {
			index++
		}
		if isQualifiedOrMapKey(query, start, index) {
			continue
		}
		keywords[upperASCII(query[start:index])] = true
	}
	return keywords
}

func isQualifiedOrMapKey(query string, start, end int) bool {
	left := start - 1
	for left >= 0 && isASCIISpace(query[left]) {
		left--
	}
	if left >= 0 && (query[left] == '.' || query[left] == ':' || query[left] == '$') {
		return true
	}
	right := end
	for right < len(query) && isASCIISpace(query[right]) {
		right++
	}
	return right < len(query) && query[right] == ':'
}

// PermissionChecker answers whether the caller holds an entitlement.
type PermissionChecker func(permission string) bool

type permissionCheckerKey struct{}

// DatabasePermissionResolver answers whether the caller holds an entitlement
// for the database where a statement is executing. "access" checks selection
// of a containing composite or alias; data permissions apply to its target.
type DatabasePermissionResolver func(database, permission string) bool

type databaseAuthorization struct {
	database string
	resolver DatabasePermissionResolver
}

type databaseAuthorizationKey struct{}

// WithPermissionChecker attaches a caller's effective entitlements to an
// execution context. Nested dynamic statements inherit the same checker.
func WithPermissionChecker(ctx context.Context, checker PermissionChecker) context.Context {
	if checker == nil {
		return ctx
	}
	return context.WithValue(ctx, permissionCheckerKey{}, checker)
}

// WithDatabasePermissionResolver attaches database-scoped entitlements to an
// execution context. USE routing updates the selected database automatically.
func WithDatabasePermissionResolver(ctx context.Context, database string, resolver DatabasePermissionResolver) context.Context {
	if resolver == nil {
		return ctx
	}
	return context.WithValue(ctx, databaseAuthorizationKey{}, databaseAuthorization{
		database: database,
		resolver: resolver,
	})
}

func withExecutionDatabase(ctx context.Context, database string) context.Context {
	ctx = context.WithValue(ctx, ctxKeyUseDatabase, database)
	authorization, ok := ctx.Value(databaseAuthorizationKey{}).(databaseAuthorization)
	if !ok {
		return ctx
	}
	authorization.database = database
	return context.WithValue(ctx, databaseAuthorizationKey{}, authorization)
}

// GraphDatabaseResolver is the catalog view needed to resolve graph selections.
// It accepts native typed constituent snapshots or the legacy generic adapter.
type GraphDatabaseResolver interface {
	IsCompositeDatabase(string) bool
	ResolveDatabase(string) (string, error)
}

// AccessDatabases returns the databases a principal must be allowed to use
// for a statement on database, the graph name it selects (a request or
// session database, a USE, :USE or subquery USE, a composite constituent):
//   - a composite constituent "composite.alias": the composite and the
//     constituent's database (the full name when the composite has no such
//     alias);
//   - any other name: the name and, when it is an alias, the database it
//     names.
//
// The executor checks each one (authorizeSelectedDatabase), and Bolt and
// HTTP check a request's or session's database with the same list, so a
// graph is allowed or refused the same way however a statement selects it.
// manager may be nil (no database manager): the name alone. When the
// composite's constituents can't be read, the list is the composite and the
// error is returned with it.
func AccessDatabases(manager GraphDatabaseResolver, database string) ([]string, error) {
	selected, target, _, err := resolveGraphSelection(manager, database)
	if err != nil {
		return []string{selected}, err
	}
	if selected != target || selected != database {
		return []string{selected, target}, nil
	}
	return []string{target}, nil
}

// resolveGraphSelection owns selection/target resolution for both list consumers
// and direct graph checks. Remote graph names never resolve locally.
func resolveGraphSelection(manager GraphDatabaseResolver, database string) (selected, target string, remote bool, err error) {
	if manager == nil {
		return database, database, false, nil
	}
	if dot := strings.IndexByte(database, '.'); dot > 0 && manager.IsCompositeDatabase(database[:dot]) {
		composite := database[:dot]
		// Native adapters can return typed snapshots without boxing each reference.
		if typed, ok := manager.(interface {
			GetCompositeConstituents(string) ([]multidb.ConstituentRef, error)
		}); ok {
			refs, err := typed.GetCompositeConstituents(composite)
			if err != nil {
				return composite, database, false, err
			}
			for _, ref := range refs {
				if strings.EqualFold(ref.Alias, database[dot+1:]) {
					return composite, ref.DatabaseName, ref.Type == "remote", nil
				}
			}
		} else if legacy, ok := manager.(interface {
			GetCompositeConstituents(string) ([]interface{}, error)
		}); ok {
			refs, err := legacy.GetCompositeConstituents(composite)
			if err != nil {
				return composite, database, false, err
			}
			for _, item := range refs {
				if ref, ok := toConstituentRef(item); ok && strings.EqualFold(ref.Alias, database[dot+1:]) {
					return composite, ref.DatabaseName, ref.Type == "remote", nil
				}
			}
		} else {
			return composite, database, false, errors.New("composite constituents unavailable")
		}
		return composite, database, false, nil
	}
	if resolved, err := manager.ResolveDatabase(database); err == nil {
		return database, resolved, false, nil
	}
	return database, database, false, nil
}

// GraphAccess decides the selected and physical database using the same owner
// as AccessDatabases, without allocating a list for a single graph decision.
func GraphAccess(manager GraphDatabaseResolver, graph string, canAccess func(string) bool) (string, bool) {
	database, _, allowed := GraphAccessTarget(manager, graph, canAccess)
	return database, allowed
}

// GraphAccessTarget also identifies remote constituents, so direct readers do
// not copy parent metadata again or reinterpret foreign names as local aliases.
func GraphAccessTarget(manager GraphDatabaseResolver, graph string, canAccess func(string) bool) (database string, remote, allowed bool) {
	selected, target, remote, err := resolveGraphSelection(manager, graph)
	if err != nil {
		return graph, remote, false
	}
	if canAccess != nil && (!canAccess(selected) || selected != target && !canAccess(target)) {
		return target, remote, false
	}
	return target, remote, true
}

// authorizeSelectedDatabase owns the selection and permission decision for a
// statement or fragment. Each containing composite or alias needs access;
// the physical target needs the statement's data permissions. An empty name
// selects the request's execution database from the context.
func (e *StorageExecutor) authorizeSelectedDatabase(ctx context.Context, database, query string) error {
	authorization, ok := ctx.Value(databaseAuthorizationKey{}).(databaseAuthorization)
	if !ok {
		return AuthorizeQuery(ctx, query)
	}
	selectionOnly := false
	if database != "" {
		_, body := parseExecutionMode(query)
		body = body[firstExecutableCypherOffset(body):]
		// A shell selection can precede another USE or a Fabric statement.
		// Those statements decide data permissions on their own targets.
		selectionOnly = startsWithKeywordFold(body, "USE") ||
			e.dbManager != nil && e.dbManager.IsCompositeDatabase(database) && e.shouldUseFabricPlanner(body)
	}
	if database == "" {
		database = authorization.database
		if database == "" {
			database = e.currentDatabaseName()
		}
	}
	var manager DatabaseManagerInterface
	if e.dbManager != nil {
		manager = e.dbManager
	}
	databases, listErr := AccessDatabases(manager, database)
	for i, selected := range databases {
		if selectionOnly || i < len(databases)-1 && listErr == nil {
			if err := authorizeDatabase(ctx, selected, "access"); err != nil {
				return err
			}
			continue
		}
		if err := AuthorizeQuery(withExecutionDatabase(ctx, selected), query); err != nil {
			return err
		}
	}
	return listErr
}

// PermissionDeniedError identifies the entitlement needed for a query.
type PermissionDeniedError struct {
	Permission string
}

// BoltErrorCode is the Neo4j status of a permission failure, on Bolt and
// HTTP alike (nornicerrors.Neo4jStatus).
func (e *PermissionDeniedError) BoltErrorCode() string {
	return "Neo.ClientError.Security.Forbidden"
}

// StatusMessage is the message clients see for a permission failure: the
// missing permission only, whatever statement or procedure it was found in.
func (e *PermissionDeniedError) StatusMessage() string {
	return e.Error()
}

func (e *PermissionDeniedError) Error() string {
	switch e.Permission {
	case "schema":
		return "Schema operations require schema permission"
	case "admin":
		return "Admin operations require admin permission"
	case "write":
		return "Write operations require write permission"
	default:
		return "Read operations require read permission"
	}
}

// statementAdmittedKey carries WithStatementAdmitted's executor and callback.
type statementAdmittedKey struct{}

type statementAdmission struct {
	executor *StorageExecutor
	admitted func()
}

// WithStatementAdmitted runs admitted when executor has authorized a
// statement on its own database (the decision Execute and nested execution
// take before running one, authorizeSelectedDatabase) and before that
// statement runs; it can run once per statement. A
// caller that builds an executor for a statement uses it to finish and share
// that executor only once the principal is admitted there. Other executors
// the statement reaches (a USE target, a nested statement) don't run it.
func WithStatementAdmitted(ctx context.Context, executor *StorageExecutor, admitted func()) context.Context {
	if executor == nil || admitted == nil {
		return ctx
	}
	return context.WithValue(ctx, statementAdmittedKey{}, statementAdmission{executor: executor, admitted: admitted})
}

// statementAdmitted runs the WithStatementAdmitted callback bound to e.
func (e *StorageExecutor) statementAdmitted(ctx context.Context) {
	if admission, ok := ctx.Value(statementAdmittedKey{}).(statementAdmission); ok && admission.executor == e {
		admission.admitted()
	}
}

// procedureGrantCheckerKey carries the principal's boosted procedure grants
// (WithProcedureGrantChecker).
type procedureGrantCheckerKey struct{}

// WithProcedureGrantChecker attaches allowsBoosted, which reports whether the
// principal's roles hold EXECUTE BOOSTED PROCEDURE for a procedure: a query
// that needs administrator rights only for the procedure it calls is then
// authorized without them (AuthorizeQuery).
func WithProcedureGrantChecker(ctx context.Context, allowsBoosted func(procedure string) bool) context.Context {
	if allowsBoosted == nil {
		return ctx
	}
	return context.WithValue(ctx, procedureGrantCheckerKey{}, allowsBoosted)
}

// AuthorizeQuery enforces requirements when an entitlement checker is present.
// Top-level and nested execution boundaries both call it so dynamically
// supplied procedure statements cannot bypass transport authorization.
func AuthorizeQuery(ctx context.Context, query string) error {
	hasPermission := contextPermissions(ctx)
	if hasPermission == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	requirements := QueryPermissionRequirements(ctx, query)
	// An interrupted structural reader has no complete permission verdict.
	// Never authorize its declined write classification as a read statement.
	if err := ctx.Err(); err != nil {
		return err
	}
	allowsBoosted, _ := ctx.Value(procedureGrantCheckerKey{}).(func(string) bool)
	if missing := MissingPermission(requirements, hasPermission, allowsBoosted); missing != "" {
		return &PermissionDeniedError{Permission: missing}
	}
	return nil
}

// contextPermissions is the principal's permission check the context
// carries (WithDatabasePermissionResolver for the statement's database, else
// WithPermissionChecker), or nil when it carries none (no authorization).
func contextPermissions(ctx context.Context) func(permission string) bool {
	if databaseAuth, ok := ctx.Value(databaseAuthorizationKey{}).(databaseAuthorization); ok {
		return func(permission string) bool {
			return databaseAuth.resolver(databaseAuth.database, permission)
		}
	}
	if checker, _ := ctx.Value(permissionCheckerKey{}).(PermissionChecker); checker != nil {
		return checker
	}
	return nil
}

// principalPermissions is the permission check of the statement's principal,
// for what a principal sees rather than what it may run (dbms.listConnections
// lists other users' connections only to a principal holding admin): the
// transport's resolver (contextPermissions); without one (an embedded
// caller), the signed-in user's roles, resolved as HTTP and Bolt resolve an
// entitlement (auth.PermissionsForRoles, auth.EntitlementsAllow) with no
// stored entitlements, which the executor doesn't have, so the built-in
// roles' defaults decide. It is nil when the statement has neither a resolver
// nor a signed-in user: the executor authorizes nothing for that caller.
func principalPermissions(ctx context.Context) func(permission string) bool {
	if allows := contextPermissions(ctx); allows != nil {
		return allows
	}
	user, signedIn := authenticatedUserFromContext(ctx)
	if !signedIn {
		return nil
	}
	entitlements := auth.PermissionsForRoles(user.Roles, nil)
	return func(permission string) bool { return auth.EntitlementsAllow(entitlements, permission) }
}

// MissingPermission is the entitlement decision used by statement execution
// (AuthorizeQuery) and non-Cypher consumers: the first
// permission requirements need that allows denies, in Neo4j's order
// (schema, admin, write, read), or "" when allows grants them all. A query
// that needs admin only for procedures that allowsBoosted grants every one
// of boosted needs no admin (AdminGrantedBy).
func MissingPermission(requirements PermissionRequirements, allows func(permission string) bool, allowsBoosted func(procedure string) bool) string {
	if requirements.Admin && !allows("admin") && requirements.AdminGrantedBy(allowsBoosted) {
		requirements.Admin = false
	}
	for _, permission := range []struct {
		name     string
		required bool
	}{
		{name: "schema", required: requirements.Schema},
		{name: "admin", required: requirements.Admin},
		{name: "write", required: requirements.Write},
		{name: "read", required: requirements.Read},
	} {
		if permission.required && !allows(permission.name) {
			return permission.name
		}
	}
	return ""
}

// PrincipalAccess is the one permission resolver for a principal, which
// HTTP and Bolt both build from their stores: whether the principal holds a
// permission on a graph.
type PrincipalAccess struct {
	// GraphAccess resolves a graph name (a database, alias or composite
	// constituent) to the database its data is in, and reports whether the
	// principal may use it; nil allows every graph.
	GraphAccess func(graph string) (database string, allowed bool)
	// DatabaseAccess is the principal's read and write on a database; nil
	// leaves read and write to Entitlement.
	DatabaseAccess func(database string) auth.ResolvedAccess
	// Entitlement reports a global entitlement (schema, admin, and read and
	// write when DatabaseAccess is nil); nil holds every one.
	Entitlement func(permission string) bool
}

// Allows reports whether the principal holds permission on graph: it may use
// the graph, read and write come from its access to the database the graph's
// data is in, and the other permissions from its global entitlements.
func (p PrincipalAccess) Allows(graph, permission string) bool {
	database := graph
	if p.GraphAccess != nil {
		target, allowed := p.GraphAccess(graph)
		if !allowed {
			return false
		}
		database = target
	}
	if permission == "access" {
		return true
	}
	if p.DatabaseAccess != nil && (permission == "read" || permission == "write") {
		access := p.DatabaseAccess(database)
		if permission == "read" {
			return access.Read
		}
		return access.Write
	}
	return p.Entitlement == nil || p.Entitlement(permission)
}

// authorizeDatabase requires permission on database when the context
// carries database-scoped entitlements, so a procedure that names another
// database (gds.graph.drop's dbName) is checked there, not only where the
// statement runs.
func authorizeDatabase(ctx context.Context, database, permission string) error {
	authorization, ok := ctx.Value(databaseAuthorizationKey{}).(databaseAuthorization)
	if !ok || authorization.resolver(database, permission) {
		return nil
	}
	return &PermissionDeniedError{Permission: permission}
}
