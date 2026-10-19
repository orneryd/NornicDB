package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/auth"
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
func QueryPermissionRequirements(query string) PermissionRequirements {
	query = classificationText(query)
	keywords := queryKeywords(query)
	requirements := PermissionRequirements{
		Read:   true,
		Schema: isSchemaPermissionQuery(query),
		Admin:  isAdminPermissionQuery(query),
	}
	if !requirements.Schema && !requirements.Admin {
		requirements.Write = keywords["CREATE"] || keywords["DELETE"] || keywords["SET"] ||
			keywords["MERGE"] || keywords["REMOVE"]
	}

	// Every procedure the query calls counts, not just the first: a later
	// CALL (chained after YIELD, or inside a subquery) needs its own mode's
	// permission too. A procedure needs admin exactly when its Admin flag
	// says so (the flag SHOW PROCEDURES lists, as in Neo4j); its mode says
	// only whether it reads, writes or changes the schema.
	// An administration command needs admin whatever it calls; otherwise
	// the admin procedures the query calls are listed for procedure grants.
	adminCommand := requirements.Admin
	for _, procedure := range registeredProceduresForCalls(query) {
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
	// GDS aggregation projection writes the same catalog as the procedure.
	_ = forEachFunctionCall(query, func(_, normalized string) error {
		if normalized == "gds.graph.project" {
			requirements.Write = true
		}
		return nil
	})
	return requirements
}

// registeredProceduresForCalls returns the registered procedures named by
// every CALL clause of query, wherever it stands (after another CALL's
// YIELD, in a CALL { } / EXISTS { } / COUNT { } subquery, ...). CALL inside
// string literals and comments is not a clause. Backticked name parts are
// unquoted, as the executor resolves them.
func registeredProceduresForCalls(query string) []ProcedureSpec {
	var procedures []ProcedureSpec
	visitRegisteredProcedureCalls(query,func(procedure ProcedureSpec,_ string) bool {
		procedures = append(procedures,procedure)
		return false
	})
	return procedures
}

// visitRegisteredProcedureCalls is the registry-backed CALL reader shared by
// authorization and transaction admission. A true visit stops the scan without
// materializing a list of complete procedure specifications.
func visitRegisteredProcedureCalls(query string,visit func(ProcedureSpec,string)bool) bool {
	ensureBuiltInProceduresRegistered()
	opts := defaultKeywordScanOpts()
	opts.SkipParens = false
	opts.SkipBrackets = false
	opts.SkipBraces = false
	for from := 0; from < len(query); {
		at := keywordIndexFrom(query, "CALL", from, opts)
		if at < 0 {
			break
		}
		from = at + len("CALL")
		name, ok := procedureNameAt(query, from)
		if !ok {
			continue // CALL { ... } or CALL (vars) { ... }: its body is scanned on
		}
		if procedure, found := globalProcedureRegistry.Get(name); found {
			if visit(procedure.Spec,query[at:]) { return true }
			continue
		}
		// Execution resolves exact names. Preserve conservative authorization
		// for an invalid case variant without making it executable: every
		// matching contract counts, since case-distinct registrations can
		// require different permissions. An exact match above remains decisive.
		for _, procedure := range globalProcedureRegistry.List() {
			if strings.EqualFold(name, procedure.Name) {
				if visit(procedure,query[at:]) { return true }
			}
		}
	}
	return false
}

// callsDataWriteProcedure uses the registered contract shared with authorization
// to decide whether procedure calls need statement-level write atomicity.
func callsDataWriteProcedure(query string,parameters ...map[string]interface{}) bool {
	var params map[string]interface{}
	if len(parameters)>0 { params = parameters[0] }
	return procedureCallsWriteData(query,params,0)
}

func procedureCallsWriteData(query string,params map[string]interface{},depth int) bool {
	return visitRegisteredProcedureCalls(query,func(procedure ProcedureSpec,call string) bool {
		if procedure.Mode == ProcedureModeWrite { return true }
		contract := procedure.Statement
		if contract == nil { return false }
		if depth >= 16 { return true }
		arguments := explicitProcedureArgumentTexts(call)
		if contract.QueryIndex >= len(arguments) { return true }
		statement,known := procedureStatementText(arguments[contract.QueryIndex],params)
		// Row-dependent or otherwise unresolved executable text may write. It
		// must enter the statement owner before evaluating that expression.
		if !known { return true }
		if analyzeQuery(classificationText(statement)).IsWriteQuery { return true }
		var innerParams map[string]interface{}
		if index := contract.ParamsIndex; index >= 0 && index < len(arguments) {
			expression := strings.TrimSpace(arguments[index])
			if strings.HasPrefix(expression,"$") {
				if value,ok := resolveSetMergeSourceFromParams(params,expression[1:]); ok { innerParams,_ = value.(map[string]interface{}) }
			}
		}
		return procedureCallsWriteData(statement,innerParams,depth+1)
	})
}

func procedureStatementText(expression string,params map[string]interface{}) (string,bool) {
	expression = strings.TrimSpace(expression)
	if isWholeCypherQuotedString(expression) { return decodeCypherQuotedString(expression) }
	if strings.HasPrefix(expression,"$") {
		if value,ok := resolveSetMergeSourceFromParams(params,expression[1:]); ok {
			statement,ok := value.(string)
			return statement,ok
		}
	}
	return "",false
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
	for {
		skipSpace()
		if i >= len(query) {
			break
		}
		partStart := i
		quoted := query[i] == '`'
		var part string
		if quoted {
			end := strings.IndexByte(query[i+1:], '`')
			if end < 0 {
				return "", false
			}
			part = query[i+1 : i+1+end]
			i += end + 2
		} else {
			for i < len(query) && (query[i] == '_' || query[i] == '$' || query[i] >= '0' && query[i] <= '9' || query[i] >= 'a' && query[i] <= 'z' || query[i] >= 'A' && query[i] <= 'Z') {
				i++
			}
			if i == partStart {
				break
			}
			part = query[partStart:i]
		}
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
		switch query[index] {
		case '/', '\'', '"', '`':
			if query[index] == '/' && index+1 < len(query) && query[index+1] == '/' {
				index += 2
				for index < len(query) && query[index] != '\n' {
					index++
				}
				continue
			}
			if query[index] == '/' && index+1 < len(query) && query[index+1] == '*' {
				index += 2
				for index+1 < len(query) && (query[index] != '*' || query[index+1] != '/') {
					index++
				}
				if index+1 < len(query) {
					index += 2
				}
				continue
			}
			if query[index] != '/' {
				quote := query[index]
				index++
				for index < len(query) {
					if query[index] == '\\' && quote != '`' && index+1 < len(query) {
						index += 2
						continue
					}
					if query[index] == quote {
						if index+1 < len(query) && query[index+1] == quote {
							index += 2
							continue
						}
						index++
						break
					}
					index++
				}
				continue
			}
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
func AccessDatabases(manager DatabaseManagerInterface, database string) ([]string, error) {
	if manager == nil {
		return []string{database}, nil
	}
	if dot := strings.IndexByte(database, '.'); dot > 0 && manager.IsCompositeDatabase(database[:dot]) {
		composite := database[:dot]
		constituents, err := manager.GetCompositeConstituents(composite)
		if err != nil {
			return []string{composite}, err
		}
		for _, item := range constituents {
			if ref, ok := toConstituentRef(item); ok && strings.EqualFold(ref.Alias, database[dot+1:]) {
				return []string{composite, ref.DatabaseName}, nil
			}
		}
		return []string{composite, database}, nil
	}
	if resolved, err := manager.ResolveDatabase(database); err == nil && resolved != database {
		return []string{database, resolved}, nil
	}
	return []string{database}, nil
}

// GraphAccess decides a request's or session's graph the way the executor
// decides a graph a statement selects: allowed is whether canAccess allows
// every database AccessDatabases lists for graph (a nil canAccess allows
// all), and dataDatabase is the last one, the database graph's data is in,
// whose privileges apply. When the databases can't be read (a composite's
// constituents), access is refused. Bolt and HTTP use it for the database a
// request names.
func GraphAccess(manager DatabaseManagerInterface, graph string, canAccess func(database string) bool) (dataDatabase string, allowed bool) {
	databases, err := AccessDatabases(manager, graph)
	if err != nil {
		return graph, false
	}
	dataDatabase = databases[len(databases)-1]
	for _, database := range databases {
		if canAccess != nil && !canAccess(database) {
			return dataDatabase, false
		}
	}
	return dataDatabase, true
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
	allowsBoosted, _ := ctx.Value(procedureGrantCheckerKey{}).(func(string) bool)
	if missing := MissingPermission(QueryPermissionRequirements(query), hasPermission, allowsBoosted); missing != "" {
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

// RegisteredProcedureForCall resolves a CALL statement through the canonical
// procedure registry without exposing handlers to callers.
func RegisteredProcedureForCall(query string) (ProcedureSpec, bool) {
	ensureBuiltInProceduresRegistered()
	procedure, found := globalProcedureRegistry.Get(extractProcedureName(query))
	if !found {
		return ProcedureSpec{}, false
	}
	return procedure.Spec, true
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
