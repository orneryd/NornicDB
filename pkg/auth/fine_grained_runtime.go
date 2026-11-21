package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// FineGrainedPublicRole applies to all authenticated FGAC principals.
const FineGrainedPublicRole = "public"

// FineGrainedRuntime admits authenticated principals against the same committed
// policy store used by the catalog editor and role administration.
type FineGrainedRuntime struct {
	roles            *RoleStore
	directory        RoleMutationStores
	actions          []FineGrainedAction
	databaseID       FineGrainedDatabaseIdentity
	home             func() string
	databaseOwner    func(string) string
	nativeABAC       bool
	authRuleCompiler func(context.Context, string) (FineGrainedAuthRuleCondition, error)

	mu           sync.Mutex
	revision     string
	policy       *FineGrainedPolicy
	rolesVersion atomic.Uint64
	authRules    []compiledFineGrainedAuthRule
}

// NewFineGrainedRuntime binds the policy store to the set of operations
// implemented by the server and its database manager.
func NewFineGrainedRuntime(roles *RoleStore, actions []FineGrainedAction, databaseID FineGrainedDatabaseIdentity, home func() string) *FineGrainedRuntime {
	return &FineGrainedRuntime{roles: roles, actions: slices.Clone(actions), databaseID: databaseID, home: home}
}

// SetPrivateDatabaseOwner binds the directory's private-database lookup
// before requests can be admitted.
func (runtime *FineGrainedRuntime) SetPrivateDatabaseOwner(owner func(string) string) {
	runtime.databaseOwner = owner
}

// SetNativeTagAuthorization enables policy evaluation of native user tags.
func (runtime *FineGrainedRuntime) SetNativeTagAuthorization(enabled bool) {
	runtime.nativeABAC = enabled
}

// Admit reads the committed revision and captures its immutable policy for
// the authenticated roles. Unknown or unreadable policy prevents admission.
func (runtime *FineGrainedRuntime) Admit(ctx context.Context, roles []string) (*FineGrainedRequest, error) {
	version, err := runtime.refreshFineGrainedRoles(ctx)
	if err != nil {
		return nil, err
	}
	directory := runtime.directory.Users
	identity := RequestIdentityFromContext(ctx)
	provider, _ := ctx.Value(fineGrainedProviderSessionKey{}).(*FineGrainedProviderSession)
	if provider != nil {
		if directory == nil || identity.ID != "sub:"+provider.userID {
			return nil, ErrFineGrainedForbidden
		}
		if err := provider.validate(ctx, directory); err != nil {
			return nil, err
		}
	}
	if directory != nil {
		if err := directory.refreshFineGrainedUsers(ctx); err != nil {
			return nil, err
		}
	}
	catalog, revision, err := runtime.roles.ReadFineGrainedCatalog(ctx)
	if err != nil {
		return nil, err
	}
	runtime.mu.Lock()
	if runtime.policy == nil || runtime.revision != revision {
		compiled, err := CompileFineGrainedPolicy(runtime.actions, catalog)
		if err != nil {
			runtime.mu.Unlock()
			return nil, err
		}
		authRules, err := runtime.compileAuthRules(ctx, catalog.AuthRules)
		if err != nil {
			runtime.mu.Unlock()
			return nil, err
		}
		runtime.policy, runtime.revision, runtime.authRules = compiled, revision, authRules
	}
	policy, authRules := runtime.policy, runtime.authRules
	runtime.mu.Unlock()

	home := runtime.home()
	homeID := runtime.databaseID(home)
	passwordChangeRequired := false
	var tags []string
	if directory != nil && identity.Username != "" && !strings.HasPrefix(identity.ID, "sub:"+clusterSubjectPrefix) {
		user := runtime.activeNativeUser(identity.Username)
		if user == nil || (identity.ID != "" && identity.ID != "sub:"+user.ID &&
			identity.ID != UsernamePrincipalID(user.Username)) {
			return nil, ErrFineGrainedForbidden
		}
		roles = make([]string, len(user.Roles))
		for i, role := range user.Roles {
			roles[i] = string(role)
		}
		passwordChangeRequired = provider == nil && user.PasswordChangeRequired
		tags = user.SecurityTags
		if user.HomeDatabase != "" {
			if id := runtime.databaseID(user.HomeDatabase); id != "" {
				home, homeID = user.HomeDatabase, id
			}
		}
	}
	if provider, ok := runtime.roles.storage.(storage.GraphMutationVersionProvider); ok {
		current, valid := provider.GraphMutationVersion()
		if !valid || current != version {
			// Do not combine roles and policy from different system revisions.
			return nil, ErrFineGrainedForbidden
		}
	}
	normalized := append(slices.Clone(roles), FineGrainedPublicRole)
	for i, role := range normalized {
		normalized[i] = NormalizeRoleName(role)
	}
	if len(authRules) > 0 && (runtime.nativeABAC || provider != nil) && identity.Username != "" &&
		!strings.HasPrefix(identity.ID, "sub:"+clusterSubjectPrefix) {
		input := FineGrainedAuthRuleInput{Context: ctx, Now: time.Now()}
		if provider != nil {
			input.Claims = provider.attributes
			if input.Claims == nil {
				input.Claims = map[string]any{}
			}
		}
		if runtime.nativeABAC {
			input.NativeTags = tags
			if input.NativeTags == nil {
				input.NativeTags = []string{}
			}
		}
		normalized = matchingFineGrainedAuthRuleRoles(authRules, normalized, input)
	}
	slices.Sort(normalized)
	normalized = slices.Compact(normalized)
	material := revision + "\x00" + homeID + "\x00" + identity.ID + "\x00" + strconv.FormatBool(passwordChangeRequired) + "\x00" + strings.Join(normalized, "\x00")
	if runtime.nativeABAC {
		for _, tag := range tags {
			material += "\x00" + strconv.Quote(tag)
		}
	}
	digest := sha256.Sum256([]byte(material))
	access := policy.ForRoles(normalized)
	access.nativeABAC, access.nativeTags = runtime.nativeABAC, tags
	return &FineGrainedRequest{
		Access: access, DatabaseID: runtime.databaseID,
		HomeDatabaseID: homeID, HomeDatabaseName: home, PasswordChangeRequired: passwordChangeRequired, Runtime: runtime, Fingerprint: hex.EncodeToString(digest[:]),
		identity: identity, databaseOwner: runtime.databaseOwner, providerSession: provider, roles: normalized,
	}, nil
}

// EffectiveRoles returns the direct and rule-assigned roles in this admission.
func (r *FineGrainedRequest) EffectiveRoles() []string {
	return slices.Clone(r.roles)
}

// ReadCatalog returns the committed catalog and the revision used to edit it.
func (runtime *FineGrainedRuntime) ReadCatalog(ctx context.Context) (FineGrainedCatalog, string, error) {
	return runtime.roles.ReadFineGrainedCatalog(ctx)
}

// Actions returns the operations registered by this FGAC runtime.
func (runtime *FineGrainedRuntime) Actions() []FineGrainedAction {
	return slices.Clone(runtime.actions)
}

// ReplaceCatalog persists an authorized edit while preserving the built-in
// administrator's ability to manage the policy.
func (runtime *FineGrainedRuntime) ReplaceCatalog(ctx context.Context, revision string, catalog FineGrainedCatalog) error {
	if err := runtime.ValidateAdministratorContext(ctx, catalog); err != nil {
		return err
	}
	return runtime.roles.ReplaceFineGrainedCatalog(ctx, revision, runtime.actions, catalog)
}

// ValidateAdministrator checks the policy-management grant on deployments
// where the policy-management action is registered.
func (runtime *FineGrainedRuntime) ValidateAdministrator(catalog FineGrainedCatalog) error {
	return runtime.ValidateAdministratorContext(context.Background(), catalog)
}

// ValidateAdministratorContext validates the administrator policy using ctx.
func (runtime *FineGrainedRuntime) ValidateAdministratorContext(ctx context.Context, catalog FineGrainedCatalog) error {
	if _, err := runtime.compileAuthRules(ctx, catalog.AuthRules); err != nil {
		return err
	}
	const manage = "dbms.security.policy.manage"
	if !slices.ContainsFunc(runtime.actions, func(action FineGrainedAction) bool { return action.ID == manage }) {
		return nil
	}
	policy, err := CompileFineGrainedPolicy(runtime.actions, catalog)
	if err != nil {
		return err
	}
	if !policy.ForRoles([]string{string(RoleAdmin), FineGrainedPublicRole}).Allows(manage,
		FineGrainedResource{Kind: FineGrainedDBMS}) {
		return fmt.Errorf("%w: the installation administrator requires an explicit policy-management grant", ErrInvalidFineGrainedPolicy)
	}
	return nil
}

// EditCatalog applies a security administration command to the current
// catalog. An edit racing a different editor reports a conflict.
func (runtime *FineGrainedRuntime) EditCatalog(ctx context.Context, edit func(*FineGrainedCatalog) error) error {
	catalog, revision, err := runtime.ReadCatalog(ctx)
	if err != nil {
		return err
	}
	if err := edit(&catalog); err != nil {
		return err
	}
	return runtime.ReplaceCatalog(ctx, revision, catalog)
}

// DefaultFineGrainedCatalog explicitly grants the registered actions to the
// installation administrator on first activation.
func DefaultFineGrainedCatalog(actions []FineGrainedAction) FineGrainedCatalog {
	catalog := FineGrainedCatalog{Assignments: map[string][]string{}}
	for _, action := range actions {
		id := "bootstrap-" + NormalizeRoleName(action.ID)
		catalog.Definitions = append(catalog.Definitions, FineGrainedEntitlement{
			ID: id, Action: action.ID, Scope: FineGrainedScope{Kind: action.ResourceKind},
		})
		catalog.Assignments[string(RoleAdmin)] = append(catalog.Assignments[string(RoleAdmin)], id)
	}
	for _, action := range actions {
		record := FineGrainedPrivilege{Role: FineGrainedPublicRole, Segment: "PROCEDURE(*)", Resource: "database", Graph: "*"}
		scope := FineGrainedScope{Kind: action.ResourceKind}
		switch action.ID {
		case "database.access":
			record.Name, record.Clause, record.Graph, record.Segment = "access", "ACCESS ON HOME DATABASE", "HOME", "database"
			scope.HomeDatabase = true
		case "dbms.procedure.execute":
			record.Name, record.Clause = "execute_procedure", "EXECUTE PROCEDURE * ON DBMS"
		case "dbms.function.execute":
			record.Name, record.Clause, record.Segment = "execute_function", "EXECUTE FUNCTION * ON DBMS", "FUNCTION(*)"
		default:
			continue
		}
		record.Key = FineGrainedPrivilegeKey("", scope.Kind, record.Clause)
		record.Entitlements = []FineGrainedEntitlement{{Action: action.ID, Scope: scope}}
		catalog.Privileges = append(catalog.Privileges, record)
	}

	return catalog
}

// GraphAccessMode is the coarse database-selection adapter. Entity and
// operation decisions still use the same fine-grained request in execution.
func (r *FineGrainedRequest) CanAccessDatabase(database string) bool {
	if database == "system" {
		return true
	}
	if r.databaseOwner != nil {
		if owner := r.databaseOwner(database); owner != "" &&
			owner != r.identity.ID && !r.AllowsDatabase("database.private.access", database, "") {
			return false
		}
	}
	return r.AllowsDatabase("database.access", database, "")
}

// CanSeeDatabase limits database discovery to the principal's accessible
// databases. The metadata database remains available for SHOW CURRENT USER.
func (r *FineGrainedRequest) CanSeeDatabase(database string) bool {
	return r.CanAccessDatabase(database)
}

// FineGrainedCredentialsExpired signals an authenticated native account that
// must change its password before entering ordinary execution.
type FineGrainedCredentialsExpired struct{}

func (*FineGrainedCredentialsExpired) Error() string { return "password change required" }
func (*FineGrainedCredentialsExpired) BoltErrorCode() string {
	return "Neo.ClientError.Security.CredentialsExpired"
}

// ErrFineGrainedImpersonationTarget reports that an authorized actor selected
// an account that cannot be impersonated. It carries no directory details.
var ErrFineGrainedImpersonationTarget = &fineGrainedImpersonationTargetError{}

type fineGrainedImpersonationTargetError struct{}

func (*fineGrainedImpersonationTargetError) Error() string {
	return "the selected account cannot be impersonated"
}

func (*fineGrainedImpersonationTargetError) BoltErrorCode() string {
	return "Neo.ClientError.Statement.ArgumentError"
}

// ErrFineGrainedImpersonatedAdministration rejects an administration update
// made while executing as a different account.
var ErrFineGrainedImpersonatedAdministration = &fineGrainedImpersonatedAdministrationError{}

type fineGrainedImpersonatedAdministrationError struct{}

func (*fineGrainedImpersonatedAdministrationError) Error() string {
	return "administration updates are forbidden while impersonating another account"
}

func (*fineGrainedImpersonatedAdministrationError) BoltErrorCode() string {
	return "Neo.ClientError.Security.Forbidden"
}

// FineGrainedPermissionError maps operation denials to the normal transport
// status without including hidden data or the entire policy in the error.
type FineGrainedPermissionError struct{}

func (*FineGrainedPermissionError) Error() string {
	return "operation is forbidden by fine-grained access control"
}

func (*FineGrainedPermissionError) BoltErrorCode() string {
	return "Neo.ClientError.Security.Forbidden"
}

// GraphMatches reports whether view already protects the given graph engine
// under this request. The execution owner reuses that view for one statement.
func (r *FineGrainedRequest) GraphMatches(view, engine storage.Engine, database string) bool {
	graph, ok := view.(*fineGrainedGraph)
	return ok && (graph == engine || graph.engine == engine) && graph.request == r && graph.database == database
}

type fineGrainedDatabaseActionKey struct{}

// WithFineGrainedDatabaseAction carries an admitted feature action into
// downstream database calls. Every downstream target must satisfy its scope.
func WithFineGrainedDatabaseAction(ctx context.Context, action string) context.Context {
	return context.WithValue(ctx, fineGrainedDatabaseActionKey{}, action)
}

// FineGrainedDatabaseActionAllowed checks the current feature's scope against
// a downstream database. Ordinary requests have no additional action scope.
func FineGrainedDatabaseActionAllowed(ctx context.Context, database string) bool {
	action, bound := ctx.Value(fineGrainedDatabaseActionKey{}).(string)
	if !bound {
		return true
	}
	request := FineGrainedRequestFromContext(ctx)
	return request != nil && request.AllowsDatabase(action, database, "")
}

// WithBoostedProcedure binds the graph privileges of one explicitly authorized
// procedure invocation to its selected database. Database and feature grants
// outside the Neo4j graph and token actions continue to use the caller's roles.
func (r *FineGrainedRequest) WithBoostedProcedure(database, procedure string) *FineGrainedRequest {
	id := r.DatabaseID(database)
	if id == "" || database == "system" || !r.CanAccessDatabase(database) ||
		!r.AllowsDBMS("dbms.procedure.execute_boosted", procedure) {
		return nil
	}
	derived := *r
	derived.Access.boostedDatabase = id
	digest := sha256.Sum256([]byte(r.Fingerprint + "\x00" + id + "\x00" + procedure))
	derived.Fingerprint = hex.EncodeToString(digest[:])
	return &derived
}

// Revalidate checks that a retained operation still has its original policy
// and role membership before publishing further results.
func (r *FineGrainedRequest) Revalidate(ctx context.Context, identity RequestIdentity) error {
	if r.Runtime == nil || r.Fingerprint == "" || r.Runtime.directory.Users == nil || identity.Username == "" {
		return ErrFineGrainedForbidden
	}
	if err := r.Runtime.directory.Users.refreshFineGrainedUsers(ctx); err != nil {
		return err
	}
	user := r.Runtime.activeNativeUser(identity.Username)
	if user == nil ||
		(strings.HasPrefix(identity.ID, "sub:") && identity.ID != "sub:"+user.ID) {
		return ErrFineGrainedForbidden
	}
	roles := make([]string, len(user.Roles))
	for i, role := range user.Roles {
		roles[i] = string(role)
	}
	current, err := r.Runtime.Admit(WithRequestIdentity(r.providerSession.WithContext(ctx), identity), roles)
	if err != nil {
		return err
	}
	if current.Fingerprint != r.Fingerprint {
		return ErrFineGrainedForbidden
	}
	return nil
}

// AllowsAllEntities checks for complete access to one kind of entity in the
// database. Feeds with IDs only require this proof because a deleted record
// cannot be evaluated against its former labels and properties.
func (r *FineGrainedRequest) AllowsAllEntities(action, database string) bool {
	id := r.DatabaseID(database)
	if id == "" || !r.CanAccessDatabase(database) {
		return false
	}
	allowed := false
	for _, grant := range r.Access.grants[action] {
		scope := grant.Scope
		if scope.DatabaseID != "" && scope.DatabaseID != id ||
			scope.HomeDatabase && id != r.HomeDatabaseID {
			continue
		}
		if grant.Deny {
			return false
		}
		allowed = allowed || (len(scope.Types) == 0 && len(scope.EntityIDs) == 0 && scope.Predicate == nil)
	}
	return allowed
}
