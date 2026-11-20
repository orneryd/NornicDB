package bolt

import (
	"context"
	"errors"
	"log/slog"

	"github.com/orneryd/nornicdb/internal/catalogadapter"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
)

// SetFineGrainedAdmission binds the optional fine-grained policy owner before
// the listener starts. A nil resolver leaves existing Bolt RBAC in use.
func (s *Server) SetFineGrainedAdmission(resolve func(context.Context, []string) (*auth.FineGrainedRequest, error)) {
	s.fineGrainedAdmission = resolve
}

func (s *Session) fineGrainedPrincipal(ctx context.Context, metadata map[string]any) (*auth.FineGrainedRequest, *auth.User, error) {
	if s.server == nil || s.server.fineGrainedAdmission == nil {
		return nil, nil, nil
	}
	if s.authResult == nil || !s.authResult.Authenticated {
		return nil, nil, auth.ErrFineGrainedForbidden
	}
	identity := s.authResult.PrincipalID
	if identity == "" {
		identity = auth.UsernamePrincipalID(s.authResult.Username)
	}
	ctx = auth.WithRequestIdentity(ctx, auth.RequestIdentity{ID: identity, Username: s.authResult.Username})
	request, err := s.server.fineGrainedAdmission(ctx, s.authResult.Roles)
	if err != nil {
		return nil, nil, err
	}
	if request == nil {
		return nil, nil, auth.ErrFineGrainedForbidden
	}
	selected, explicit := metadata["imp_user"]
	if s.inTransaction {
		pinned, hasPin := s.txMetadata["imp_user"].(string)
		if proposed, ok := selected.(string); explicit && (!hasPin || !ok || proposed != pinned) {
			return nil, nil, auth.ErrFineGrainedForbidden
		}
		selected, explicit = pinned, hasPin
	}
	if !explicit {
		return request, nil, nil
	}
	username, valid := selected.(string)
	if !valid || username == "" {
		return nil, nil, auth.ErrFineGrainedForbidden
	}
	if request.Runtime == nil {
		return nil, nil, auth.ErrFineGrainedForbidden
	}
	return request.Runtime.Impersonate(ctx, request, username)
}

func (s *Session) fineGrainedAccess(request *auth.FineGrainedRequest) cypher.PrincipalAccess {
	var manager cypher.GraphDatabaseResolver
	if s.server != nil {
		if db, ok := s.server.dbManager.(*multidb.DatabaseManager); ok {
			manager = &boltDatabaseManagerAdapter{IdentifiedExecutionCatalog: catalogadapter.ForIdentifiedExecution(db)}
		}
	}
	return cypher.FineGrainedCypherAccess(manager, request)
}

func (s *Session) withFineGrainedIdentity(ctx context.Context, principal *auth.FineGrainedRequest, user *auth.User) context.Context {
	ctx = auth.WithFineGrainedRequest(ctx, principal)
	if user == nil {
		return ctx
	}
	identity := *s.requestIdentity()
	roles := make([]string, len(user.Roles))
	for i, role := range user.Roles {
		roles[i] = string(role)
	}
	identity.User = &cypher.AuthenticatedUser{Name: user.Username, Roles: roles}
	ctx = cypher.WithRequestIdentity(ctx, &identity)
	return cypher.WithAuthenticatedPrincipal(ctx, "sub:"+user.ID)
}

type fineGrainedResultScope struct {
	fingerprint string
	username    string
}

func newFineGrainedResultScope(request *auth.FineGrainedRequest, user *auth.User) fineGrainedResultScope {
	if request == nil {
		return fineGrainedResultScope{}
	}
	scope := fineGrainedResultScope{fingerprint: request.Fingerprint}
	if user != nil {
		scope.username = user.Username
	}
	return scope
}

func (s *Session) validateFineGrainedResult(ctx context.Context, scope fineGrainedResultScope) (*auth.FineGrainedRequest, *auth.User, error) {
	var metadata map[string]any
	if scope.username != "" {
		metadata = map[string]any{"imp_user": scope.username}
	}
	request, user, err := s.fineGrainedPrincipal(ctx, metadata)
	if err != nil || request == nil || request.PasswordChangeRequired || request.Fingerprint != scope.fingerprint {
		return nil, nil, auth.ErrFineGrainedForbidden
	}
	return request, user, nil
}

func (s *Session) resultRowContext(ctx context.Context, scope fineGrainedResultScope) context.Context {
	if scope.fingerprint != "" {
		return ctx
	}
	return s.withSessionIdentity(ctx)
}

var errFineGrainedRouteMetadata = errors.New("invalid Bolt ROUTE metadata")

func (s *Session) fineGrainedRouteDatabase(data []byte) (string, error) {
	if s.server == nil || s.server.fineGrainedAdmission == nil {
		return "", nil
	}
	s.refreshAuthResult()
	metadata := map[string]any{}
	offset := 0
	for field := 0; offset < len(data) && field < 3; field++ {
		value, consumed, err := decodePackStreamValue(data, offset, s.useUTCDateTimeStructs())
		if err != nil {
			return "", err
		}
		offset += consumed
		if field != 2 {
			continue
		}
		switch value := value.(type) {
		case nil:
		case string:
			metadata["db"] = value
		case map[string]any:
			metadata = value
		default:
			return "", errFineGrainedRouteMetadata
		}
	}
	if offset != len(data) {
		return "", errFineGrainedRouteMetadata
	}
	principal, target, err := s.fineGrainedPrincipal(s.parentContext(), metadata)
	if err != nil {
		return "", err
	}
	if principal.PasswordChangeRequired {
		return "", &auth.FineGrainedCredentialsExpired{}
	}
	database, _ := databaseFromMetadata(metadata)
	if database == "" {
		database = s.database
	}
	if database == "" {
		database = principal.HomeDatabaseName
	}
	if !s.fineGrainedAccess(principal).Allows(database, "access") {
		return "", auth.ErrFineGrainedForbidden
	}
	s.auditFineGrainedImpersonation(s.parentContext(), target, "ROUTE")
	return database, nil
}

func (s *Session) auditFineGrainedImpersonation(ctx context.Context, user *auth.User, operation string) {
	if user == nil || s.authResult == nil {
		return
	}
	s.server.logger().LogAttrs(ctx, slog.LevelInfo, "Bolt impersonation admitted",
		slog.String("operation", operation),
		slog.String("authenticated_user", s.authResult.Username),
		slog.String("authenticated_principal", s.authResult.PrincipalID),
		slog.String("executing_user", user.Username),
		slog.String("executing_principal", "sub:"+user.ID))
}
