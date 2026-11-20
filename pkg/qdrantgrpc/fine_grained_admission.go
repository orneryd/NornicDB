package qdrantgrpc

import (
	"context"
	"errors"
	"sync"

	"github.com/orneryd/nornicdb/internal/grpcresponse"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"google.golang.org/grpc/codes"
)

// fineGrainedRPCAction is the declared fine-grained admission of one registered
// RPC method: the action its target requires, and whether that target is one
// database (a Qdrant collection is a database namespace) or the server.
//
// A database action is checked by the database owners of this package
// (AllowDatabaseAccess, VisibleDatabases, AllowAllDatabaseAccess) against the
// collection the request selects, so a method that can select more than one
// database is decided per database. A DBMS action is checked once, before the
// service dispatch, because the operation itself is server-wide.
//
// Default deny: a registered method missing from this inventory is refused for
// a fine-grained principal, exactly as an unclassified method is refused by
// authorizeMethod for RBAC.
type fineGrainedRPCAction struct {
	target auth.FineGrainedResourceKind
	action string
}

func fineGrainedDatabaseAction(action string) fineGrainedRPCAction {
	return fineGrainedRPCAction{target: auth.FineGrainedDatabase, action: action}
}

func fineGrainedDBMSAction(action string) fineGrainedRPCAction {
	return fineGrainedRPCAction{target: auth.FineGrainedDBMS, action: action}
}

// fineGrainedRPCActions maps every registered method to the action that admits
// it. The mapping reuses the action IDs the canonical owners already register:
//
//   - Collection create/update/delete are database create/alter/drop, because a
//     collection is a database namespace.
//   - Point reads need the database the points live in; per-node, per-property
//     and per-embedding decisions stay with request.Graph in the point owners.
//   - Point vector search is the search execution the HTTP search endpoints
//     charge to database.search.execute.
//   - Point count is the population statistic database.stats.read covers.
//   - Field indexes and named-vector configuration are database schema.
//   - Snapshots and cluster/shard configuration are server management, and
//     reflection is server introspection.
var fineGrainedRPCActionsOnce sync.Once
var fineGrainedRPCActionsTable map[string]fineGrainedRPCAction

func fineGrainedRPCActions() map[string]fineGrainedRPCAction {
	fineGrainedRPCActionsOnce.Do(func() {
		fineGrainedRPCActionsTable = map[string]fineGrainedRPCAction{
			"Collections/Create":                       fineGrainedDBMSAction("dbms.database.create"),
			"Collections/Update":                       fineGrainedDBMSAction("dbms.database.alter"),
			"Collections/UpdateAliases":                fineGrainedDBMSAction("dbms.alias.alter"),
			"Collections/Delete":                       fineGrainedDBMSAction("dbms.database.drop"),
			"Collections/CreateShardKey":               fineGrainedDBMSAction("dbms.server.manage"),
			"Collections/DeleteShardKey":               fineGrainedDBMSAction("dbms.server.manage"),
			"Collections/UpdateCollectionClusterSetup": fineGrainedDBMSAction("dbms.server.manage"),
			"Collections/Get":                          fineGrainedDatabaseAction("database.access"),
			"Collections/List":                         fineGrainedDatabaseAction("database.access"),
			"Collections/CollectionExists":             fineGrainedDatabaseAction("database.access"),
			"Collections/CollectionClusterInfo":        fineGrainedDatabaseAction("database.access"),
			"Collections/ListShardKeys":                fineGrainedDatabaseAction("database.access"),
			"Collections/ListAliases":                  fineGrainedDatabaseAction("database.access"),
			"Collections/ListCollectionAliases":        fineGrainedDatabaseAction("database.access"),

			"Points/Get":              fineGrainedDatabaseAction("database.access"),
			"Points/Scroll":           fineGrainedDatabaseAction("database.access"),
			"Points/Count":            fineGrainedDatabaseAction("database.stats.read"),
			"Points/Upsert":           fineGrainedDatabaseAction("database.access"),
			"Points/Delete":           fineGrainedDatabaseAction("database.access"),
			"Points/UpdateBatch":      fineGrainedDatabaseAction("database.access"),
			"Points/SetPayload":       fineGrainedDatabaseAction("database.access"),
			"Points/OverwritePayload": fineGrainedDatabaseAction("database.access"),
			"Points/DeletePayload":    fineGrainedDatabaseAction("database.access"),
			"Points/ClearPayload":     fineGrainedDatabaseAction("database.access"),
			"Points/UpdateVectors":    fineGrainedDatabaseAction("database.access"),
			"Points/DeleteVectors":    fineGrainedDatabaseAction("database.access"),
			"Points/CreateFieldIndex": fineGrainedDatabaseAction("database.schema.manage"),
			"Points/DeleteFieldIndex": fineGrainedDatabaseAction("database.schema.manage"),
			"Points/CreateVectorName": fineGrainedDatabaseAction("database.schema.manage"),
			"Points/DeleteVectorName": fineGrainedDatabaseAction("database.schema.manage"),

			"Points/Search":              fineGrainedDatabaseAction("database.search.execute"),
			"Points/SearchBatch":         fineGrainedDatabaseAction("database.search.execute"),
			"Points/SearchGroups":        fineGrainedDatabaseAction("database.search.execute"),
			"Points/Recommend":           fineGrainedDatabaseAction("database.search.execute"),
			"Points/RecommendBatch":      fineGrainedDatabaseAction("database.search.execute"),
			"Points/RecommendGroups":     fineGrainedDatabaseAction("database.search.execute"),
			"Points/Discover":            fineGrainedDatabaseAction("database.search.execute"),
			"Points/DiscoverBatch":       fineGrainedDatabaseAction("database.search.execute"),
			"Points/Query":               fineGrainedDatabaseAction("database.search.execute"),
			"Points/QueryBatch":          fineGrainedDatabaseAction("database.search.execute"),
			"Points/QueryGroups":         fineGrainedDatabaseAction("database.search.execute"),
			"Points/Facet":               fineGrainedDatabaseAction("database.search.execute"),
			"Points/SearchMatrixPairs":   fineGrainedDatabaseAction("database.search.execute"),
			"Points/SearchMatrixOffsets": fineGrainedDatabaseAction("database.search.execute"),

			"Snapshots/Create":     fineGrainedDBMSAction("dbms.server.manage"),
			"Snapshots/CreateFull": fineGrainedDBMSAction("dbms.server.manage"),
			"Snapshots/List":       fineGrainedDBMSAction("dbms.server.manage"),
			"Snapshots/ListFull":   fineGrainedDBMSAction("dbms.server.manage"),
			"Snapshots/Delete":     fineGrainedDBMSAction("dbms.server.manage"),
			"Snapshots/DeleteFull": fineGrainedDBMSAction("dbms.server.manage"),

			"NornicSearch/SearchText": fineGrainedDatabaseAction("database.search.execute"),

			"ServerReflection/ServerReflectionInfo": fineGrainedDBMSAction("dbms.server.show"),
		}
	})
	return fineGrainedRPCActionsTable
}

// SetFineGrainedAdmission binds the optional fine-grained policy owner before
// the listener starts. It is the same shared callback Bolt and the HTTP server
// use. A nil resolver (fine-grained access control not enabled) leaves the
// existing RBAC principal in place, so installations without it are unaffected.
func (s *Server) SetFineGrainedAdmission(resolve func(context.Context, []string) (*auth.FineGrainedRequest, error)) {
	s.fineGrainedAdmission = resolve
}

// fineGrainedAdmission binds one admitted request's policy snapshot to the
// action the RPC declared. Entity scope (node, property, embedding) remains
// with request.Graph in the point owners.
type fineGrainedAdmission struct {
	request *auth.FineGrainedRequest
	rpc     fineGrainedRPCAction
}

type fineGrainedAdmissionKey struct{}

func fineGrainedAdmissionFromContext(ctx context.Context) *fineGrainedAdmission {
	admission, _ := ctx.Value(fineGrainedAdmissionKey{}).(*fineGrainedAdmission)
	return admission
}

// allowsDatabase reports whether the admitted method may act on database. A
// server-wide method was already admitted against its DBMS action before
// dispatch, so it does not additionally need a grant on each database it
// addresses.
func (a *fineGrainedAdmission) allowsDatabase(database string) bool {
	return a.rpc.target == auth.FineGrainedDBMS || a.request.AllowsDatabase(a.rpc.action, database, "")
}

// fineGrainedDeniedStatus is the single denial status of this owner. It names no
// database, point or grant, so a denial cannot be used to probe the policy.
func fineGrainedDeniedStatus(ctx context.Context, manager *localization.Manager) error {
	return grpcresponse.LocalizedStatus(ctx, manager, codes.PermissionDenied, localization.QdrantPermissionDenied(), grpcresponse.QdrantSourceFallback)
}

// admitFineGrained resolves the request's fine-grained principal and checks the
// method's declared action. A request without a resolver, or one that is not
// authenticated, keeps the RBAC principal. An unclassified method is refused, so
// a newly registered RPC cannot be reached without an explicit action.
func (s *Server) admitFineGrained(ctx context.Context, fullMethod string, claims *auth.JWTClaims) (context.Context, error) {
	if s.fineGrainedAdmission == nil || claims == nil {
		return ctx, nil
	}
	key, registered := methodKey(fullMethod)
	rpc, classified := fineGrainedRPCActions()[key]
	if !registered || !classified {
		return nil, fineGrainedDeniedStatus(ctx, s.config.Localizer)
	}
	request, err := s.fineGrainedAdmission(auth.WithRequestIdentity(ctx, auth.RequestIdentity{ID: auth.PrincipalID(claims), Username: claims.Username}), claims.Roles)
	if err == nil && request == nil {
		err = auth.ErrFineGrainedForbidden
	}
	if err != nil {
		// An unreadable or invalid policy admits nobody. The cause stays visible
		// to the operator without being disclosed to the client.
		if s.config.Logger != nil {
			s.config.Logger.Error("qdrant grpc: fine-grained admission failed", "method", key, "error", err)
		}
		return nil, fineGrainedDeniedStatus(ctx, s.config.Localizer)
	}
	if request.PasswordChangeRequired {
		return nil, fineGrainedDeniedStatus(ctx, s.config.Localizer)
	}
	if rpc.target == auth.FineGrainedDBMS && !request.AllowsDBMS(rpc.action, "") {
		return nil, fineGrainedDeniedStatus(ctx, s.config.Localizer)
	}
	ctx = auth.WithFineGrainedRequest(ctx, request)
	return context.WithValue(ctx, fineGrainedAdmissionKey{}, &fineGrainedAdmission{request: request, rpc: rpc}), nil
}

// pointStore binds one collection's engine to the request's fine-grained graph,
// so every direct point read and write is decided per node, per property and per
// embedding by the auth owner. Without a principal the collection engine stays
// the owner.
//
// The returned view is private to this request: it must never be handed to the
// shared search provider or published as a database's index, because that owner
// indexes and answers for the whole database, not for one principal.
func (s *PointsService) pointStore(ctx context.Context, collection string, store storage.Engine) storage.Engine {
	if store == nil {
		return nil
	}
	request := auth.FineGrainedRequestFromContext(ctx)
	if request == nil {
		return store
	}
	return request.Graph(store, collection)
}

// pointWriteFailed maps a scoped point write's failure. A fine-grained denial is
// the owner's denial status, not a retryable server failure, so a refused write
// is reported as the authorization outcome it is. Every other cause keeps the
// operation's own message.
func (s *PointsService) pointWriteFailed(ctx context.Context, cause error, message localization.Message) error {
	if errors.Is(cause, auth.ErrFineGrainedForbidden) {
		return fineGrainedDeniedStatus(ctx, s.config.Localizer)
	}
	return grpcresponse.LocalizedStatus(ctx, s.config.Localizer, codes.Internal, message, grpcresponse.QdrantSourceFallback)
}
