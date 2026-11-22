package qdrantgrpc

import (
	"context"
	"fmt"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// fineGrainedActions is the inventory a test policy compiles against: the core
// catalog plus the extension actions the server catalog registers for search
// execution and population statistics.
func fineGrainedActions() []auth.FineGrainedAction {
	actions := auth.FineGrainedCoreActions()
	for _, id := range []string{"database.search.execute", "database.stats.read"} {
		actions = append(actions, auth.FineGrainedAction{ID: id, ResourceKind: auth.FineGrainedDatabase})
	}
	return actions
}

// fineGrainedPrincipal compiles a catalog that grants one test role exactly the
// supplied entitlements and binds it to a request as the runtime does, without a
// policy store.
func fineGrainedPrincipal(t *testing.T, home string, entitlements ...auth.FineGrainedEntitlement) *auth.FineGrainedRequest {
	t.Helper()
	catalog := auth.FineGrainedCatalog{Assignments: map[string][]string{"tester": nil}}
	for i, entitlement := range entitlements {
		entitlement.ID = fmt.Sprintf("grant-%d", i)
		catalog.Definitions = append(catalog.Definitions, entitlement)
		catalog.Assignments["tester"] = append(catalog.Assignments["tester"], entitlement.ID)
	}
	policy, err := auth.CompileFineGrainedPolicy(fineGrainedActions(), catalog)
	require.NoError(t, err)
	return &auth.FineGrainedRequest{
		Access:         policy.ForRoles([]string{"tester"}),
		DatabaseID:     func(name string) string { return name },
		HomeDatabaseID: home,
		Fingerprint:    "test-policy",
	}
}

func fineGrainedOnDatabase(action, database string) auth.FineGrainedEntitlement {
	return auth.FineGrainedEntitlement{Action: action, Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase, DatabaseID: database}}
}

// fineGrainedOnNodes scopes a node action. The policy owner requires a database
// for an entity-scoped grant, because entity IDs are database-local.
func fineGrainedOnNodes(action, database string, scope auth.FineGrainedScope) auth.FineGrainedEntitlement {
	scope.Kind, scope.DatabaseID = auth.FineGrainedNode, database
	return auth.FineGrainedEntitlement{Action: action, Scope: scope}
}

// fineGrainedAdmittingServer builds a server that admits every request through
// the supplied principal, which is the callback shape the runtime provides.
func fineGrainedAdmittingServer(request *auth.FineGrainedRequest) *Server {
	return &Server{
		config: DefaultConfig(),
		fineGrainedAdmission: func(context.Context, []string) (*auth.FineGrainedRequest, error) {
			return request, nil
		},
	}
}

func fineGrainedTestClaims() *auth.JWTClaims {
	return &auth.JWTClaims{Roles: []string{string(auth.RoleAdmin)}}
}

// TestFineGrainedInventoryCoversEveryRegisteredMethod proves the admission
// inventory is complete and current: every registered method is classified,
// nothing is classified that is no longer registered, and every declared action
// exists in the catalog with the resource kind the inventory assumes.
func TestFineGrainedInventoryCoversEveryRegisteredMethod(t *testing.T) {
	descriptor := rule73DescriptorMethods()
	// Reflection is registered separately from the Qdrant service descriptors and
	// is still an admitted, classified method.
	descriptor["ServerReflection/ServerReflectionInfo"] = struct{}{}
	methods := fineGrainedRPCActions()
	require.Len(t, methods, len(descriptor))
	for method := range descriptor {
		_, classified := methods[method]
		require.True(t, classified, "unclassified registered gRPC method %s", method)
	}
	for method := range methods {
		_, registered := descriptor[method]
		require.True(t, registered, "stale fine-grained inventory method %s", method)
	}

	registered := map[string]auth.FineGrainedAction{}
	for _, action := range fineGrainedActions() {
		registered[action.ID] = action
	}
	for method, rpc := range methods {
		action, known := registered[rpc.action]
		require.True(t, known, "%s declares unregistered action %q", method, rpc.action)
		require.Equal(t, rpc.target, action.ResourceKind, "%s declares action %q of another resource kind", method, rpc.action)
	}
}

// TestFineGrainedAdmissionPreservesDisabledRBAC proves an installation without
// fine-grained access control keeps the existing RBAC principal unchanged.
func TestFineGrainedAdmissionPreservesDisabledRBAC(t *testing.T) {
	server := &Server{config: DefaultConfig()}
	ctx := context.Background()
	claims := fineGrainedTestClaims()

	admitted, err := server.admitFineGrained(ctx, "/qdrant.Points/Get", claims)
	require.NoError(t, err)
	require.Equal(t, ctx, admitted, "no resolver keeps the request context as it was")
	require.Nil(t, fineGrainedAdmissionFromContext(admitted))
	require.Nil(t, auth.FineGrainedRequestFromContext(admitted))

	// RBAC admission is unchanged and still the only gate.
	require.NoError(t, server.authorizeMethod(ctx, "/qdrant.Points/Get", claims))
	require.Equal(t, codes.PermissionDenied, status.Code(server.authorizeMethod(ctx, "/qdrant.Points/Get",
		&auth.JWTClaims{Roles: []string{string(auth.RoleNone)}})))
	require.NoError(t, server.AllowDatabaseAccess(ctx, "any", false), "no database admission is configured")
}

// TestFineGrainedAdmissionClassifiesMethodAndDeclaresAction proves admission is
// default-deny for unclassified methods, refuses an action the principal lacks,
// and that a server-wide method does not additionally need a database grant.
func TestFineGrainedAdmissionClassifiesMethodAndDeclaresAction(t *testing.T) {
	ctx := context.Background()
	claims := fineGrainedTestClaims()

	databaseOnly := fineGrainedPrincipal(t, "fgac", fineGrainedOnDatabase("database.access", "fgac"))
	server := fineGrainedAdmittingServer(databaseOnly)

	_, err := server.admitFineGrained(ctx, "/qdrant.Points/Unclassified", claims)
	require.Equal(t, codes.PermissionDenied, status.Code(err), "an unclassified method is refused")

	_, err = server.admitFineGrained(ctx, "/qdrant.Collections/Create", claims)
	require.Equal(t, codes.PermissionDenied, status.Code(err), "a server-wide method needs its DBMS action")

	failing := &Server{config: DefaultConfig(), fineGrainedAdmission: func(context.Context, []string) (*auth.FineGrainedRequest, error) {
		return nil, auth.ErrFineGrainedForbidden
	}}
	_, err = failing.admitFineGrained(ctx, "/qdrant.Points/Get", claims)
	require.Equal(t, codes.PermissionDenied, status.Code(err), "an unusable policy admits nobody")

	// The granted server-wide action admits the method for a database that does
	// not exist yet, because the operation itself is what is authorized.
	creating := fineGrainedPrincipal(t, "fgac", auth.FineGrainedEntitlement{
		Action: "dbms.database.create", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDBMS},
	})
	creatingServer := fineGrainedAdmittingServer(creating)
	admitted, err := creatingServer.admitFineGrained(ctx, "/qdrant.Collections/Create", claims)
	require.NoError(t, err)
	require.Same(t, creating, auth.FineGrainedRequestFromContext(admitted))
	require.NoError(t, creatingServer.AllowDatabaseAccess(admitted, "brand-new", true))
}

// TestFineGrainedAdmissionScopesDatabaseDecisions proves the method's declared
// action decides which collections a principal may use, and that the decision is
// per database rather than per transport.
func TestFineGrainedAdmissionScopesDatabaseDecisions(t *testing.T) {
	ctx := context.Background()
	claims := fineGrainedTestClaims()

	reader := fineGrainedPrincipal(t, "alpha", fineGrainedOnDatabase("database.access", "alpha"))
	server := fineGrainedAdmittingServer(reader)
	admitted, err := server.admitFineGrained(ctx, "/qdrant.Points/Get", claims)
	require.NoError(t, err)

	require.NoError(t, server.AllowDatabaseAccess(admitted, "alpha", false))
	require.NoError(t, server.AllowDatabaseAccess(admitted, "alpha", true), "writes are decided per entity, not per database")
	require.Equal(t, codes.PermissionDenied, status.Code(server.AllowDatabaseAccess(admitted, "beta", false)))
	visible, err := server.VisibleDatabases(admitted, []string{"alpha", "beta"})
	require.NoError(t, err)
	require.Equal(t, []string{"alpha"}, visible)

	// The declared action belongs to the method: a statistics grant does not
	// admit a point read, and a point read grant does not admit a count.
	countOnly := fineGrainedPrincipal(t, "alpha", fineGrainedOnDatabase("database.stats.read", "alpha"))
	countServer := fineGrainedAdmittingServer(countOnly)
	countAdmitted, err := countServer.admitFineGrained(ctx, "/qdrant.Points/Count", claims)
	require.NoError(t, err)
	require.NoError(t, countServer.AllowDatabaseAccess(countAdmitted, "alpha", false))

	countingReader := fineGrainedAdmittingServer(countOnly)
	getAdmitted, err := countingReader.admitFineGrained(ctx, "/qdrant.Points/Get", claims)
	require.NoError(t, err)
	require.Equal(t, codes.PermissionDenied, status.Code(countingReader.AllowDatabaseAccess(getAdmitted, "alpha", false)))
}

// TestFineGrainedStreamAdmissionInstallsPrincipal proves the stream owner admits
// the same policy and refuses an unclassified method before the handler runs.
func TestFineGrainedStreamAdmissionInstallsPrincipal(t *testing.T) {
	authenticator, token := fineGrainedAuthenticator(t)
	request := fineGrainedPrincipal(t, "fgac", fineGrainedOnDatabase("database.access", "fgac"))
	server := fineGrainedAdmittingServer(request)
	server.authenticator = authenticator
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))

	seen := false
	err := server.streamAuthInterceptor(nil, &testServerStream{ctx: ctx}, &grpc.StreamServerInfo{FullMethod: "/qdrant.Points/Get"},
		func(_ interface{}, stream grpc.ServerStream) error {
			seen = true
			require.Same(t, request, auth.FineGrainedRequestFromContext(stream.Context()))
			require.NoError(t, server.AllowDatabaseAccess(stream.Context(), "fgac", false))
			return nil
		})
	require.NoError(t, err)
	require.True(t, seen)

	denied := server.streamAuthInterceptor(nil, &testServerStream{ctx: ctx}, &grpc.StreamServerInfo{FullMethod: "/qdrant.Unknown/Stream"},
		func(interface{}, grpc.ServerStream) error {
			t.Fatal("an unclassified stream method must not reach its handler")
			return nil
		})
	require.Equal(t, codes.PermissionDenied, status.Code(denied))
}

// fineGrainedAuthenticator creates the one account the transport tests use. The
// role holds every RBAC permission, so each refusal below is a fine-grained
// decision rather than a coarse one.
func fineGrainedAuthenticator(t *testing.T) (*auth.Authenticator, string) {
	t.Helper()
	store := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	cfg := auth.DefaultAuthConfig()
	cfg.JWTSecret = []byte("0123456789abcdef0123456789abcdef")
	cfg.SecurityEnabled = true
	authenticator, err := auth.NewAuthenticator(cfg, store)
	require.NoError(t, err)
	_, err = authenticator.CreateUser(context.Background(), "tester", "TesterPass123!", []auth.Role{auth.RoleAdmin})
	require.NoError(t, err)
	tokens, _, err := authenticator.Authenticate(context.Background(), "tester", "TesterPass123!", "127.0.0.1", "unit-test")
	require.NoError(t, err)
	return authenticator, tokens.AccessToken
}

func fineGrainedPoint(id string, vector []float32, payload map[string]*qpb.Value) *qpb.PointStruct {
	return &qpb.PointStruct{
		Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: id}},
		Vectors: &qpb.Vectors{VectorsOptions: &qpb.Vectors_Vector{
			Vector: &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: vector}}},
		}},
		Payload: payload,
	}
}

func fineGrainedString(value string) *qpb.Value {
	return &qpb.Value{Kind: &qpb.Value_StringValue{StringValue: value}}
}

func fineGrainedIDs(ids ...string) *qpb.PointsSelector {
	list := make([]*qpb.PointId, 0, len(ids))
	for _, id := range ids {
		list = append(list, &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: id}})
	}
	return &qpb.PointsSelector{PointsSelectorOneOf: &qpb.PointsSelector_Points{Points: &qpb.PointsIdsList{Ids: list}}}
}

// TestQdrantGRPCFineGrainedPointOperationsAreScoped proves the product outcome
// over the public transport: a principal admitted by the fine-grained policy
// reads, counts, lists and writes only the points its policy covers, its refusals
// change nothing, and the shared vector index keeps answering for the whole
// collection rather than for one principal.
func TestQdrantGRPCFineGrainedPointOperationsAreScoped(t *testing.T) {
	ctx := context.Background()
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	dbm, err := multidb.NewDatabaseManager(t.Context(), base, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbm.Close() })
	collections, err := NewDatabaseCollectionStore(dbm, nil)
	require.NoError(t, err)

	const collection = "fgac"
	require.NoError(t, collections.Create(ctx, collection, 4, qpb.Distance_Cosine))
	require.NoError(t, collections.Create(ctx, "other", 4, qpb.Distance_Cosine))

	// Seed through the ordinary point writer, so the scoped paths read the nodes
	// the write path stores.
	seedCfg := DefaultConfig()
	seedCfg.AllowVectorMutations = true
	seed := NewPointsService(seedCfg, collections, nil, nil)
	_, err = seed.Upsert(ctx, &qpb.UpsertPoints{CollectionName: collection, Points: []*qpb.PointStruct{
		fineGrainedPoint("keep-a", []float32{1, 0, 0, 0}, map[string]*qpb.Value{"category": fineGrainedString("A"), "secret": fineGrainedString("s1"), "tag": fineGrainedString("t0")}),
		fineGrainedPoint("keep-b", []float32{0, 1, 0, 0}, map[string]*qpb.Value{"category": fineGrainedString("B"), "secret": fineGrainedString("s1b")}),
		fineGrainedPoint("hidden", []float32{0, 0, 1, 0}, map[string]*qpb.Value{"category": fineGrainedString("C")}),
	}})
	require.NoError(t, err)

	engine, _, err := collections.Open(ctx, collection)
	require.NoError(t, err)
	visible := make([]string, 0, 2)
	for _, id := range []string{"keep-a", "keep-b"} {
		node, err := engine.GetNode(ctx, storage.NodeID("qdrant:point:"+id))
		require.NoError(t, err)
		visible = append(visible, auth.FineGrainedEntityID(node.Identity().LocalID, node.CreatedAt))
	}

	principal := fineGrainedPrincipal(t, collection,
		fineGrainedOnDatabase("database.access", collection),
		fineGrainedOnDatabase("database.stats.read", collection),
		fineGrainedOnDatabase("database.search.execute", collection),
		fineGrainedOnNodes("graph.node.traverse", collection, auth.FineGrainedScope{EntityIDs: visible}),
		fineGrainedOnNodes("graph.node.embedding.read", collection, auth.FineGrainedScope{EntityIDs: visible}),
		fineGrainedOnNodes("graph.node.embedding.create", collection, auth.FineGrainedScope{Types: []string{QdrantPointLabel}}),
		fineGrainedOnNodes("graph.node.embedding.set", collection, auth.FineGrainedScope{EntityIDs: visible[1:]}),
		fineGrainedOnNodes("graph.node.delete", collection, auth.FineGrainedScope{EntityIDs: visible}),
		fineGrainedOnNodes("graph.node.property.read", collection, auth.FineGrainedScope{Properties: []string{"category"}}),
		fineGrainedOnNodes("graph.node.property.read", collection, auth.FineGrainedScope{EntityIDs: visible[1:], Properties: []string{"tag"}}),
		fineGrainedOnNodes("graph.node.property.set", collection, auth.FineGrainedScope{Properties: []string{"tag", "unknown"}}),
		fineGrainedOnNodes("graph.node.create", collection, auth.FineGrainedScope{Types: []string{QdrantPointLabel, "Point"}}),
	)

	authenticator, token := fineGrainedAuthenticator(t)
	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.EnableReflection = false
	cfg.AllowVectorMutations = true
	shared := sharedSearchProvider()
	var indexStore storage.Engine
	provider := func(database string, store storage.Engine) (*search.Service, error) {
		indexStore = store
		return shared(database, store)
	}
	server, err := NewServer(cfg, collections, base, provider, authenticator)
	require.NoError(t, err)
	server.SetFineGrainedAdmission(func(context.Context, []string) (*auth.FineGrainedRequest, error) {
		return principal, nil
	})
	require.NoError(t, server.Start())
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient(server.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	points := qpb.NewPointsClient(conn)
	collectionsClient := qpb.NewCollectionsClient(conn)
	call := func() context.Context {
		return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	}

	// A read returns the visible point only, with the readable property only.
	got, err := points.Get(call(), &qpb.GetPoints{
		CollectionName: collection,
		Ids:            []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "keep-a"}}, {PointIdOptions: &qpb.PointId_Uuid{Uuid: "hidden"}}},
		WithPayload:    &qpb.WithPayloadSelector{SelectorOptions: &qpb.WithPayloadSelector_Enable{Enable: true}},
		WithVectors:    &qpb.WithVectorsSelector{SelectorOptions: &qpb.WithVectorsSelector_Enable{Enable: true}},
	})
	require.NoError(t, err)
	require.Len(t, got.Result, 1)
	require.Equal(t, "keep-a", got.Result[0].GetId().GetUuid())
	require.Contains(t, got.Result[0].Payload, "category")
	require.NotContains(t, got.Result[0].Payload, "secret", "an unreadable property is not disclosed")
	require.NotContains(t, got.Result[0].Payload, "tag")
	require.NotEmpty(t, got.Result[0].Vectors, "the readable embedding is returned")

	// Scroll and Count see the same population.
	scroll, err := points.Scroll(call(), &qpb.ScrollPoints{CollectionName: collection, Limit: &[]uint32{10}[0]})
	require.NoError(t, err)
	require.Len(t, scroll.Result, 2)
	count, err := points.Count(call(), &qpb.CountPoints{CollectionName: collection})
	require.NoError(t, err)
	require.EqualValues(t, 2, count.Result.GetCount())

	// Collection listing is filtered by the same decision.
	list, err := collectionsClient.List(call(), &qpb.ListCollectionsRequest{})
	require.NoError(t, err)
	require.Len(t, list.Collections, 1)
	require.Equal(t, collection, list.Collections[0].GetName())

	// A point outside the policy cannot be deleted, and the refusal changes nothing.
	_, err = points.Delete(call(), &qpb.DeletePoints{CollectionName: collection, Points: fineGrainedIDs("hidden")})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	stored, err := engine.GetNode(ctx, storage.NodeID("qdrant:point:hidden"))
	require.NoError(t, err, "a refused deletion leaves the point stored")
	require.NotNil(t, stored)

	// Without read access on this point, the known and unknown names receive
	// the same write denial.
	for _, key := range []string{"tag", "unknown"} {
		_, err = points.SetPayload(call(), &qpb.SetPayloadPoints{
			CollectionName: collection, Payload: map[string]*qpb.Value{key: fineGrainedString("blocked")}, PointsSelector: fineGrainedIDs("keep-a"),
		})
		require.Equal(t, codes.PermissionDenied, status.Code(err))
	}
	stored, err = engine.GetNode(ctx, storage.NodeID("qdrant:point:keep-a"))
	require.NoError(t, err)
	require.Equal(t, "t0", stored.Properties["tag"])
	require.NotContains(t, stored.Properties, "unknown")

	// A property the policy allows is written; one it does not is refused and the
	// stored value survives.
	_, err = points.SetPayload(call(), &qpb.SetPayloadPoints{
		CollectionName: collection, Payload: map[string]*qpb.Value{"tag": fineGrainedString("t1")}, PointsSelector: fineGrainedIDs("keep-b"),
	})
	require.NoError(t, err)
	_, err = points.SetPayload(call(), &qpb.SetPayloadPoints{
		CollectionName: collection, Payload: map[string]*qpb.Value{"secret": fineGrainedString("s2")}, PointsSelector: fineGrainedIDs("keep-b"),
	})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	stored, err = engine.GetNode(ctx, storage.NodeID("qdrant:point:keep-b"))
	require.NoError(t, err)
	require.Equal(t, "t1", stored.Properties["tag"], "the allowed write is stored")
	require.Equal(t, "s1b", stored.Properties["secret"], "the refused write is not applied")

	// The creation rule applies to new vectors; replacing an existing
	// point's vectors needs the separate action on that entity.
	_, err = points.Upsert(call(), &qpb.UpsertPoints{CollectionName: collection, Points: []*qpb.PointStruct{
		fineGrainedPoint("fresh", []float32{0, 0, 0, 1}, nil),
	}})
	require.NoError(t, err)
	created, err := engine.GetNode(ctx, storage.NodeID("qdrant:point:fresh"))
	require.NoError(t, err)
	require.NotNil(t, created)

	_, err = points.Upsert(call(), &qpb.UpsertPoints{CollectionName: collection, Points: []*qpb.PointStruct{
		fineGrainedPoint("keep-a", []float32{0, 0, 0, 1}, map[string]*qpb.Value{"category": fineGrainedString("A")}),
	}})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	stored, err = engine.GetNode(ctx, storage.NodeID("qdrant:point:keep-a"))
	require.NoError(t, err)
	require.Equal(t, []float32{1, 0, 0, 0}, stored.NamedEmbeddings["default"], "a refused upsert leaves the stored vector unchanged")

	// Ranking sees only vectors the caller is allowed to read.
	matches, err := points.Search(call(), &qpb.SearchPoints{CollectionName: collection, Vector: []float32{1, 0, 0, 0}, Limit: 10})
	require.NoError(t, err)
	require.Len(t, matches.Result, 2)
	require.Equal(t, "keep-a", matches.Result[0].Id.GetUuid())
	for _, match := range matches.Result {
		require.Contains(t, []string{"keep-a", "keep-b"}, match.Id.GetUuid())
	}

	// A method whose declared action the policy does not grant is refused.
	_, err = points.CreateFieldIndex(call(), &qpb.CreateFieldIndexCollection{CollectionName: collection, FieldName: "category"})
	require.Equal(t, codes.PermissionDenied, status.Code(err))

	// A collection outside the policy is refused at the database decision.
	_, err = points.Count(call(), &qpb.CountPoints{CollectionName: "other"})
	require.Equal(t, codes.PermissionDenied, status.Code(err))

	// The shared index owner keeps the collection engine: the principal-scoped
	// view is never published there, so the index still holds every point.
	require.NotNil(t, indexStore)
	indexed, err := indexStore.GetNodesByLabel(ctx, QdrantPointLabel)
	require.NoError(t, err)
	require.Len(t, indexed, 4, "the shared index population is not narrowed to one principal")
}

func TestQdrantGRPCFineGrainedCollectionMutationsUseDatabaseIdentity(t *testing.T) {
	ctx := t.Context()
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	dbm, err := multidb.NewDatabaseManager(ctx, base, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbm.Close() })
	store, err := NewDatabaseCollectionStore(dbm, nil)
	require.NoError(t, err)
	for _, collection := range []string{"reports", "other"} {
		require.NoError(t, store.Create(ctx, collection, 4, qpb.Distance_Cosine))
	}
	reportsID, _ := dbm.DatabaseIdentity("reports")
	principal := fineGrainedPrincipal(t, "",
		fineGrainedOnDatabase("database.access", reportsID),
		auth.FineGrainedEntitlement{Action: "dbms.database.alter", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDBMS}},
		auth.FineGrainedEntitlement{Action: "dbms.database.drop", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDBMS}},
	)
	principal.DatabaseID = func(name string) string {
		id, _ := dbm.DatabaseIdentity(name)
		return id
	}

	authenticator, token := fineGrainedAuthenticator(t)
	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.EnableReflection = false
	server, err := NewServer(cfg, store, base, nil, authenticator)
	require.NoError(t, err)
	server.SetFineGrainedAdmission(func(context.Context, []string) (*auth.FineGrainedRequest, error) {
		return principal, nil
	})
	require.NoError(t, server.Start())
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient(server.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	client := qpb.NewCollectionsClient(conn)
	authorized := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

	_, err = client.Update(authorized, &qpb.UpdateCollection{CollectionName: "reports"})
	require.NoError(t, err)
	_, err = client.Update(authorized, &qpb.UpdateCollection{CollectionName: "other"})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	_, err = client.Delete(authorized, &qpb.DeleteCollection{CollectionName: "other"})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	_, err = client.Delete(authorized, &qpb.DeleteCollection{CollectionName: "reports"})
	require.NoError(t, err)

	require.NoError(t, store.Create(ctx, "reports", 4, qpb.Distance_Cosine))
	replacement, _ := dbm.DatabaseIdentity("reports")
	require.NotEqual(t, reportsID, replacement)
	_, err = client.Delete(authorized, &qpb.DeleteCollection{CollectionName: "reports"})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	require.ErrorIs(t, dbm.DropDatabase(multidb.WithDropDatabaseIdentity(ctx, reportsID), "reports"), multidb.ErrDatabaseNotFound)
	list, err := store.List(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"reports", "other"}, list)
}
