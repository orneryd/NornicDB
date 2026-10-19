package qdrantgrpc

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicgrpc"
	native "github.com/orneryd/nornicdb/pkg/nornicgrpc/gen"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

type readAdmissionSearcher struct{}

func (readAdmissionSearcher) Search(context.Context, string, []float32, *search.SearchOptions) (*search.SearchResponse, error) {
	return &search.SearchResponse{}, nil
}

// Walk actual registered descriptors over the wire, including empty requests
// and batches. Global admin entitlement deliberately does not imply database
// read: each principal below has a different per-database decision.
func TestDatabaseReadAdmissionThroughEveryGRPCReadMethod(t *testing.T) {
	ctx := context.Background()
	base := storage.NewMemoryEngine()
	manager, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	collections, err := NewDatabaseCollectionStore(manager, nil)
	require.NoError(t, err)
	for _, database := range []string{"read_default", "read_named"} {
		require.NoError(t, collections.Create(ctx, database, 4, qpb.Distance_Cosine))
	}
	profiles := map[string]map[string]auth.ResolvedAccess{
		"default-only": {"read_default": {Read: true, Write: true}},
		"named-only": {"read_named": {Read: true, Write: true}},
		"write-only": {"read_default": {Write: true}, "read_named": {Write: true}},
		"no-access": {},
		"reader": {"read_default": {Read: true}, "read_named": {Read: true}, "nornic": {Read: true}},
		"admin": {"read_default": {Read: true, Write: true}, "read_named": {Read: true, Write: true}, "nornic": {Read: true, Write: true}},
	}
	authConfig := auth.DefaultAuthConfig()
	authConfig.SecurityEnabled = true
	authConfig.JWTSecret = []byte("0123456789abcdef0123456789abcdef")
	users, err := auth.NewAuthenticator(authConfig, storage.NewMemoryEngine())
	require.NoError(t, err)
	tokens := map[string]string{}
	for name := range profiles {
		_, err := users.CreateUser(name, "Password123!", []auth.Role{auth.RoleAdmin, auth.Role(name)})
		require.NoError(t, err)
		token, _, err := users.Authenticate(name, "Password123!", "127.0.0.1", "test")
		require.NoError(t, err)
		tokens[name] = token.AccessToken
	}
	config := DefaultConfig()
	config.ListenAddr = "127.0.0.1:0"
	config.EnableReflection = false
	config.SnapshotDir = t.TempDir()
	config.DatabaseAccessModeResolver = func([]string) auth.DatabaseAccessMode { return auth.FullDatabaseAccessMode }
	config.ResolvedAccessResolver = func(roles []string, database string) auth.ResolvedAccess {
		for i := len(roles) - 1; i >= 0; i-- {
			if profile, ok := profiles[roles[i]]; ok {
				return profile[database]
			}
		}
		return auth.ResolvedAccess{}
	}
	server, err := NewServer(config, collections, base, nil, users)
	require.NoError(t, err)
	nativeSearch, err := nornicgrpc.NewService(nornicgrpc.Config{
		DefaultDatabase: "read_default",
		AllowDatabase: func(ctx context.Context, database string) error {
			return server.AllowDatabaseAccess(ctx, database, false)
		},
		ResolveDatabase: func(context.Context, string) (nornicgrpc.DatabaseDependencies, error) {
			return nornicgrpc.DatabaseDependencies{Searcher: readAdmissionSearcher{}}, nil
		},
	}, nil, nil, readAdmissionSearcher{})
	require.NoError(t, err)
	require.NoError(t, server.RegisterAdditionalServices(func(registry *grpc.Server) {
		native.RegisterNornicSearchServer(registry, nativeSearch)
	}))
	require.NoError(t, server.Start())
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient(server.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = connection.Close() })
	classes := rule73GRPCInventory()
	for _, service := range []*grpc.ServiceDesc{&qpb.Collections_ServiceDesc, &qpb.Points_ServiceDesc, &qpb.Snapshots_ServiceDesc, &native.NornicSearch_ServiceDesc} {
		descriptor, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(service.ServiceName))
		require.NoError(t, err)
		methods := descriptor.(protoreflect.ServiceDescriptor).Methods()
		for i := 0; i < methods.Len(); i++ {
			method := methods.Get(i)
			key := string(descriptor.Name()) + "/" + string(method.Name())
			class := classes[key]
			if class != rule73GRPCRead && class != rule73GRPCAdmin {
				continue
			}
			for _, database := range []string{"read_default", "read_named"} {
				for name, profile := range profiles {
					t.Run(key+"/"+database+"/"+name, func(t *testing.T) {
						request := dynamicpb.NewMessage(method.Input())
						field := method.Input().Fields().ByName("collection_name")
						if key == "NornicSearch/SearchText" {
							field = method.Input().Fields().ByName("database")
							request.Set(method.Input().Fields().ByName("query"), protoreflect.ValueOfString("read admission"))
						}
						if field != nil {
							request.Set(field, protoreflect.ValueOfString(database))
						}
						callContext := metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+tokens[name]))
						response := dynamicpb.NewMessage(method.Output())
						callErr := connection.Invoke(callContext, "/"+service.ServiceName+"/"+string(method.Name()), request, response)
						if key == "Collections/List" {
							require.NoError(t, callErr)
							rows := response.Get(method.Output().Fields().ByName("collections")).List()
							for j := 0; j < rows.Len(); j++ {
								row := rows.Get(j).Message()
								visible := row.Get(row.Descriptor().Fields().ByName("name")).String()
								require.True(t, profile[visible].Read, "unreadable collection listed: %s", visible)
							}
							return
						}
						allowed := profile[database].Read
						if key == "Snapshots/Delete" {
							allowed = profile[database].Write
						}
						if field == nil { // Full-storage snapshots include all databases.
							allowed = name == "admin"
						}
						if allowed {
							require.NotEqual(t, codes.PermissionDenied, status.Code(callErr), "%v", callErr)
						} else {
							require.Equal(t, codes.PermissionDenied, status.Code(callErr), "%v", callErr)
						}
					})
				}
			}
		}
	}
}
