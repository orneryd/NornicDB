package qdrantgrpc

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type rule73GRPCClass string

const (
	rule73GRPCRead        rule73GRPCClass = "read"
	rule73GRPCWrite       rule73GRPCClass = "write"
	rule73GRPCDBMS        rule73GRPCClass = "dbms"
	rule73GRPCAdmin       rule73GRPCClass = "admin"
	rule73GRPCUnsupported rule73GRPCClass = "unsupported"
)

func rule73GRPCInventory() map[string]rule73GRPCClass {
	return map[string]rule73GRPCClass{
		"Collections/Get": rule73GRPCRead, "Collections/List": rule73GRPCRead,
		"Collections/Create": rule73GRPCDBMS, "Collections/Update": rule73GRPCDBMS, "Collections/Delete": rule73GRPCDBMS,
		"Collections/UpdateAliases": rule73GRPCUnsupported, "Collections/ListCollectionAliases": rule73GRPCUnsupported,
		"Collections/ListAliases": rule73GRPCUnsupported, "Collections/CollectionClusterInfo": rule73GRPCUnsupported,
		"Collections/CollectionExists": rule73GRPCRead, "Collections/UpdateCollectionClusterSetup": rule73GRPCUnsupported,
		"Collections/CreateShardKey": rule73GRPCUnsupported, "Collections/DeleteShardKey": rule73GRPCUnsupported,
		"Collections/ListShardKeys": rule73GRPCUnsupported,

		"Points/Upsert": rule73GRPCWrite, "Points/Delete": rule73GRPCWrite, "Points/Get": rule73GRPCRead,
		"Points/UpdateVectors": rule73GRPCWrite, "Points/DeleteVectors": rule73GRPCWrite,
		"Points/SetPayload": rule73GRPCWrite, "Points/OverwritePayload": rule73GRPCWrite,
		"Points/DeletePayload": rule73GRPCWrite, "Points/ClearPayload": rule73GRPCWrite,
		"Points/CreateFieldIndex": rule73GRPCWrite, "Points/DeleteFieldIndex": rule73GRPCWrite,
		"Points/CreateVectorName": rule73GRPCUnsupported, "Points/DeleteVectorName": rule73GRPCUnsupported,
		"Points/Search": rule73GRPCRead, "Points/SearchBatch": rule73GRPCRead, "Points/SearchGroups": rule73GRPCRead,
		"Points/Scroll": rule73GRPCRead, "Points/Recommend": rule73GRPCRead, "Points/RecommendBatch": rule73GRPCRead,
		"Points/RecommendGroups": rule73GRPCUnsupported, "Points/Discover": rule73GRPCUnsupported,
		"Points/DiscoverBatch": rule73GRPCUnsupported, "Points/Count": rule73GRPCRead,
		"Points/UpdateBatch": rule73GRPCUnsupported, "Points/Query": rule73GRPCRead, "Points/QueryBatch": rule73GRPCRead,
		"Points/QueryGroups": rule73GRPCUnsupported, "Points/Facet": rule73GRPCUnsupported,
		"Points/SearchMatrixPairs": rule73GRPCUnsupported, "Points/SearchMatrixOffsets": rule73GRPCUnsupported,

		"Snapshots/Create": rule73GRPCAdmin, "Snapshots/List": rule73GRPCAdmin, "Snapshots/Delete": rule73GRPCAdmin,
		"Snapshots/CreateFull": rule73GRPCAdmin, "Snapshots/ListFull": rule73GRPCAdmin, "Snapshots/DeleteFull": rule73GRPCAdmin,
		"NornicSearch/SearchText": rule73GRPCRead,
	}
}

func rule73DescriptorMethods() map[string]struct{} {
	out := map[string]struct{}{}
	for _, service := range []*grpc.ServiceDesc{&qpb.Collections_ServiceDesc, &qpb.Points_ServiceDesc, &qpb.Snapshots_ServiceDesc} {
		short := service.ServiceName
		for i := len(short) - 1; i >= 0; i-- {
			if short[i] == '.' {
				short = short[i+1:]
				break
			}
		}
		for _, method := range service.Methods {
			out[short+"/"+method.MethodName] = struct{}{}
		}
	}
	out["NornicSearch/SearchText"] = struct{}{}
	return out
}

func TestRule73SelfAttackEveryGRPCMethodIsClassifiedAndAdmitted(t *testing.T) {
	inventory := rule73GRPCInventory()
	descriptor := rule73DescriptorMethods()
	require.Len(t, inventory, len(descriptor))
	for method := range descriptor {
		_, ok := inventory[method]
		require.True(t, ok, "unclassified registered gRPC method %s", method)
	}

	implemented := map[string]bool{
		"Collections/Create": true, "Collections/Get": true, "Collections/List": true, "Collections/Delete": true,
		"Collections/Update": true, "Collections/CollectionExists": true,
		"Points/Upsert": true, "Points/Get": true, "Points/Delete": true, "Points/Count": true, "Points/Search": true,
		"Points/Scroll": true, "Points/SetPayload": true, "Points/OverwritePayload": true, "Points/DeletePayload": true,
		"Points/ClearPayload": true, "Points/UpdateVectors": true, "Points/DeleteVectors": true, "Points/SearchBatch": true,
		"Points/Recommend": true, "Points/RecommendBatch": true, "Points/SearchGroups": true,
		"Points/CreateFieldIndex": true, "Points/DeleteFieldIndex": true, "Points/Query": true, "Points/QueryBatch": true,
		"Snapshots/Create": true, "Snapshots/List": true, "Snapshots/Delete": true,
		"Snapshots/CreateFull": true, "Snapshots/ListFull": true, "Snapshots/DeleteFull": true,
		"NornicSearch/SearchText": true,
	}

	server := &Server{config: DefaultConfig()}
	admin := &auth.JWTClaims{Roles: []string{string(auth.RoleAdmin)}}
	viewer := &auth.JWTClaims{Roles: []string{string(auth.RoleViewer)}}
	editor := &auth.JWTClaims{Roles: []string{string(auth.RoleEditor)}}
	none := &auth.JWTClaims{Roles: []string{string(auth.RoleNone)}}
	for method, class := range inventory {
		if !implemented[method] {
			continue
		}
		t.Run(method+"/admin", func(t *testing.T) {
			require.NoError(t, server.authorizeMethod(context.Background(), "/qdrant."+method, admin), "implemented RPC over-denies admin")
		})
		t.Run(method+"/no-access", func(t *testing.T) {
			require.Equal(t, codes.PermissionDenied, status.Code(server.authorizeMethod(context.Background(), "/qdrant."+method, none)))
		})
		switch class {
		case rule73GRPCRead:
			t.Run(method+"/reader", func(t *testing.T) {
				require.NoError(t, server.authorizeMethod(context.Background(), "/qdrant."+method, viewer))
			})
		case rule73GRPCWrite, rule73GRPCDBMS:
			t.Run(method+"/writer", func(t *testing.T) {
				require.NoError(t, server.authorizeMethod(context.Background(), "/qdrant."+method, editor))
			})
		}
	}

	// The descriptor inventory and the runtime map must use the same names;
	// stale aliases are another form of optional, drifting admission.
	for method := range defaultMethodPermissions {
		if method == "ServerReflection/ServerReflectionInfo" {
			continue
		}
		_, registered := descriptor[method]
		require.True(t, registered, "stale permission-map method %s", method)
	}
}
