package qdrantgrpc

import (
	"context"
	"strings"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/status"
)

type onlyVisibleCollection struct{}

func (onlyVisibleCollection) CanSeeDatabase(name string) bool    { return name == "visible_collection" }
func (onlyVisibleCollection) CanAccessDatabase(name string) bool { return name == "visible_collection" }

// TestHiddenCollectionAnswersAsAbsent compares, for a principal without access, a
// collection that exists with one that does not, on every operation that names
// one: the status code and message must match, apart from the name itself.
func TestHiddenCollectionAnswersAsAbsent(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	collections := newTestCollectionStore(t)
	background := context.Background()
	require.NoError(t, collections.Create(background, "visible_collection", 4, qpb.Distance_Cosine))
	require.NoError(t, collections.Create(background, "hidden_collection", 4, qpb.Distance_Cosine))

	config := DefaultConfig()
	config.DatabaseAccessModeResolver = func([]string) auth.DatabaseAccessMode { return onlyVisibleCollection{} }
	config.ResolvedAccessResolver = func([]string, string) auth.ResolvedAccess {
		return auth.ResolvedAccess{Read: true, Write: true}
	}
	server, err := NewServer(config, collections, base, nil, nil)
	require.NoError(t, err)
	ctx := context.WithValue(background, contextKeyClaims{}, &auth.JWTClaims{Roles: []string{"viewer"}})

	service := NewCollectionsService(collections, server)
	snapshots := NewSnapshotsService(config, collections, base, t.TempDir(), server)
	for label, operation := range map[string]func(name string) error{
		"Get": func(name string) error {
			_, err := service.Get(ctx, &qpb.GetCollectionInfoRequest{CollectionName: name})
			return err
		},
		"CollectionExists": func(name string) error {
			_, err := service.CollectionExists(ctx, &qpb.CollectionExistsRequest{CollectionName: name})
			return err
		},
		"Update": func(name string) error {
			_, err := service.Update(ctx, &qpb.UpdateCollection{CollectionName: name})
			return err
		},
		"Delete": func(name string) error {
			_, err := service.Delete(ctx, &qpb.DeleteCollection{CollectionName: name})
			return err
		},
		"Snapshots.Create": func(name string) error {
			_, err := snapshots.Create(ctx, &qpb.CreateSnapshotRequest{CollectionName: name})
			return err
		},
		"Snapshots.List": func(name string) error {
			_, err := snapshots.List(ctx, &qpb.ListSnapshotsRequest{CollectionName: name})
			return err
		},
		"Snapshots.Delete": func(name string) error {
			_, err := snapshots.Delete(ctx, &qpb.DeleteSnapshotRequest{CollectionName: name, SnapshotName: "s"})
			return err
		},
		"Points.Count admission": func(name string) error {
			return server.authorizeRequestDatabase(ctx, &qpb.CountPoints{CollectionName: name}, "/qdrant.Points/Count")
		},
	} {
		t.Run(label, func(t *testing.T) {
			hidden, absent := operation("hidden_collection"), operation("absent_collection")
			require.Error(t, hidden)
			require.Error(t, absent)
			require.Equal(t, status.Code(absent), status.Code(hidden))
			normalize := func(err error) string {
				message := status.Convert(err).Message()
				return strings.ReplaceAll(strings.ReplaceAll(message, "hidden_collection", "NAME"), "absent_collection", "NAME")
			}
			require.Equal(t, normalize(absent), normalize(hidden))
		})
	}

	listed, err := service.List(ctx, &qpb.ListCollectionsRequest{})
	require.NoError(t, err)
	names := make([]string, 0, len(listed.GetCollections()))
	for _, collection := range listed.GetCollections() {
		names = append(names, collection.GetName())
	}
	require.Contains(t, names, "visible_collection")
	require.NotContains(t, names, "hidden_collection")
}
