//go:build qdrantgrpc_e2e
// +build qdrantgrpc_e2e

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"time"

	nornicpb "github.com/orneryd/nornicdb/pkg/nornicgrpc/gen"
	pb "github.com/orneryd/nornicdb/pkg/qdrantgrpc/gen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if err := run(); err != nil {
		log.Printf("FAIL: %v", err)
		os.Exit(1)
	}
}

func run() error {
	var addr string
	var verbose bool
	flag.StringVar(&addr, "addr", "127.0.0.1:6334", "Qdrant gRPC listen address")
	flag.BoolVar(&verbose, "verbose", true, "Enable verbose step logging")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	health := pb.NewHealthClient(conn)
	collections := pb.NewCollectionsClient(conn)
	points := pb.NewPointsClient(conn)
	snapshots := pb.NewSnapshotsClient(conn)
	nornicSearch := nornicpb.NewNornicSearchClient(conn)

	if err := stage(verbose, "health.check", func() error { return checkHealth(ctx, health) }); err != nil {
		return err
	}

	collection := "e2e_col"
	dims := uint64(4)

	// Collections
	if err := stage(verbose, "collections.create", func() error { return createCollection(ctx, collections, collection, dims) }); err != nil {
		return err
	}
	if err := stage(verbose, "collections.exists(true)", func() error { return collectionExists(ctx, collections, collection, true) }); err != nil {
		return err
	}
	if err := stage(verbose, "collections.info", func() error { return getCollectionInfo(ctx, collections, collection, int(dims)) }); err != nil {
		return err
	}
	if err := stage(verbose, "collections.list", func() error { return listCollectionsContains(ctx, collections, collection) }); err != nil {
		return err
	}
	if err := stage(verbose, "collections.update", func() error { return updateCollection(ctx, collections, collection) }); err != nil {
		return err
	}

	// Points
	if err := stage(verbose, "points.upsert(named_vectors)", func() error { return upsertNamedVectors(ctx, points, collection, dims) }); err != nil {
		return err
	}
	if err := stage(verbose, "points.get(subset_vectors)", func() error { return getPointsSubsetVectors(ctx, points, collection) }); err != nil {
		return err
	}
	if err := stage(verbose, "points.search(vector_name)", func() error { return searchVectorName(ctx, points, collection) }); err != nil {
		return err
	}
	if err := stage(verbose, "points.search_batch", func() error { return searchBatch(ctx, points, collection) }); err != nil {
		return err
	}
	if err := stage(verbose, "points.scroll", func() error { return scrollPoints(ctx, points, collection) }); err != nil {
		return err
	}
	if err := stage(verbose, "points.count(+filter)", func() error { return countPoints(ctx, points, collection) }); err != nil {
		return err
	}
	if err := stage(verbose, "points.payload_ops", func() error { return payloadOps(ctx, points, collection) }); err != nil {
		return err
	}
	if err := stage(verbose, "points.vector_ops", func() error { return vectorOps(ctx, points, collection, dims) }); err != nil {
		return err
	}
	if err := stage(verbose, "points.delete(filter)", func() error { return deleteByFilter(ctx, points, collection) }); err != nil {
		return err
	}
	if err := stage(verbose, "points.recommend", func() error { return recommend(ctx, points, collection) }); err != nil {
		return err
	}

	// Snapshots (writes artifacts to the configured snapshot dir)
	if err := stage(verbose, "snapshots.cycle", func() error { return snapshotCycle(ctx, snapshots, collection) }); err != nil {
		return err
	}
	if err := stage(verbose, "nornic.search_text", func() error { return nornicSearchText(ctx, nornicSearch, "database performance") }); err != nil {
		return err
	}

	// Cleanup
	if err := stage(verbose, "collections.delete", func() error { return deleteCollection(ctx, collections, collection) }); err != nil {
		return err
	}
	if err := stage(verbose, "collections.exists(false)", func() error { return collectionExists(ctx, collections, collection, false) }); err != nil {
		return err
	}

	log.Printf("PASS: gRPC E2E checks against %s", addr)
	return nil
}

func stage(verbose bool, name string, fn func() error) error {
	start := time.Now()
	if verbose {
		log.Printf("[E2E] -> %s", name)
	}
	err := fn()
	d := time.Since(start)
	if err != nil {
		if verbose {
			log.Printf("[E2E] <- %s FAILED (%s): %v", name, d, err)
		}
		return fmt.Errorf("%s: %w", name, err)
	}
	if verbose {
		log.Printf("[E2E] <- %s OK (%s)", name, d)
	}
	return nil
}

func checkHealth(ctx context.Context, c pb.HealthClient) error {
	resp, err := c.Check(ctx, &pb.HealthCheckRequest{Service: "qdrant"})
	if err != nil {
		return err
	}
	if resp.Status != pb.ServingStatus_SERVING {
		return fmt.Errorf("expected SERVING, got %v", resp.Status)
	}
	return nil
}

func createCollection(ctx context.Context, c pb.CollectionsClient, name string, dims uint64) error {
	_, err := c.CreateCollection(ctx, &pb.CreateCollectionRequest{
		CollectionName: name,
		VectorsConfig: &pb.VectorsConfig{
			Config: &pb.VectorsConfig_Params{
				Params: &pb.VectorParams{
					Size:     dims,
					Distance: pb.Distance_COSINE,
				},
			},
		},
	})
	if err != nil {
		return err
	}
	return nil
}

func deleteCollection(ctx context.Context, c pb.CollectionsClient, name string) error {
	_, err := c.DeleteCollection(ctx, &pb.DeleteCollectionRequest{CollectionName: name})
	if err != nil {
		return err
	}
	return nil
}

func collectionExists(ctx context.Context, c pb.CollectionsClient, name string, want bool) error {
	resp, err := c.CollectionExists(ctx, &pb.CollectionExistsRequest{CollectionName: name})
	if err != nil {
		return err
	}
	if resp.Result != want {
		return fmt.Errorf("want=%v got=%v", want, resp.Result)
	}
	return nil
}

func getCollectionInfo(ctx context.Context, c pb.CollectionsClient, name string, wantDims int) error {
	resp, err := c.GetCollectionInfo(ctx, &pb.GetCollectionInfoRequest{CollectionName: name})
	if err != nil {
		return err
	}
	if resp.Result.Config == nil {
		return fmt.Errorf("missing vectors_config")
	}

	var gotDims int
	switch cfg := resp.Result.Config.Config.(type) {
	case *pb.VectorsConfig_Params:
		gotDims = int(cfg.Params.Size)
	case *pb.VectorsConfig_ParamsMap:
		// params_map isn't used in the E2E flow yet; pick first entry for a sanity check.
		for _, v := range cfg.ParamsMap.Map {
			gotDims = int(v.Size)
			break
		}
	default:
		return fmt.Errorf("unknown vectors_config")
	}
	if gotDims != wantDims {
		return fmt.Errorf("want dims=%d got=%d", wantDims, gotDims)
	}
	return nil
}

func listCollectionsContains(ctx context.Context, c pb.CollectionsClient, name string) error {
	resp, err := c.ListCollections(ctx, &pb.ListCollectionsRequest{})
	if err != nil {
		return err
	}
	for _, col := range resp.Collections {
		if col.Name == name {
			return nil
		}
	}
	return fmt.Errorf("missing %q", name)
}

func updateCollection(ctx context.Context, c pb.CollectionsClient, name string) error {
	// We don't enforce a particular behavior here yet; just ensure the RPC is wired.
	_, err := c.UpdateCollection(ctx, &pb.UpdateCollectionRequest{CollectionName: name})
	if err != nil {
		return err
	}
	return nil
}

func upsertNamedVectors(ctx context.Context, p pb.PointsClient, collection string, dims uint64) error {
	_, err := p.Upsert(ctx, &pb.UpsertPointsRequest{
		CollectionName: collection,
		Points: []*pb.PointStruct{
			{
				Id: &pb.PointId{PointIdOptions: &pb.PointId_Uuid{Uuid: "p1"}},
				Vectors: &pb.Vectors{
					VectorsOptions: &pb.Vectors_Vectors{
						Vectors: &pb.NamedVectors{
							Vectors: map[string]*pb.Vector{
								"a": {Data: []float32{1, 0, 0, 0}},
								"b": {Data: []float32{0, 1, 0, 0}},
							},
						},
					},
				},
				Payload: map[string]*pb.Value{
					"tag": {Kind: &pb.Value_StringValue{StringValue: "first"}},
				},
			},
			{
				Id: &pb.PointId{PointIdOptions: &pb.PointId_Uuid{Uuid: "p2"}},
				Vectors: &pb.Vectors{
					VectorsOptions: &pb.Vectors_Vectors{
						Vectors: &pb.NamedVectors{
							Vectors: map[string]*pb.Vector{
								"a": {Data: []float32{0, 1, 0, 0}},
								"b": {Data: []float32{1, 0, 0, 0}},
							},
						},
					},
				},
				Payload: map[string]*pb.Value{
					"tag": {Kind: &pb.Value_StringValue{StringValue: "second"}},
				},
			},
		},
	})
	if err != nil {
		return err
	}

	// sanity: ensure stored count matches
	countResp, err := p.Count(ctx, &pb.CountPointsRequest{CollectionName: collection})
	if err != nil {
		return err
	}
	if countResp.Result.Count < 2 {
		return fmt.Errorf("expected >=2 got=%d", countResp.Result.Count)
	}

	// dimension sanity to catch wiring errors early
	if dims != 4 {
		return fmt.Errorf("internal e2e invariant: expected dims=4 got=%d", dims)
	}
	return nil
}

func getPointsSubsetVectors(ctx context.Context, p pb.PointsClient, collection string) error {
	resp, err := p.Get(ctx, &pb.GetPointsRequest{
		CollectionName: collection,
		Ids: []*pb.PointId{
			{PointIdOptions: &pb.PointId_Uuid{Uuid: "p1"}},
		},
		WithVectors: &pb.WithVectorsSelector{
			SelectorOptions: &pb.WithVectorsSelector_Include{
				Include: &pb.VectorsSelector{Names: []string{"b"}},
			},
		},
		WithPayload: &pb.WithPayloadSelector{SelectorOptions: &pb.WithPayloadSelector_Enable{Enable: true}},
	})
	if err != nil {
		return err
	}
	if len(resp.Result) != 1 {
		return fmt.Errorf("expected 1 got %d", len(resp.Result))
	}
	if resp.Result[0].Payload == nil || resp.Result[0].Payload["tag"] == nil {
		return fmt.Errorf("expected payload tag")
	}
	v := resp.Result[0].Vectors.GetVectors()
	if v == nil || len(v.Vectors) != 1 || v.Vectors["b"] == nil {
		return fmt.Errorf("expected subset vectors {b}")
	}
	return nil
}

func searchVectorName(ctx context.Context, p pb.PointsClient, collection string) error {
	vn := "a"
	resp, err := p.Search(ctx, &pb.SearchPointsRequest{
		CollectionName: collection,
		Vector:         []float32{1, 0, 0, 0},
		Limit:          1,
		VectorName:     &vn,
		WithPayload:    &pb.WithPayloadSelector{SelectorOptions: &pb.WithPayloadSelector_Enable{Enable: true}},
	})
	if err != nil {
		return err
	}
	if len(resp.Result) != 1 {
		return fmt.Errorf("expected 1 got %d", len(resp.Result))
	}
	if got := resp.Result[0].GetId().GetUuid(); got != "p1" {
		return fmt.Errorf("expected p1 got %q", got)
	}
	return nil
}

func searchBatch(ctx context.Context, p pb.PointsClient, collection string) error {
	vn := "b"
	resp, err := p.SearchBatch(ctx, &pb.SearchBatchPointsRequest{
		CollectionName: collection,
		SearchRequests: []*pb.SearchPointsRequest{
			{
				Vector:     []float32{1, 0, 0, 0},
				Limit:      1,
				VectorName: &vn,
			},
			{
				Vector:     []float32{0, 1, 0, 0},
				Limit:      1,
				VectorName: &vn,
			},
		},
	})
	if err != nil {
		return err
	}
	if len(resp.Result) != 2 {
		return fmt.Errorf("expected 2 got %d", len(resp.Result))
	}
	return nil
}

func scrollPoints(ctx context.Context, p pb.PointsClient, collection string) error {
	limit := uint32(1)
	resp, err := p.Scroll(ctx, &pb.ScrollPointsRequest{
		CollectionName: collection,
		Limit:          &limit,
		WithPayload:    &pb.WithPayloadSelector{SelectorOptions: &pb.WithPayloadSelector_Enable{Enable: true}},
	})
	if err != nil {
		return err
	}
	if len(resp.Result) != 1 {
		return fmt.Errorf("expected 1 got %d", len(resp.Result))
	}
	if resp.NextPageOffset == nil {
		return fmt.Errorf("expected next_page_offset")
	}
	return nil
}

func countPoints(ctx context.Context, p pb.PointsClient, collection string) error {
	resp, err := p.Count(ctx, &pb.CountPointsRequest{CollectionName: collection})
	if err != nil {
		return err
	}
	if resp.Result.Count < 2 {
		return fmt.Errorf("expected >=2 got=%d", resp.Result.Count)
	}

	// filtered count
	filter := &pb.Filter{
		Must: []*pb.Condition{
			{
				ConditionOneOf: &pb.Condition_Field{
					Field: &pb.FieldCondition{
						Key: "tag",
						Match: &pb.Match{
							MatchValue: &pb.Match_Keyword{Keyword: "first"},
						},
					},
				},
			},
		},
	}
	fResp, err := p.Count(ctx, &pb.CountPointsRequest{CollectionName: collection, Filter: filter})
	if err != nil {
		return err
	}
	if fResp.Result.Count != 1 {
		return fmt.Errorf("expected 1 got=%d", fResp.Result.Count)
	}
	return nil
}

func payloadOps(ctx context.Context, p pb.PointsClient, collection string) error {
	// set payload key
	_, err := p.SetPayload(ctx, &pb.SetPayloadPointsRequest{
		CollectionName: collection,
		Payload: map[string]*pb.Value{
			"extra": {Kind: &pb.Value_IntegerValue{IntegerValue: 123}},
		},
		PointsSelector: &pb.PointsSelector{
			PointsSelectorOneOf: &pb.PointsSelector_Points{
				Points: &pb.PointsIdsList{
					Ids: []*pb.PointId{{PointIdOptions: &pb.PointId_Uuid{Uuid: "p1"}}},
				},
			},
		},
	})
	if err != nil {
		return err
	}

	// delete payload key
	_, err = p.DeletePayload(ctx, &pb.DeletePayloadPointsRequest{
		CollectionName: collection,
		Keys:           []string{"extra"},
		PointsSelector: &pb.PointsSelector{
			PointsSelectorOneOf: &pb.PointsSelector_Points{
				Points: &pb.PointsIdsList{
					Ids: []*pb.PointId{{PointIdOptions: &pb.PointId_Uuid{Uuid: "p1"}}},
				},
			},
		},
	})
	if err != nil {
		return err
	}

	// clear payload
	_, err = p.ClearPayload(ctx, &pb.ClearPayloadPointsRequest{
		CollectionName: collection,
		Points: &pb.PointsSelector{
			PointsSelectorOneOf: &pb.PointsSelector_Points{
				Points: &pb.PointsIdsList{
					Ids: []*pb.PointId{{PointIdOptions: &pb.PointId_Uuid{Uuid: "p1"}}},
				},
			},
		},
	})
	if err != nil {
		return err
	}
	return nil
}

func vectorOps(ctx context.Context, p pb.PointsClient, collection string, dims uint64) error {
	// UpdateVectors (named vectors)
	_, err := p.UpdateVectors(ctx, &pb.UpdatePointVectorsRequest{
		CollectionName: collection,
		Points: []*pb.PointVectors{
			{
				Id: &pb.PointId{PointIdOptions: &pb.PointId_Uuid{Uuid: "p1"}},
				Vectors: &pb.Vectors{
					VectorsOptions: &pb.Vectors_Vectors{
						Vectors: &pb.NamedVectors{
							Vectors: map[string]*pb.Vector{
								"a": {Data: []float32{0, 0, 1, 0}},
								"b": {Data: []float32{0, 0, 0, 1}},
							},
						},
					},
				},
			},
		},
	})
	if err != nil {
		return err
	}

	// DeleteVectors: delete just vector "b" from p1
	_, err = p.DeleteVectors(ctx, &pb.DeletePointVectorsRequest{
		CollectionName: collection,
		PointsSelector: &pb.PointsSelector{
			PointsSelectorOneOf: &pb.PointsSelector_Points{
				Points: &pb.PointsIdsList{
					Ids: []*pb.PointId{{PointIdOptions: &pb.PointId_Uuid{Uuid: "p1"}}},
				},
			},
		},
		Vectors: &pb.VectorsSelector{Names: []string{"b"}},
	})
	if err != nil {
		return err
	}

	// Ensure vector "b" was removed (and "a" remains)
	resp, err := p.Get(ctx, &pb.GetPointsRequest{
		CollectionName: collection,
		Ids: []*pb.PointId{
			{PointIdOptions: &pb.PointId_Uuid{Uuid: "p1"}},
		},
		WithVectors: &pb.WithVectorsSelector{SelectorOptions: &pb.WithVectorsSelector_Enable{Enable: true}},
	})
	if err != nil {
		return err
	}
	if len(resp.Result) != 1 {
		return fmt.Errorf("points.get (post delete_vectors): expected 1 got %d", len(resp.Result))
	}
	nv := resp.Result[0].Vectors.GetVectors()
	if nv == nil || nv.Vectors["a"] == nil {
		return fmt.Errorf("points.get (post delete_vectors): expected vector 'a'")
	}
	if _, ok := nv.Vectors["b"]; ok {
		return fmt.Errorf("points.get (post delete_vectors): expected vector 'b' deleted")
	}

	// sanity dimension guard: ensure our "a" vector length matches collection dims
	if got := uint64(len(nv.Vectors["a"].Data)); got != dims {
		return fmt.Errorf("points.get (post delete_vectors): expected dims=%d got=%d", dims, got)
	}
	return nil
}

func deleteByFilter(ctx context.Context, p pb.PointsClient, collection string) error {
	filter := &pb.Filter{
		Must: []*pb.Condition{
			{
				ConditionOneOf: &pb.Condition_Field{
					Field: &pb.FieldCondition{
						Key: "tag",
						Match: &pb.Match{
							MatchValue: &pb.Match_Keyword{Keyword: "second"},
						},
					},
				},
			},
		},
	}
	_, err := p.Delete(ctx, &pb.DeletePointsRequest{
		CollectionName: collection,
		Points: &pb.PointsSelector{
			PointsSelectorOneOf: &pb.PointsSelector_Filter{
				Filter: filter,
			},
		},
	})
	if err != nil {
		return err
	}

	// ensure p2 is gone
	resp, err := p.Get(ctx, &pb.GetPointsRequest{
		CollectionName: collection,
		Ids: []*pb.PointId{
			{PointIdOptions: &pb.PointId_Uuid{Uuid: "p2"}},
		},
	})
	if err != nil {
		return err
	}
	if len(resp.Result) != 0 {
		return fmt.Errorf("points.get (after delete): expected 0 got %d", len(resp.Result))
	}
	return nil
}

func recommend(ctx context.Context, p pb.PointsClient, collection string) error {
	// Recommend uses point IDs; we just ensure it returns something and scores look sane.
	resp, err := p.Recommend(ctx, &pb.RecommendPointsRequest{
		CollectionName: collection,
		Positive:       []*pb.PointId{{PointIdOptions: &pb.PointId_Uuid{Uuid: "p1"}}},
		Negative:       nil,
		Limit:          5,
		WithPayload:    &pb.WithPayloadSelector{SelectorOptions: &pb.WithPayloadSelector_Enable{Enable: true}},
		ScoreThreshold: float32Ptr(-1.0),
	})
	if err != nil {
		return err
	}
	for _, r := range resp.Result {
		if math.IsNaN(float64(r.Score)) {
			return fmt.Errorf("points.recommend: got NaN score")
		}
	}
	return nil
}

func snapshotCycle(ctx context.Context, s pb.SnapshotsClient, collection string) error {
	wait := true
	createResp, err := s.Create(ctx, &pb.CreateSnapshotRequest{CollectionName: collection, Wait: &wait})
	if err != nil {
		return err
	}
	if createResp.Result.Name == "" {
		return fmt.Errorf("snapshots.create: missing snapshot name")
	}

	listResp, err := s.List(ctx, &pb.ListSnapshotsRequest{CollectionName: collection})
	if err != nil {
		return err
	}
	found := false
	for _, d := range listResp.Snapshots {
		if d.Name == createResp.Result.Name {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("snapshots.list: missing created snapshot %q", createResp.Result.Name)
	}

	_, err = s.Delete(ctx, &pb.DeleteSnapshotRequest{CollectionName: collection, SnapshotName: createResp.Result.Name})
	if err != nil {
		return err
	}
	return nil
}

func float32Ptr(v float32) *float32 { return &v }

func nornicSearchText(ctx context.Context, c nornicpb.NornicSearchClient, query string) error {
	resp, err := c.SearchText(ctx, &nornicpb.SearchTextRequest{
		Query: query,
		Limit: 5,
	})
	if err != nil {
		return err
	}
	if resp.SearchMethod == "" {
		return fmt.Errorf("expected search_method")
	}
	// This endpoint is primarily for auto-embedded query search; it should be callable even
	// when the dataset is empty. We only validate basic response shape here.
	return nil
}
