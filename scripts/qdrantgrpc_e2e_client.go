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
	qpb "github.com/qdrant/go-client/qdrant"
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

	collections := qpb.NewCollectionsClient(conn)
	points := qpb.NewPointsClient(conn)
	snapshots := qpb.NewSnapshotsClient(conn)
	nornicSearch := nornicpb.NewNornicSearchClient(conn)

	collection := "e2e_col"
	dims := uint64(4)

	if err := stage(verbose, "collections.create", func() error { return createCollection(ctx, collections, collection, dims) }); err != nil {
		return err
	}
	if err := stage(verbose, "collections.exists(true)", func() error { return collectionExists(ctx, collections, collection, true) }); err != nil {
		return err
	}
	if err := stage(verbose, "collections.get", func() error { return getCollectionInfo(ctx, collections, collection, int(dims)) }); err != nil {
		return err
	}
	if err := stage(verbose, "collections.list", func() error { return listCollectionsContains(ctx, collections, collection) }); err != nil {
		return err
	}
	if err := stage(verbose, "collections.update", func() error { return updateCollection(ctx, collections, collection) }); err != nil {
		return err
	}

	if err := stage(verbose, "points.upsert(named_vectors)", func() error { return upsertNamedVectors(ctx, points, collection) }); err != nil {
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

	if err := stage(verbose, "snapshots.cycle", func() error { return snapshotCycle(ctx, snapshots, collection) }); err != nil {
		return err
	}
	if err := stage(verbose, "nornic.search_text", func() error { return nornicSearchText(ctx, nornicSearch, "database performance") }); err != nil {
		return err
	}

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

func createCollection(ctx context.Context, c qpb.CollectionsClient, name string, dims uint64) error {
	_, err := c.Create(ctx, &qpb.CreateCollection{
		CollectionName: name,
		VectorsConfig: &qpb.VectorsConfig{
			Config: &qpb.VectorsConfig_Params{
				Params: &qpb.VectorParams{
					Size:     dims,
					Distance: qpb.Distance_Cosine,
				},
			},
		},
	})
	return err
}

func deleteCollection(ctx context.Context, c qpb.CollectionsClient, name string) error {
	_, err := c.Delete(ctx, &qpb.DeleteCollection{CollectionName: name})
	return err
}

func collectionExists(ctx context.Context, c qpb.CollectionsClient, name string, want bool) error {
	resp, err := c.CollectionExists(ctx, &qpb.CollectionExistsRequest{CollectionName: name})
	if err != nil {
		return err
	}
	if resp.GetResult().GetExists() != want {
		return fmt.Errorf("want=%v got=%v", want, resp.GetResult().GetExists())
	}
	return nil
}

func getCollectionInfo(ctx context.Context, c qpb.CollectionsClient, name string, wantDims int) error {
	resp, err := c.Get(ctx, &qpb.GetCollectionInfoRequest{CollectionName: name})
	if err != nil {
		return err
	}
	gotDims := int(resp.GetResult().GetConfig().GetParams().GetVectorsConfig().GetParams().GetSize())
	if gotDims != wantDims {
		return fmt.Errorf("want dims=%d got=%d", wantDims, gotDims)
	}
	return nil
}

func listCollectionsContains(ctx context.Context, c qpb.CollectionsClient, name string) error {
	resp, err := c.List(ctx, &qpb.ListCollectionsRequest{})
	if err != nil {
		return err
	}
	for _, col := range resp.Collections {
		if col.GetName() == name {
			return nil
		}
	}
	return fmt.Errorf("missing %q", name)
}

func updateCollection(ctx context.Context, c qpb.CollectionsClient, name string) error {
	_, err := c.Update(ctx, &qpb.UpdateCollection{CollectionName: name})
	return err
}

func upsertNamedVectors(ctx context.Context, p qpb.PointsClient, collection string) error {
	_, err := p.Upsert(ctx, &qpb.UpsertPoints{
		CollectionName: collection,
		Points: []*qpb.PointStruct{
			{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "p1"}},
				Vectors: &qpb.Vectors{
					VectorsOptions: &qpb.Vectors_Vectors{
						Vectors: &qpb.NamedVectors{
							Vectors: map[string]*qpb.Vector{
								"a": {Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{1, 0, 0, 0}}}},
								"b": {Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{0, 1, 0, 0}}}},
							},
						},
					},
				},
				Payload: map[string]*qpb.Value{
					"tag": {Kind: &qpb.Value_StringValue{StringValue: "first"}},
				},
			},
			{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "p2"}},
				Vectors: &qpb.Vectors{
					VectorsOptions: &qpb.Vectors_Vectors{
						Vectors: &qpb.NamedVectors{
							Vectors: map[string]*qpb.Vector{
								"a": {Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{0, 1, 0, 0}}}},
								"b": {Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{1, 0, 0, 0}}}},
							},
						},
					},
				},
				Payload: map[string]*qpb.Value{
					"tag": {Kind: &qpb.Value_StringValue{StringValue: "second"}},
				},
			},
		},
	})
	if err != nil {
		return err
	}

	countResp, err := p.Count(ctx, &qpb.CountPoints{CollectionName: collection, Exact: boolPtr(true)})
	if err != nil {
		return err
	}
	if countResp.GetResult().GetCount() < 2 {
		return fmt.Errorf("expected >=2 got=%d", countResp.GetResult().GetCount())
	}
	return nil
}

func getPointsSubsetVectors(ctx context.Context, p qpb.PointsClient, collection string) error {
	resp, err := p.Get(ctx, &qpb.GetPoints{
		CollectionName: collection,
		Ids:            []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "p1"}}},
		WithVectors: &qpb.WithVectorsSelector{
			SelectorOptions: &qpb.WithVectorsSelector_Include{
				Include: &qpb.VectorsSelector{Names: []string{"b"}},
			},
		},
		WithPayload: &qpb.WithPayloadSelector{SelectorOptions: &qpb.WithPayloadSelector_Enable{Enable: true}},
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
	nv := resp.Result[0].Vectors.GetVectors()
	if nv == nil || len(nv.Vectors) != 1 || nv.Vectors["b"] == nil {
		return fmt.Errorf("expected subset vectors {b}")
	}
	return nil
}

func searchVectorName(ctx context.Context, p qpb.PointsClient, collection string) error {
	resp, err := p.Search(ctx, &qpb.SearchPoints{
		CollectionName: collection,
		Vector:         []float32{1, 0, 0, 0},
		Limit:          1,
		VectorName:     ptrString("a"),
		WithPayload:    &qpb.WithPayloadSelector{SelectorOptions: &qpb.WithPayloadSelector_Enable{Enable: true}},
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

func searchBatch(ctx context.Context, p qpb.PointsClient, collection string) error {
	resp, err := p.SearchBatch(ctx, &qpb.SearchBatchPoints{
		CollectionName: collection,
		SearchPoints: []*qpb.SearchPoints{
			{Vector: []float32{1, 0, 0, 0}, Limit: 1, VectorName: ptrString("b")},
			{Vector: []float32{0, 1, 0, 0}, Limit: 1, VectorName: ptrString("b")},
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

func scrollPoints(ctx context.Context, p qpb.PointsClient, collection string) error {
	limit := uint32(1)
	resp, err := p.Scroll(ctx, &qpb.ScrollPoints{
		CollectionName: collection,
		Limit:          &limit,
		WithPayload:    &qpb.WithPayloadSelector{SelectorOptions: &qpb.WithPayloadSelector_Enable{Enable: true}},
		WithVectors:    &qpb.WithVectorsSelector{SelectorOptions: &qpb.WithVectorsSelector_Enable{Enable: true}},
	})
	if err != nil {
		return err
	}
	if len(resp.Result) != 1 {
		return fmt.Errorf("expected 1 got %d", len(resp.Result))
	}
	return nil
}

func countPoints(ctx context.Context, p qpb.PointsClient, collection string) error {
	resp, err := p.Count(ctx, &qpb.CountPoints{CollectionName: collection, Exact: boolPtr(true)})
	if err != nil {
		return err
	}
	if resp.GetResult().GetCount() != 2 {
		return fmt.Errorf("expected 2 got %d", resp.GetResult().GetCount())
	}

	filter := &qpb.Filter{
		Must: []*qpb.Condition{
			{
				ConditionOneOf: &qpb.Condition_Field{
					Field: &qpb.FieldCondition{
						Key: "tag",
						Match: &qpb.Match{
							MatchValue: &qpb.Match_Keyword{Keyword: "first"},
						},
					},
				},
			},
		},
	}
	fResp, err := p.Count(ctx, &qpb.CountPoints{CollectionName: collection, Exact: boolPtr(true), Filter: filter})
	if err != nil {
		return err
	}
	if fResp.GetResult().GetCount() != 1 {
		return fmt.Errorf("expected 1 got %d", fResp.GetResult().GetCount())
	}
	return nil
}

func payloadOps(ctx context.Context, p qpb.PointsClient, collection string) error {
	_, err := p.SetPayload(ctx, &qpb.SetPayloadPoints{
		CollectionName: collection,
		Payload: map[string]*qpb.Value{
			"extra": {Kind: &qpb.Value_IntegerValue{IntegerValue: 123}},
		},
		PointsSelector: &qpb.PointsSelector{
			PointsSelectorOneOf: &qpb.PointsSelector_Points{
				Points: &qpb.PointsIdsList{
					Ids: []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "p1"}}},
				},
			},
		},
	})
	if err != nil {
		return err
	}

	_, err = p.DeletePayload(ctx, &qpb.DeletePayloadPoints{
		CollectionName: collection,
		Keys:           []string{"extra"},
		PointsSelector: &qpb.PointsSelector{
			PointsSelectorOneOf: &qpb.PointsSelector_Points{
				Points: &qpb.PointsIdsList{
					Ids: []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "p1"}}},
				},
			},
		},
	})
	if err != nil {
		return err
	}

	_, err = p.ClearPayload(ctx, &qpb.ClearPayloadPoints{
		CollectionName: collection,
		Points: &qpb.PointsSelector{
			PointsSelectorOneOf: &qpb.PointsSelector_Points{
				Points: &qpb.PointsIdsList{
					Ids: []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "p1"}}},
				},
			},
		},
	})
	return err
}

func vectorOps(ctx context.Context, p qpb.PointsClient, collection string, dims uint64) error {
	_, err := p.UpdateVectors(ctx, &qpb.UpdatePointVectors{
		CollectionName: collection,
		Points: []*qpb.PointVectors{
			{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "p1"}},
				Vectors: &qpb.Vectors{
					VectorsOptions: &qpb.Vectors_Vectors{
						Vectors: &qpb.NamedVectors{
							Vectors: map[string]*qpb.Vector{
								"a": {Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{0, 0, 1, 0}}}},
								"b": {Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{0, 0, 0, 1}}}},
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

	_, err = p.DeleteVectors(ctx, &qpb.DeletePointVectors{
		CollectionName: collection,
		PointsSelector: &qpb.PointsSelector{
			PointsSelectorOneOf: &qpb.PointsSelector_Points{
				Points: &qpb.PointsIdsList{
					Ids: []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "p1"}}},
				},
			},
		},
		Vectors: &qpb.VectorsSelector{Names: []string{"b"}},
	})
	if err != nil {
		return err
	}

	resp, err := p.Get(ctx, &qpb.GetPoints{
		CollectionName: collection,
		Ids:            []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "p1"}}},
		WithVectors:    &qpb.WithVectorsSelector{SelectorOptions: &qpb.WithVectorsSelector_Enable{Enable: true}},
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
	if got := uint64(len(nv.Vectors["a"].GetDense().GetData())); got != dims {
		return fmt.Errorf("points.get (post delete_vectors): expected dims=%d got=%d", dims, got)
	}
	return nil
}

func deleteByFilter(ctx context.Context, p qpb.PointsClient, collection string) error {
	filter := &qpb.Filter{
		Must: []*qpb.Condition{
			{
				ConditionOneOf: &qpb.Condition_Field{
					Field: &qpb.FieldCondition{
						Key: "tag",
						Match: &qpb.Match{
							MatchValue: &qpb.Match_Keyword{Keyword: "second"},
						},
					},
				},
			},
		},
	}
	_, err := p.Delete(ctx, &qpb.DeletePoints{
		CollectionName: collection,
		Points: &qpb.PointsSelector{
			PointsSelectorOneOf: &qpb.PointsSelector_Filter{
				Filter: filter,
			},
		},
	})
	if err != nil {
		return err
	}

	resp, err := p.Get(ctx, &qpb.GetPoints{
		CollectionName: collection,
		Ids:            []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "p2"}}},
	})
	if err != nil {
		return err
	}
	if len(resp.Result) != 0 {
		return fmt.Errorf("points.get (after delete): expected 0 got %d", len(resp.Result))
	}
	return nil
}

func recommend(ctx context.Context, p qpb.PointsClient, collection string) error {
	resp, err := p.Recommend(ctx, &qpb.RecommendPoints{
		CollectionName: collection,
		Positive:       []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "p1"}}},
		Limit:          5,
		WithPayload:    &qpb.WithPayloadSelector{SelectorOptions: &qpb.WithPayloadSelector_Enable{Enable: true}},
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

func snapshotCycle(ctx context.Context, s qpb.SnapshotsClient, collection string) error {
	createResp, err := s.Create(ctx, &qpb.CreateSnapshotRequest{CollectionName: collection})
	if err != nil {
		return err
	}
	if createResp.GetSnapshotDescription().GetName() == "" {
		return fmt.Errorf("snapshots.create: missing snapshot name")
	}

	listResp, err := s.List(ctx, &qpb.ListSnapshotsRequest{CollectionName: collection})
	if err != nil {
		return err
	}
	found := false
	for _, d := range listResp.SnapshotDescriptions {
		if d.GetName() == createResp.GetSnapshotDescription().GetName() {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("snapshots.list: missing created snapshot %q", createResp.GetSnapshotDescription().GetName())
	}

	_, err = s.Delete(ctx, &qpb.DeleteSnapshotRequest{
		CollectionName: collection,
		SnapshotName:   createResp.GetSnapshotDescription().GetName(),
	})
	return err
}

func boolPtr(v bool) *bool          { return &v }
func float32Ptr(v float32) *float32 { return &v }
func ptrString(v string) *string    { return &v }

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
	return nil
}
