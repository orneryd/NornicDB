package qdrantgrpc

// Scratch, test-only: FGAC differential for Qdrant gRPC search. The same
// restricted principal queries a collection whose hidden data is present (S0),
// changed (S1) or absent (S2); the visible points and their vectors are the
// same in every state. Any difference in what the principal gets back means a
// value it may not read decided the answer.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func fqKeyword(key, value string) *qpb.Condition {
	return &qpb.Condition{ConditionOneOf: &qpb.Condition_Field{Field: &qpb.FieldCondition{
		Key: key, Match: &qpb.Match{MatchValue: &qpb.Match_Keyword{Keyword: value}},
	}}}
}

func fqOutcome(value any, err error) string {
	if err != nil {
		return "ERROR " + err.Error()
	}
	encoded, _ := json.Marshal(value)
	return string(encoded)
}

func fqSearchSummary(response *qpb.SearchResponse, err error) string {
	if err != nil {
		return "ERROR " + err.Error()
	}
	var rows []string
	for _, point := range response.GetResult() {
		payload := map[string]string{}
		for key, value := range point.GetPayload() {
			payload[key] = value.GetStringValue()
		}
		encoded, _ := json.Marshal(payload)
		rows = append(rows, fmt.Sprintf("%s:%.6f:%s", point.GetId().GetUuid(), point.GetScore(), encoded))
	}
	return strings.Join(rows, " | ")
}

// fqRun seeds one state, starts a server for the principal and returns every
// restricted outcome by name.
func fqRun(t *testing.T, state string) map[string]string {
	t.Helper()
	ctx := context.Background()
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	dbm, err := multidb.NewDatabaseManager(t.Context(), base, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbm.Close() })
	collections, err := NewDatabaseCollectionStore(dbm, nil)
	require.NoError(t, err)
	const collection = "fgq"
	require.NoError(t, collections.Create(ctx, collection, 4, qpb.Distance_Cosine))
	seedCfg := DefaultConfig()
	seedCfg.AllowVectorMutations = true
	seed := NewPointsService(seedCfg, collections, nil, nil)
	points := []*qpb.PointStruct{
		fineGrainedPoint("vis-a", []float32{1, 0, 0, 0}, map[string]*qpb.Value{"category": fineGrainedString("A")}),
		fineGrainedPoint("vis-b", []float32{0.8, 0.6, 0, 0}, map[string]*qpb.Value{"category": fineGrainedString("B")}),
		fineGrainedPoint("vis-c", []float32{0, 1, 0, 0}, map[string]*qpb.Value{"category": fineGrainedString("A")}),
	}
	switch state {
	case "S0":
		points[0].Payload["secret"] = fineGrainedString("zqx-one")
		points[1].Payload["secret"] = fineGrainedString("zqx-two")
		points = append(points, fineGrainedPoint("hidden", []float32{1, 0.01, 0, 0}, map[string]*qpb.Value{"category": fineGrainedString("A"), "secret": fineGrainedString("zqx-one")}))
	case "S1":
		points[1].Payload["secret"] = fineGrainedString("zqx-one")
		points[2].Payload["secret"] = fineGrainedString("zqx-two")
		points = append(points, fineGrainedPoint("hidden", []float32{0, 1, 0.01, 0}, map[string]*qpb.Value{"category": fineGrainedString("B"), "secret": fineGrainedString("zqx-two")}))
	}
	_, err = seed.Upsert(ctx, &qpb.UpsertPoints{CollectionName: collection, Points: points})
	require.NoError(t, err)
	engine, _, err := collections.Open(ctx, collection)
	require.NoError(t, err)
	visible := make([]string, 0, 3)
	for _, id := range []string{"vis-a", "vis-b", "vis-c"} {
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
		fineGrainedOnNodes("graph.node.property.read", collection, auth.FineGrainedScope{Properties: []string{"category"}}),
	)
	authenticator, token := fineGrainedAuthenticator(t)
	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.EnableReflection = false
	server, err := NewServer(cfg, collections, base, sharedSearchProvider(), authenticator)
	require.NoError(t, err)
	server.SetFineGrainedAdmission(func(context.Context, []string) (*auth.FineGrainedRequest, error) { return principal, nil })
	require.NoError(t, server.Start())
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient(server.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	client := qpb.NewPointsClient(conn)
	call := func() context.Context { return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token) }
	payload := &qpb.WithPayloadSelector{SelectorOptions: &qpb.WithPayloadSelector_Enable{Enable: true}}
	threshold := float32(0.9)
	ten := uint32(10)
	out := map[string]string{}
	search := func(name string, request *qpb.SearchPoints) {
		request.CollectionName, request.WithPayload = collection, payload
		out[name] = fqSearchSummary(client.Search(call(), request))
	}
	search("search-a-limit1", &qpb.SearchPoints{Vector: []float32{1, 0, 0, 0}, Limit: 1})
	search("search-a-limit2", &qpb.SearchPoints{Vector: []float32{1, 0, 0, 0}, Limit: 2})
	search("search-a-limit10", &qpb.SearchPoints{Vector: []float32{1, 0, 0, 0}, Limit: 10})
	search("search-c-limit1", &qpb.SearchPoints{Vector: []float32{0, 1, 0, 0}, Limit: 1})
	search("search-threshold", &qpb.SearchPoints{Vector: []float32{1, 0, 0, 0}, Limit: 10, ScoreThreshold: &threshold})
	search("search-filter-hidden-one", &qpb.SearchPoints{Vector: []float32{1, 0, 0, 0}, Limit: 10, Filter: &qpb.Filter{Must: []*qpb.Condition{fqKeyword("secret", "zqx-one")}}})
	search("search-filter-hidden-two", &qpb.SearchPoints{Vector: []float32{1, 0, 0, 0}, Limit: 10, Filter: &qpb.Filter{Must: []*qpb.Condition{fqKeyword("secret", "zqx-two")}}})
	search("search-filter-not-hidden", &qpb.SearchPoints{Vector: []float32{1, 0, 0, 0}, Limit: 10, Filter: &qpb.Filter{MustNot: []*qpb.Condition{fqKeyword("secret", "zqx-one")}}})
	search("search-filter-visible", &qpb.SearchPoints{Vector: []float32{1, 0, 0, 0}, Limit: 10, Filter: &qpb.Filter{Must: []*qpb.Condition{fqKeyword("category", "A")}}})
	search("search-offset", &qpb.SearchPoints{Vector: []float32{1, 0, 0, 0}, Limit: 2, Offset: &[]uint64{1}[0]})
	count := func(name string, filter *qpb.Filter) {
		response, err := client.Count(call(), &qpb.CountPoints{CollectionName: collection, Filter: filter, Exact: &[]bool{true}[0]})
		if err != nil {
			out[name] = "ERROR " + err.Error()
			return
		}
		out[name] = fmt.Sprint(response.GetResult().GetCount())
	}
	count("count-all", nil)
	count("count-filter-hidden", &qpb.Filter{Must: []*qpb.Condition{fqKeyword("secret", "zqx-one")}})
	count("count-filter-category", &qpb.Filter{Must: []*qpb.Condition{fqKeyword("category", "A")}})
	scroll := func(name string, filter *qpb.Filter) {
		response, err := client.Scroll(call(), &qpb.ScrollPoints{CollectionName: collection, Filter: filter, Limit: &ten, WithPayload: payload})
		if err != nil {
			out[name] = "ERROR " + err.Error()
			return
		}
		var rows []string
		for _, point := range response.GetResult() {
			rows = append(rows, point.GetId().GetUuid())
		}
		out[name] = strings.Join(rows, ",")
	}
	scroll("scroll-all", nil)
	scroll("scroll-filter-hidden", &qpb.Filter{Must: []*qpb.Condition{fqKeyword("secret", "zqx-one")}})
	recommend, err := client.Recommend(call(), &qpb.RecommendPoints{CollectionName: collection, Positive: []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "vis-a"}}}, Limit: 10, WithPayload: payload})
	out["recommend-vis-a"] = fqOutcome(recommend.GetResult(), err)
	groups, err := client.SearchGroups(call(), &qpb.SearchPointGroups{CollectionName: collection, Vector: []float32{1, 0, 0, 0}, GroupBy: "category", Limit: 5, GroupSize: 2, WithPayload: payload})
	out["search-groups-category"] = fqOutcome(groups.GetResult(), err)
	groupsHidden, err := client.SearchGroups(call(), &qpb.SearchPointGroups{CollectionName: collection, Vector: []float32{1, 0, 0, 0}, GroupBy: "secret", Limit: 5, GroupSize: 2, WithPayload: payload})
	out["search-groups-hidden"] = fqOutcome(groupsHidden.GetResult(), err)
	return out
}

func TestQdrantFGACSearchDifferential(t *testing.T) {
	results := map[string]map[string]string{}
	for _, state := range []string{"S0", "S1", "S2"} {
		t.Run(state, func(t *testing.T) { results[state] = fqRun(t, state) })
	}
	differences, leaks := 0, 0
	for name, base := range results["S2"] {
		for _, state := range []string{"S0", "S1"} {
			got := results[state][name]
			if strings.Contains(strings.ToLower(got), "zqx") && !strings.Contains(name, "filter-hidden") {
				leaks++
				t.Errorf("LEAK [%s] %s: %s", state, name, got)
			}
			if got != base {
				differences++
				t.Errorf("DIFF [%s] %s differs from S2\n  %s: %s\n  S2: %s", state, name, state, got, base)
			}
		}
	}
	for name, base := range results["S2"] {
		t.Logf("OUT %s S0=%s | S2=%s", name, results["S0"][name], base)
	}
	t.Logf("SUMMARY qdrant differences=%d leaks=%d", differences, leaks)
}
