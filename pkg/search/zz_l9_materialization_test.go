package search

import (
    "testing"
    "slices"
    "time"

    "github.com/orneryd/nornicdb/pkg/storage"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

const (
	nor309Query            = "nor309"
	nor309FirstHit         = storage.NodeID("nor309-a")
	nor309SecondHit        = storage.NodeID("nor309-b")
	nor309InjectedProperty = "injected_by_caller"
	nor309Database         = "session"
)

// batchlessEngine hides the embedding-free batch reader that the wrapped engine
// forwards, so a search hit is read one id at a time: the branch an embedded
// engine without that optional capability takes.
type batchlessEngine struct{ storage.Engine }

// nor309Store is the supported embedded fixture: two stored hits whose labels
// and supported nested property values are what a delivery must detach, the
// engine the search reads them from, and the search service retaining the hits.
type nor309Store struct {
	engine  storage.Engine
	service *Service
}

func newNOR309Store(t *testing.T, wrap func(storage.Engine) storage.Engine) *nor309Store {
	t.Helper()
	engine := newNamespacedEngine(t)
	if wrap != nil {
		engine = wrap(engine)
	}
	for _, node := range []*storage.Node{
		nor309Node(nor309FirstHit, "nor309 anchored nor309"),
		nor309Node(nor309SecondHit, "nor309 secondary"),
	} {
		_, err := engine.CreateNode(t.Context(), node)
		require.NoError(t, err)
	}
	service := NewService(engine, DefaultVectorDimensions, nil)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	for _, id := range []storage.NodeID{nor309FirstHit, nor309SecondHit} {
		indexed, err := engine.GetNode(t.Context(), id)
		require.NoError(t, err)
		require.NoError(t, service.IndexNode(indexed))
	}
	return &nor309Store{engine: engine, service: service}
}

func nor309Node(id storage.NodeID, content string) *storage.Node {
	return &storage.Node{
		ID:     id,
		Labels: []string{"Document"},
		Properties: map[string]any{
			"content": content,
			"meta":    map[string]any{"origin": "graph", "revision": int64(2)},
			"tags":    []any{"alpha", "beta"},
		},
	}
}

func nor309SearchOptions(selection []string) *SearchOptions {
	options := DefaultSearchOptions()
	options.Limit = 5
	options.IncludeProperties = selection
	return options
}

// nor309DeliveryExpectation is what a stored hit's delivery must keep, read back
// through the engine so the expectation is the value that was actually stored.
type nor309DeliveryExpectation struct {
	labels     []string
	properties map[string]any
	created    time.Time
	updated    time.Time
	embedMeta  map[string]any
	database   string
}

// nor309StoredHit reads the stored value of id. A non-nil selection keeps only
// the projected property keys a caller asked for.
func nor309StoredHit(t *testing.T, engine storage.Engine, id storage.NodeID, selection []string) nor309DeliveryExpectation {
	t.Helper()
	stored, err := engine.GetNode(t.Context(), id)
	require.NoError(t, err)
	properties, ok := storage.CopyValue(stored.Properties, nil).(map[string]any)
	require.True(t, ok, "a stored hit carries its property map")
	if selection != nil {
		projected := make(map[string]any, len(selection))
		for _, property := range selection {
			projected[property] = properties[property]
		}
		properties = projected
	}
	return nor309DeliveryExpectation{
		labels:     slices.Clone(stored.Labels),
		properties: properties,
		created:    stored.CreatedAt,
		updated:    stored.UpdatedAt,
		embedMeta:  stored.EmbedMeta,
		database:   stored.RuntimeMetadata().Database,
	}
}

// nor309ResultValue is the value and order a search response must keep while a
// caller works with one of its deliveries.
type nor309ResultValue struct {
	ID             string
	NodeID         string
	Labels         []string
	Properties     map[string]any
	Type           string
	Title          string
	Description    string
	ContentPreview string
	Score          float64
	Similarity     float64
	RRFScore       float64
}

func nor309ResultValues(results []SearchResult) []nor309ResultValue {
	values := make([]nor309ResultValue, len(results))
	for index, result := range results {
		var properties map[string]any
		if result.Properties != nil {
			properties, _ = storage.CopyValue(result.Properties, nil).(map[string]any)
		}
		values[index] = nor309ResultValue{
			ID:             result.ID,
			NodeID:         string(result.NodeID),
			Labels:         slices.Clone(result.Labels),
			Properties:     properties,
			Type:           result.Type,
			Title:          result.Title,
			Description:    result.Description,
			ContentPreview: result.ContentPreview,
			Score:          result.Score,
			Similarity:     result.Similarity,
			RRFScore:       result.RRFScore,
		}
	}
	return values
}

// nor309MutateDelivery changes what the caller owns after delivery: the label
// list, a supported nested property map, a supported nested property list and a
// new top-level property.
func nor309MutateDelivery(t *testing.T, delivered *storage.Node) {
	t.Helper()
	require.NotEmpty(t, delivered.Labels, "the delivery carries the hit's label list")
	delivered.Labels[0] = "Mutated"
	delivered.Labels = append(delivered.Labels, "Appended")
	meta, ok := delivered.Properties["meta"].(map[string]any)
	require.True(t, ok, "the delivery carries its supported nested property map, got %T", delivered.Properties["meta"])
	meta["origin"] = "mutated"
	switch tags := delivered.Properties["tags"].(type) {
	case []any:
		tags[0] = "mutated"
	case []string:
		tags[0] = "mutated"
	default:
		t.Fatalf("the delivery carries its supported nested property list, got %T", delivered.Properties["tags"])
	}
	delivered.Properties[nor309InjectedProperty] = "caller"
}

// nor309RequireDetachedDelivery proves one hit's delivery contract: a repeated
// delivery of the same hit and the hit retained in the results keep the value
// that was read after the caller changed the first delivery, every field the
// holder writes explicitly is still written, and storage is untouched.
func nor309RequireDetachedDelivery(t *testing.T, engine storage.Engine, results []SearchResult, id storage.NodeID, want nor309DeliveryExpectation) {
	t.Helper()
	position := -1
	for index := range results {
		if results[index].NodeID == id {
			position = index
			break
		}
	}
	require.NotEqual(t, -1, position, "the results hold the fixture hit")
	before := nor309ResultValues(results)
	retained := results[position]

	first := retained.MaterializedNode()
	require.NotSame(t, retained.sourceNode, first, "a delivery is a node of its own")
	require.Equal(t, want.labels, first.Labels, "the delivery carries the hit's labels")
	require.Equal(t, want.properties, first.Properties, "the delivery carries the hit's properties")
	require.Equal(t, id, first.ID, "the delivery carries the hit's node id")
	require.True(t, first.CreatedAt.Equal(want.created), "the delivery carries the read's creation time")
	require.True(t, first.UpdatedAt.Equal(want.updated), "the delivery carries the read's update time")
	nor309MutateDelivery(t, first)

	second := retained.MaterializedNode(nor309Database)
	// The caller-visible leaks of this reproduction are asserted independently so
	// one run reports all of them: labels, supported nested property values, a new
	// top-level property, and the retained response the caller is still holding.
	assert.Equal(t, want.labels, second.Labels, "a repeated delivery must not observe the caller's label change")
	assert.Equal(t, want.properties, second.Properties, "a repeated delivery must not observe the caller's nested property change")
	assert.NotContains(t, second.Properties, nor309InjectedProperty, "a repeated delivery must not observe the caller's new property")
	assert.Equal(t, before, nor309ResultValues(results), "materializing a hit changed the retained results or their order")

	require.Equal(t, id, second.ID, "every delivery writes the hit's node id")
	require.Nil(t, second.ChunkEmbeddings, "managed vectors stay in storage")
	require.Nil(t, second.NamedEmbeddings, "managed vectors stay in storage")
	require.True(t, second.CreatedAt.Equal(want.created), "every delivery keeps the read's creation time")
	require.True(t, second.UpdatedAt.Equal(want.updated), "every delivery keeps the read's update time")
	require.Equal(t, want.embedMeta, second.EmbedMeta, "every delivery keeps the read's embedding metadata")
	provenance := want.database
	if provenance == "" {
		provenance = nor309Database
	}
	require.Equal(t, provenance, second.RuntimeMetadata().Database, "a database context supplies provenance only when the hit has none")
	nor309RequireStoredHit(t, engine, id, want)
}

func nor309RequireStoredHit(t *testing.T, engine storage.Engine, id storage.NodeID, want nor309DeliveryExpectation) {
	t.Helper()
	stored, err := engine.GetNode(t.Context(), id)
	require.NoError(t, err)
	require.Equal(t, want.labels, stored.Labels, "the stored hit's labels are unchanged")
	for property, value := range want.properties {
		require.Equal(t, value, stored.Properties[property], "the stored property %q is unchanged", property)
	}
	require.NotContains(t, stored.Properties, nor309InjectedProperty, "the caller's new property never reaches storage")
}

func TestSearchMaterializedNodeRepeatedDeliveryIsDetached(t *testing.T) {
	t.Run("public hit over the storage batch read", func(t *testing.T) {
		store := newNOR309Store(t, nil)
		require.True(t, store.service.supportsBatchNodesWithoutEmbeddings(), "the default fixture reads its hits through the storage batch reader")
		response, err := store.service.Search(t.Context(), nor309Query, nil, nor309SearchOptions(nil))
		require.NoError(t, err)
		require.Len(t, response.Results, 2, "both stored hits match the query")
		nor309RequireDetachedDelivery(t, store.engine, response.Results, nor309FirstHit, nor309StoredHit(t, store.engine, nor309FirstHit, nil))
	})

	t.Run("public hit with projected properties", func(t *testing.T) {
		store := newNOR309Store(t, nil)
		selection := []string{"meta", "tags"}
		response, err := store.service.Search(t.Context(), nor309Query, nil, nor309SearchOptions(selection))
		require.NoError(t, err)
		require.Len(t, response.Results, 2, "both stored hits match the query")
		for _, result := range response.Results {
			require.Nil(t, result.sourceNode.Properties, "a projected hit retains metadata without its property payload")
		}
		nor309RequireDetachedDelivery(t, store.engine, response.Results, nor309FirstHit, nor309StoredHit(t, store.engine, nor309FirstHit, selection))
	})

	t.Run("public hit over the point-read fallback", func(t *testing.T) {
		store := newNOR309Store(t, func(engine storage.Engine) storage.Engine { return &batchlessEngine{Engine: engine} })
		require.False(t, store.service.supportsBatchNodesWithoutEmbeddings(), "the wrapped engine hides the batch reader, so hits are read one id at a time")
		response, err := store.service.Search(t.Context(), nor309Query, nil, nor309SearchOptions(nil))
		require.NoError(t, err)
		require.Len(t, response.Results, 2, "both stored hits match the query")
		nor309RequireDetachedDelivery(t, store.engine, response.Results, nor309FirstHit, nor309StoredHit(t, store.engine, nor309FirstHit, nil))
	})

	t.Run("lazy continuation page", func(t *testing.T) {
		store := newNOR309Store(t, nil)
		request := SearchContinuationRequest{Owner: "alice", Database: "nornic", Mode: SearchContinuationID, N: 2}
		page, err := store.service.SearchTextContinuation(t.Context(), "", DefaultSearchOptions(), request, nil, nil, nil, ChunkedSearchErrorPolicy{})
		require.NoError(t, err)
		require.Len(t, page.Results, 2, "the page materializes both stored hits")
		require.Equal(t, []string{string(nor309FirstHit), string(nor309SecondHit)}, []string{page.Results[0].ID, page.Results[1].ID}, "the page enumerates the stored hits in id order")
		require.NotEmpty(t, page.Results[0].Properties, "the page hydrated the row from storage")
		require.NotEmpty(t, page.Results[0].Labels, "the page hydrated the row from storage")
		nor309RequireDetachedDelivery(t, store.engine, page.Results, nor309FirstHit, nor309StoredHit(t, store.engine, nor309FirstHit, nil))
	})
}
