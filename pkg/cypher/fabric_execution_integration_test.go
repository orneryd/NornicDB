package cypher

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestExecute_FabricCallUseChain_OnCompositeConstituents(t *testing.T) {
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer mgr.Close()

	require.NoError(t, mgr.CreateDatabase("nornic_tr"))
	require.NoError(t, mgr.CreateDatabase("nornic_txt"))
	require.NoError(t, mgr.CreateCompositeDatabase("nornic_cmp_a", []multidb.ConstituentRef{
		{Alias: "tr", DatabaseName: "nornic_tr", Type: "local", AccessMode: "read_write"},
		{Alias: "txt", DatabaseName: "nornic_txt", Type: "local", AccessMode: "read_write"},
	}))

	trStore, err := mgr.GetStorage("nornic_tr")
	require.NoError(t, err)
	txtStore, err := mgr.GetStorage("nornic_txt")
	require.NoError(t, err)

	_, err = trStore.CreateNode(&storage.Node{ID: "t-1", Labels: []string{"Translation"}, Properties: map[string]interface{}{
		"id":      "t-1",
		"textKey": "orders.where",
	}})
	require.NoError(t, err)
	_, err = txtStore.CreateNode(&storage.Node{ID: "tt-1", Labels: []string{"TranslationText"}, Properties: map[string]interface{}{
		"translationId": "t-1",
		"text":          "Where are my orders?",
	}})
	require.NoError(t, err)

	defaultStore, err := mgr.GetStorage(mgr.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(defaultStore)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})

	query := `
USE nornic_cmp_a
CALL {
  USE nornic_cmp_a.tr
  MATCH (t:Translation)
  RETURN t.id AS translationId, t.textKey AS textKey
}
CALL {
  USE nornic_cmp_a.txt
  MATCH (tt:TranslationText)
  RETURN count(tt) AS textCount
}
RETURN translationId, textKey, textCount
`

	res, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotEmpty(t, res.Rows)

	found := false
	for _, row := range res.Rows {
		if len(row) < 3 {
			continue
		}
		if id, ok := row[0].(string); ok && id == "t-1" {
			found = true
			require.Equal(t, "orders.where", row[1])
			switch v := row[2].(type) {
			case int64:
				require.Equal(t, int64(1), v)
			case int:
				require.Equal(t, 1, v)
			default:
				t.Fatalf("unexpected textCount type: %T (%v)", row[2], row[2])
			}
		}
	}
	require.True(t, found, "expected row for translation t-1")
}

func TestExecute_FabricNestedCallUseChain_OnCompositeConstituents(t *testing.T) {
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer mgr.Close()

	require.NoError(t, mgr.CreateDatabase("nornic_tr"))
	require.NoError(t, mgr.CreateCompositeDatabase("nornic_cmp_b", []multidb.ConstituentRef{
		{Alias: "tr", DatabaseName: "nornic_tr", Type: "local", AccessMode: "read_write"},
	}))

	trStore, err := mgr.GetStorage("nornic_tr")
	require.NoError(t, err)
	_, err = trStore.CreateNode(&storage.Node{ID: "t-2", Labels: []string{"Translation"}, Properties: map[string]interface{}{
		"id":      "t-2",
		"textKey": "nested.use",
	}})
	require.NoError(t, err)

	defaultStore, err := mgr.GetStorage(mgr.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(defaultStore)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})

	query := `
USE nornic_cmp_b
CALL {
  CALL {
    USE nornic_cmp_b.tr
    MATCH (t:Translation {id: "t-2"})
    RETURN t.id AS translationId, t.textKey AS textKey
  }
  RETURN translationId, textKey
}
RETURN translationId, textKey
`

	res, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotEmpty(t, res.Rows)

	found := false
	for _, row := range res.Rows {
		if len(row) < 2 {
			continue
		}
		if id, ok := row[0].(string); ok && id == "t-2" {
			found = true
			require.Equal(t, "nested.use", row[1])
		}
	}
	require.True(t, found, "expected row for nested translation t-2")
}

func TestExecute_FabricCallUseSubquery_OutOfScopeUseFails(t *testing.T) {
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer mgr.Close()

	require.NoError(t, mgr.CreateDatabase("nornic_tr"))
	require.NoError(t, mgr.CreateCompositeDatabase("nornic_cmp_c", []multidb.ConstituentRef{
		{Alias: "tr", DatabaseName: "nornic_tr", Type: "local", AccessMode: "read_write"},
	}))
	require.NoError(t, mgr.CreateDatabase("other_tr"))
	require.NoError(t, mgr.CreateCompositeDatabase("other", []multidb.ConstituentRef{
		{Alias: "tr", DatabaseName: "other_tr", Type: "local", AccessMode: "read_write"},
	}))

	defaultStore, err := mgr.GetStorage(mgr.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(defaultStore)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})

	query := `
USE nornic_cmp_c
CALL {
  USE other.tr
  RETURN 1 AS x
}
RETURN x
`

	_, err = exec.Execute(context.Background(), query, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid USE target")
}

func TestExecute_FabricCorrelatedCallUseChain_EmptyOuterRowsStillHasColumns(t *testing.T) {
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer mgr.Close()

	require.NoError(t, mgr.CreateDatabase("nornic_tr"))
	require.NoError(t, mgr.CreateDatabase("nornic_txt"))
	require.NoError(t, mgr.CreateCompositeDatabase("translations", []multidb.ConstituentRef{
		{Alias: "tr", DatabaseName: "nornic_tr", Type: "local", AccessMode: "read_write"},
		{Alias: "txr", DatabaseName: "nornic_txt", Type: "local", AccessMode: "read_write"},
	}))

	txtStore, err := mgr.GetStorage("nornic_txt")
	require.NoError(t, err)
	_, err = txtStore.CreateNode(&storage.Node{ID: "tt-1", Labels: []string{"TranslationText"}, Properties: map[string]interface{}{
		"translationId": "missing",
		"text":          "orphan",
	}})
	require.NoError(t, err)

	defaultStore, err := mgr.GetStorage(mgr.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(defaultStore)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})

	query := `
USE translations
CALL {
  USE translations.tr
  MATCH (t:Translation)
  RETURN t.id AS translationId, t.textKey AS textKey, t.textKey128 AS textKey128
}
CALL {
  WITH translationId
  USE translations.txr
  MATCH (tt:TranslationText)
  WHERE tt.translationId = translationId
  RETURN collect(tt) AS texts
}
RETURN translationId, textKey, textKey128, texts;
`

	res, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, []string{"translationId", "textKey", "textKey128", "texts"}, res.Columns)
	require.Empty(t, res.Rows)
}

func TestExecute_FabricCorrelatedCallUseChain_AppliesWherePerOuterRow(t *testing.T) {
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer mgr.Close()

	require.NoError(t, mgr.CreateDatabase("nornic_tr"))
	require.NoError(t, mgr.CreateDatabase("nornic_txt"))
	require.NoError(t, mgr.CreateCompositeDatabase("translations", []multidb.ConstituentRef{
		{Alias: "tr", DatabaseName: "nornic_tr", Type: "local", AccessMode: "read_write"},
		{Alias: "txr", DatabaseName: "nornic_txt", Type: "local", AccessMode: "read_write"},
	}))

	trStore, err := mgr.GetStorage("nornic_tr")
	require.NoError(t, err)
	_, err = trStore.CreateNode(&storage.Node{ID: "tr-1", Labels: []string{"Translation"}, Properties: map[string]interface{}{
		"textKey":    "k1",
		"textKey128": "h1",
	}})
	require.NoError(t, err)
	_, err = trStore.CreateNode(&storage.Node{ID: "tr-2", Labels: []string{"Translation"}, Properties: map[string]interface{}{
		"textKey":    "k2",
		"textKey128": "h2",
	}})
	require.NoError(t, err)

	txrStore, err := mgr.GetStorage("nornic_txt")
	require.NoError(t, err)
	_, err = txrStore.CreateNode(&storage.Node{ID: "txr-1", Labels: []string{"TranslationText"}, Properties: map[string]interface{}{
		"textKey128": "h1",
		"text":       "one",
	}})
	require.NoError(t, err)
	_, err = txrStore.CreateNode(&storage.Node{ID: "txr-2", Labels: []string{"TranslationText"}, Properties: map[string]interface{}{
		"textKey128": "h2",
		"text":       "two",
	}})
	require.NoError(t, err)
	_, err = txrStore.CreateNode(&storage.Node{ID: "txr-x", Labels: []string{"TranslationText"}, Properties: map[string]interface{}{
		"textKey128": "hx",
		"text":       "other",
	}})
	require.NoError(t, err)

	defaultStore, err := mgr.GetStorage(mgr.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(defaultStore)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})

	query := `
USE translations
CALL {
  USE translations.tr
  MATCH (t:Translation)
  RETURN t.textKey AS textKey, t.textKey128 AS textKey128
  LIMIT 2
}
CALL {
  WITH textKey128
  USE translations.txr
  MATCH (tt:TranslationText)
  WHERE tt.textKey128 = textKey128
  RETURN collect(tt) AS texts
}
RETURN textKey, textKey128, texts
`

	catalog, err := exec.buildFabricCatalog()
	require.NoError(t, err)
	frag, err := fabric.NewFabricPlanner(catalog).Plan(query, "nornic")
	require.NoError(t, err)
	var fragmentExecs []*fabric.FragmentExec
	var walk func(fabric.Fragment)
	walk = func(f fabric.Fragment) {
		switch n := f.(type) {
		case *fabric.FragmentExec:
			fragmentExecs = append(fragmentExecs, n)
		case *fabric.FragmentApply:
			walk(n.Input)
			walk(n.Inner)
		case *fabric.FragmentUnion:
			walk(n.LHS)
			walk(n.RHS)
		}
	}
	walk(frag)
	hasTXR := false
	hasTrailing := false
	for _, ex := range fragmentExecs {
		if ex.GraphName == "translations.txr" {
			hasTXR = true
			require.NotContains(t, ex.Query, "USE translations.txr")
		}
		if ex.GraphName == "translations" && strings.HasPrefix(strings.TrimSpace(strings.ToUpper(ex.Query)), "RETURN ") {
			hasTrailing = true
		}
	}
	require.True(t, hasTXR, "planner must route second CALL block to translations.txr")
	require.True(t, hasTrailing, "planner must keep trailing RETURN as separate fragment")

	res, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"textKey", "textKey128", "texts"}, res.Columns)
	require.Len(t, res.Rows, 2)
	byKey := make(map[string][]interface{}, len(res.Rows))
	for _, row := range res.Rows {
		require.Len(t, row, 3)
		textKey, _ := row[0].(string)
		textKey128, _ := row[1].(string)
		require.NotEqual(t, "coalesce(textKey, textKey128)", textKey)
		require.NotEqual(t, "textKey128", textKey128)
		texts, ok := row[2].([]interface{})
		require.True(t, ok, "texts should be a list")
		byKey[textKey128] = texts
	}
	require.Contains(t, byKey, "h1")
	require.Contains(t, byKey, "h2")
	require.Len(t, byKey["h1"], 1)
	require.Len(t, byKey["h2"], 1)
}

func TestExecute_FabricCorrelatedCallUseChain_ListComprehensionJoin(t *testing.T) {
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer mgr.Close()

	require.NoError(t, mgr.CreateDatabase("nornic_tr"))
	require.NoError(t, mgr.CreateDatabase("nornic_txt"))
	require.NoError(t, mgr.CreateCompositeDatabase("translations", []multidb.ConstituentRef{
		{Alias: "tr", DatabaseName: "nornic_tr", Type: "local", AccessMode: "read_write"},
		{Alias: "txr", DatabaseName: "nornic_txt", Type: "local", AccessMode: "read_write"},
	}))

	trStore, err := mgr.GetStorage("nornic_tr")
	require.NoError(t, err)
	_, err = trStore.CreateNode(&storage.Node{ID: "tr-1", Labels: []string{"MongoDocument"}, Properties: map[string]interface{}{
		"textKey":    "k1",
		"textKey128": "h1",
	}})
	require.NoError(t, err)
	_, err = trStore.CreateNode(&storage.Node{ID: "tr-2", Labels: []string{"MongoDocument"}, Properties: map[string]interface{}{
		"textKey":    "k2",
		"textKey128": "h2",
	}})
	require.NoError(t, err)

	txrStore, err := mgr.GetStorage("nornic_txt")
	require.NoError(t, err)
	_, err = txrStore.CreateNode(&storage.Node{ID: "txr-1", Labels: []string{"MongoDocument"}, Properties: map[string]interface{}{
		"textKey128": "h1",
		"text":       "one",
	}})
	require.NoError(t, err)
	_, err = txrStore.CreateNode(&storage.Node{ID: "txr-2", Labels: []string{"MongoDocument"}, Properties: map[string]interface{}{
		"textKey128": "h2",
		"text":       "two",
	}})
	require.NoError(t, err)

	defaultStore, err := mgr.GetStorage(mgr.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(defaultStore)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})

	query := `
USE translations
CALL {
  USE translations.tr
  MATCH (t:MongoDocument)
  WHERE t.textKey128 IS NOT NULL
  RETURN t.textKey AS textKey, t.textKey128 AS textKey128
  ORDER BY t.textKey128
  LIMIT 25
}
WITH collect({textKey: textKey, textKey128: textKey128}) AS rows
CALL {
  WITH rows
  UNWIND rows AS r
  WITH collect(DISTINCT r.textKey128) AS keys
  USE translations.txr
  MATCH (tt:MongoDocument)
  WHERE tt.textKey128 IN keys
  RETURN tt.textKey128 AS k, collect(tt) AS texts
}
WITH rows, collect({k: k, texts: texts}) AS grouped
UNWIND rows AS r
WITH r, [g IN grouped WHERE g.k = r.textKey128][0] AS hit
RETURN r.textKey AS textKey, r.textKey128 AS textKey128, coalesce(hit.texts, []) AS texts
`

	keysProbe := `
USE translations
CALL {
  USE translations.tr
  MATCH (t:MongoDocument)
  WHERE t.textKey128 IS NOT NULL
  RETURN t.textKey AS textKey, t.textKey128 AS textKey128
  ORDER BY t.textKey128
  LIMIT 25
}
WITH collect({textKey: textKey, textKey128: textKey128}) AS rows
UNWIND rows AS r
WITH collect(DISTINCT r.textKey128) AS keys
RETURN keys
`
	keysRes, err := exec.Execute(context.Background(), keysProbe, nil)
	require.NoError(t, err)
	require.Len(t, keysRes.Rows, 1)
	keysList, ok := keysRes.Rows[0][0].([]interface{})
	require.True(t, ok, "unexpected keys type=%T value=%#v", keysRes.Rows[0][0], keysRes.Rows[0][0])
	require.ElementsMatch(t, []interface{}{"h1", "h2"}, keysList)

	res, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"textKey", "textKey128", "texts"}, res.Columns)
	require.Len(t, res.Rows, 2)
	for _, row := range res.Rows {
		require.Len(t, row, 3)
		require.NotEmpty(t, row[0])
		require.NotEmpty(t, row[1])
		texts, ok := row[2].([]interface{})
		require.True(t, ok, "unexpected texts type=%T value=%#v", row[2], row[2])
		require.Len(t, texts, 1, "expected exactly one matching collected row per key")
	}
}

func TestExecute_FabricCorrelatedCallUseChain_CoalesceReturn_NoInternalColumns(t *testing.T) {
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer mgr.Close()

	require.NoError(t, mgr.CreateDatabase("nornic_tr"))
	require.NoError(t, mgr.CreateDatabase("nornic_txt"))
	require.NoError(t, mgr.CreateCompositeDatabase("translations", []multidb.ConstituentRef{
		{Alias: "tr", DatabaseName: "nornic_tr", Type: "local", AccessMode: "read_write"},
		{Alias: "txr", DatabaseName: "nornic_txt", Type: "local", AccessMode: "read_write"},
	}))

	trStore, err := mgr.GetStorage("nornic_tr")
	require.NoError(t, err)
	_, err = trStore.CreateNode(&storage.Node{ID: "tr-1", Labels: []string{"MongoDocument"}, Properties: map[string]interface{}{
		"textKey":    "k1",
		"textKey128": "h1",
	}})
	require.NoError(t, err)
	_, err = trStore.CreateNode(&storage.Node{ID: "tr-2", Labels: []string{"MongoDocument"}, Properties: map[string]interface{}{
		"textKey128": "h2",
	}})
	require.NoError(t, err)

	txrStore, err := mgr.GetStorage("nornic_txt")
	require.NoError(t, err)
	_, err = txrStore.CreateNode(&storage.Node{ID: "txr-1", Labels: []string{"MongoDocument"}, Properties: map[string]interface{}{
		"translationId":  "h1",
		"translatedText": "one",
	}})
	require.NoError(t, err)
	_, err = txrStore.CreateNode(&storage.Node{ID: "txr-2", Labels: []string{"MongoDocument"}, Properties: map[string]interface{}{
		"translationId":  "h2",
		"translatedText": "two",
	}})
	require.NoError(t, err)

	defaultStore, err := mgr.GetStorage(mgr.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(defaultStore)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})

	query := `
USE translations
CALL {
  USE translations.tr
  MATCH (t:MongoDocument)
  WHERE t.textKey128 IS NOT NULL
  RETURN t.textKey AS textKey, t.textKey128 AS textKey128
  ORDER BY t.textKey128
  LIMIT 25
}
CALL {
  WITH textKey128
  USE translations.txr
  MATCH (tt:MongoDocument)
  WHERE tt.translationId = textKey128
  RETURN collect(tt) AS texts
}
RETURN
  coalesce(textKey, textKey128) AS textKey,
  textKey128,
  coalesce(texts, []) AS texts
ORDER BY textKey128
`

	res, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"textKey", "textKey128", "texts"}, res.Columns)
	require.Len(t, res.Rows, 2)
	for _, row := range res.Rows {
		require.Len(t, row, 3)
		require.NotEqual(t, "__fabric_row", row[0])
		require.NotEqual(t, "coalesce(textKey, textKey128)", row[0])
		require.NotEqual(t, "textKey128", row[1])
		require.NotEmpty(t, row[1])
		texts, ok := row[2].([]interface{})
		require.True(t, ok, "unexpected texts type=%T value=%#v", row[2], row[2])
		require.Len(t, texts, 1)
	}
}

func TestNormalizeFabricRowWrapper_MapInterfaceKeys(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic"))
	query := "RETURN sourceId, textKey, originalText, language, translatedText"
	stream := &fabric.ResultStream{
		Columns: []string{"__fabric_row"},
		Rows: [][]interface{}{
			{
				map[interface{}]interface{}{
					"sourceId":       "s1",
					"textKey":        "k1",
					"originalText":   "o1",
					"language":       "es",
					"translatedText": "t1",
				},
			},
		},
	}

	exec.normalizeFabricRowWrapper(query, stream)
	require.Equal(t, []string{"sourceId", "textKey", "originalText", "language", "translatedText"}, stream.Columns)
	require.Len(t, stream.Rows, 1)
	require.Equal(t, []interface{}{"s1", "k1", "o1", "es", "t1"}, stream.Rows[0])
}

func TestExecute_FabricSetBasedSourceTranslationJoin_NoFabricRowLeak(t *testing.T) {
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer mgr.Close()

	require.NoError(t, mgr.CreateDatabase("nornic_tr"))
	require.NoError(t, mgr.CreateDatabase("nornic_txt"))
	require.NoError(t, mgr.CreateCompositeDatabase("translations", []multidb.ConstituentRef{
		{Alias: "tr", DatabaseName: "nornic_tr", Type: "local", AccessMode: "read_write"},
		{Alias: "txr", DatabaseName: "nornic_txt", Type: "local", AccessMode: "read_write"},
	}))

	trStore, err := mgr.GetStorage("nornic_tr")
	require.NoError(t, err)
	_, err = trStore.CreateNode(&storage.Node{ID: "tr-1", Labels: []string{"MongoDocument"}, Properties: map[string]interface{}{
		"sourceId":     "src1",
		"textKey":      "k1",
		"originalText": "hello",
	}})
	require.NoError(t, err)
	_, err = trStore.CreateNode(&storage.Node{ID: "tr-2", Labels: []string{"MongoDocument"}, Properties: map[string]interface{}{
		"sourceId":     "src2",
		"textKey128":   "h2",
		"originalText": "world",
	}})
	require.NoError(t, err)

	txrStore, err := mgr.GetStorage("nornic_txt")
	require.NoError(t, err)
	_, err = txrStore.CreateNode(&storage.Node{ID: "txr-1", Labels: []string{"MongoDocument"}, Properties: map[string]interface{}{
		"translationId":  "src1",
		"language":       "es",
		"translatedText": "hola",
	}})
	require.NoError(t, err)
	_, err = txrStore.CreateNode(&storage.Node{ID: "txr-2", Labels: []string{"MongoDocument"}, Properties: map[string]interface{}{
		"translationId":  "src2",
		"language":       "fr",
		"translatedText": "monde",
	}})
	require.NoError(t, err)

	defaultStore, err := mgr.GetStorage(mgr.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(defaultStore)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})

	query := `
USE translations
CALL {
  USE translations.tr
  MATCH (t:MongoDocument)
  WHERE t.sourceId IS NOT NULL
  RETURN t.sourceId AS sourceId, coalesce(t.textKey, t.textKey128) AS textKey, t.originalText AS originalText
  ORDER BY t.sourceId
  LIMIT 25
}
WITH collect({sourceId: sourceId, textKey: textKey, originalText: originalText}) AS rows
CALL {
  WITH rows
  UNWIND rows AS r
  WITH collect(DISTINCT r.sourceId) AS ids
  USE translations.txr
  MATCH (tt:MongoDocument)
  WHERE tt.translationId IN ids
  RETURN tt.translationId AS sourceId, tt.language AS language, tt.translatedText AS translatedText
}
WITH rows, collect({sourceId: sourceId, language: language, translatedText: translatedText}) AS hits
UNWIND rows AS r
WITH r, [h IN hits WHERE h.sourceId = r.sourceId] AS ms
UNWIND ms AS m
RETURN r.sourceId AS sourceId, r.textKey AS textKey, r.originalText AS originalText, m.language AS language, m.translatedText AS translatedText
ORDER BY sourceId, language
`

	res, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"sourceId", "textKey", "originalText", "language", "translatedText"}, res.Columns)
	require.Len(t, res.Rows, 2)
	for _, row := range res.Rows {
		require.Len(t, row, 5)
		for _, v := range row {
			require.NotEqual(t, "__fabric_row", v)
		}
	}
}

func TestExecute_FabricCorrelatedSourceTranslationCount_HotPathResult(t *testing.T) {
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer mgr.Close()

	require.NoError(t, mgr.CreateDatabase("nornic_tr"))
	require.NoError(t, mgr.CreateDatabase("nornic_txt"))
	require.NoError(t, mgr.CreateCompositeDatabase("translations", []multidb.ConstituentRef{
		{Alias: "tr", DatabaseName: "nornic_tr", Type: "local", AccessMode: "read_write"},
		{Alias: "txr", DatabaseName: "nornic_txt", Type: "local", AccessMode: "read_write"},
	}))

	trStore, err := mgr.GetStorage("nornic_tr")
	require.NoError(t, err)
	_, err = trStore.CreateNode(&storage.Node{ID: "tr-c1", Labels: []string{"MongoDocument"}, Properties: map[string]interface{}{
		"sourceId": "src1",
	}})
	require.NoError(t, err)
	_, err = trStore.CreateNode(&storage.Node{ID: "tr-c2", Labels: []string{"MongoDocument"}, Properties: map[string]interface{}{
		"sourceId": "src2",
	}})
	require.NoError(t, err)

	txrStore, err := mgr.GetStorage("nornic_txt")
	require.NoError(t, err)
	_, err = txrStore.CreateNode(&storage.Node{ID: "txr-c1", Labels: []string{"MongoDocument"}, Properties: map[string]interface{}{
		"translationId": "src1",
		"language":      "es",
	}})
	require.NoError(t, err)
	_, err = txrStore.CreateNode(&storage.Node{ID: "txr-c2", Labels: []string{"MongoDocument"}, Properties: map[string]interface{}{
		"translationId": "src1",
		"language":      "fr",
	}})
	require.NoError(t, err)
	_, err = txrStore.CreateNode(&storage.Node{ID: "txr-c3", Labels: []string{"MongoDocument"}, Properties: map[string]interface{}{
		"translationId": "src2",
		"language":      "es",
	}})
	require.NoError(t, err)

	defaultStore, err := mgr.GetStorage(mgr.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(defaultStore)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})

	query := `
USE translations
CALL {
  USE translations.tr
  MATCH (t:MongoDocument)
  WHERE t.sourceId IS NOT NULL
  RETURN t.sourceId AS sourceId
  ORDER BY t.sourceId
  LIMIT 25
}
CALL {
  WITH sourceId
  USE translations.txr
  MATCH (tt:MongoDocument)
  WHERE tt.translationId = sourceId AND sourceId IS NOT NULL
  RETURN count(*) AS c
}
RETURN sourceId, c
ORDER BY sourceId
`

	res, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"sourceId", "c"}, res.Columns)
	require.Len(t, res.Rows, 2)
	require.Equal(t, "src1", res.Rows[0][0])
	require.Equal(t, int64(2), res.Rows[0][1])
	require.Equal(t, "src2", res.Rows[1][0])
	require.Equal(t, int64(1), res.Rows[1][1])
}

func BenchmarkFabricCorrelatedSourceTranslationJoin_Profile(b *testing.B) {
	const trNodeCount = 100000
	const txrNodeCount = 100000

	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(b, err)
	defer mgr.Close()

	require.NoError(b, mgr.CreateDatabase("nornic_tr"))
	require.NoError(b, mgr.CreateDatabase("nornic_txt"))
	require.NoError(b, mgr.CreateCompositeDatabase("translations", []multidb.ConstituentRef{
		{Alias: "tr", DatabaseName: "nornic_tr", Type: "local", AccessMode: "read_write"},
		{Alias: "txr", DatabaseName: "nornic_txt", Type: "local", AccessMode: "read_write"},
	}))

	trStore, err := mgr.GetStorage("nornic_tr")
	require.NoError(b, err)
	txrStore, err := mgr.GetStorage("nornic_txt")
	require.NoError(b, err)

	for i := 0; i < trNodeCount; i++ {
		_, err = trStore.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("tr-%d", i)),
			Labels: []string{"MongoDocument"},
			Properties: map[string]interface{}{
				"sourceId":     fmt.Sprintf("src-%06d", i),
				"textKey":      fmt.Sprintf("key-%06d", i),
				"originalText": fmt.Sprintf("original-%06d", i),
			},
		})
		require.NoError(b, err)
	}

	for i := 0; i < txrNodeCount; i++ {
		_, err = txrStore.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("txr-%d", i)),
			Labels: []string{"MongoDocument"},
			Properties: map[string]interface{}{
				"translationId":  fmt.Sprintf("src-%06d", i%trNodeCount),
				"language":       "es",
				"translatedText": fmt.Sprintf("translated-%06d", i),
			},
		})
		require.NoError(b, err)
	}
	waitForCount := func(store storage.Engine, label string, want int) {
		deadline := time.Now().Add(20 * time.Second)
		for {
			nodes, err := store.GetNodesByLabel(label)
			if err == nil && len(nodes) >= want {
				return
			}
			if time.Now().After(deadline) {
				got := 0
				if err == nil {
					got = len(nodes)
				}
				b.Fatalf("benchmark setup timeout waiting for %s nodes visibility: want=%d got=%d err=%v", label, want, got, err)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	waitForCount(trStore, "MongoDocument", trNodeCount)
	waitForCount(txrStore, "MongoDocument", txrNodeCount)
	trExec := NewStorageExecutor(trStore)
	_, err = trExec.Execute(context.Background(), "CREATE INDEX bench_tr_sourceid_idx FOR (n:MongoDocument) ON (n.sourceId)", nil)
	require.NoError(b, err)
	txrExec := NewStorageExecutor(txrStore)
	_, err = txrExec.Execute(context.Background(), "CREATE INDEX bench_txr_translationid_idx FOR (n:MongoDocument) ON (n.translationId)", nil)
	require.NoError(b, err)

	defaultStore, err := mgr.GetStorage(mgr.DefaultDatabaseName())
	require.NoError(b, err)
	exec := NewStorageExecutor(defaultStore)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	// Measure execution path cost, not result-cache hits.
	exec.cache = nil

	query := `
USE translations
CALL {
  USE translations.tr
  MATCH (t:MongoDocument)
  WHERE t.sourceId IS NOT NULL
  RETURN t.sourceId AS sourceId, coalesce(t.textKey, t.textKey128) AS textKey, t.originalText AS originalText
  ORDER BY t.sourceId
  LIMIT 25
}
CALL {
  WITH sourceId
  USE translations.txr
  MATCH (tt:MongoDocument)
  WHERE tt.translationId = sourceId
  RETURN tt.language AS language, tt.translatedText AS translatedText
}
RETURN sourceId, textKey, originalText, language, translatedText
ORDER BY sourceId, language
`

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := exec.Execute(ctx, query, nil)
		if err != nil {
			b.Fatal(err)
		}
		if len(res.Rows) == 0 {
			b.Fatal("expected at least one row")
		}
	}
}

func TestExecute_FabricCorrelatedSourceTranslationJoin_HotPathTrace(t *testing.T) {
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer mgr.Close()

	require.NoError(t, mgr.CreateDatabase("nornic_tr"))
	require.NoError(t, mgr.CreateDatabase("nornic_txt"))
	require.NoError(t, mgr.CreateCompositeDatabase("translations", []multidb.ConstituentRef{
		{Alias: "tr", DatabaseName: "nornic_tr", Type: "local", AccessMode: "read_write"},
		{Alias: "txr", DatabaseName: "nornic_txt", Type: "local", AccessMode: "read_write"},
	}))

	trStore, err := mgr.GetStorage("nornic_tr")
	require.NoError(t, err)
	txrStore, err := mgr.GetStorage("nornic_txt")
	require.NoError(t, err)

	for i := 0; i < 200; i++ {
		_, err = trStore.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("tr-hot-%d", i)),
			Labels: []string{"MongoDocument"},
			Properties: map[string]interface{}{
				"sourceId":     fmt.Sprintf("src-hot-%03d", i),
				"textKey":      fmt.Sprintf("key-hot-%03d", i),
				"originalText": fmt.Sprintf("orig-hot-%03d", i),
			},
		})
		require.NoError(t, err)
		_, err = txrStore.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("txr-hot-%d", i)),
			Labels: []string{"MongoDocument"},
			Properties: map[string]interface{}{
				"translationId":  fmt.Sprintf("src-hot-%03d", i),
				"language":       "es",
				"translatedText": fmt.Sprintf("tr-hot-%03d", i),
			},
		})
		require.NoError(t, err)
	}

	trExec := NewStorageExecutor(trStore)
	_, err = trExec.Execute(context.Background(), "CREATE INDEX tr_hot_sourceid_idx FOR (n:MongoDocument) ON (n.sourceId)", nil)
	require.NoError(t, err)
	txrExec := NewStorageExecutor(txrStore)
	_, err = txrExec.Execute(context.Background(), "CREATE INDEX txr_hot_translationid_idx FOR (n:MongoDocument) ON (n.translationId)", nil)
	require.NoError(t, err)

	defaultStore, err := mgr.GetStorage(mgr.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(defaultStore)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	exec.cache = nil

	query := `
USE translations
CALL {
  USE translations.tr
  MATCH (t:MongoDocument)
  WHERE t.sourceId IS NOT NULL
  RETURN t.sourceId AS sourceId, coalesce(t.textKey, t.textKey128) AS textKey, t.originalText AS originalText
  ORDER BY t.sourceId
  LIMIT 25
}
CALL {
  WITH sourceId
  USE translations.txr
  MATCH (tt:MongoDocument)
  WHERE tt.translationId = sourceId
  RETURN tt.language AS language, tt.translatedText AS translatedText
}
RETURN sourceId, textKey, originalText, language, translatedText
ORDER BY sourceId
`

	res, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.NotEmpty(t, res.Rows)

	trace := exec.LastHotPathTrace()
	require.True(t, trace.OuterIndexTopK, "outer MATCH should use index top-k path")
	require.True(t, trace.FabricBatchedApplyRows, "inner correlated subquery should use batched APPLY row lookup")
}

func TestExecute_FabricCorrelatedSourceTranslationJoin_HotPathShapeMatrix(t *testing.T) {
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer mgr.Close()

	require.NoError(t, mgr.CreateDatabase("nornic_tr"))
	require.NoError(t, mgr.CreateDatabase("nornic_txt"))
	require.NoError(t, mgr.CreateCompositeDatabase("translations", []multidb.ConstituentRef{
		{Alias: "tr", DatabaseName: "nornic_tr", Type: "local", AccessMode: "read_write"},
		{Alias: "txr", DatabaseName: "nornic_txt", Type: "local", AccessMode: "read_write"},
	}))

	trStore, err := mgr.GetStorage("nornic_tr")
	require.NoError(t, err)
	txrStore, err := mgr.GetStorage("nornic_txt")
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		_, err = trStore.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("tr-mat-%d", i)),
			Labels: []string{"MongoDocument"},
			Properties: map[string]interface{}{
				"sourceId":     fmt.Sprintf("src-%d", i),
				"textKey":      fmt.Sprintf("key-%d", i),
				"originalText": "Get it delivered",
			},
		})
		require.NoError(t, err)
	}
	for i := 0; i < 3; i++ {
		for _, lang := range []string{"en", "es"} {
			_, err = txrStore.CreateNode(&storage.Node{
				ID:     storage.NodeID(fmt.Sprintf("txr-mat-%d-%s", i, lang)),
				Labels: []string{"MongoDocument"},
				Properties: map[string]interface{}{
					"translationId":  fmt.Sprintf("src-%d", i),
					"language":       lang,
					"translatedText": fmt.Sprintf("%s-%d", lang, i),
				},
			})
			require.NoError(t, err)
		}
	}

	defaultStore, err := mgr.GetStorage(mgr.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(defaultStore)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	exec.cache = nil

	tests := []struct {
		name                string
		innerWhere          string
		innerReturnMods     string
		expectHotPathRows   bool
		expectRowsPerSource int
	}{
		{
			name:                "where_1_no_order",
			innerWhere:          "WHERE tt.translationId = sourceId",
			innerReturnMods:     "",
			expectHotPathRows:   true,
			expectRowsPerSource: 2,
		},
		{
			name:                "where_n_with_order_limit",
			innerWhere:          "WHERE tt.translationId = sourceId AND tt.language IS NOT NULL AND tt.translatedText IS NOT NULL",
			innerReturnMods:     "ORDER BY tt.language DESC LIMIT 1",
			expectHotPathRows:   true,
			expectRowsPerSource: 1,
		},
		{
			name:                "where_0_label_only",
			innerWhere:          "",
			innerReturnMods:     "",
			expectHotPathRows:   false,
			expectRowsPerSource: 6,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var whereClause string
			if strings.TrimSpace(tc.innerWhere) != "" {
				whereClause = "  " + tc.innerWhere + "\n"
			}
			var returnMods string
			if strings.TrimSpace(tc.innerReturnMods) != "" {
				returnMods = "  " + tc.innerReturnMods + "\n"
			}
			query := fmt.Sprintf(`
USE translations
CALL {
  USE translations.tr
  MATCH (t:MongoDocument)
  WHERE t.originalText = "Get it delivered"
  RETURN t.sourceId AS sourceId, coalesce(t.textKey, t.textKey128) AS textKey, t.originalText AS originalText
  ORDER BY t.sourceId
  LIMIT 1
}
CALL {
  WITH sourceId
  USE translations.txr
  MATCH (tt:MongoDocument)
%s  RETURN tt.language AS language, tt.translatedText AS translatedText
%s}
RETURN sourceId, textKey, originalText, language, translatedText
ORDER BY sourceId, language
`, whereClause, returnMods)

			res, err := exec.Execute(context.Background(), query, nil)
			require.NoError(t, err)
			require.NotEmpty(t, res.Rows)
			require.Equal(t, tc.expectRowsPerSource, len(res.Rows))

			trace := exec.LastHotPathTrace()
			require.Equal(t, tc.expectHotPathRows, trace.FabricBatchedApplyRows)
		})
	}
}

func TestExecute_FabricPlanCache_HitOnRepeatQuery(t *testing.T) {
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer mgr.Close()

	require.NoError(t, mgr.CreateDatabase("nornic_tr"))
	require.NoError(t, mgr.CreateCompositeDatabase("translations", []multidb.ConstituentRef{
		{Alias: "tr", DatabaseName: "nornic_tr", Type: "local", AccessMode: "read_write"},
	}))

	trStore, err := mgr.GetStorage("nornic_tr")
	require.NoError(t, err)
	_, err = trStore.CreateNode(&storage.Node{
		ID:     "tr-1",
		Labels: []string{"MongoDocument"},
		Properties: map[string]interface{}{
			"sourceId": "src-1",
		},
	})
	require.NoError(t, err)

	defaultStore, err := mgr.GetStorage(mgr.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(defaultStore)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	require.NotNil(t, exec.fabricPlanCache)
	// Isolate fabric plan cache behavior from result cache short-circuiting.
	exec.cache = nil

	query := `
USE translations
CALL {
  USE translations.tr
  MATCH (t:MongoDocument)
  WHERE t.sourceId IS NOT NULL
  RETURN t.sourceId AS sourceId
  LIMIT 1
}
RETURN sourceId
`

	res, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.NotEmpty(t, res.Rows)

	hits1, misses1, size1 := exec.fabricPlanCache.Stats()
	require.Equal(t, int64(0), hits1)
	require.Equal(t, int64(1), misses1)
	require.Equal(t, 1, size1)

	res, err = exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.NotEmpty(t, res.Rows)

	hits2, misses2, size2 := exec.fabricPlanCache.Stats()
	require.GreaterOrEqual(t, hits2, int64(1))
	require.Equal(t, misses1, misses2)
	require.Equal(t, size1, size2)
}

func TestExecute_FabricResultCache_HitOnRepeatReadQuery(t *testing.T) {
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer mgr.Close()

	require.NoError(t, mgr.CreateDatabase("nornic_tr"))
	require.NoError(t, mgr.CreateCompositeDatabase("translations", []multidb.ConstituentRef{
		{Alias: "tr", DatabaseName: "nornic_tr", Type: "local", AccessMode: "read_write"},
	}))

	trStore, err := mgr.GetStorage("nornic_tr")
	require.NoError(t, err)
	_, err = trStore.CreateNode(&storage.Node{
		ID:     "tr-1",
		Labels: []string{"MongoDocument"},
		Properties: map[string]interface{}{
			"sourceId": "src-1",
		},
	})
	require.NoError(t, err)

	defaultStore, err := mgr.GetStorage(mgr.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(defaultStore)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	require.NotNil(t, exec.cache)

	query := `
USE translations
CALL {
  USE translations.tr
  MATCH (t:MongoDocument)
  WHERE t.sourceId IS NOT NULL
  RETURN t.sourceId AS sourceId
  LIMIT 1
}
RETURN sourceId
`

	res, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.NotEmpty(t, res.Rows)

	h1, m1, _, _, _ := exec.cache.Stats()
	require.Equal(t, int64(0), h1)
	require.GreaterOrEqual(t, m1, int64(1))

	res, err = exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.NotEmpty(t, res.Rows)

	h2, m2, _, _, _ := exec.cache.Stats()
	require.GreaterOrEqual(t, h2, h1+1)
	require.Equal(t, m1, m2)
}

func TestExecute_FabricResultCache_SkipsRemoteConstituentTargets(t *testing.T) {
	var remote *httptest.Server
	remote = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/tx") && r.Method == http.MethodPost {
			body, _ := io.ReadAll(r.Body)
			payload := string(body)
			if strings.Contains(payload, `"statements":[]`) || strings.Contains(payload, `"statements": []`) {
				commitURL := remote.URL + r.URL.Path + "/commit"
				_, _ = w.Write([]byte(`{"commit":"` + commitURL + `","errors":[]}`))
				return
			}
			_, _ = w.Write([]byte(`{"results":[{"columns":["sourceId"],"data":[{"row":["src-remote"],"meta":[null]}]}],"errors":[]}`))
			return
		}
		if strings.HasSuffix(r.URL.Path, "/tx/commit") && r.Method == http.MethodPost {
			_, _ = w.Write([]byte(`{"results":[{"columns":["sourceId"],"data":[{"row":["src-remote"],"meta":[null]}]}],"errors":[]}`))
			return
		}
		if strings.HasSuffix(r.URL.Path, "/tx") && r.Method == http.MethodDelete {
			_, _ = w.Write([]byte(`{"results":[],"errors":[]}`))
			return
		}
		_, _ = w.Write([]byte(`{"results":[],"errors":[]}`))
	}))
	defer remote.Close()

	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer mgr.Close()

	require.NoError(t, mgr.CreateCompositeDatabase("translations", []multidb.ConstituentRef{
		{
			Alias:        "txr",
			DatabaseName: "tenant_remote",
			Type:         "remote",
			AccessMode:   "read_write",
			URI:          remote.URL,
			AuthMode:     "oidc_forwarding",
		},
	}))

	defaultStore, err := mgr.GetStorage(mgr.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(defaultStore)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	require.NotNil(t, exec.cache)

	query := `
USE translations
CALL {
  USE translations.txr
  MATCH (tt:MongoDocument)
  RETURN tt.sourceId AS sourceId
  LIMIT 1
}
RETURN sourceId
`

	res, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.NotEmpty(t, res.Rows)

	h1, m1, size1, _, _ := exec.cache.Stats()
	require.Equal(t, int64(0), h1)
	require.Equal(t, int64(0), m1)
	require.Equal(t, 0, size1)

	res, err = exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.NotEmpty(t, res.Rows)

	h2, m2, size2, _, _ := exec.cache.Stats()
	require.Equal(t, h1, h2)
	require.Equal(t, m1, m2)
	require.Equal(t, size1, size2)
}
