package server

// Lab-only reproduction, round 2: pulse A (one collection's profile alone) and
// pulse B (three profile transactions + DEFINITION_DUE over the library + a
// burst of CITED_SOURCES reads), with the product's exact statements and the
// live library's node shape. Runs only with HS2_RUN=1.

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Exact statements (product statement census at 3417b7ac).
const (
	hs2TEXTS        = "UNWIND $ids AS id MATCH (s:PDSearchSource {id: id}) RETURN s.id AS source_id, s.document_id AS document_id, s.text AS text, properties(s) AS source"
	hs2TEXTSNoProps = "UNWIND $ids AS id MATCH (s:PDSearchSource {id: id}) RETURN s.id AS source_id, s.document_id AS document_id, s.text AS text"
	hs2SIGNATURES   = "UNWIND $sources AS x MATCH (s:PDSearchSource {id: x.source_id}) SET s.set_keys = x.keys, s.set_weights = x.weights, s.set_total = x.total, s.set_pool_runs = x.pool_runs, s.set_pool_keys = x.pool_keys, s.set_pool_records = x.pool_records, s.profiled_current = x.current"
	hs2PROFILED     = "UNWIND $profiled AS x MATCH (s:PDSearchSource {id: x.source_id}) SET s.record_profiled = x.generation, s.profiled_run_layout = x.layout"
	hs2RUNSETS      = "UNWIND $sources AS x MATCH (s:PDSearchSource {id: x.source_id}) CALL { WITH s, x OPTIONAL MATCH (s)-[:PD_HAS_RECORD_SET]->(old:PDRecordSet {kind: 'run'}) WHERE NOT old.id IN [r IN x.sets | r.id] DETACH DELETE old RETURN count(*) AS removed } CALL { WITH s, x UNWIND x.sets AS set MERGE (r:PDGraphNode:PDRecordSet {id: set.id}) SET r.previous_table_key = CASE WHEN r.table_key IS NULL OR set.properties.table_key IS NULL OR r.table_key = set.properties.table_key THEN r.previous_table_key ELSE r.table_key END SET r += set.properties MERGE (s)-[l:PD_HAS_RECORD_SET]->(r) SET l.run = set.run, l.sample = set.properties.sample RETURN count(*) AS written } RETURN count(*) AS sources"
	hs2GROUPS       = "CALL { UNWIND $unlinked AS x MATCH (old:PDRecordSet {id: x.record_set_id})<-[l:PD_HAS_RECORD_SET]-(s:PDSearchSource {id: x.source_id}) WHERE l.layout IS NOT NULL DELETE l RETURN count(*) AS unpooled } CALL { UNWIND $departed AS x MATCH (old:PDRecordSet {id: x.record_set_id})<-[m:PD_MEMBER_OF_SET]-(s:PDSearchSource {id: x.source_id}) REMOVE s.record_set_id, s.set_residual_starts, s.set_residual_ends, s.set_repeat_starts, s.set_repeat_ends DELETE m RETURN count(*) AS left } CALL { OPTIONAL MATCH (old:PDRecordSet {collection_id: $collection}) WHERE old.kind IN ['group', 'pool'] AND NOT old.id IN [g IN $groups | g.id] DETACH DELETE old RETURN count(*) AS removed } CALL { UNWIND $groups AS group MERGE (g:PDGraphNode:PDRecordSet {id: group.id}) SET g.previous_table_key = CASE WHEN g.table_key IS NULL OR group.properties.table_key IS NULL OR g.table_key = group.properties.table_key THEN g.previous_table_key ELSE g.table_key END SET g += group.properties, g.template_digest = coalesce(g.template_digest, group.template_digest) RETURN count(*) AS written } CALL { UNWIND $members AS x MATCH (s:PDSearchSource {id: x.source_id}) MATCH (g:PDRecordSet {id: x.record_set_id}) OPTIONAL MATCH (s)-[other:PD_MEMBER_OF_SET]->(elsewhere:PDRecordSet) WHERE elsewhere.id <> g.id DELETE other MERGE (s)-[m:PD_MEMBER_OF_SET]->(g) SET m.records = x.records, m.layout = x.layout, s.record_set_id = x.record_set_id, s.set_residual_starts = x.residual_starts, s.set_residual_ends = x.residual_ends, s.set_repeat_starts = x.repeat_starts, s.set_repeat_ends = x.repeat_ends RETURN count(*) AS joined } CALL { UNWIND $links AS x MATCH (s:PDSearchSource {id: x.source_id}) MATCH (g:PDRecordSet {id: x.record_set_id}) OPTIONAL MATCH (s)-[old:PD_HAS_RECORD_SET]->(g) WHERE old.layout IS NOT NULL DELETE old WITH DISTINCT s, g, x UNWIND x.runs AS run CREATE (s)-[l:PD_HAS_RECORD_SET]->(g) SET l.run = run.run, l.records = run.records, l.layout = x.layout, l.unit = run.unit RETURN count(*) AS linked } CALL { UNWIND $links AS x UNWIND x.run_sets AS id MATCH (own:PDRecordSet {id: id, kind: 'run'}) DETACH DELETE own RETURN count(*) AS unrun } CALL { UNWIND $versions AS x MATCH (s:PDSearchSource {id: x.source_id}) SET s.set_version = x.version RETURN count(*) AS versioned } RETURN unpooled, left, removed, written, joined, linked, unrun, versioned"
	hs2COPIES       = "UNWIND $sources AS x MATCH (s:PDSearchSource {id: x.source_id}) SET s.set_copy_runs = x.runs, s.set_copy_of = x.of, s.set_copy_documents = x.documents, s.set_copy_whole = x.whole, s.set_copy_version = x.version"
	hs2VIEWS        = "UNWIND $sources AS x MATCH (s:PDSearchSource {id: x.source_id}) SET s.view_text = x.view.view_text, s.view_starts = x.view.view_starts, s.view_ends = x.view.view_ends, s.view_source_starts = x.view.view_source_starts, s.view_source_ends = x.view.view_source_ends, s.view_library = x.view.view_library, s.view_own = x.view.view_own, s.view_basis = x.basis, s.read_observation = coalesce(x.observation, s.read_observation)"
	hs2DEFDUE       = "UNWIND $items AS item MATCH (d:PDDocument {id: item.document_id}) WHERE coalesce(d.read_reading_id, '') = coalesce(item.reading_id, '') WITH d, $now AS need_at, item.cause.kind AS need_kind, coalesce(item.cause.element_id, '') AS need_element WITH d, need_kind, need_element, [i IN range(0, size(coalesce(d.due_kinds, [])) - 1) WHERE NOT (d.due_kinds[i] = need_kind AND d.due_elements[i] = need_element)] AS kept, reduce(latest = need_at, i IN range(0, size(coalesce(d.due_kinds, [])) - 1) | CASE WHEN d.due_kinds[i] = need_kind AND d.due_elements[i] = need_element AND true THEN d.due_times[i] ELSE latest END) AS need_at SET d.due_kinds = [i IN kept | d.due_kinds[i]] + [need_kind], d.due_elements = [i IN kept | d.due_elements[i]] + [need_element], d.due_times = [i IN kept | d.due_times[i]] + [need_at], d.due_at = CASE WHEN d.due_at IS NULL OR datetime(d.due_at) < datetime(need_at) THEN need_at ELSE d.due_at END RETURN count(d) AS marked"
	hs2ONESET       = "UNWIND $sources AS x MATCH (s:PDSearchSource {id: x.source_id}) SET s.set_keys = x.keys, s.set_weights = x.weights, s.set_total = x.total, s.set_pool_runs = x.pool_runs, s.set_pool_keys = x.pool_keys, s.set_pool_records = x.pool_records, s.profiled_current = x.current, s.record_profiled = x.generation, s.profiled_run_layout = x.layout, s.record_set_id = x.record_set_id, s.set_residual_starts = x.residual_starts, s.set_residual_ends = x.residual_ends, s.set_repeat_starts = x.repeat_starts, s.set_repeat_ends = x.repeat_ends, s.set_version = x.version, s.set_copy_runs = x.runs, s.set_copy_of = x.of, s.set_copy_documents = x.documents, s.set_copy_whole = x.whole, s.set_copy_version = x.version"
	hs2CITED        = "UNWIND $ids AS id MATCH (s:PDSearchSource {id: id}) RETURN s.id AS id, s.document_id AS document_id, s.text AS text"
	hs2PENDING      = "OPTIONAL MATCH (definition:PDDefinition) WITH definition ORDER BY definition.id WITH collect(definition) AS definitions MATCH (d:PDDocument) MATCH (v:PDVersion {document_id: d.id}) MATCH (s:PDSearchSource {id: v.current_source_id}) WHERE d.derived IS NOT NULL OR ((coalesce(s.record_profiled, 0) = 1 AND coalesce(s.profiled_run_layout, '') = coalesce(s.run_layout, '')) AND coalesce(s.profiled_current, false) = (v.id = d.current_version_id) AND coalesce(s.view_basis, '') = coalesce(s.view_version, '') + '|' + coalesce(s.run_layout, '') + '|' + coalesce(s.profiled_run_layout, '') + '|' + toString(coalesce(s.record_profiled, 0)) + '|' + coalesce(CASE WHEN s.set_copy_version IS NULL THEN s.set_version WHEN s.set_version IS NULL THEN s.set_copy_version ELSE s.set_version + '|' + s.set_copy_version END, '') + '|' + CASE WHEN d.rows_requested_at IS NULL THEN '' ELSE reduce(request = 'requested', skipped IN coalesce(d.rows_requested_skipped, []) | request + ',' + skipped) END) WITH definitions, d, v, s, v.id = d.current_version_id AS current, apoc.coll.sort([(d)-[:MEMBER_OF]->(__pd_collection:PDCollection) | __pd_collection.id]) AS collection_ids WITH d, v, s, current, collection_ids, [definition IN definitions WHERE (definition.status = 'active' AND definition.scope IN ['library'] + collection_ids)] AS applicable, CASE WHEN current THEN d.read_collection_ids ELSE s.read_collection_ids END AS read_in OPTIONAL MATCH (r:PDReading {id: CASE WHEN current THEN d.read_reading_id ELSE s.read_reading_id END}) WITH d, v, s, current, r, applicable, coalesce(r.definition_ids, []) <> [definition IN applicable | definition.id] OR coalesce(r.definition_versions, []) <> [definition IN applicable | definition.version] AS definitions_differ, size(coalesce(read_in, [])) <> size(collection_ids) OR any(id IN collection_ids WHERE NOT id IN coalesce(read_in, [])) AS collections_differ, coalesce(r.generation, 0) < CASE WHEN (size(coalesce(s.email_own_keys, [])) > 0 OR any(role IN coalesce(s.section_roles, []) WHERE role <> 'own')) THEN $email_generation ELSE $generation END AS generation_low WITH d, v, s, current, r, definitions_differ, collections_differ, generation_low, CASE WHEN definitions_differ THEN [definition IN applicable WHERE NOT any(i IN range(0, size(coalesce(r.definition_ids, [])) - 1) WHERE r.definition_ids[i] = definition.id AND coalesce(r.definition_versions, [])[i] = definition.version) | definition.id] + [id IN coalesce(r.definition_ids, []) WHERE NOT id IN [definition IN applicable | definition.id]] ELSE [] END AS changed_definitions WITH d, v, s, current, r, definitions_differ, changed_definitions, collections_differ, generation_low, r IS NULL OR definitions_differ OR collections_differ OR generation_low AS stale WITH d, v, s, current, r, stale, definitions_differ, changed_definitions, collections_differ, generation_low, current AND coalesce(d.read_source_id = s.id, false) AND stale AS definitions_changed WHERE (current AND (d.read_source_id IS NULL OR d.read_source_id <> s.id OR definitions_changed OR (d.due_at IS NOT NULL))) OR (NOT current AND (s.read_reading_id IS NULL OR stale)) RETURN d.id AS document_id, v.id AS version_id, s.id AS source_id, definitions_changed, current, r.id AS reading_id, CASE WHEN r IS NULL OR (current AND d.read_source_id IS NULL) OR (NOT current AND s.read_reading_id IS NULL) THEN 'new_upload' WHEN current AND d.read_source_id <> s.id THEN 'source_changed' WHEN generation_low THEN 'release_reprocessing' WHEN definitions_differ THEN 'definition_changed' WHEN collections_differ THEN 'collection_changed' ELSE last(coalesce(d.due_kinds, [])) END AS cause, CASE WHEN r IS NULL OR (current AND d.read_source_id IS NULL) OR (NOT current AND s.read_reading_id IS NULL) OR (current AND d.read_source_id <> s.id) OR generation_low THEN [] WHEN definitions_differ THEN changed_definitions WHEN collections_differ OR CASE WHEN coalesce(last(coalesce(d.due_elements, [])), '') = '' THEN null ELSE last(d.due_elements) END IS NULL THEN [] ELSE [CASE WHEN coalesce(last(coalesce(d.due_elements, [])), '') = '' THEN null ELSE last(d.due_elements) END] END AS cause_ids ORDER BY CASE WHEN r IS NULL OR (current AND (d.read_source_id IS NULL OR d.read_source_id <> s.id)) THEN 0 ELSE 1 END, CASE WHEN current THEN 0 ELSE 1 END, document_id, version_id"
)

func TestHeapSpikePulses(t *testing.T) {
	if os.Getenv("HS2_RUN") != "1" {
		t.Skip("lab-only: set HS2_RUN=1")
	}
	stopMiB := uint64(hsEnvI("HS_STOP_MIB", 9000))
	if vs := os.Getenv("HS2_VARIANTS"); vs != "" {
		f := hsEnvF("HS2_VARIANT_FRAC", 0.125)
		knobs := map[string]map[string]string{
			"baseline":   {},
			"oneset":     {"HS2_ONESET": "1"},
			"smalltx":    {"HS2_SMALLTX": "50"},
			"citedconc8": {"HS2_CITED_CONC": "8"},
			"noprops":    {"HS2_TEXTS_PROPS": "0"},
			"nocited":    {"HS2_CITED": "0", "HS2_DEFDUE": "0"},
			"chunk1656":  {"HS_CHUNK_CHARS": "1656"},
			"chunk828":   {"HS_CHUNK_CHARS": "828"},
			"chunk414":   {"HS_CHUNK_CHARS": "414"},
		}
		base := map[string]string{}
		for _, k := range []string{"HS_CHUNK_CHARS"} {
			if v, ok := os.LookupEnv(k); ok {
				base[k] = v
			}
		}
		for _, v := range strings.Split(vs, ",") {
			for _, k := range []string{"HS2_ONESET", "HS2_SMALLTX", "HS2_CITED_CONC", "HS2_TEXTS_PROPS", "HS2_CITED", "HS2_DEFDUE", "HS_CHUNK_CHARS"} {
				os.Unsetenv(k)
			}
			for k, val := range base {
				os.Setenv(k, val)
			}
			for k, val := range knobs[v] {
				os.Setenv(k, val)
			}
			hsLog("VARIANT %s", v)
			var peak uint64
			t.Run("variant_"+v, func(t *testing.T) { peak = hs2RunOne(t, f) })
			hsLog("VARIANT_DONE %s peak_MiB=%d", v, peak>>20)
			runtime.GC()
			debug.FreeOSMemory()
		}
		return
	}
	for _, sc := range strings.Split(hsEnvS("HS2_SCALES", "0.0625"), ",") {
		f, _ := strconv.ParseFloat(sc, 64)
		var peak uint64
		t.Run(fmt.Sprintf("f%g", f), func(t *testing.T) { peak = hs2RunOne(t, f) })
		runtime.GC()
		debug.FreeOSMemory()
		if peak>>20 > stopMiB {
			hsLog("STOP peak_MiB=%d exceeds HS_STOP_MIB=%d; larger scales skipped", peak>>20, stopMiB)
			break
		}
	}
}

type hs2Src struct {
	id, doc, coll string
	size          int
	keys          int
}

func hs2Strs(prefix string, n int, r *rand.Rand) []interface{} {
	o := make([]interface{}, n)
	for i := range o {
		o[i] = fmt.Sprintf("%s%07d", prefix, r.Intn(10000000))
	}
	return o
}
func hs2Floats(n int, r *rand.Rand) []interface{} {
	o := make([]interface{}, n)
	for i := range o {
		o[i] = r.Float64()
	}
	return o
}

func hs2RunOne(t *testing.T, frac float64) uint64 {
	hsStart = time.Now()
	chunkChars := hsEnvI("HS_CHUNK_CHARS", 207)
	bgTotal := hsEnvI("HS2_BG_TOTAL", 460000)
	citedN := hsEnvI("HS2_CITED_N", 369)
	citedConc := hsEnvI("HS2_CITED_CONC", 0)
	citedWindow := time.Duration(hsEnvI("HS2_CITED_WINDOW_MS", 20000)) * time.Millisecond
	textsProps := hsEnvI("HS2_TEXTS_PROPS", 1) == 1
	smallTx := hsEnvI("HS2_SMALLTX", 0)
	oneSet := hsEnvI("HS2_ONESET", 0) == 1
	defDue := hsEnvI("HS2_DEFDUE", 1) == 1
	cited := hsEnvI("HS2_CITED", 1) == 1
	pulseBOn := hsEnvI("HS2_PULSE_B", 1) == 1
	pendingEvery := time.Duration(hsEnvI("HS2_PENDING_MS", 30000)) * time.Millisecond
	forceGC := time.Duration(hsEnvI("HS_FORCE_GC_MS", 0)) * time.Millisecond
	memLimit := int64(hsEnvF("HS_MEMLIMIT", 59055800320))
	debug.SetMemoryLimit(memLimit)
	bg := int(float64(bgTotal) * frac)
	hsLog("CONFIG2 frac=%.4f chunkChars=%d bg=%d citedN=%d citedConc=%d citedWindow=%s textsProps=%v smallTx=%d oneSet=%v defDue=%v cited=%v pulseB=%v pendingEvery=%s forceGC=%s memLimit=%d gomaxprocs=%d",
		frac, chunkChars, bg, citedN, citedConc, citedWindow, textsProps, smallTx, oneSet, defDue, cited, pulseBOn, pendingEvery, forceGC, memLimit, runtime.GOMAXPROCS(0))

	emb := &hsEmbedder{dims: 2048}
	server, authenticator := setupTestServerWithDatabaseConfig(t, func(c *nornicdb.Config) {
		c.Database.MVCCRetentionMaxVersions = 100
		c.Database.WALSyncMode = "immediate"
		c.Database.AsyncWritesEnabled = false
		c.Database.MVCCLifecycleEnabled = true
		c.Database.MVCCLifecycleCycleInterval = 0
		c.Memory.EmbeddingDimensions = 2048
		if inc := os.Getenv("HS2_EMBED_INCLUDE"); inc != "" {
			c.EmbeddingWorker.PropertiesInclude = strings.Split(inc, ",")
		}
	}, nil)
	hsLog("EMBED_INCLUDE %q", os.Getenv("HS2_EMBED_INCLUDE"))
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	cl := &hsClient{h: server.buildRouter(), token: token, db: server.dbManager.DefaultDatabaseName()}
	store, err := server.dbManager.GetStorage(server.dbManager.DefaultDatabaseName())
	if err != nil {
		t.Fatal(err)
	}
	if !server.db.SetEmbedder(emb) {
		t.Fatal("SetEmbedder refused")
	}
	for _, q := range []string{
		"CREATE INDEX hs_source_id IF NOT EXISTS FOR (s:PDSearchSource) ON (s.id)",
		"CREATE INDEX hs_doc_id IF NOT EXISTS FOR (d:PDDocument) ON (d.id)",
		"CREATE INDEX hs_ver_doc IF NOT EXISTS FOR (v:PDVersion) ON (v.document_id)",
		"CREATE INDEX hs_read_id IF NOT EXISTS FOR (r:PDReading) ON (r.id)",
		"CREATE INDEX hs_set_id IF NOT EXISTS FOR (g:PDRecordSet) ON (g.id)",
		"CREATE INDEX hs_coll_id IF NOT EXISTS FOR (c:PDCollection) ON (c.id)",
	} {
		if _, _, err := cl.autocommit(q, nil); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}

	// ---- load: library of the live shape, scaled by frac ----
	ctx := context.Background()
	r := rand.New(rand.NewSource(20261120))
	vocab := hsVocab(r, 30000)
	mk := func(n *storage.Node) {
		if _, err := store.CreateNode(ctx, n); err != nil {
			t.Fatal(err)
		}
	}
	defIDs, defVers := []interface{}{}, []interface{}{}
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("def-%d", i)
		defIDs, defVers = append(defIDs, id), append(defVers, int64(1))
		mk(&storage.Node{ID: storage.NodeID(id), Labels: []string{"PDDefinition"}, Properties: map[string]interface{}{"id": id, "status": "active", "scope": "library", "version": int64(1)}})
	}
	collSrc := map[string][]hs2Src{}
	var all []hs2Src
	var totalText, totalChunks, totalCells, totalKeys int
	for _, c := range hsCollections {
		mk(&storage.Node{ID: storage.NodeID("coll:" + c.name), Labels: []string{"PDCollection"}, Properties: map[string]interface{}{"id": c.name, "name": c.name}})
		n := int(float64(c.sources)*frac + 0.5)
		if n < 1 {
			n = 1
		}
		rest := (c.textChars - c.maxText) / max(1, c.sources-1)
		for i := 0; i < n; i++ {
			size := rest
			if i == 0 {
				size = c.maxText
			}
			src := fmt.Sprintf("%s:src:%05d", c.name, i)
			doc := fmt.Sprintf("%s:doc:%05d", c.name, i)
			ver := fmt.Sprintf("%s:ver:%05d", c.name, i)
			read := fmt.Sprintf("%s:read:%05d", c.name, i)
			text := vocab.text(r, size)
			nk := min(2000, size/160+10)
			nc := min(68898, size/124)
			ns := size/830 + 1
			props := map[string]interface{}{
				"id": src, "document_id": doc, "version_id": ver, "text": text, "view_text": text,
				"view_starts": []interface{}{int64(0)}, "view_ends": []interface{}{int64(size)},
				"view_source_starts": []interface{}{int64(0)}, "view_source_ends": []interface{}{int64(size)},
				"view_library": []interface{}{c.name}, "view_own": true, "view_basis": "vv|L|L|1|", "view_version": "vv", "read_observation": "obs",
				"set_keys": hs2Strs("k", nk, r), "set_weights": hs2Floats(nk, r), "set_total": r.Float64(),
				"set_pool_runs": hs2Strs("p", 5, r), "set_pool_keys": hs2Strs("q", 10, r), "set_pool_records": hs2Strs("w", 10, r),
				"cell_starts": hsIntList(nc, 124), "cell_ends": hsIntList(nc, 124), "cell_refs": hsIntList(nc, 1),
				"section_starts": hsIntList(ns, 830), "section_ends": hsIntList(ns, 830), "section_roles": hs2Strs("own", 0, r),
				"content_hash": "h" + src, "method": "parse", "page_count": int64(size/3000 + 1), "profiled_current": true,
				"profiled_run_layout": "L", "run_layout": "L", "quality_flags": []interface{}{}, "record_profiled": int64(1),
				"search_hidden": false, "tool": "pdf", "tool_version": "1", "email_message_ids": []interface{}{}, "email_own_keys": []interface{}{},
			}
			var chunks [][]float32
			for c0 := 0; chunkChars > 0 && c0 < size; c0 += chunkChars {
				chunks = append(chunks, emb.vec(text[c0:min(size, c0+chunkChars)]))
			}
			mk(&storage.Node{ID: storage.NodeID(src), Labels: []string{"PDSearchSource"}, Properties: props, ChunkEmbeddings: chunks})
			mk(&storage.Node{ID: storage.NodeID(ver), Labels: []string{"PDVersion"}, Properties: map[string]interface{}{"id": ver, "document_id": doc, "current_source_id": src}})
			mk(&storage.Node{ID: storage.NodeID(doc), Labels: []string{"PDDocument"}, Properties: map[string]interface{}{"id": doc, "current_version_id": ver, "read_source_id": src, "read_reading_id": read,
				"read_collection_ids": []interface{}{c.name}, "due_kinds": []interface{}{"release"}, "due_elements": []interface{}{""}, "due_times": []interface{}{"2026-11-20T04:00:00Z"}, "due_at": "2026-11-20T04:00:00Z", "title": "doc " + doc}})
			mk(&storage.Node{ID: storage.NodeID(read), Labels: []string{"PDReading"}, Properties: map[string]interface{}{"id": read, "definition_ids": defIDs, "definition_versions": defVers, "generation": int64(1)}})
			if err := store.CreateEdge(ctx, &storage.Edge{ID: storage.EdgeID("mo:" + doc), StartNode: storage.NodeID(doc), EndNode: storage.NodeID("coll:" + c.name), Type: "MEMBER_OF"}); err != nil {
				t.Fatal(err)
			}
			s := hs2Src{id: src, doc: doc, coll: c.name, size: size, keys: nk}
			collSrc[c.name] = append(collSrc[c.name], s)
			all = append(all, s)
			totalText += size
			totalChunks += len(chunks)
			totalCells += nc
			totalKeys += nk
		}
	}
	for i := 0; i < bg; i++ {
		id := fmt.Sprintf("bg:%07d", i)
		mk(&storage.Node{ID: storage.NodeID(id), Labels: []string{"PDOther"}, Properties: map[string]interface{}{"id": id, "name": vocab.text(r, 80)}})
	}
	hsLog("LOADED2 sources=%d text_chars=%d chunk_vectors=%d cells=%d set_keys=%d bg=%d", len(all), totalText, totalChunks, totalCells, totalKeys, bg)
	if err := server.db.BuildSearchIndexes(ctx); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var m0 runtime.MemStats
	runtime.ReadMemStats(&m0)
	hsLog("BASELINE live_after_gc_MiB=%d sys_MiB=%d", m0.HeapAlloc>>20, m0.Sys>>20)
	var baseProf bytes.Buffer
	_ = pprof.Lookup("heap").WriteTo(&baseProf, 0)

	smp := &hsSampler{peak: map[string]hsSample{}, stop: make(chan struct{}), done: make(chan struct{}), phase: "idle"}
	go smp.run(100*time.Millisecond, forceGC)
	stopAux := make(chan struct{})
	var auxWG sync.WaitGroup
	auxWG.Add(2)
	go func() {
		defer auxWG.Done()
		for {
			d, n, err := cl.autocommit(hs2PENDING, map[string]interface{}{"generation": int64(1), "email_generation": int64(1)})
			hsLog("PENDING dur_ms=%d bytes=%d err=%v", d.Milliseconds(), n, err)
			select {
			case <-stopAux:
				return
			case <-time.After(pendingEvery):
			}
		}
	}()
	go func() {
		defer auxWG.Done()
		tk := time.NewTicker(30 * time.Second)
		defer tk.Stop()
		for {
			select {
			case <-stopAux:
				return
			case <-tk.C:
			}
			code, _, d, _ := cl.do(http.MethodGet, "/nornicdb/embed/failures", nil)
			hsLog("EMBEDFAIL status=%d dur_ms=%d", code, d.Milliseconds())
		}
	}()

	var mu sync.Mutex
	commitDur := map[string][]int64{}
	var errs []string
	fail := func(what string, err error) {
		mu.Lock()
		errs = append(errs, what+": "+err.Error())
		mu.Unlock()
		hsLog("ERROR %s %v", what, err)
	}
	profileOne := func(coll string, srcs []hs2Src, rr *rand.Rand) error {
		batch := len(srcs)
		if smallTx > 0 {
			batch = smallTx
		}
		texts := hs2TEXTS
		if !textsProps {
			texts = hs2TEXTSNoProps
		}
		for b0 := 0; b0 < len(srcs); b0 += batch {
			part := srcs[b0:min(len(srcs), b0+batch)]
			var ids, sigs, prof, runsets, members, versions, copies []interface{}
			var groups []interface{}
			for i, s := range part {
				ids = append(ids, s.id)
				sigs = append(sigs, map[string]interface{}{"source_id": s.id, "keys": hs2Strs("k", s.keys, rr), "weights": hs2Floats(s.keys, rr), "total": rr.Float64(),
					"pool_runs": hs2Strs("p", 5, rr), "pool_keys": hs2Strs("q", 10, rr), "pool_records": hs2Strs("w", 10, rr), "current": true})
				prof = append(prof, map[string]interface{}{"source_id": s.id, "generation": int64(2), "layout": "L2"})
				runsets = append(runsets, map[string]interface{}{"source_id": s.id, "sets": []interface{}{map[string]interface{}{"id": s.id + ":run", "run": int64(1), "properties": map[string]interface{}{"table_key": "tk", "sample": "s", "kind": "run"}}}})
				gid := fmt.Sprintf("%s:grp:%04d", coll, (b0+i)/10)
				if (b0+i)%10 == 0 {
					groups = append(groups, map[string]interface{}{"id": gid, "template_digest": "d", "properties": map[string]interface{}{"table_key": "g", "kind": "group", "collection_id": coll}})
				}
				members = append(members, map[string]interface{}{"source_id": s.id, "record_set_id": gid, "records": int64(3), "layout": "L2",
					"residual_starts": []interface{}{int64(0)}, "residual_ends": []interface{}{int64(1)}, "repeat_starts": []interface{}{}, "repeat_ends": []interface{}{}})
				versions = append(versions, map[string]interface{}{"source_id": s.id, "version": "v2"})
				copies = append(copies, map[string]interface{}{"source_id": s.id, "runs": []interface{}{int64(1)}, "of": "x", "documents": []interface{}{s.doc}, "whole": false, "version": "v2"})
			}
			tx, d, n, err := cl.begin(texts, map[string]interface{}{"ids": ids})
			if err != nil {
				return fmt.Errorf("TEXTS: %w", err)
			}
			hsLog("STEP %s TEXTS n=%d dur_ms=%d resp_bytes=%d", coll, len(ids), d.Milliseconds(), n)
			type stmt struct {
				name, q string
				p       map[string]interface{}
			}
			stmts := []stmt{
				{"WRITE_SIGNATURES", hs2SIGNATURES, map[string]interface{}{"sources": sigs}},
				{"WRITE_PROFILED", hs2PROFILED, map[string]interface{}{"profiled": prof}},
				{"WRITE_RUN_SETS", hs2RUNSETS, map[string]interface{}{"sources": runsets}},
				{"WRITE_GROUPS", hs2GROUPS, map[string]interface{}{"unlinked": []interface{}{}, "departed": []interface{}{}, "collection": coll, "groups": groups, "members": members, "links": []interface{}{}, "versions": versions}},
				{"WRITE_COPIES", hs2COPIES, map[string]interface{}{"sources": copies}},
			}
			if oneSet {
				var combined []interface{}
				for i := range sigs {
					x := map[string]interface{}{}
					for _, part := range []interface{}{sigs[i], prof[i], members[i], versions[i], copies[i]} {
						for k, v := range part.(map[string]interface{}) {
							x[k] = v
						}
					}
					combined = append(combined, x)
				}
				// node properties in one statement; relationship work keeps its statements without node SETs
				stmts = []stmt{
					{"WRITE_ONESET", hs2ONESET, map[string]interface{}{"sources": combined}},
					{"WRITE_RUN_SETS", hs2RUNSETS, map[string]interface{}{"sources": runsets}},
					{"WRITE_GROUPS", hs2GROUPS, map[string]interface{}{"unlinked": []interface{}{}, "departed": []interface{}{}, "collection": coll, "groups": groups, "members": []interface{}{}, "links": []interface{}{}, "versions": []interface{}{}}},
				}
			}
			for _, st := range stmts {
				d, _, err := tx.run(st.q, st.p)
				if err != nil {
					return fmt.Errorf("%s: %w", st.name, err)
				}
				hsLog("STEP %s %s dur_ms=%d", coll, st.name, d.Milliseconds())
			}
			cd, err := tx.commit()
			if err != nil {
				return fmt.Errorf("COMMIT: %w", err)
			}
			mu.Lock()
			commitDur[coll] = append(commitDur[coll], cd.Milliseconds())
			mu.Unlock()
			hsLog("STEP %s COMMIT dur_ms=%d", coll, cd.Milliseconds())
		}
		for i := 0; i < len(srcs); i += 100 {
			var xs []interface{}
			for _, s := range srcs[i:min(len(srcs), i+100)] {
				xs = append(xs, map[string]interface{}{"source_id": s.id, "basis": "vv|L|L2|2|v2", "observation": nil, "view": map[string]interface{}{
					"view_text": vocab.text(rr, s.size), "view_starts": []interface{}{int64(0)}, "view_ends": []interface{}{int64(s.size)},
					"view_source_starts": []interface{}{int64(0)}, "view_source_ends": []interface{}{int64(s.size)}, "view_library": []interface{}{s.coll}, "view_own": true}})
			}
			d, _, err := cl.autocommit(hs2VIEWS, map[string]interface{}{"sources": xs})
			if err != nil {
				return fmt.Errorf("VIEWS: %w", err)
			}
			hsLog("STEP %s WRITE_VIEW_TEXT n=%d dur_ms=%d", coll, len(xs), d.Milliseconds())
		}
		return nil
	}

	var wg sync.WaitGroup
	run := func(name string, fn func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(); err != nil {
				fail(name, err)
			}
		}()
	}
	// Pulse A: the first collection (holding the largest source) alone.
	smp.setPhase("A")
	wst := time.Now()
	run("profile:disputes", func() error { return profileOne("disputes", collSrc["disputes"], rand.New(rand.NewSource(1))) })
	var citedCalls, citedBytes, citedMaxMs atomic.Int64
	if pulseBOn {
		time.Sleep(time.Duration(hsEnvI("HS2_B_DELAY_MS", 5000)) * time.Millisecond)
		smp.setPhase("B")
		for ci, c := range hsCollections[1:] {
			name := c.name
			seed := int64(ci + 2)
			run("profile:"+name, func() error { return profileOne(name, collSrc[name], rand.New(rand.NewSource(seed))) })
		}
		if defDue {
			time.Sleep(3 * time.Second)
			var items []interface{}
			for _, s := range all {
				read := strings.Replace(s.doc, ":doc:", ":read:", 1)
				items = append(items, map[string]interface{}{"document_id": s.doc, "reading_id": read, "cause": map[string]interface{}{"kind": "definition_changed", "element_id": "def-1"}})
			}
			run("DEFINITION_DUE", func() error {
				d, n, err := cl.autocommit(hs2DEFDUE, map[string]interface{}{"items": items, "now": "2026-11-20T04:15:39Z"})
				hsLog("STEP DEFINITION_DUE items=%d dur_ms=%d bytes=%d", len(items), d.Milliseconds(), n)
				return err
			})
		}
		if cited {
			time.Sleep(4 * time.Second)
			perCall := max(1, int(4.4*float64(len(all))/float64(citedN)+0.5))
			hsLog("CITED start calls=%d ids_per_call=%d window=%s conc=%d", citedN, perCall, citedWindow, citedConc)
			var sem chan struct{}
			if citedConc > 0 {
				sem = make(chan struct{}, citedConc)
			}
			cr := rand.New(rand.NewSource(99))
			for i := 0; i < citedN; i++ {
				var ids []interface{}
				for k := 0; k < perCall; k++ {
					ids = append(ids, all[cr.Intn(len(all))].id)
				}
				delay := time.Duration(int64(citedWindow) * int64(i) / int64(citedN))
				run("CITED", func() error {
					time.Sleep(delay)
					if sem != nil {
						sem <- struct{}{}
						defer func() { <-sem }()
					}
					d, n, err := cl.autocommit(hs2CITED, map[string]interface{}{"ids": ids})
					citedCalls.Add(1)
					citedBytes.Add(int64(n))
					for {
						cur := citedMaxMs.Load()
						if d.Milliseconds() <= cur || citedMaxMs.CompareAndSwap(cur, d.Milliseconds()) {
							break
						}
					}
					return err
				})
			}
		}
	}
	wg.Wait()
	hsLog("WORKLOAD_DONE dur_s=%.1f cited_calls=%d cited_bytes=%d cited_max_ms=%d", time.Since(wst).Seconds(), citedCalls.Load(), citedBytes.Load(), citedMaxMs.Load())
	smp.setPhase("after")
	var endProf bytes.Buffer
	_ = pprof.Lookup("heap").WriteTo(&endProf, 0)
	time.Sleep(15 * time.Second)
	close(stopAux)
	auxWG.Wait()
	close(smp.stop)
	<-smp.done

	phases := []string{}
	for p := range smp.phaseObj {
		phases = append(phases, p)
	}
	sort.Strings(phases)
	for _, p := range phases {
		o, l := smp.phaseObj[p], smp.phaseLive[p]
		hsLog("PHASEPEAK %s heapObj_MiB=%d at=%.1f liveAtLastGC_MiB=%d at=%.1f rss_MiB=%d cg_anon_MiB=%d", p, o.heapObj>>20, o.at, l.live>>20, l.at, o.rss>>20, o.cgAnon>>20)
	}
	if forceGC > 0 {
		hsLog("PEAK forcedGCLive MiB=%d at=%.1f", smp.forcedLive>>20, smp.forcedAt)
	}
	for _, c := range hsCollections {
		hsLog("COMMITS %s ms=%v", c.name, commitDur[c.name])
	}
	hsLog("GC gogc_percent=%d gomemlimit=%d", smp.gcPercent, smp.memLimit)
	hsAnalyze("baseline", baseProf.Bytes(), nil, "inuse_space", 12)
	if smp.liveProf != nil {
		hsAnalyze(fmt.Sprintf("maxlive@%.1f", smp.liveProfAt), smp.liveProf, nil, "inuse_space", 25)
	}
	if smp.forcedProf != nil {
		hsAnalyze(fmt.Sprintf("forcedmax@%.1f", smp.forcedAt), smp.forcedProf, nil, "inuse_space", 25)
	}
	hsAnalyze("workload-allocs", endProf.Bytes(), baseProf.Bytes(), "alloc_space", 25)
	pk := smp.peak["heapObjects"]
	hsLog("SUMMARY2 frac=%.4f sources=%d text_chars=%d chunks=%d bg=%d baseline_live_MiB=%d peak_heapObj_MiB=%d peak_live_MiB=%d peak_rss_MiB=%d peak_cg_anon_MiB=%d forced_live_MiB=%d citedConc=%d textsProps=%v smallTx=%d errors=%d",
		frac, len(all), totalText, totalChunks, bg, m0.HeapAlloc>>20, pk.heapObj>>20, smp.maxLive>>20, smp.peak["rss"].rss>>20, smp.peak["cgroupAnon"].cgAnon>>20, smp.forcedLive>>20, citedConc, textsProps, smallTx, len(errs))
	if len(errs) > 0 {
		t.Errorf("workload errors (%d): %v", len(errs), errs[:min(5, len(errs))])
	}
	return pk.heapObj
}
