package search

// Frozen compatibility INPUTS, independent of NornicDB's current writers/types.
// Sources: v1.4.3 and 24d08776c storage/{badger_helpers,property_codec,
// property_key_dictionary,id_dictionary,badger_serialization}.go and search
// {fulltext_index_v2_persist,vector_file_store,hnsw_index,build_settings}.go.
// The original archives at 297d0fb1a:pkg/search/testdata/store_compat/*.tgz.b64
// independently supplied the timestamps, map order, raw-record/file SHA256s.
// Badger's SST bytes vary; all 36 logical key/value records must match exactly.
// Do not replace these encoders, schemas or literal prefixes with current ones.

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// A map in its original serialized order, including the historical scalar widths.
// This uses only the third-party MessagePack primitive, never a production schema.
type legacyCompatField struct {
	key   string
	value any
}
type legacyCompatMap []legacyCompatField

func (fields legacyCompatMap) MarshalMsgpack() ([]byte, error) {
	var out bytes.Buffer
	enc := msgpack.NewEncoder(&out)
	if err := enc.EncodeMapLen(len(fields)); err != nil {
		return nil, err
	}
	for _, field := range fields {
		if err := enc.EncodeString(field.key); err != nil {
			return nil, err
		}
		if err := enc.Encode(field.value); err != nil {
			return nil, err
		}
	}
	return out.Bytes(), nil
}

func legacyCompatUint64(value uint64) []byte {
	return binary.BigEndian.AppendUint64(nil, value)
}

func legacyCompatRecords(t *testing.T, scalar bool) map[string][]byte {
	t.Helper()
	created := []int64{1792322758307347420, 1792322758307474945, 1792322758307522636, 1792322758313961565}
	versions := []uint64{1792322758307353010, 1792322758307476025, 1792322758307523346, 1792322758313966935}
	if scalar {
		created = []int64{1792322762489285187, 1792322762489520015, 1792322762489613448, 1792322762498576266}
		versions = []uint64{1792322762489292097, 1792322762489522025, 1792322762489615268, 1792322762498583026}
	}
	texts := []string{"alpha written first", "bravo named vector", "charlie property vector", "delta live write"}
	records := map[string][]byte{}
	for i, text := range texts {
		id := "nornic:" + string(rune('a'+i))
		num := uint64(i + 1)
		// Historical property dictionary IDs: text=1, p=2. Properties have a
		// uvarint count and (uvarint ID, MessagePack value) pairs, not a map.
		props := []byte{1, 1}
		if i == 2 {
			props[0] = 2
		}
		props = append(props, fixtureowner.Encode[any](t, msgpack.Marshal, text)...)
		if i == 2 {
			props = append(props, 2)
			props = append(props, fixtureowner.Encode[any](t, msgpack.Marshal, []float32{0, 0, 1})...)
		}
		var named, chunks any
		switch i {
		case 0:
			chunks = [][]float32{{1, 0, 0}}
		case 1:
			named = legacyCompatMap{{"v", []float32{0, 1, 0}}}
		case 3:
			chunks = [][]float32{{0.6, 0.8, 0}}
		}
		timestamp := time.Unix(0, created[i]).UTC()
		body := fixtureowner.Encode[any](t, msgpack.Marshal, legacyCompatMap{
			{"ID", id}, {"Labels", []string{"Doc"}}, {"Properties", nil},
			{"CreatedAt", timestamp}, {"UpdatedAt", timestamp},
			{"NamedEmbeddings", named}, {"ChunkEmbeddings", chunks}, {"EmbedMeta", nil},
			{"EmbeddingsStoredSeparately", false}, {"VisibilitySuppressed", false},
		})
		// V2 node discriminator and NDB serializer header are intentionally
		// literal: a coordinated current writer/reader format change must fail.
		value := binary.AppendUvarint([]byte{0x10}, uint64(len(props)))
		value = append(value, props...)
		value = append(value, []byte{0xff, 'N', 'D', 'B', 1, 2}...)
		value = append(value, body...)
		records[string(append([]byte{0x01}, id...))] = value
		labelKey := append([]byte{0x03, 'D', 'o', 'c', 0}, legacyCompatUint64(num)...)
		records[string(labelKey)] = []byte{}
		head := append([]byte{1, 0}, legacyCompatUint64(versions[i])...)
		head = append(head, legacyCompatUint64(num)...)
		records[string(append([]byte{0x0e}, legacyCompatUint64(num)...))] = head
		records[string(append([]byte{0x1a}, id...))] = legacyCompatUint64(num)
		records[string(append([]byte{0x1d}, legacyCompatUint64(num)...))] = []byte(id)
		records["\x10accessmeta:"+id] = fixtureowner.Encode[any](t, msgpack.Marshal, legacyCompatMap{
			{"targetId", id}, {"targetScope", "NODE"},
			{"fixed", legacyCompatMap{{"accessCount", int64(0)}, {"lastAccessedAt", int64(0)}, {"traversalCount", int64(0)}, {"lastTraversedAt", int64(0)}}},
			{"lastMutatedAt", int64(0)}, {"mutationCount", int64(0)},
			{"indexKeys", [][]byte{labelKey}}, {"hasIndexKeys", true},
		})
	}
	// Schema/readiness markers, numeric ID counter and namespace label count.
	records["\x10\x02"] = legacyCompatUint64(3)
	records["\x10\x03"] = []byte{1}
	records["\x10\x05"] = []byte{3}
	records["\x10\x06"] = []byte{1}
	records["\x10\x07nornic\x00Doc"] = legacyCompatUint64(4)
	records["\x10\x09"] = []byte{1}
	records["\x1cn"] = legacyCompatUint64(4)
	// Dictionary key layout: prefix, uvarint namespace length, namespace,
	// then uvarint name length/name (forward) or uvarint ID (reverse).
	records["\x20\x06nornic\x04text"] = []byte{1}
	records["\x20\x06nornic\x01p"] = []byte{2}
	records["\x21\x06nornic\x01"] = []byte("text")
	records["\x21\x06nornic\x02"] = []byte("p")
	records["\x22\x06nornic"] = []byte{2}
	return records
}

func legacyCompatBM25(scalar bool) legacyCompatMap {
	docs := []string{"Doc alpha written first", "Doc bravo named vector", "Doc charlie property vector p 0 0 1", "Doc delta live write"}
	ids := []string{"nornic:a", "nornic:b", "nornic:c", "nornic:d"}
	docOrder, numOrder := []int{2, 3, 0, 1}, []int{1, 3, 2, 0}
	termOrder := "written first alpha bravo named live delta 1 0 property doc write vector p charlie"
	if scalar {
		docOrder, numOrder = []int{0, 3, 1, 2}, []int{0, 1, 2, 3}
		termOrder = "bravo written property p vector alpha live 1 first 0 delta named charlie doc write"
	}
	var documents, numbers, terms legacyCompatMap
	for _, i := range docOrder {
		documents = append(documents, legacyCompatField{ids[i], docs[i]})
	}
	for _, i := range numOrder {
		numbers = append(numbers, legacyCompatField{ids[i], uint32(i)})
	}
	// Frozen postings, not current tokenization/Index/Save. TF is uint16;
	// document numbers/lengths are uint32; IDF and average length are float64.
	postings := map[string][]uint32{
		"alpha": {0}, "written": {0}, "first": {0}, "bravo": {1}, "named": {1},
		"charlie": {2}, "property": {2}, "p": {2}, "0": {2}, "1": {2},
		"delta": {3}, "live": {3}, "write": {3}, "vector": {1, 2}, "doc": {0, 1, 2, 3},
	}
	for _, term := range strings.Fields(termOrder) {
		var list []legacyCompatMap
		for _, doc := range postings[term] {
			tf := uint16(1)
			if term == "0" {
				tf = 2
			}
			list = append(list, legacyCompatMap{{"d", doc}, {"t", tf}})
		}
		terms = append(terms, legacyCompatField{term, legacyCompatMap{{"p", list}, {"i", float64(0)}}})
	}
	return legacyCompatMap{
		{"v", "2.1.0"}, {"docs", documents}, {"doc_id_to_num", numbers}, {"doc_num_to_id", ids},
		{"doc_lengths", []uint32{4, 4, 8, 4}}, {"terms", terms},
		{"lexicon", strings.Fields("0 1 alpha bravo charlie delta doc first live named p property vector write written")},
		{"avg_doc_len", float64(5)}, {"doc_count", 4}, {"total_doc_len", int64(20)},
	}
}

func legacyCompatVectorMeta(scalar bool) legacyCompatMap {
	ids := []string{"nornic:a", "nornic:b-named-v", "nornic:c-prop-p", "nornic:d"}
	var ordinals, labels, chunks legacyCompatMap
	for _, i := range []int{2, 3, 0, 1} {
		ordinals = append(ordinals, legacyCompatField{ids[i], int64(i)})
	}
	labelOrder, chunkOrder := []int{2, 3, 1, 0}, []int{0, 3}
	if scalar {
		labelOrder, chunkOrder = []int{1, 3, 0, 2}, []int{3, 0}
	}
	for _, i := range labelOrder {
		labels = append(labels, legacyCompatField{"nornic:" + string(rune('a'+i)), []string{"Doc"}})
	}
	for _, i := range chunkOrder {
		chunks = append(chunks, legacyCompatField{ids[i], []string{ids[i]}})
	}
	return legacyCompatMap{
		{"v", 2}, {"dim", 3}, {"id2ord", ordinals}, {"slots", int64(4)}, {"node_labels", labels},
		{"node_named_vectors", legacyCompatMap{{"nornic:b", legacyCompatMap{{"v", ids[1]}}}}},
		{"node_property_vectors", legacyCompatMap{{"nornic:c", legacyCompatMap{{"p", ids[2]}}}}},
		{"node_chunk_vectors", chunks}, {"node_metadata_persisted", true}, {"node_types_persisted", true},
	}
}

func legacyCompatHNSW(scalar bool) legacyCompatMap {
	quantConfig := legacyCompatMap{{"Mode", "scalar"}, {"BinaryQueryBits", 0}, {"PQCompression", 0}, {"TrainingSampleMax", 0}, {"TrainingIterations", 0}}
	var config, snapshot legacyCompatMap
	if scalar {
		config = append(config, legacyCompatField{"Quantization", quantConfig})
		snapshot = append(snapshot, legacyCompatField{"Quantization", legacyCompatMap{
			{"Config", quantConfig}, {"Dimensions", 3}, {"Stride", 3},
			{"Codes", []byte{127, 0, 0, 0, 127, 0, 0, 0, 127, 95, 127, 0}},
			{"Scales", []float32{1.0 / 127, 1.0 / 127, 1.0 / 127, 0.8 / 127}}, {"Binary", nil}, {"Books", nil},
		}})
	}
	config = append(config, legacyCompatMap{
		{"M", 16}, {"EfConstruction", 100}, {"EfSearch", 50}, {"SearchBeamFactor", 4},
		{"LevelMultiplier", float64(0.36067376022224085)}, {"UseGPUBuild", true},
		{"GPUBuildBatchSize", 2048}, {"GPUBuildCandidateK", 128}, {"GPUBuildDistancePrecision", "fp32"},
	}...)
	neighbors := make([]uint32, 128)
	for i, row := range [][]uint32{{1, 2, 3}, {0, 2, 3}, {0, 1, 3}, {1, 0, 2}} {
		copy(neighbors[i*32:], row)
	}
	ids := []string{"nornic:a", "nornic:b-named-v", "nornic:c-prop-p", "nornic:d"}
	order := []int{3, 2, 1, 0}
	if scalar {
		order = []int{2, 0, 3, 1}
	}
	var idToInternal legacyCompatMap
	for _, i := range order {
		idToInternal = append(idToInternal, legacyCompatField{ids[i], uint32(i)})
	}
	return append(snapshot, legacyCompatMap{
		{"Version", "1.2.0"}, {"Config", config}, {"Dimensions", 3}, {"NodeLevel", []uint16{0, 0, 0, 0}},
		{"VecOff", nil}, {"NeighborsArena", neighbors}, {"NeighborsOff", []int32{0, 32, 64, 96}},
		{"NeighborCountsAr", []uint16{3, 3, 3, 3}}, {"NeighborCountsOff", []int32{0, 1, 2, 3}},
		{"IDToInternal", idToInternal}, {"InternalToID", ids}, {"Deleted", []bool{false, false, false, false}},
		{"LiveCount", 4}, {"Vectors", nil}, {"EntryPoint", uint32(0)}, {"HasEntryPoint", true}, {"MaxLevel", 0},
	}...)
}

func legacyCompatFiles(t *testing.T, scalar bool) map[string][]byte {
	t.Helper()
	saved := int64(1792322758)
	if scalar {
		saved = 1792322762
	}
	settings := legacyCompatMap{
		{"format_version", 1}, {"saved_at_unix", saved},
		{"bm25", "schema=3;format=2.1.0;tokenizer=unicode-nfkc-casefold-v1;stemmer=none;stemmer_api=none;stemmer_version=none;stemmer_sha256=none;props=*"},
		{"vector", "schema=2;format=1.0.0;dimensions=3"},
		{"hnsw", "schema=1;format=1.2.0;m=16;efc=100;efs=50;gpu_build=true"},
		{"routing", "schema=1;mode=hybrid;w_sem=0.7000;w_lex=0.3000;lex_profile=1;kmeans_max_iter=5"},
	}
	// NVF v2 is a fixed 64-byte header and four contiguous little-endian
	// float32 rows; IDs/ordinals live in .meta. No current vector writer.
	vectors := make([]byte, 64)
	copy(vectors, []byte{'N', 'V', 'F', '\n', 2, 3})
	for _, row := range [][]float32{{1, 0, 0}, {0, 1, 0}, {0, 0, 1}, {0.6, 0.8, 0}} {
		for _, value := range row {
			vectors = binary.LittleEndian.AppendUint32(vectors, math.Float32bits(value))
		}
	}
	return map[string][]byte{
		"bm25":           fixtureowner.Encode[any](t, msgpack.Marshal, legacyCompatBM25(scalar)),
		"build_settings": fixtureowner.Encode[any](t, msgpack.Marshal, settings),
		"hnsw":           fixtureowner.Encode[any](t, msgpack.Marshal, legacyCompatHNSW(scalar)),
		"vectors.meta":   fixtureowner.Encode[any](t, msgpack.Marshal, legacyCompatVectorMeta(scalar)),
		"vectors.vec":    vectors,
	}
}

// Archive-derived SHA256s, not outputs blessed from this generator. Records
// are raw-byte-key sorted, rendered lowercase hex(key)+" "+hex(value)+"\n".
// File hashes cover every original byte, including map order and saved timestamps.
func requireLegacyCompatGoldens(t *testing.T, scalar bool, records map[string][]byte, files map[string][]byte) {
	t.Helper()
	wantRecords := "c700d8afd55189b41a8200868c59f785f0c04fc4ee61d782cd92be7cf7b87369"
	wantFiles := map[string]string{
		"bm25":           "cad5c6fa32f58edfca5fbcb90b13e5620892152293014b2ee0f55aa483260a6a",
		"build_settings": "487d511a4fbc1e861262f84bc34cba1008857b077d19b3112f58863c3a6ce157",
		"hnsw":           "42a9e0751cae0195d5a0129fced2257246f4a6b506ab270cd1a1515ea6e3c7db",
		"vectors.meta":   "c913dd08b9b9e95fa710c456646887e07caf8ef146edbd16c6e259b472870602",
		"vectors.vec":    "c45c15cbe49febaeea95320c382f19b69e2d51008988b1ae1cebf850f5200cf7",
	}
	if scalar {
		wantRecords = "287a1f71b8dde530a4ef92335dac9126d5ae802d9a5a0d8d051a8ca2d8a9ec03"
		wantFiles = map[string]string{
			"bm25":           "eb5557815f7b7c3ad8c287ca966c56d7d6bb64981c01ac02c1c0cb03a76e0827",
			"build_settings": "07e2da45b50a5607f5f8db2eded960cbc1561f0af9552c3262da86c89e498cad",
			"hnsw":           "d46b4e1a799f8a6a37bd4fcff61ac99526e7948b5623b38abdb136bfee740702",
			"vectors.meta":   "5352f2294a37abd341f029c261a736ddf1ad799a081668387fd1dfd2a9a40e3b",
			"vectors.vec":    "c45c15cbe49febaeea95320c382f19b69e2d51008988b1ae1cebf850f5200cf7",
		}
	}
	var keys []string
	for key := range records {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	hash := sha256.New()
	for _, key := range keys {
		fmt.Fprintf(hash, "%s %s\n", hex.EncodeToString([]byte(key)), hex.EncodeToString(records[key]))
	}
	require.Len(t, records, 36)
	require.Equal(t, wantRecords, fmt.Sprintf("%x", hash.Sum(nil)), "original archive raw Badger records")
	require.Len(t, files, len(wantFiles))
	for name, want := range wantFiles {
		require.Equal(t, want, fmt.Sprintf("%x", sha256.Sum256(files[name])), "original archive file %s", name)
	}
}

func writeLegacyCompatStore(t *testing.T, dir string, scalar bool) {
	t.Helper()
	records, files := legacyCompatRecords(t, scalar), legacyCompatFiles(t, scalar)
	requireLegacyCompatGoldens(t, scalar, records, files)
	graphDir := filepath.Join(dir, "graph")
	require.NoError(t, os.MkdirAll(graphDir, 0o755))
	// Direct third-party raw KV writes; never storage.NewBadgerEngine/CreateNode.
	db, err := fixtureowner.TrackResult[*badger.DB](t, nil)(badger.Open(badger.DefaultOptions(graphDir).WithLogger(nil)))
	if db != nil {
		fixtureowner.Own(t, db)
	}
	require.NoError(t, err)
	err = db.Update(func(txn *badger.Txn) error {
		for key, value := range records {
			if err := txn.Set([]byte(key), value); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, db.Close())
	searchDir := filepath.Join(dir, "search")
	require.NoError(t, os.MkdirAll(searchDir, 0o755))
	for name, data := range files {
		require.NoError(t, os.WriteFile(filepath.Join(searchDir, name), data, 0o644))
	}
}
