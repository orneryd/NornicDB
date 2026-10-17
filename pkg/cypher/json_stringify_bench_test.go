package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// The serializer profiles at the sizes they hash: a three-part name-based
// ID, a summary input (a 20-key record with 500-entry span lists, nested
// python-sorted text in a canonical list) and a 64 KiB payload digest.
func benchmarkJSONStringifyQuery(b *testing.B, query string, params map[string]interface{}) {
	exec := newJSONStringifyExecutor(b)
	ctx := context.Background()
	if _, err := exec.Execute(ctx, query, params); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := exec.Execute(ctx, query, params); err != nil {
			b.Fatal(err)
		}
	}
}

func jsonStringifySummaryInput() map[string]interface{} {
	starts, ends, roles := make([]interface{}, 500), make([]interface{}, 500), make([]interface{}, 500)
	for i := range starts {
		starts[i], ends[i], roles[i] = int64(i*40), int64(i*40+39), "own"
	}
	record := map[string]interface{}{
		"id": "record-0001", "version_id": "version-0001", "source_id": "source-0001",
		"content_hash":   "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
		"section_starts": starts, "section_ends": ends, "section_roles": roles,
		"collections": []interface{}{"c1", "c2", "c3"}, "generation": int64(2),
		"corrections": []interface{}{[]interface{}{"k1", int64(3), "accepted"}, []interface{}{"k2", int64(1), "open"}},
	}
	for i := 0; i < 10; i++ {
		record[fmt.Sprintf("field_%02d", i)] = fmt.Sprintf("value %d with é", i)
	}
	return record
}

func jsonStringifyPayload() []interface{} {
	items := make([]interface{}, 200)
	for i := range items {
		items[i] = map[string]interface{}{
			"id": fmt.Sprintf("item-%04d", i), "score": float64(i) / 7, "rank": int64(i),
			"text": "A sentence of ordinary text that stands for a passage, long enough to make the payload realistic é",
			"tags": []interface{}{"alpha", "beta", "gamma"}, "flag": i%2 == 0,
			"span": []interface{}{int64(i * 10), int64(i*10 + 9)},
		}
	}
	return items
}

func BenchmarkJSONStringifyStableID(b *testing.B) {
	benchmarkJSONStringifyQuery(b,
		"RETURN toString(uuid.v5('6ba7b811-9dad-11d1-80b4-00c04fd430c8', 'example:' + $ns + ':' + nornicdb.json.stringify($parts, {profile: 'python-canonical'}))) AS id",
		map[string]interface{}{"ns": "record", "parts": []interface{}{"source-0001", int64(3), "é"}})
}

func BenchmarkJSONStringifySummaryInput(b *testing.B) {
	benchmarkJSONStringifyQuery(b, `
WITH nornicdb.json.stringify($value, {profile: 'python-sorted'}) AS inner
RETURN toString(uuid.v5('6ba7b811-9dad-11d1-80b4-00c04fd430c8', 'example:summary-input:' + nornicdb.json.stringify([inner], {profile: 'python-canonical'}))) AS fingerprint`,
		map[string]interface{}{"value": jsonStringifySummaryInput()})
}

func BenchmarkJSONStringifyPayloadHash64KiB(b *testing.B) {
	payload := jsonStringifyPayload()
	var text strings.Builder
	if err := jsonProfiles["python-canonical"].write(&text, payload); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(text.Len()))
	benchmarkJSONStringifyQuery(b,
		"RETURN apoc.util.sha256([nornicdb.json.stringify($payload, {profile: 'python-canonical'})]) AS hash",
		map[string]interface{}{"payload": payload})
}
