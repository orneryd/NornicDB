package cypher

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/orneryd/nornicdb/apoc/util"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func newJSONStringifyExecutor(t testing.TB) *StorageExecutor {
	return NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "jsonstringify"))
}

func jsonStringifyValue(t *testing.T, exec *StorageExecutor, query string, params map[string]interface{}) interface{} {
	t.Helper()
	result, err := exec.Execute(context.Background(), query, params)
	require.NoError(t, err, query)
	require.Len(t, result.Rows, 1, query)
	return result.Rows[0][0]
}

// jsonStringifyFixture holds what a byte-exact serializer must get right:
// key order by code point, whole and exponent floats, escapes, non-ASCII,
// nested empty containers and null.
func jsonStringifyFixture() map[string]interface{} {
	return map[string]interface{}{
		"b": int64(1),
		"a": []interface{}{1.0, 100.0, 1e16, 1e15, 1e-5, 0.0001, math.Copysign(0, -1), 0.1, 1.5e300,
			5e-324, 1.7976931348623157e308, -1.5e-7, 2.5},
		"é": "x",
		"z": nil,
		"c": true,
		"s": "q\"\\/\n\r\t\b\f\x01\x7f é😀\u2028<>&",
		"n": map[string]interface{}{"y": []interface{}{}, "x": map[string]interface{}{}},
	}
}

// The python-canonical profile is json.dumps(value, sort_keys=True,
// separators=(",", ":"), ensure_ascii=False, allow_nan=False), byte for
// byte; python-sorted is json.dumps(value, sort_keys=True, default=str).
func TestJSONStringifyProfilesAreByteExact(t *testing.T) {
	exec := newJSONStringifyExecutor(t)
	params := map[string]interface{}{"value": jsonStringifyFixture()}

	canonical := jsonStringifyValue(t, exec, "RETURN nornicdb.json.stringify($value, {profile: 'python-canonical'}) AS v", params)
	require.Equal(t, `{"a":[1.0,100.0,1e+16,1000000000000000.0,1e-05,0.0001,-0.0,0.1,1.5e+300,5e-324,1.7976931348623157e+308,-1.5e-07,2.5],`+
		`"b":1,"c":true,"n":{"x":{},"y":[]},`+
		"\"s\":\"q\\\"\\\\/\\n\\r\\t\\b\\f\\u0001\x7f é😀\u2028<>&\","+
		`"z":null,"é":"x"}`, canonical)

	sorted := jsonStringifyValue(t, exec, "RETURN nornicdb.json.stringify($value, {profile: 'python-sorted'}) AS v", params)
	require.Equal(t, `{"a": [1.0, 100.0, 1e+16, 1000000000000000.0, 1e-05, 0.0001, -0.0, 0.1, 1.5e+300, 5e-324, 1.7976931348623157e+308, -1.5e-07, 2.5], `+
		`"b": 1, "c": true, "n": {"x": {}, "y": []}, `+
		`"s": "q\"\\/\n\r\t\b\f\u0001\u007f \u00e9\ud83d\ude00\u2028<>&", `+
		`"z": null, "\u00e9": "x"}`, sorted)

	// Keys sort by code point: U+FFFF before U+1F600, which UTF-16 order
	// would reverse.
	keys := jsonStringifyValue(t, exec, "RETURN nornicdb.json.stringify({b: 1, a: 2, B: 3, `ä`: 4, `😀`: 5, `\uffff`: 6}, {profile: 'python-canonical'}) AS v", nil)
	require.Equal(t, "{\"B\":3,\"a\":2,\"b\":1,\"ä\":4,\"\uffff\":6,\"😀\":5}", keys)

	// Literal floats stay floats, integers integers; null is the text null.
	for _, c := range []struct {
		query string
		want  interface{}
	}{
		{"RETURN nornicdb.json.stringify({b: 1, a: 1.0}, {profile: 'python-canonical'}) AS v", `{"a":1.0,"b":1}`},
		{"RETURN nornicdb.json.stringify([1, 2.0, 'x'], {profile: 'python-sorted'}) AS v", `[1, 2.0, "x"]`},
		{"RETURN nornicdb.json.stringify(null, {profile: 'python-canonical'}) AS v", `null`},
		{"RETURN nornicdb.json.stringify('a\"b', {profile: 'python-canonical'}) AS v", `"a\"b"`},
		{"UNWIND [1.0, 1e16] AS f RETURN nornicdb.json.stringify(collect(f), {profile: 'python-canonical'}) AS v", `[1.0,1e+16]`},
	} {
		query, want := c.query, c.want
		require.Equal(t, want, jsonStringifyValue(t, exec, query, nil), query)
	}

	// Non-finite floats: NaN, Infinity, -Infinity in python-sorted.
	nonFinite := map[string]interface{}{"value": []interface{}{math.NaN(), math.Inf(1), math.Inf(-1)}}
	require.Equal(t, `[NaN, Infinity, -Infinity]`, jsonStringifyValue(t, exec, "RETURN nornicdb.json.stringify($value, {profile: 'python-sorted'}) AS v", nonFinite))

	// Typed lists and maps from parameters and properties.
	typed := map[string]interface{}{"value": map[string]interface{}{"i": []int64{3, -1}, "f": []float64{1, 0.5}, "s": []string{"x"}, "m": map[string]string{"k": "v"}}}
	require.Equal(t, `{"f":[1.0,0.5],"i":[3,-1],"m":{"k":"v"},"s":["x"]}`, jsonStringifyValue(t, exec, "RETURN nornicdb.json.stringify($value, {profile: 'python-canonical'}) AS v", typed))
	_, err := exec.Execute(context.Background(), "CREATE (:Record {starts: [0, 5, 12], weights: [1.0, 2.5]})", nil)
	require.NoError(t, err)
	require.Equal(t, `{"starts": [0, 5, 12], "weights": [1.0, 2.5]}`, jsonStringifyValue(t, exec, "MATCH (r:Record) RETURN nornicdb.json.stringify(properties(r), {profile: 'python-sorted'}) AS v", nil))
}

// python-sorted writes temporal values and byte arrays as the Neo4j Python
// driver's str() of the value it reads; durations and points are the
// driver's tuples, lists in both profiles.
func TestJSONStringifyDriverValues(t *testing.T) {
	exec := newJSONStringifyExecutor(t)
	for _, c := range []struct {
		query string
		want  interface{}
	}{
		{"RETURN nornicdb.json.stringify(date('2024-03-05'), {profile: 'python-sorted'}) AS v", `"2024-03-05"`},
		{"RETURN nornicdb.json.stringify(localtime('09:08:07.000000123'), {profile: 'python-sorted'}) AS v", `"09:08:07.000000123"`},
		{"RETURN nornicdb.json.stringify(time('09:08:07+05:30'), {profile: 'python-sorted'}) AS v", `"09:08:07.000000000+05:30"`},
		{"RETURN nornicdb.json.stringify(time('09:08:07-03:30'), {profile: 'python-sorted'}) AS v", `"09:08:07.000000000-03:30"`},
		{"RETURN nornicdb.json.stringify(localdatetime('2024-03-05T09:08:07.5'), {profile: 'python-sorted'}) AS v", `"2024-03-05T09:08:07.500000000"`},
		{"RETURN nornicdb.json.stringify(datetime('2024-03-05T09:08:07Z'), {profile: 'python-sorted'}) AS v", `"2024-03-05T09:08:07.000000000+00:00"`},
		{"RETURN nornicdb.json.stringify(datetime('2024-03-05T09:08:07-03:30'), {profile: 'python-sorted'}) AS v", `"2024-03-05T09:08:07.000000000-03:30"`},
		{"RETURN nornicdb.json.stringify(datetime('2024-07-01T12:00:00[Europe/London]'), {profile: 'python-sorted'}) AS v", `"2024-07-01T12:00:00.000000000+01:00"`},
		// A named zone's negative half-hour offset: the driver floors it.
		{"RETURN nornicdb.json.stringify(datetime('2024-01-15T12:00:00[America/St_Johns]'), {profile: 'python-sorted'}) AS v", `"2024-01-15T12:00:00.000000000-04:30"`},
		{"RETURN nornicdb.json.stringify(duration({months: 14, days: 3, hours: 1, seconds: 5, nanoseconds: 7}), {profile: 'python-sorted'}) AS v", `[14, 3, 3605, 7]`},
		{"RETURN nornicdb.json.stringify(duration('-PT1.5S'), {profile: 'python-canonical'}) AS v", `[0,0,-1,-500000000]`},
		{"RETURN nornicdb.json.stringify(point({x: 1, y: 2}), {profile: 'python-sorted'}) AS v", `[1.0, 2.0]`},
		{"RETURN nornicdb.json.stringify(point({x: 1, y: 2, z: 3}), {profile: 'python-canonical'}) AS v", `[1.0,2.0,3.0]`},
		{"RETURN nornicdb.json.stringify({d: date('2024-03-05')}, {profile: 'python-sorted'}) AS v", `{"d": "2024-03-05"}`},
	} {
		query, want := c.query, c.want
		require.Equal(t, want, jsonStringifyValue(t, exec, query, nil), query)
	}
	bytes := map[string]interface{}{"value": []byte{0, 'a', '\'', '\\', 0xff, '\n'}}
	require.Equal(t, `"b\"\\x00a'\\\\\\xff\\n\""`, jsonStringifyValue(t, exec, "RETURN nornicdb.json.stringify($value, {profile: 'python-sorted'}) AS v", bytes))

	// The driver's rendering rules, value by value.
	require.Equal(t, `b'a"\''`, pythonBytesRepr([]byte(`a"'`)))
	require.Equal(t, `b'\t\r\x7f'`, pythonBytesRepr([]byte("\t\r\x7f")))
	lmt := time.Date(1850, 1, 1, 11, 40, 28, 0, time.UTC).In(time.FixedZone("Test/Zone", 19*60+32))
	text, ok := stringDefaultText(CypherDateTime{Time: lmt, ZoneID: "Test/Zone"})
	require.True(t, ok)
	require.Equal(t, "1850-01-01T12:00:28.000000000+00:20", text, "pytz rounds the offset to the minute; the clock is UTC plus it")
	text, _ = stringDefaultText(time.Date(2024, 1, 2, 3, 4, 5, 6, time.FixedZone("", -(9*3600+30*60))))
	require.Equal(t, "2024-01-02T03:04:05.000000006-09:30", text, "a fixed offset keeps its sign")
	text, _ = stringDefaultText(time.Date(2024, 1, 2, 3, 4, 5, 6, time.FixedZone("Pacific/Test", -(9*3600+30*60))))
	require.Equal(t, "2024-01-02T03:04:05.000000006-10:30", text, "a named zone's offset is floored")
	text, _ = stringDefaultText(time.Date(2024, 1, 2, 3, 4, 5, 0, time.FixedZone("Test/East", 30*60)))
	require.Equal(t, "2024-01-02T03:04:05.000000000+00:30", text)
	text, _ = stringDefaultText(time.Date(2024, 1, 2, 3, 4, 5, 0, time.FixedZone("Test/West", -30*60)))
	require.Equal(t, "2024-01-02T03:04:05.000000000-01:30", text)
	require.Equal(t, []interface{}{int64(0), int64(0), int64(1), int64(999999999)}, pythonDurationTuple(CypherDuration{Seconds: 2, Nanos: -1}))
	require.Equal(t, []interface{}{int64(0), int64(0), int64(0), int64(-999999999)}, pythonDurationTuple(CypherDuration{Seconds: -1, Nanos: 1}))
	require.Equal(t, "-001", jsonPad(-1, 4))
}

// The profiles reject what Python's json.dumps rejects, and the call's own
// mistakes, with a client error.
func TestJSONStringifyErrors(t *testing.T) {
	exec := newJSONStringifyExecutor(t)
	ctx := context.Background()
	for _, query := range []string{
		"RETURN nornicdb.json.stringify(date('2024-03-05'), {profile: 'python-canonical'})",
		"RETURN nornicdb.json.stringify([datetime('2024-03-05T09:08:07Z')], {profile: 'python-canonical'})",
		"RETURN nornicdb.json.stringify(uuid.v5('6ba7b811-9dad-11d1-80b4-00c04fd430c8', 'x'), {profile: 'python-sorted'})",
		"RETURN nornicdb.json.stringify([1], {profile: 'python'})",
		"RETURN nornicdb.json.stringify([1], {})",
		"RETURN nornicdb.json.stringify([1], null)",
		"RETURN nornicdb.json.stringify([1], 'python-canonical')",
		"RETURN nornicdb.json.stringify([1])",
		"CREATE (n:Item) RETURN nornicdb.json.stringify(n, {profile: 'python-sorted'})",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
	}
	for _, value := range []interface{}{math.NaN(), math.Inf(1), []byte("x")} {
		_, err := exec.Execute(ctx, "RETURN nornicdb.json.stringify($value, {profile: 'python-canonical'})", map[string]interface{}{"value": value})
		require.Error(t, err, value)
	}
	_, err := exec.Execute(ctx, "RETURN nornicdb.json.stringify([1], {profile: 'python'})", nil)
	require.ErrorContains(t, err, "python-canonical or python-sorted")
}

// apoc.util.md5 / sha1 / sha256 are APOC's: the digest of the values'
// concatenated strings, null adding nothing; built in, and the plugin's
// handlers give the same digests.
func TestApocUtilHashFunctions(t *testing.T) {
	exec := newJSONStringifyExecutor(t)
	digest := func(text string) string {
		sum := sha256.Sum256([]byte(text))
		return hex.EncodeToString(sum[:])
	}
	for _, c := range []struct {
		query string
		want  interface{}
	}{
		// FIPS 180-2 and RFC 1321 test vectors.
		{"RETURN apoc.util.sha256(['abc']) AS v", "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"},
		{"RETURN apoc.util.sha256(['a', 'b', null, 'c']) AS v", "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"},
		{"RETURN apoc.util.sha1(['abc']) AS v", "a9993e364706816aba3e25717850c26c9cd0d89d"},
		{"RETURN apoc.util.md5(['abc']) AS v", "900150983cd24fb0d6963f7d28e17f72"},
		{"RETURN apoc.util.sha256('abc') AS v", "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"},
		{"RETURN apoc.util.sha256(['é']) AS v", digest("é")},
		{"RETURN apoc.util.sha256(['ab', 1, true, 2.5, 1.0, 1e7, null]) AS v", digest("ab1true2.51.01.0E7")},
		{"RETURN apoc.util.sha256([[1, 'a', null], {k: 2}]) AS v", digest("[1, a, ]{k=2}")},
		{"UNWIND ['abc'] AS s RETURN apoc.util.sha256([s]) AS v", "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"},
	} {
		query, want := c.query, c.want
		require.Equal(t, want, jsonStringifyValue(t, exec, query, nil), query)
	}
	require.Equal(t, "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad", util.SHA256([]interface{}{"abc"}), "plugin handler")
	_, err := exec.Execute(context.Background(), "RETURN apoc.util.sha256(['a'], ['b'])", nil)
	require.Error(t, err)
}

// The contract's compositions in Cypher: a payload hash, a name-based ID
// over canonical parts, and a fingerprint over the python-sorted text,
// each equal to the digest or UUID computed outside the database.
func TestJSONStringifyHashAndIDCompositions(t *testing.T) {
	exec := newJSONStringifyExecutor(t)
	payload := map[string]interface{}{"kind": "note", "tags": []interface{}{"é", int64(2)}, "score": 0.5}
	canonicalPayload := `{"kind":"note","score":0.5,"tags":["é",2]}`
	sum := sha256.Sum256([]byte(canonicalPayload))
	require.Equal(t, hex.EncodeToString(sum[:]), jsonStringifyValue(t, exec,
		"RETURN apoc.util.sha256([nornicdb.json.stringify($payload, {profile: 'python-canonical'})]) AS v",
		map[string]interface{}{"payload": payload}))

	parts := []interface{}{"record", int64(3), "é"}
	wantID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(`example:record:["record",3,"é"]`)).String()
	require.Equal(t, wantID, jsonStringifyValue(t, exec,
		"RETURN toString(uuid.v5('6ba7b811-9dad-11d1-80b4-00c04fd430c8', 'example:' + $ns + ':' + nornicdb.json.stringify($parts, {profile: 'python-canonical'}))) AS v",
		map[string]interface{}{"ns": "record", "parts": parts}))

	value := map[string]interface{}{"b": []interface{}{int64(1), int64(2)}, "a": "é"}
	inner := `{"a": "\u00e9", "b": [1, 2]}`
	outer := `["{\"a\": \"\\u00e9\", \"b\": [1, 2]}"]`
	wantFingerprint := uuid.NewSHA1(uuid.NameSpaceURL, []byte("example:summary-input:"+outer)).String()
	result, err := exec.Execute(context.Background(), `
WITH nornicdb.json.stringify($value, {profile: 'python-sorted'}) AS inner
WITH inner, nornicdb.json.stringify([inner], {profile: 'python-canonical'}) AS outer
RETURN inner, outer, toString(uuid.v5('6ba7b811-9dad-11d1-80b4-00c04fd430c8', 'example:summary-input:' + outer)) AS fingerprint`,
		map[string]interface{}{"value": value})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{inner, outer, wantFingerprint}}, result.Rows)
}

// writeFloat is Python's float repr at the plain/exponent boundaries.
func TestJSONStringifyPythonFloatRepr(t *testing.T) {
	profile := jsonProfiles["python-canonical"]
	for value, want := range map[float64]string{
		1e-4: "0.0001", 9.999e-5: "9.999e-05", 1e16: "1e+16", 9999999999999998: "9999999999999998.0",
		123456.789: "123456.789", 1e100: "1e+100", -2.5e-10: "-2.5e-10", 0.3: "0.3", 1e21: "1e+21",
		float64(1 << 53): "9007199254740992.0", 1.5: "1.5",
	} {
		var out strings.Builder
		require.NoError(t, profile.writeFloat(&out, value))
		require.Equal(t, want, out.String(), value)
	}
}
