package cypher

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/orneryd/nornicdb/apoc/util"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func newDigestExecutor(t testing.TB) *StorageExecutor {
	return NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "digest"))
}

func digestValue(t *testing.T, exec *StorageExecutor, query string, params map[string]interface{}) interface{} {
	t.Helper()
	result, err := exec.Execute(context.Background(), query, params)
	require.NoError(t, err, query)
	require.Len(t, result.Rows, 1, query)
	return result.Rows[0][0]
}

func TestApocUtilHashFunctions(t *testing.T) {
	exec := newDigestExecutor(t)
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
		require.Equal(t, want, digestValue(t, exec, query, nil), query)
	}
	require.Equal(t, "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad", util.SHA256([]interface{}{"abc"}), "plugin handler")
	_, err := exec.Execute(context.Background(), "RETURN apoc.util.sha256(['a'], ['b'])", nil)
	require.Error(t, err)
}

// Expected bytes follow APOC 5.26.30 Utils.toHashString recursively. The
// standard library hashes source-specified text, independently of DigestText.
func TestApocUtilHashRecursiveNulls(t *testing.T) {
	testApocUtilHashRecursiveNulls(t)
}

func TestApocUtilHashTypedParameterLists(t *testing.T) {
	exec := newDigestExecutor(t)
	for _, tc := range []struct {
		value interface{}
		text  string
	}{
		{[]int64{1, 2}, "12"},
		{[]float64{1, 2.5}, "1.02.5"},
		{[]bool{true, false}, "truefalse"},
		{[]int64(nil), ""},
		{[]interface{}{[]int64{1, 2}, map[string]interface{}{"n": nil, "v": []float64{1, 2}}}, "[1, 2]{n=, v=[1.0, 2.0]}"},
	} {
		for _, name := range []string{"md5", "sha1", "sha256"} {
			var want string
			switch name {
			case "md5":
				sum := md5.Sum([]byte(tc.text))
				want = hex.EncodeToString(sum[:])
			case "sha1":
				sum := sha1.Sum([]byte(tc.text))
				want = hex.EncodeToString(sum[:])
			case "sha256":
				sum := sha256.Sum256([]byte(tc.text))
				want = hex.EncodeToString(sum[:])
			}
			require.Equal(t, want, digestValue(t, exec, "RETURN apoc.util."+name+"($items)", map[string]interface{}{"items": tc.value}), "%s %T", name, tc.value)
		}
	}
}

func testApocUtilHashRecursiveNulls(t *testing.T) {
	exec := newDigestExecutor(t)
	for _, tc := range []struct{ expression, text string }{
		{"[[1,null]]", "[1, ]"},
		{"[{a:null}]", "{a=}"},
		{"[[null,[null]]]", "[, []]"},
		{"[{z:[null,{b:null,a:1}],a:null}]", "{a=, z=[, {a=1, b=}]}"},
		{"[4.9e-324]", "4.9E-324"},
		{"[9.9e-324]", "9.9E-324"},
	} {
		for _, name := range []string{"md5", "sha1", "sha256"} {
			t.Run(name+tc.expression, func(t *testing.T) {
				var want string
				switch name {
				case "md5":
					sum := md5.Sum([]byte(tc.text))
					want = hex.EncodeToString(sum[:])
				case "sha1":
					sum := sha1.Sum([]byte(tc.text))
					want = hex.EncodeToString(sum[:])
				case "sha256":
					sum := sha256.Sum256([]byte(tc.text))
					want = hex.EncodeToString(sum[:])
				}
				require.Equal(t, want, digestValue(t, exec, "RETURN apoc.util."+name+"("+tc.expression+")", nil))
			})
		}
	}
	for _, name := range []string{"md5", "sha1", "sha256"} {
		for _, expr := range []string{"null", "$nil"} {
			_, err := exec.Execute(context.Background(), "RETURN apoc.util."+name+"("+expr+")", map[string]interface{}{"nil": nil})
			requireStatusCode(t, err, "Neo.ClientError.Procedure.ProcedureCallFailed")
		}
	}
}
