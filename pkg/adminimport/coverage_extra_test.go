package adminimport

import (
	"archive/zip"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// (*Error).Error and (*Error).Unwrap
// ----------------------------------------------------------------------------
// These pure-data methods are not exercised by the higher-level table-driven
// scenarios in importer_test.go. They have to handle four states cleanly:
//   1. nil receiver         → empty string
//   2. wrapped + message    → "message: wrapped.Error()"
//   3. only message         → "message"
//   4. wrapped accessible via errors.Unwrap / errors.Is
// ============================================================================

func TestImportError_ErrorAndUnwrap_ReturnsExpectedShapes(t *testing.T) {
	cases := []struct {
		name string
		err  *Error
		want string
	}{
		{
			name: "nil receiver returns empty string",
			err:  nil,
			want: "",
		},
		{
			name: "message only, no wrapped error",
			err:  &Error{ExitCode: ExitCSV, Message: "boom"},
			want: "boom",
		},
		{
			name: "message plus wrapped error",
			err:  &Error{ExitCode: ExitCSV, Message: "boom", Err: errors.New("io eof")},
			want: "boom: io eof",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, tc.err.Error())
		})
	}

	t.Run("unwrap exposes the wrapped sentinel for errors.Is", func(t *testing.T) {
		inner := errors.New("inner sentinel")
		outer := &Error{ExitCode: ExitCSV, Message: "outer", Err: inner}
		require.True(t, errors.Is(outer, inner), "errors.Is must traverse Unwrap")
		require.Same(t, inner, errors.Unwrap(outer))
	})

	t.Run("unwrap returns nil when no wrapped error", func(t *testing.T) {
		bare := &Error{Message: "plain"}
		require.Nil(t, bare.Unwrap())
	})
}

func TestImportError_PreservesEnglishAndCarriesSemanticMessage(t *testing.T) {
	cause := errors.New("permission denied")
	err := newImportError(ExitCSV, localization.AdminImportOpenCSVFailed(), cause)

	require.Equal(t, ExitCSV, err.ExitCode)
	require.Equal(t, "failed to open CSV file: permission denied", err.Error())
	require.Equal(t, localization.MessageAdminImportOpenCSVFailed, err.LocalizedMessage.ID)
	require.True(t, errors.Is(err, cause))
}

// ============================================================================
// canonicalID — explicit coverage for integer vs string ID modes.
// Existing tests only exercise the default string path indirectly.
// ============================================================================

func TestCanonicalID_NormalizesAndRoundtrips(t *testing.T) {
	cases := []struct {
		name      string
		value     string
		idType    string
		want      string
		expectErr bool
	}{
		{"default string mode passes through", "u1", "string", "u1", false},
		{"empty idType defaults to string", "abc", "", "abc", false},
		{"integer mode strips leading zeros", "0042", "integer", "42", false},
		{"integer mode keeps negative sign", "-7", "integer", "-7", false},
		{"integer mode rejects non-integer", "abc", "integer", "", true},
		{"integer mode rejects empty value", "", "integer", "", true},
		{"integer keyword is case-insensitive", "9", "INTEGER", "9", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := canonicalID(tc.value, tc.idType)
			if tc.expectErr {
				require.Error(t, err)
				require.Empty(t, got)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// ============================================================================
// DefaultNeo4jCSV*Path — trivial but uncovered by existing tests.
// ============================================================================

func TestDefaultNeo4jCSVPaths_UseConstantFileNames(t *testing.T) {
	dir := "/tmp/csv-out"
	require.Equal(t, filepath.Join(dir, Neo4jCSVSchemaFileName), DefaultNeo4jCSVSchemaPath(dir))
	require.Equal(t, filepath.Join(dir, Neo4jCSVNornicSchemaFileName), DefaultNeo4jCSVNornicSchemaPath(dir))
	require.Equal(t, "schema.cypher", Neo4jCSVSchemaFileName)
	require.Equal(t, "schema.nornic.json", Neo4jCSVNornicSchemaFileName)
}

// ============================================================================
// inferValueShape — exhaustive table covering every documented branch.
// ============================================================================

func TestInferValueShape_CoversEveryTypeArm(t *testing.T) {
	// Reflect-slice arm: a custom slice type that does not match any explicit
	// type switch arm forces inferValueShape to fall through to the reflect
	// path (and exercises allFloatLikeReflect on a non-empty non-float slice).
	type strSlice []string
	type floatSlice []float32

	cases := []struct {
		name       string
		input      any
		kind       string
		scalarType string
		dims       int
	}{
		{"float32 slice → vector", []float32{1, 2, 3}, "vector", "vector", 3},
		{"float64 slice → vector", []float64{1, 2}, "vector", "vector", 2},
		{"empty []any → empty string array", []any{}, "array", "string", 0},
		{"[]any of float → vector inferred", []any{float64(1), float64(2)}, "vector", "vector", 2},
		{"[]any mixed → array with scalar from first element", []any{"a", "b"}, "array", "string", 0},
		{"[]any whose first element is a vector demotes to string scalar",
			[]any{[]float32{1, 2}, []float32{3, 4}}, "array", "string", 0},
		{"[]string → string array", []string{"x"}, "array", "string", 0},
		{"[]bool → boolean array", []bool{true, false}, "array", "boolean", 0},
		{"[]int → long array", []int{1, 2}, "array", "long", 0},
		{"[]int64 → long array", []int64{3, 4}, "array", "long", 0},
		{"string scalar", "hello", "property", "string", 0},
		{"bool scalar", true, "property", "boolean", 0},
		{"int scalar", int(7), "property", "long", 0},
		{"int8 scalar", int8(7), "property", "long", 0},
		{"int16 scalar", int16(7), "property", "long", 0},
		{"int32 scalar", int32(7), "property", "long", 0},
		{"int64 scalar", int64(7), "property", "long", 0},
		{"uint scalar", uint(7), "property", "long", 0},
		{"uint8 scalar", uint8(7), "property", "long", 0},
		{"uint16 scalar", uint16(7), "property", "long", 0},
		{"uint32 scalar", uint32(7), "property", "long", 0},
		{"uint64 scalar", uint64(7), "property", "long", 0},
		{"float32 scalar", float32(1.5), "property", "double", 0},
		{"float64 scalar", float64(1.5), "property", "double", 0},
		{"time.Time scalar → datetime", time.Unix(0, 0), "property", "datetime", 0},
		{"unrecognised scalar falls back to string", struct{ X int }{1}, "property", "string", 0},
		{"reflect slice of strings → array string", strSlice{"a", "b"}, "array", "string", 0},
		{"reflect slice of float32 → vector", floatSlice{1, 2, 3}, "vector", "vector", 3},
		{"reflect slice empty → empty string array", strSlice{}, "array", "string", 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kind, scalarType, dims := inferValueShape(tc.input)
			require.Equal(t, tc.kind, kind)
			require.Equal(t, tc.scalarType, scalarType)
			require.Equal(t, tc.dims, dims)
		})
	}
}

// ============================================================================
// allFloatLikeReflect — every branch of the reflect-based predicate.
// ============================================================================

func TestAllFloatLikeReflect_HandlesEdgeCases(t *testing.T) {
	t.Run("invalid value returns false", func(t *testing.T) {
		var zero reflect.Value
		require.False(t, allFloatLikeReflect(zero))
	})
	t.Run("non-slice returns false", func(t *testing.T) {
		require.False(t, allFloatLikeReflect(reflect.ValueOf("nope")))
	})
	t.Run("empty slice returns false", func(t *testing.T) {
		require.False(t, allFloatLikeReflect(reflect.ValueOf([]float32{})))
	})
	t.Run("float32 slice returns true", func(t *testing.T) {
		require.True(t, allFloatLikeReflect(reflect.ValueOf([]float32{1, 2})))
	})
	t.Run("float64 slice returns true", func(t *testing.T) {
		require.True(t, allFloatLikeReflect(reflect.ValueOf([]float64{1, 2})))
	})
	t.Run("mixed-type element returns false at first non-float", func(t *testing.T) {
		require.False(t, allFloatLikeReflect(reflect.ValueOf([]any{float64(1), "x"})))
	})
}

// ============================================================================
// formatScalar — every type arm including time formatting branches.
// ============================================================================

func TestFormatScalar_EveryTypeAndTimeBranch(t *testing.T) {
	someTime := time.Date(2026, 3, 14, 15, 9, 26, 535000000, time.UTC)
	cases := []struct {
		name       string
		value      any
		scalarType string
		want       string
	}{
		{"string passes through", "abc", "string", "abc"},
		{"bool true", true, "boolean", "true"},
		{"bool false", false, "boolean", "false"},
		{"int", int(-5), "long", "-5"},
		{"int8", int8(7), "long", "7"},
		{"int16", int16(7), "long", "7"},
		{"int32", int32(7), "long", "7"},
		{"int64", int64(-1), "long", "-1"},
		{"uint", uint(11), "long", "11"},
		{"uint8", uint8(11), "long", "11"},
		{"uint16", uint16(11), "long", "11"},
		{"uint32", uint32(11), "long", "11"},
		{"uint64", uint64(11), "long", "11"},
		{"float32 trims trailing zeros", float32(1.5), "double", "1.5"},
		{"float64 trims trailing zeros", float64(2.25), "double", "2.25"},
		{"time.Time with datetime hint → RFC3339Nano in UTC", someTime, "datetime",
			"2026-03-14T15:09:26.535Z"},
		{"time.Time with non-datetime hint → t.String()", someTime, "string",
			someTime.String()},
		{"unsupported type falls back to fmt.Sprint", struct{ V int }{42}, "string", "{42}"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, formatScalar(tc.value, tc.scalarType))
		})
	}
}

// ============================================================================
// formatPropertyValue — vector, array, and default scalar paths.
// ============================================================================

func TestFormatPropertyValue_RendersVectorArrayAndScalar(t *testing.T) {
	opts := Neo4jCSVExportOptions{ArrayDelimiter: ';', VectorDelimiter: ','}

	t.Run("nil value renders as empty string", func(t *testing.T) {
		require.Equal(t, "", formatPropertyValue(nil, inferredColumn{Kind: "property", ScalarType: "string"}, opts))
	})

	t.Run("vector from []float32 uses vector delimiter", func(t *testing.T) {
		got := formatPropertyValue([]float32{0.1, 0.2, 0.3},
			inferredColumn{Kind: "vector", VectorDims: 3}, opts)
		require.Equal(t, "0.10000000149011612,0.20000000298023224,0.30000001192092896", got)
	})

	t.Run("vector from []float64 uses vector delimiter and trims zeros", func(t *testing.T) {
		got := formatPropertyValue([]float64{1.5, 2.5},
			inferredColumn{Kind: "vector", VectorDims: 2}, opts)
		require.Equal(t, "1.5,2.5", got)
	})

	t.Run("vector from []any of float renders each via formatScalar", func(t *testing.T) {
		got := formatPropertyValue([]any{float64(1.5), float64(2.5)},
			inferredColumn{Kind: "vector", VectorDims: 2}, opts)
		require.Equal(t, "1.5,2.5", got)
	})

	t.Run("vector with unsupported underlying type falls back to fmt.Sprint", func(t *testing.T) {
		got := formatPropertyValue("scalar-not-vector",
			inferredColumn{Kind: "vector"}, opts)
		require.Equal(t, "scalar-not-vector", got)
	})

	t.Run("array of []any uses array delimiter", func(t *testing.T) {
		got := formatPropertyValue([]any{"a", "b"},
			inferredColumn{Kind: "array", ScalarType: "string"}, opts)
		require.Equal(t, "a;b", got)
	})

	t.Run("array of []string uses array delimiter", func(t *testing.T) {
		got := formatPropertyValue([]string{"x", "y"},
			inferredColumn{Kind: "array", ScalarType: "string"}, opts)
		require.Equal(t, "x;y", got)
	})

	t.Run("array of []bool", func(t *testing.T) {
		got := formatPropertyValue([]bool{true, false},
			inferredColumn{Kind: "array", ScalarType: "boolean"}, opts)
		require.Equal(t, "true;false", got)
	})

	t.Run("array of []int", func(t *testing.T) {
		got := formatPropertyValue([]int{1, 2, 3},
			inferredColumn{Kind: "array", ScalarType: "long"}, opts)
		require.Equal(t, "1;2;3", got)
	})

	t.Run("array of []int64", func(t *testing.T) {
		got := formatPropertyValue([]int64{1, 2, 3},
			inferredColumn{Kind: "array", ScalarType: "long"}, opts)
		require.Equal(t, "1;2;3", got)
	})

	t.Run("array reflect-path renders via formatScalar", func(t *testing.T) {
		type custom []float64
		got := formatPropertyValue(custom{1.5, 2.5},
			inferredColumn{Kind: "array", ScalarType: "double"}, opts)
		require.Equal(t, "1.5;2.5", got)
	})

	t.Run("array of non-slice value falls back to fmt.Sprint", func(t *testing.T) {
		got := formatPropertyValue("scalar",
			inferredColumn{Kind: "array", ScalarType: "string"}, opts)
		require.Equal(t, "scalar", got)
	})

	t.Run("property scalar delegates to formatScalar", func(t *testing.T) {
		got := formatPropertyValue(int64(42),
			inferredColumn{Kind: "property", ScalarType: "long"}, opts)
		require.Equal(t, "42", got)
	})
}

// ============================================================================
// parseScalar — unsupported type plus error wrapping.
// ============================================================================

func TestParseScalar_AcceptsAllNumericAndRejectsUnsupported(t *testing.T) {
	opts := Options{}.withDefaults()

	cases := []struct {
		name      string
		value     string
		typ       string
		wantVal   any
		expectErr bool
	}{
		{"empty type defaults to string", "abc", "", "abc", false},
		{"explicit string", "abc", "string", "abc", false},
		{"char treated as string", "abc", "char", "abc", false},
		{"point opaque string", "[1,2]", "point", "[1,2]", false},
		{"date opaque string", "2024-01-01", "date", "2024-01-01", false},
		{"localtime opaque", "12:00:00", "localtime", "12:00:00", false},
		{"time opaque", "12:00:00Z", "time", "12:00:00Z", false},
		{"localdatetime opaque", "2024-01-01T00:00:00", "localdatetime", "2024-01-01T00:00:00", false},
		{"datetime opaque", "2024-01-01T00:00:00Z", "datetime", "2024-01-01T00:00:00Z", false},
		{"duration opaque", "PT1H", "duration", "PT1H", false},
		{"int parses", "42", "int", int64(42), false},
		{"long parses", "42", "long", int64(42), false},
		{"short parses", "42", "short", int64(42), false},
		{"byte parses", "42", "byte", int64(42), false},
		{"int rejects non-numeric", "abc", "int", nil, true},
		{"float parses", "1.5", "float", float64(1.5), false},
		{"double parses", "1.5", "double", float64(1.5), false},
		{"float rejects non-numeric", "abc", "float", nil, true},
		{"boolean true", "true", "boolean", true, false},
		{"boolean uppercase TRUE", "TRUE", "boolean", true, false},
		{"boolean false", "false", "boolean", false, false},
		{"boolean rejects garbage", "maybe", "boolean", nil, true},
		{"unsupported type errors", "x", "json", nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseScalar(tc.value, tc.typ, opts)
			if tc.expectErr {
				require.Error(t, err)
				require.Nil(t, got)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantVal, got)
		})
	}
}

func TestParseScalar_RespectsNormalizeTypesFlagPath(t *testing.T) {
	// The "if opts.NormalizeTypes" branch in the int arm is otherwise
	// unreachable through importer-level tests. Both arms return the same
	// int64, but we cover the branch.
	opts := Options{NormalizeTypes: true}.withDefaults()
	got, err := parseScalar("123", "int", opts)
	require.NoError(t, err)
	require.Equal(t, int64(123), got)
}

// ============================================================================
// parseColumnSpec — header tokens (LABEL, IGNORE, START_ID, END_ID, TYPE,
// EMBEDDING, etc.) and error branches that the existing scenario tests skim
// over.
// ============================================================================

func TestParseColumnSpec_RecognisesEveryHeaderToken(t *testing.T) {
	cases := []struct {
		name     string
		raw      string
		node     bool
		expected columnSpec
		wantErr  bool
	}{
		{"empty header column becomes ignore",
			"", true, columnSpec{Kind: kindIgnore}, false},
		{":ID without space",
			":ID", true, columnSpec{Kind: kindID, IDSpace: ""}, false},
		{":ID(User) carries the ID space",
			":ID(User)", true, columnSpec{Kind: kindID, IDSpace: "User"}, false},
		{":LABEL alone",
			":LABEL", true, columnSpec{Kind: kindLabel}, false},
		{":IGNORE alone",
			":IGNORE", true, columnSpec{Kind: kindIgnore}, false},
		{":START_ID(User)",
			":START_ID(User)", false, columnSpec{Kind: kindStartID, IDSpace: "User"}, false},
		{":END_ID(User)",
			":END_ID(User)", false, columnSpec{Kind: kindEndID, IDSpace: "User"}, false},
		{":TYPE alone",
			":TYPE", false, columnSpec{Kind: kindType}, false},
		{"named embedding uses key argument",
			":EMBEDDING(text)", true,
			columnSpec{Kind: kindNamedEmbedding, EmbedKey: "text", Options: map[string]string{}, VectorDims: 0}, false},
		{"named embedding parses dimensions option",
			":EMBEDDING(text){dimensions:7}", true,
			columnSpec{Kind: kindNamedEmbedding, EmbedKey: "text", Options: map[string]string{"dimensions": "7"}, VectorDims: 7}, false},
		{"named embedding without key defaults to 'default'",
			":EMBEDDING", true,
			columnSpec{Kind: kindNamedEmbedding, EmbedKey: "default", Options: map[string]string{}, VectorDims: 0}, false},
		{"named embedding rejects non-numeric dimensions",
			":EMBEDDING(text){dimensions:NaN}", true, columnSpec{}, true},
		{"unsupported header token errors",
			":SOMETHING_ELSE", true, columnSpec{}, true},
		{"named property defaults to string when no type",
			"name", true, columnSpec{Name: "name", Kind: kindProperty, Type: "string", Options: map[string]string{}, IDSpace: ""}, false},
		{"named property with explicit type",
			"age:int", true, columnSpec{Name: "age", Kind: kindProperty, Type: "int", Options: map[string]string{}, IDSpace: ""}, false},
		{"named ID column",
			"slug:ID(User)", true, columnSpec{Name: "slug", Kind: kindID, IDSpace: "User"}, false},
		{"vector property captures dimensions",
			"emb:vector{dimensions:3}", true,
			columnSpec{Name: "emb", Kind: kindProperty, Type: "vector", Options: map[string]string{"dimensions": "3"}, VectorDims: 3, IDSpace: ""}, false},
		{"vector property with non-numeric dimensions errors",
			"emb:vector{dimensions:bad}", true, columnSpec{}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseColumnSpec(tc.raw, tc.node)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, got)
		})
	}
}

// ============================================================================
// openOne — gzip and zip archive variants exercised end-to-end (regular
// CSV path is already covered by the importer scenario tests).
// ============================================================================

func TestOpenOne_ReadsGzipAndZipArchives(t *testing.T) {
	dir := t.TempDir()

	t.Run("plain CSV passes through and Close closes the underlying file", func(t *testing.T) {
		path := filepath.Join(dir, "plain.csv")
		require.NoError(t, os.WriteFile(path, []byte("id,name\n1,alice\n"), 0o600))
		reader, closer, err := openOne(path)
		require.NoError(t, err)
		body, readErr := io.ReadAll(reader)
		require.NoError(t, readErr)
		require.Equal(t, "id,name\n1,alice\n", string(body))
		require.NoError(t, closer.Close())
	})

	t.Run("gzip archive decompresses", func(t *testing.T) {
		path := filepath.Join(dir, "compact.csv.gz")
		f, err := os.Create(path)
		require.NoError(t, err)
		gz := gzip.NewWriter(f)
		_, err = gz.Write([]byte("id,name\n1,alice\n"))
		require.NoError(t, err)
		require.NoError(t, gz.Close())
		require.NoError(t, f.Close())

		reader, closer, err := openOne(path)
		require.NoError(t, err)
		body, readErr := io.ReadAll(reader)
		require.NoError(t, readErr)
		require.Equal(t, "id,name\n1,alice\n", string(body))
		require.NoError(t, closer.Close())
	})

	t.Run("zip archive with single member decompresses", func(t *testing.T) {
		path := filepath.Join(dir, "single.zip")
		f, err := os.Create(path)
		require.NoError(t, err)
		zw := zip.NewWriter(f)
		w, err := zw.Create("inner.csv")
		require.NoError(t, err)
		_, err = w.Write([]byte("id,name\n1,alice\n"))
		require.NoError(t, err)
		require.NoError(t, zw.Close())
		require.NoError(t, f.Close())

		reader, closer, err := openOne(path)
		require.NoError(t, err)
		body, readErr := io.ReadAll(reader)
		require.NoError(t, readErr)
		require.Equal(t, "id,name\n1,alice\n", string(body))
		require.NoError(t, closer.Close())
	})

	t.Run("zip archive with multiple members is rejected", func(t *testing.T) {
		path := filepath.Join(dir, "multi.zip")
		f, err := os.Create(path)
		require.NoError(t, err)
		zw := zip.NewWriter(f)
		for _, name := range []string{"a.csv", "b.csv"} {
			w, werr := zw.Create(name)
			require.NoError(t, werr)
			_, werr = w.Write([]byte("id\n1\n"))
			require.NoError(t, werr)
		}
		require.NoError(t, zw.Close())
		require.NoError(t, f.Close())

		_, _, err = openOne(path)
		require.Error(t, err)
		var ierr *Error
		require.ErrorAs(t, err, &ierr)
		require.Equal(t, ExitUnsupported, ierr.ExitCode)
	})

	t.Run("missing file returns ExitCSV error", func(t *testing.T) {
		_, _, err := openOne(filepath.Join(dir, "nope.csv"))
		require.Error(t, err)
		var ierr *Error
		require.ErrorAs(t, err, &ierr)
		require.Equal(t, ExitCSV, ierr.ExitCode)
	})

	t.Run("corrupted gzip returns ExitCSV error", func(t *testing.T) {
		path := filepath.Join(dir, "garbage.csv.gz")
		require.NoError(t, os.WriteFile(path, []byte("not a gzip"), 0o600))
		_, _, err := openOne(path)
		require.Error(t, err)
		var ierr *Error
		require.ErrorAs(t, err, &ierr)
		require.Equal(t, ExitCSV, ierr.ExitCode)
	})

	t.Run("corrupted zip returns ExitCSV error", func(t *testing.T) {
		path := filepath.Join(dir, "garbage.zip")
		require.NoError(t, os.WriteFile(path, []byte("not a zip"), 0o600))
		_, _, err := openOne(path)
		require.Error(t, err)
		var ierr *Error
		require.ErrorAs(t, err, &ierr)
		require.Equal(t, ExitCSV, ierr.ExitCode)
	})
}

// ============================================================================
// closers.Close — propagates the first error while still closing remaining
// members. Hand-rolled multi-closer should not short-circuit and lose
// secondary cleanup if the first close returns an error.
// ============================================================================

type fakeCloser struct {
	closed *bool
	err    error
}

func (f fakeCloser) Close() error {
	*f.closed = true
	return f.err
}

func TestClosers_ClosesAllMembersAndReturnsFirstError(t *testing.T) {
	t.Run("all successes returns nil", func(t *testing.T) {
		var a, b bool
		c := closers{fakeCloser{closed: &a}, fakeCloser{closed: &b}}
		require.NoError(t, c.Close())
		require.True(t, a)
		require.True(t, b)
	})

	t.Run("returns first non-nil error and still closes the rest", func(t *testing.T) {
		var a, b, c bool
		boom := errors.New("boom")
		later := errors.New("later")
		group := closers{
			fakeCloser{closed: &a, err: boom},
			fakeCloser{closed: &b, err: later},
			fakeCloser{closed: &c},
		}
		err := group.Close()
		require.ErrorIs(t, err, boom)
		require.True(t, a)
		require.True(t, b, "second closer must still run after first error")
		require.True(t, c, "third closer must still run after first error")
	})

	t.Run("empty closers slice returns nil", func(t *testing.T) {
		require.NoError(t, closers{}.Close())
	})
}

// ============================================================================
// renderConstraintStatement — every constraint type, including the
// validation branches that drop malformed configurations.
// ============================================================================

func TestRenderConstraintStatement_HandlesEveryConstraintType(t *testing.T) {
	cases := []struct {
		name   string
		input  storage.Constraint
		expect string
		ok     bool
	}{
		{
			name: "unique constraint",
			input: storage.Constraint{
				Name: "person_email_unique", Label: "Person",
				Type: storage.ConstraintUnique, Properties: []string{"email"},
			},
			expect: "CREATE CONSTRAINT `person_email_unique` IF NOT EXISTS FOR (n:`Person`) REQUIRE n.`email` IS UNIQUE",
			ok:     true,
		},
		{
			name: "unique constraint with multiple properties is rejected",
			input: storage.Constraint{
				Name: "x", Label: "Person", Type: storage.ConstraintUnique,
				Properties: []string{"a", "b"},
			},
			ok: false,
		},
		{
			name: "node key composite",
			input: storage.Constraint{
				Name: "person_natural_key", Label: "Person",
				Type: storage.ConstraintNodeKey, Properties: []string{"country", "passport"},
			},
			expect: "CREATE CONSTRAINT `person_natural_key` IF NOT EXISTS FOR (n:`Person`) REQUIRE (n.`country`, n.`passport`) IS NODE KEY",
			ok:     true,
		},
		{
			name: "exists constraint",
			input: storage.Constraint{
				Name: "person_name_exists", Label: "Person",
				Type: storage.ConstraintExists, Properties: []string{"name"},
			},
			expect: "CREATE CONSTRAINT `person_name_exists` IF NOT EXISTS FOR (n:`Person`) REQUIRE n.`name` IS NOT NULL",
			ok:     true,
		},
		{
			name: "exists constraint with multiple properties is rejected",
			input: storage.Constraint{
				Name: "x", Label: "Person", Type: storage.ConstraintExists,
				Properties: []string{"a", "b"},
			},
			ok: false,
		},
		{
			name: "relationship key constraint",
			input: storage.Constraint{
				Name: "rel_key", Label: "KNOWS", EntityType: storage.ConstraintEntityRelationship,
				Type: storage.ConstraintRelationshipKey, Properties: []string{"since"},
			},
			expect: "CREATE CONSTRAINT `rel_key` IF NOT EXISTS FOR ()-[r:`KNOWS`]-() REQUIRE (r.`since`) IS RELATIONSHIP KEY",
			ok:     true,
		},
		{
			name: "temporal no-overlap constraint",
			input: storage.Constraint{
				Name: "rel_temporal", Label: "WORKS_AT",
				EntityType: storage.ConstraintEntityRelationship,
				Type:       storage.ConstraintTemporal, Properties: []string{"validFrom", "validTo"},
			},
			expect: "CREATE CONSTRAINT `rel_temporal` IF NOT EXISTS FOR ()-[r:`WORKS_AT`]-() REQUIRE (r.`validFrom`, r.`validTo`) IS TEMPORAL NO OVERLAP",
			ok:     true,
		},
		{
			name: "domain constraint with allowed values",
			input: storage.Constraint{
				Name: "person_status_domain", Label: "Person",
				Type:          storage.ConstraintDomain,
				Properties:    []string{"status"},
				AllowedValues: []interface{}{"active", "inactive", int64(3), true, 1.5},
			},
			expect: "CREATE CONSTRAINT `person_status_domain` IF NOT EXISTS FOR (n:`Person`) REQUIRE n.`status` IN ['active', 'inactive', 3, true, 1.5]",
			ok:     true,
		},
		{
			name: "domain constraint with multiple properties is rejected",
			input: storage.Constraint{
				Name: "x", Label: "Person", Type: storage.ConstraintDomain,
				Properties:    []string{"a", "b"},
				AllowedValues: []interface{}{"x"},
			},
			ok: false,
		},
		{
			name: "cardinality on relationship emits MAX COUNT",
			input: storage.Constraint{
				Name: "rel_max", Label: "KNOWS",
				EntityType: storage.ConstraintEntityRelationship,
				Type:       storage.ConstraintCardinality,
				Direction:  "OUTGOING",
				MaxCount:   5,
			},
			expect: "CREATE CONSTRAINT `rel_max` IF NOT EXISTS FOR ()-[r:`KNOWS`]->() REQUIRE MAX COUNT 5",
			ok:     true,
		},
		{
			name: "cardinality on node is rejected",
			input: storage.Constraint{
				Name: "n_max", Label: "Person",
				Type: storage.ConstraintCardinality, MaxCount: 1,
			},
			ok: false,
		},
		{
			name: "policy on relationship emits raw policy mode",
			input: storage.Constraint{
				Name: "rel_policy", Label: "KNOWS",
				EntityType:  storage.ConstraintEntityRelationship,
				Type:        storage.ConstraintPolicy,
				SourceLabel: "Person", TargetLabel: "Person",
				PolicyMode: "ALLOW",
			},
			expect: "CREATE CONSTRAINT `rel_policy` IF NOT EXISTS FOR (:`Person`)-[r:`KNOWS`]->(:`Person`) REQUIRE ALLOW",
			ok:     true,
		},
		{
			name: "policy on node is rejected",
			input: storage.Constraint{
				Name: "n_policy", Label: "Person",
				Type: storage.ConstraintPolicy, PolicyMode: "ALLOW",
			},
			ok: false,
		},
		{
			name: "unknown constraint type is rejected",
			input: storage.Constraint{
				Name: "x", Label: "Person", Type: storage.ConstraintType("BOGUS"),
			},
			ok: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := renderConstraintStatement(tc.input)
			require.Equal(t, tc.ok, ok)
			if tc.ok {
				require.Equal(t, tc.expect, got)
			}
		})
	}
}

// ============================================================================
// cypherRelationshipPattern — direction + endpoint label combinations.
// ============================================================================

func TestCypherRelationshipPattern_DirectionAndEndpoints(t *testing.T) {
	cases := []struct {
		name      string
		label     string
		direction string
		src       string
		dst       string
		want      string
	}{
		{"outgoing default direction with no endpoints",
			"KNOWS", "", "", "",
			"()-[r:`KNOWS`]->()"},
		{"outgoing explicit",
			"KNOWS", "OUTGOING", "", "",
			"()-[r:`KNOWS`]->()"},
		{"incoming",
			"KNOWS", "INCOMING", "", "",
			"()<-[r:`KNOWS`]-()"},
		{"incoming lowercase",
			"KNOWS", "incoming", "", "",
			"()<-[r:`KNOWS`]-()"},
		{"outgoing with both labels",
			"KNOWS", "OUTGOING", "Person", "Company",
			"(:`Person`)-[r:`KNOWS`]->(:`Company`)"},
		{"incoming with only source label",
			"KNOWS", "INCOMING", "Person", "",
			"(:`Person`)<-[r:`KNOWS`]-()"},
		{"unknown direction falls back to outgoing",
			"KNOWS", "DIAGONAL", "", "",
			"()-[r:`KNOWS`]->()"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, cypherRelationshipPattern(tc.label, tc.direction, tc.src, tc.dst))
		})
	}
}

// ============================================================================
// formatConstraintValues — every type arm including default fallback.
// ============================================================================

func TestFormatConstraintValues_HandlesEveryType(t *testing.T) {
	type custom struct{ V int }
	values := []interface{}{
		"plain",
		"with'quote",
		true, false,
		int(1), int64(2), float64(3.5),
		uint8(4),
		custom{V: 9},
	}
	got := formatConstraintValues(values)
	require.Equal(t, []string{
		"'plain'",
		"'with\\'quote'",
		"true", "false",
		"1", "2", "3.5",
		"4",
		"'{9}'",
	}, got)

	t.Run("empty input returns empty slice (not nil)", func(t *testing.T) {
		out := formatConstraintValues(nil)
		require.NotNil(t, out)
		require.Len(t, out, 0)
	})
}

// ============================================================================
// stringSliceValue — every input shape.
// ============================================================================

func TestStringSliceValue_AcceptsStringSliceAndInterfaceSlice(t *testing.T) {
	cases := []struct {
		name string
		in   interface{}
		want []string
	}{
		{"[]string passes through", []string{"a", "b"}, []string{"a", "b"}},
		{"[]interface{} of strings is filtered", []interface{}{"a", 1, "b", true}, []string{"a", "b"}},
		{"[]interface{} of non-strings yields empty result", []interface{}{1, 2, true}, []string{}},
		{"nil returns nil", nil, nil},
		{"unrelated type returns nil", 42, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := stringSliceValue(tc.in)
			require.Equal(t, tc.want, got)
		})
	}
}

// ============================================================================
// max(int, int) — the local helper. Trivial but uncovered.
// ============================================================================

func TestMax_PicksLarger(t *testing.T) {
	require.Equal(t, 3, max(3, 1))
	require.Equal(t, 3, max(1, 3))
	require.Equal(t, 0, max(0, 0))
	require.Equal(t, -1, max(-2, -1))
}

// ============================================================================
// preserveEmptyStringProperty — type-routing predicate.
// ============================================================================

func TestPreserveEmptyStringProperty_RecognisesScalarStringTypes(t *testing.T) {
	cases := []struct {
		name string
		col  columnSpec
		want bool
	}{
		{"explicit string", columnSpec{Type: "string"}, true},
		{"char", columnSpec{Type: "char"}, true},
		{"upper case STRING", columnSpec{Type: "STRING"}, true},
		{"empty type defaults to string", columnSpec{Type: ""}, true},
		{"int is not preserved", columnSpec{Type: "int"}, false},
		{"array type is not preserved", columnSpec{Type: "string[]"}, false},
		{"vector type is not preserved", columnSpec{Type: "vector"}, false},
		{"vector upper case is not preserved", columnSpec{Type: "VECTOR"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, preserveEmptyStringProperty(tc.col))
		})
	}
}

// ============================================================================
// uniqueNonEmpty — preserves order, deduplicates, drops blanks.
// ============================================================================

func TestUniqueNonEmpty_OrderedAndDeduplicated(t *testing.T) {
	require.Equal(t, []string{"a", "b", "c"},
		uniqueNonEmpty([]string{"a", " ", "a", "b", "", "c", "b"}))
	require.Equal(t, []string{}, uniqueNonEmpty(nil))
	require.Equal(t, []string{}, uniqueNonEmpty([]string{"", "   "}))
}

// ============================================================================
// defaultString — uncovered fallback path.
// ============================================================================

func TestDefaultString(t *testing.T) {
	require.Equal(t, "fallback", defaultString("", "fallback"))
	require.Equal(t, "value", defaultString("value", "fallback"))
}

// ============================================================================
// writeReport — empty path is a no-op; populated path round-trips JSON.
// ============================================================================

func TestWriteReport_HandlesEmptyPathAndWritesValidJSON(t *testing.T) {
	t.Run("empty path is a no-op", func(t *testing.T) {
		require.NoError(t, writeReport("", Report{DatabaseName: "x"}))
	})

	t.Run("populated path writes round-trippable JSON", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "report.json")
		input := Report{
			DatabaseName:          "db1",
			NodesImported:         3,
			RelationshipsImported: 1,
			Errors:                []string{"oops"},
			Status:                "success",
			StartedAt:             fixedImportTime,
			CompletedAt:           fixedImportTime,
			Duration:              0,
		}
		require.NoError(t, writeReport(path, input))

		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		require.True(t, strings.HasSuffix(string(raw), "\n"), "must end with newline")
		var decoded Report
		require.NoError(t, json.Unmarshal(raw, &decoded))
		require.Equal(t, input, decoded)
	})
}

// ============================================================================
// applySchemaDefinition — error paths that the round-trip test does not hit.
// ============================================================================

func TestApplySchemaDefinition_ErrorsOnMissingAndInvalidJSON(t *testing.T) {
	dir := t.TempDir()

	t.Run("missing file returns os error", func(t *testing.T) {
		err := applySchemaDefinition(storage.NewMemoryEngine(),
			filepath.Join(dir, "missing.json"))
		require.Error(t, err)
		require.True(t, os.IsNotExist(err) || errors.Is(err, os.ErrNotExist))
	})

	t.Run("malformed JSON returns decode error", func(t *testing.T) {
		path := filepath.Join(dir, "bad.json")
		require.NoError(t, os.WriteFile(path, []byte("{not json"), 0o600))
		err := applySchemaDefinition(storage.NewMemoryEngine(), path)
		require.Error(t, err)
	})
}

// ============================================================================
// applySchema dispatch — JSON vs cypher file extension.
// ============================================================================

func TestApplySchema_DispatchesByExtension(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	t.Run("JSON path uses applySchemaDefinition", func(t *testing.T) {
		path := filepath.Join(dir, "schema.json")
		def := storage.SchemaDefinition{
			Constraints: []storage.Constraint{{
				Name: "person_email_unique", Label: "Person",
				Type:       storage.ConstraintUnique,
				Properties: []string{"email"},
			}},
		}
		raw, err := json.Marshal(def)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, raw, 0o600))

		base := storage.NewMemoryEngine()
		target := storage.NewNamespacedEngine(base, "appdb")
		require.NoError(t, applySchema(ctx, target, path))

		constraints := target.GetSchema().GetConstraintsForLabels([]string{"Person"})
		require.Len(t, constraints, 1)
		require.Equal(t, "person_email_unique", constraints[0].Name)
	})

	t.Run("cypher path reads file and executes each statement", func(t *testing.T) {
		path := filepath.Join(dir, "schema.cypher")
		require.NoError(t, os.WriteFile(path, []byte(
			"// comment line\n"+
				"# pound comment\n"+
				"CREATE CONSTRAINT person_name_unique IF NOT EXISTS FOR (n:Person) REQUIRE n.name IS UNIQUE;\n"+
				"   \n"+ // whitespace-only statement should be skipped
				"CREATE CONSTRAINT person_email_unique IF NOT EXISTS FOR (n:Person) REQUIRE n.email IS UNIQUE;\n",
		), 0o600))

		base := storage.NewMemoryEngine()
		target := storage.NewNamespacedEngine(base, "appdb-cypher")
		require.NoError(t, applySchema(ctx, target, path))

		names := make([]string, 0)
		for _, c := range target.GetSchema().GetConstraintsForLabels([]string{"Person"}) {
			names = append(names, c.Name)
		}
		require.ElementsMatch(t, []string{"person_name_unique", "person_email_unique"}, names)
	})

	t.Run("cypher path bubbles up read errors", func(t *testing.T) {
		err := applySchema(ctx, storage.NewMemoryEngine(),
			filepath.Join(dir, "missing.cypher"))
		require.Error(t, err)
	})
}

// ============================================================================
// splitCypherStatements — comment stripping, trailing-statement handling.
// ============================================================================

func TestSplitCypherStatements_StripsCommentsAndSplitsOnSemicolons(t *testing.T) {
	out := splitCypherStatements(
		"// header comment\n" +
			"# pound comment\n" +
			"CREATE INDEX a IF NOT EXISTS FOR (n:X) ON (n.x);\n" +
			"CREATE INDEX b IF NOT EXISTS FOR (n:Y) ON (n.y);\n",
	)
	// splitCypherStatements always emits N+1 segments (the trailing piece
	// after the last semicolon is also returned, trimmed). The trailing
	// segment for this input is the empty string.
	require.Equal(t, []string{
		"CREATE INDEX a IF NOT EXISTS FOR (n:X) ON (n.x)",
		"CREATE INDEX b IF NOT EXISTS FOR (n:Y) ON (n.y)",
		"",
	}, out)
}

// ============================================================================
// preflightSources — every per-source error path that the importer rejects
// before any writes happen.
// ============================================================================

func TestPreflightSources_RejectsBadInputsBeforeAnyWrite(t *testing.T) {
	dir := t.TempDir()

	t.Run("missing node file is rejected as ExitCSV", func(t *testing.T) {
		err := preflightSources(Options{
			NodeSources: []string{filepath.Join(dir, "missing.csv")},
		}.withDefaults())
		require.Error(t, err)
		var ie *Error
		require.ErrorAs(t, err, &ie)
		require.Equal(t, ExitCSV, ie.ExitCode)
	})

	t.Run("node source with malformed prefix is rejected as ExitUnsupported", func(t *testing.T) {
		// parseSourceSpec rejects empty source string outright.
		err := preflightSources(Options{
			NodeSources: []string{""},
		}.withDefaults())
		require.Error(t, err)
		var ie *Error
		require.ErrorAs(t, err, &ie)
		require.Equal(t, ExitUnsupported, ie.ExitCode)
	})

	t.Run("relationship source missing START/END columns is rejected", func(t *testing.T) {
		nodes := filepath.Join(dir, "nodes.csv")
		rels := filepath.Join(dir, "rels.csv")
		require.NoError(t, os.WriteFile(nodes, []byte(":ID,name\nn1,Alice\n"), 0o600))
		// Header has neither :START_ID nor :END_ID.
		require.NoError(t, os.WriteFile(rels, []byte("a,b,c\n1,2,3\n"), 0o600))
		err := preflightSources(Options{
			NodeSources: []string{nodes},
			RelSources:  []string{rels},
		}.withDefaults())
		require.Error(t, err)
		var ie *Error
		require.ErrorAs(t, err, &ie)
		require.Equal(t, ExitUnsupported, ie.ExitCode)
	})

	t.Run("relationship source with multi-prefix is rejected", func(t *testing.T) {
		nodes := filepath.Join(dir, "nodes-multi.csv")
		rels := filepath.Join(dir, "rels-multi.csv")
		require.NoError(t, os.WriteFile(nodes, []byte(":ID,name\nn1,Alice\n"), 0o600))
		require.NoError(t, os.WriteFile(rels, []byte(":START_ID,:END_ID,:TYPE\nn1,n1,LIKES\n"), 0o600))
		err := preflightSources(Options{
			NodeSources: []string{nodes},
			RelSources:  []string{"A:B=" + rels},
		}.withDefaults())
		require.Error(t, err)
		var ie *Error
		require.ErrorAs(t, err, &ie)
		require.Equal(t, ExitUnsupported, ie.ExitCode)
	})
}

// ============================================================================
// openCSVSource — rejects non-default quote characters up front.
// ============================================================================

func TestOpenCSVSource_RejectsCustomQuote(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nodes.csv")
	require.NoError(t, os.WriteFile(path, []byte(":ID\nn1\n"), 0o600))
	_, err := openCSVSource([]string{path}, Options{Quote: '\''}.withDefaults())
	require.Error(t, err)
	var ie *Error
	require.ErrorAs(t, err, &ie)
	require.Equal(t, ExitUnsupported, ie.ExitCode)
}

// ============================================================================
// (*csvSourceReader).Read before ReadHeader → io.EOF and per-row error
// branches without invoking the full importer.
// ============================================================================

func TestCSVSourceReader_ReadBeforeHeaderReturnsEOF(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nodes.csv")
	require.NoError(t, os.WriteFile(path, []byte(":ID\nn1\n"), 0o600))
	src, err := openCSVSource([]string{path}, Options{}.withDefaults())
	require.NoError(t, err)
	defer src.Close()

	_, _, _, readErr := src.Read()
	require.ErrorIs(t, readErr, io.EOF)
}

func TestCSVSourceReader_ReadAdvancesAcrossFilesAndReturnsEOF(t *testing.T) {
	// neo4j-admin-style chunked imports: only the first file carries the
	// header; subsequent files are pure data continuations (see
	// TestImporterMultiFileAnonymousNodeIDsRemainUnique).
	dir := t.TempDir()
	first := filepath.Join(dir, "a.csv")
	second := filepath.Join(dir, "b.csv")
	require.NoError(t, os.WriteFile(first, []byte(":ID,name\nn1,Alice\n"), 0o600))
	require.NoError(t, os.WriteFile(second, []byte("n2,Bob\n"), 0o600))

	src, err := openCSVSource([]string{first, second}, Options{}.withDefaults())
	require.NoError(t, err)
	defer src.Close()

	header, err := src.ReadHeader()
	require.NoError(t, err)
	require.Equal(t, []string{":ID", "name"}, header)

	first1, _, _, err := src.Read()
	require.NoError(t, err)
	require.Equal(t, []string{"n1", "Alice"}, first1)

	// The reader transparently advances to the second (header-less) file.
	first2, _, _, err := src.Read()
	require.NoError(t, err)
	require.Equal(t, []string{"n2", "Bob"}, first2)

	_, _, _, eof := src.Read()
	require.ErrorIs(t, eof, io.EOF)
}

// ============================================================================
// ReadHeader: calling it twice should yield io.EOF on the second call.
// ============================================================================

func TestCSVSourceReader_ReadHeaderTwiceReturnsEOF(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.csv")
	require.NoError(t, os.WriteFile(path, []byte(":ID\nn1\n"), 0o600))
	src, err := openCSVSource([]string{path}, Options{}.withDefaults())
	require.NoError(t, err)
	defer src.Close()
	_, err = src.ReadHeader()
	require.NoError(t, err)
	_, err = src.ReadHeader()
	require.ErrorIs(t, err, io.EOF)
}

// ============================================================================
// ensureEmpty — rejects non-empty engines.
// ============================================================================

func TestEnsureEmpty_RejectsNonEmptyEngine(t *testing.T) {
	base := storage.NewMemoryEngine()
	ns := storage.NewNamespacedEngine(base, "occupied")
	_, err := ns.CreateNode(&storage.Node{ID: "n1", Labels: []string{"X"}})
	require.NoError(t, err)
	require.Error(t, ensureEmpty(ns))

	empty := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "empty")
	require.NoError(t, ensureEmpty(empty))
}

// ============================================================================
// rebuildSchemaDerivedState — covers the index-insertion branches when the
// destination engine already has nodes that should be indexed.
// ============================================================================

func TestRebuildSchemaDerivedState_IndexesExistingNodes(t *testing.T) {
	base := storage.NewMemoryEngine()
	ns := storage.NewNamespacedEngine(base, "rebuild-db")

	// Seed two Person nodes BEFORE the schema knows about indexes.
	_, err := ns.CreateNode(&storage.Node{
		ID: "u1", Labels: []string{"Person"},
		Properties: map[string]any{"email": "alice@example.com", "country": "US", "city": "NYC"},
	})
	require.NoError(t, err)
	_, err = ns.CreateNode(&storage.Node{
		ID: "u2", Labels: []string{"Person"},
		Properties: map[string]any{"email": "bob@example.com", "country": "US", "city": "LA"},
	})
	require.NoError(t, err)

	schema := ns.GetSchema()
	require.NoError(t, schema.AddPropertyIndex("person_email_idx", "Person", []string{"email"}))
	require.NoError(t, schema.AddCompositeIndex("person_location_idx", "Person", []string{"country", "city"}))

	require.NoError(t, rebuildSchemaDerivedState(ns, schema))

	// After rebuild the property index should resolve both seeded nodes.
	got := ns.GetSchema().PropertyIndexLookup("Person", "email", "alice@example.com")
	require.Contains(t, got, storage.NodeID("u1"))
}

// ============================================================================
// renderIndexStatement — every typed branch with valid + invalid inputs.
// ============================================================================

func TestRenderIndexStatement_AllShapes(t *testing.T) {
	cases := []struct {
		name   string
		input  any
		expect string
		ok     bool
	}{
		{"non-map input returns ok=false", "not-a-map", "", false},
		{"PROPERTY index with one property",
			map[string]interface{}{
				"name": "person_name_idx", "type": "PROPERTY",
				"label": "Person", "properties": []string{"name"},
			},
			"CREATE INDEX `person_name_idx` IF NOT EXISTS FOR (n:`Person`) ON (n.`name`)",
			true},
		{"COMPOSITE index spans properties",
			map[string]interface{}{
				"name": "person_loc_idx", "type": "COMPOSITE",
				"label": "Person", "properties": []string{"country", "city"},
			},
			"CREATE INDEX `person_loc_idx` IF NOT EXISTS FOR (n:`Person`) ON (n.`country`, n.`city`)",
			true},
		{"PROPERTY index with no properties is rejected",
			map[string]interface{}{
				"name": "x", "type": "PROPERTY", "label": "Person",
				"properties": []string{},
			},
			"", false},
		{"RANGE owned by another constraint is suppressed",
			map[string]interface{}{
				"name": "x", "type": "RANGE", "label": "Person",
				"properties":       []string{"name"},
				"owningConstraint": "person_name_unique",
			},
			"", false},
		{"RANGE index falls back to single 'property' field when 'properties' empty",
			map[string]interface{}{
				"name": "person_age_range", "type": "RANGE", "label": "Person",
				"property": "age",
			},
			"CREATE INDEX `person_age_range` IF NOT EXISTS FOR (n:`Person`) ON (n.`age`)",
			true},
		{"RANGE index with no properties at all is rejected",
			map[string]interface{}{
				"name": "x", "type": "RANGE", "label": "Person",
			},
			"", false},
		{"RANGE index on relationship uses r variable",
			map[string]interface{}{
				"name": "rel_range_idx", "type": "RANGE", "label": "KNOWS",
				"entityType": string(storage.ConstraintEntityRelationship),
				"properties": []string{"since"},
			},
			"CREATE INDEX `rel_range_idx` IF NOT EXISTS FOR ()-[r:`KNOWS`]-() ON (r.`since`)",
			true},
		{"FULLTEXT index over multiple labels",
			map[string]interface{}{
				"name": "person_search", "type": "FULLTEXT",
				"properties": []string{"name", "bio"},
				"labels":     []string{"Person", "User"},
			},
			"CREATE FULLTEXT INDEX `person_search` IF NOT EXISTS FOR (n:`Person`|`User`) ON EACH [n.`name`, n.`bio`]",
			true},
		{"FULLTEXT index over relationship types",
			map[string]interface{}{
				"name": "knows_note_idx", "type": "FULLTEXT",
				"properties":        []string{"note"},
				"relationshipTypes": []string{"KNOWS", "FOLLOWS"},
			},
			"CREATE FULLTEXT INDEX `knows_note_idx` IF NOT EXISTS FOR ()-[r:`KNOWS`|`FOLLOWS`]-() ON EACH [r.`note`]",
			true},
		{"FULLTEXT with neither labels nor relationship types is rejected",
			map[string]interface{}{
				"name": "x", "type": "FULLTEXT",
				"properties": []string{"name"},
			},
			"", false},
		{"FULLTEXT with no properties is rejected",
			map[string]interface{}{
				"name": "x", "type": "FULLTEXT",
				"labels": []string{"Person"},
			},
			"", false},
		{"VECTOR index with int dimensions",
			map[string]interface{}{
				"name": "person_emb_idx", "type": "VECTOR",
				"label": "Person", "property": "embedding",
				"dimensions": int(3), "similarityFunc": "COSINE",
			},
			"CREATE VECTOR INDEX `person_emb_idx` IF NOT EXISTS FOR (n:`Person`) ON (n.`embedding`) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}",
			true},
		{"VECTOR index with float64 dimensions (JSON decode case)",
			map[string]interface{}{
				"name": "person_emb_idx", "type": "VECTOR",
				"label": "Person", "property": "embedding",
				"dimensions": float64(7),
			},
			"CREATE VECTOR INDEX `person_emb_idx` IF NOT EXISTS FOR (n:`Person`) ON (n.`embedding`) OPTIONS {indexConfig: {`vector.dimensions`: 7}}",
			true},
		{"VECTOR index on relationship",
			map[string]interface{}{
				"name": "rel_emb_idx", "type": "VECTOR",
				"label": "KNOWS", "property": "embedding",
				"dimensions": int(2),
				"entityType": string(storage.ConstraintEntityRelationship),
			},
			"CREATE VECTOR INDEX `rel_emb_idx` IF NOT EXISTS FOR ()-[r:`KNOWS`]-() ON (r.`embedding`) OPTIONS {indexConfig: {`vector.dimensions`: 2}}",
			true},
		{"VECTOR index with missing property is rejected",
			map[string]interface{}{
				"name": "x", "type": "VECTOR", "label": "Person",
				"dimensions": int(3),
			},
			"", false},
		{"VECTOR index with missing label is rejected",
			map[string]interface{}{
				"name": "x", "type": "VECTOR", "property": "embedding",
				"dimensions": int(3),
			},
			"", false},
		{"VECTOR index with zero dimensions is rejected",
			map[string]interface{}{
				"name": "x", "type": "VECTOR", "label": "Person", "property": "embedding",
			},
			"", false},
		{"unknown index type is rejected",
			map[string]interface{}{
				"name": "x", "type": "BOGUS", "label": "Person",
			},
			"", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := renderIndexStatement(tc.input)
			require.Equal(t, tc.ok, ok)
			if tc.ok {
				require.Equal(t, tc.expect, got)
			}
		})
	}
}

// ============================================================================
// mergeColumn — the type-conflict path (existing.Kind != new.Kind) demotes
// to property/string, plus the dims-only ascent path.
// ============================================================================

func TestMergeColumn_DemotesOnConflictAndAscendsDims(t *testing.T) {
	t.Run("type conflict demotes to property/string", func(t *testing.T) {
		// First a string property is recorded, then an integer for the same key.
		col := mergeColumn(inferredColumn{}, "age", "thirty")
		require.Equal(t, "property", col.Kind)
		require.Equal(t, "string", col.ScalarType)
		col = mergeColumn(col, "age", int64(30))
		require.Equal(t, "property", col.Kind)
		require.Equal(t, "string", col.ScalarType)
		require.Equal(t, "age:string", col.Header)
	})

	t.Run("vector dims ascend when a longer vector is observed", func(t *testing.T) {
		col := mergeColumn(inferredColumn{}, "emb", []float32{1, 2})
		require.Equal(t, 2, col.VectorDims)
		col = mergeColumn(col, "emb", []float32{1, 2, 3, 4})
		require.Equal(t, 4, col.VectorDims)
		require.Equal(t, "emb:vector{coordinateType:float,dimensions:4}", col.Header)
	})

	t.Run("array header carries scalar element type", func(t *testing.T) {
		col := mergeColumn(inferredColumn{}, "tags", []string{"a"})
		require.Equal(t, "array", col.Kind)
		require.Equal(t, "tags:string[]", col.Header)
	})
}

// ============================================================================
// inferNodeColumns — id-collision elision and named-embedding capture.
// ============================================================================

func TestInferNodeColumns_DropsRedundantIDAndCapturesEmbedding(t *testing.T) {
	cols := inferNodeColumns([]*storage.Node{
		{
			ID:     "u1",
			Labels: []string{"Person"},
			Properties: map[string]any{
				"id":   "u1", // matches node ID, should be dropped
				"name": "Alice",
			},
			NamedEmbeddings: map[string][]float32{
				"default": {0.1, 0.2, 0.3},
				"summary": {0.5, 0.6},
			},
		},
	})

	headers := make([]string, 0, len(cols))
	for _, c := range cols {
		headers = append(headers, c.Header)
	}
	require.Contains(t, headers, "name:string")
	require.Contains(t, headers, ":EMBEDDING(default)")
	require.Contains(t, headers, ":EMBEDDING(summary)")
	require.NotContains(t, headers, "id:string", "redundant id column should be elided")

	// An 'id' value that does NOT match the node ID is kept.
	colsKept := inferNodeColumns([]*storage.Node{
		{
			ID:         "u1",
			Labels:     []string{"Person"},
			Properties: map[string]any{"id": "different"},
		},
	})
	keptHeaders := make([]string, 0, len(colsKept))
	for _, c := range colsKept {
		keptHeaders = append(keptHeaders, c.Header)
	}
	require.Contains(t, keptHeaders, "id:string")
}

// ============================================================================
// writeCSV — error paths around file open, write, and underlying flush.
// ============================================================================

func TestWriteCSV_BubblesUpOpenAndWriteErrors(t *testing.T) {
	t.Run("path under non-existent directory errors", func(t *testing.T) {
		bad := filepath.Join(t.TempDir(), "nope", "x.csv")
		err := writeCSV(bad, []string{"a"}, [][]string{{"1"}}, ',')
		require.Error(t, err)
	})

	t.Run("happy path writes header and rows", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "ok.csv")
		require.NoError(t, writeCSV(path, []string{"a", "b"}, [][]string{{"1", "2"}, {"3", "4"}}, ','))
		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, "a,b\n1,2\n3,4\n", string(raw))
	})
}

// ============================================================================
// hasSchemaDefinitionContent — false branches and a positive case for each
// kind of definition entry.
// ============================================================================

func TestHasSchemaDefinitionContent_HandlesAllKinds(t *testing.T) {
	require.False(t, hasSchemaDefinitionContent(nil))
	require.False(t, hasSchemaDefinitionContent(&storage.SchemaDefinition{}))

	thr := 0.25
	cases := []struct {
		name string
		def  storage.SchemaDefinition
	}{
		{"constraints",
			storage.SchemaDefinition{Constraints: []storage.Constraint{{Name: "x"}}}},
		{"constraint contracts",
			storage.SchemaDefinition{ConstraintContracts: []storage.ConstraintContract{{Name: "x"}}}},
		{"property type constraints",
			storage.SchemaDefinition{PropertyTypeConstraints: []storage.PropertyTypeConstraint{{Name: "x"}}}},
		{"property indexes",
			storage.SchemaDefinition{PropertyIndexes: []storage.SchemaPropertyIndexDef{{Name: "x"}}}},
		{"composite indexes",
			storage.SchemaDefinition{CompositeIndexes: []storage.SchemaCompositeIndexDef{{Name: "x"}}}},
		{"fulltext indexes",
			storage.SchemaDefinition{FulltextIndexes: []storage.FulltextIndex{{Name: "x"}}}},
		{"vector indexes",
			storage.SchemaDefinition{VectorIndexes: []storage.VectorIndex{{Name: "x"}}}},
		{"range indexes",
			storage.SchemaDefinition{RangeIndexes: []storage.SchemaRangeIndexDef{{Name: "x"}}}},
		{"decay profile bundles",
			storage.SchemaDefinition{DecayProfileBundles: []knowledgepolicy.DecayProfileBundle{{Name: "x"}}}},
		{"decay profile bindings",
			storage.SchemaDefinition{DecayProfileBindings: []knowledgepolicy.DecayProfileBinding{{Name: "x", VisibilityThreshold: &thr}}}},
		{"promotion profiles",
			storage.SchemaDefinition{PromotionProfiles: []knowledgepolicy.PromotionProfileDef{{Name: "x"}}}},
		{"promotion policies",
			storage.SchemaDefinition{PromotionPolicies: []knowledgepolicy.PromotionPolicyDef{{Name: "x"}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.True(t, hasSchemaDefinitionContent(&tc.def))
		})
	}
}

// ============================================================================
// ExportNeo4jCSV — early-return branches.
// ============================================================================

func TestExportNeo4jCSV_RequiresOutputDirAndDefaultQuote(t *testing.T) {
	base := storage.NewMemoryEngine()
	ns := storage.NewNamespacedEngine(base, "export-db")

	t.Run("empty output dir errors with ExitUnsupported", func(t *testing.T) {
		err := ExportNeo4jCSV(ns, Neo4jCSVExportOptions{})
		require.Error(t, err)
		var ie *Error
		require.ErrorAs(t, err, &ie)
		require.Equal(t, ExitUnsupported, ie.ExitCode)
	})

	t.Run("custom quote character errors", func(t *testing.T) {
		err := ExportNeo4jCSV(ns, Neo4jCSVExportOptions{
			OutputDir: t.TempDir(),
			Quote:     '\'',
		})
		require.Error(t, err)
		var ie *Error
		require.ErrorAs(t, err, &ie)
		require.Equal(t, ExitUnsupported, ie.ExitCode)
	})
}

// ============================================================================
// DiscoverNeo4jCSVSources — empty directory rejected; nested files discovered.
// ============================================================================

func TestDiscoverNeo4jCSVSources_RejectsEmptyDirAndFollowsNesting(t *testing.T) {
	t.Run("empty directory is rejected with ExitUnsupported", func(t *testing.T) {
		_, _, err := DiscoverNeo4jCSVSources(t.TempDir(), Options{})
		require.Error(t, err)
		var ie *Error
		require.ErrorAs(t, err, &ie)
		require.Equal(t, ExitUnsupported, ie.ExitCode)
	})

	t.Run("directory without node CSVs (only rels) is rejected", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "rels.csv"),
			[]byte(":START_ID,:END_ID,:TYPE\nu1,u2,KNOWS\n"), 0o600))
		_, _, err := DiscoverNeo4jCSVSources(dir, Options{})
		require.Error(t, err)
		var ie *Error
		require.ErrorAs(t, err, &ie)
		require.Equal(t, ExitUnsupported, ie.ExitCode)
	})

	t.Run("non-CSV files are silently skipped", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "nodes.csv"),
			[]byte(":ID,name\nu1,Alice\n"), 0o600))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "readme.txt"), []byte("hi"), 0o600))
		nodes, rels, err := DiscoverNeo4jCSVSources(dir, Options{})
		require.NoError(t, err)
		require.Len(t, nodes, 1)
		require.Empty(t, rels)
	})

	t.Run("unsupported CSV header is rejected", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "weird.csv"),
			[]byte(":UNKNOWN_TOKEN\nvalue\n"), 0o600))
		_, _, err := DiscoverNeo4jCSVSources(dir, Options{})
		require.Error(t, err)
		var ie *Error
		require.ErrorAs(t, err, &ie)
		require.Equal(t, ExitUnsupported, ie.ExitCode)
	})
}

// ============================================================================
// classifyNeo4jCSVFile — direct coverage for header read errors.
// ============================================================================

func TestClassifyNeo4jCSVFile_PropagatesHeaderErrors(t *testing.T) {
	dir := t.TempDir()
	t.Run("empty file fails on header read", func(t *testing.T) {
		path := filepath.Join(dir, "empty.csv")
		require.NoError(t, os.WriteFile(path, nil, 0o600))
		_, err := classifyNeo4jCSVFile(path, Options{})
		require.Error(t, err)
		var ie *Error
		require.ErrorAs(t, err, &ie)
		require.Equal(t, ExitCSV, ie.ExitCode)
	})

	t.Run("missing file fails with ExitCSV", func(t *testing.T) {
		_, err := classifyNeo4jCSVFile(filepath.Join(dir, "missing.csv"), Options{})
		require.Error(t, err)
		var ie *Error
		require.ErrorAs(t, err, &ie)
		require.Equal(t, ExitCSV, ie.ExitCode)
	})
}

// ============================================================================
// ImportFull — every up-front validation error.
// ============================================================================

func TestImportFull_ValidatesInputsBeforeAnyWrite(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	nodes := filepath.Join(dir, "nodes.csv")
	require.NoError(t, os.WriteFile(nodes, []byte(":ID,name\nu1,Alice\n"), 0o600))

	cases := []struct {
		name string
		opts Options
		want int
	}{
		{
			name: "missing database name",
			opts: Options{NodeSources: []string{nodes}},
			want: ExitUnsupported,
		},
		{
			name: "missing node sources",
			opts: Options{DatabaseName: "x"},
			want: ExitUnsupported,
		},
		{
			name: "unsupported id-type=actual",
			opts: Options{
				DatabaseName: "x",
				NodeSources:  []string{nodes},
				IDType:       "actual",
			},
			want: ExitUnsupported,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			base := storage.NewMemoryEngine()
			report, err := ImportFull(ctx, base, tc.opts)
			require.Error(t, err)
			require.Equal(t, "failed", report.Status)
			var ie *Error
			require.ErrorAs(t, err, &ie)
			require.Equal(t, tc.want, ie.ExitCode)
		})
	}

	t.Run("nil engine is rejected", func(t *testing.T) {
		report, err := ImportFull(ctx, nil, Options{
			DatabaseName: "x",
			NodeSources:  []string{nodes},
		})
		require.Error(t, err)
		require.Equal(t, "failed", report.Status)
		var ie *Error
		require.ErrorAs(t, err, &ie)
		require.Equal(t, ExitUnsupported, ie.ExitCode)
	})

	t.Run("non-empty target database is rejected", func(t *testing.T) {
		base := storage.NewMemoryEngine()
		// Pre-seed a node under the target namespace.
		ns := storage.NewNamespacedEngine(base, "occupied")
		_, err := ns.CreateNode(&storage.Node{ID: "n1", Labels: []string{"X"}})
		require.NoError(t, err)

		report, err := ImportFull(ctx, base, Options{
			DatabaseName: "occupied",
			NodeSources:  []string{nodes},
		})
		require.Error(t, err)
		require.Equal(t, "failed", report.Status)
		var ie *Error
		require.ErrorAs(t, err, &ie)
		require.Equal(t, ExitUnsupported, ie.ExitCode)
	})
}

// ============================================================================
// writeReport — IO failure on a path under a non-existent directory.
// ============================================================================

func TestWriteReport_PropagatesIOError(t *testing.T) {
	bad := filepath.Join(t.TempDir(), "missing-dir", "report.json")
	err := writeReport(bad, Report{DatabaseName: "x"})
	require.Error(t, err)
}

// ============================================================================
// Knowledge-policy round-trip path is exercised end-to-end above by the
// schema export tests, but the explicit `knowledgepolicy` import is required
// for cross-package symbol visibility (and keeps this file aligned with the
// existing import set in importer_test.go).
// ============================================================================

var _ = knowledgepolicy.DecayFunctionExponential
