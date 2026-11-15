package adminimport

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// csvFieldSink records every byte the field adapter forwards and counts the
// single-byte forwards that take the sink's byte-writer path.
type csvFieldSink struct {
	bytes.Buffer
	byteWrites int
}

func (s *csvFieldSink) WriteByte(b byte) error {
	s.byteWrites++
	return s.Buffer.WriteByte(b)
}

// plainFieldSink hides io.ByteWriter, so a released withheld byte takes the
// plain io.Writer path.
type plainFieldSink struct {
	buf bytes.Buffer
}

func (s *plainFieldSink) Write(p []byte) (int, error) { return s.buf.Write(p) }

// byteFailingFieldSink accepts field bytes but fails the single-byte forward of
// a byte withheld from an earlier chunk.
type byteFailingFieldSink struct {
	bytes.Buffer
	err error
}

func (s *byteFailingFieldSink) WriteByte(byte) error { return s.err }

// shortFieldSink accepts at most limit bytes of any write and reports fewer
// bytes than it was handed without an error.
type shortFieldSink struct {
	buf   bytes.Buffer
	limit int
}

func (s *shortFieldSink) Write(p []byte) (int, error) {
	if len(p) > s.limit {
		p = p[:s.limit]
	}
	return s.buf.Write(p)
}

var errCSVFieldSink = errors.New("field sink failed")

// TestCSVFieldTailWriterStreamsChunks pins the streaming contract the export
// relies on: the adapter forwards a chunk's leading bytes, withholds its final
// byte, and releases that byte only when the next chunk arrives, so
// csvExportWriter can drop the record newline encoding/csv appends to a field it
// encodes on its own.
func TestCSVFieldTailWriterStreamsChunks(t *testing.T) {
	sink := &csvFieldSink{}
	writer := &csvFieldTailWriter{out: sink}

	n, err := writer.Write([]byte("ab"))
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, "a", sink.String(), "the chunk's final byte stays withheld")
	require.Zero(t, sink.byteWrites)

	n, err = writer.WriteString("cd")
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, "abc", sink.String(), "the next chunk releases the withheld byte first")
	require.Equal(t, 1, sink.byteWrites, "a released byte uses the sink's byte-writer path")

	// A single-byte chunk releases the withheld byte and holds the new one.
	n, err = writer.WriteString("e")
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, "abcd", sink.String())

	// encoding/csv flushed the field, so its withheld byte is the record newline.
	writer.discardTail()
	require.Equal(t, "abcd", sink.String())
}

// TestCSVFieldTailWriterEmptyChunksChangeNothing keeps io.Writer's empty-write
// contract: an empty chunk reports no bytes and neither releases nor replaces
// the withheld byte.
func TestCSVFieldTailWriterEmptyChunksChangeNothing(t *testing.T) {
	sink := &csvFieldSink{}
	writer := &csvFieldTailWriter{out: sink}

	n, err := writer.Write([]byte("ab"))
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, "a", sink.String())

	n, err = writer.Write(nil)
	require.NoError(t, err)
	require.Zero(t, n)
	n, err = writer.WriteString("")
	require.NoError(t, err)
	require.Zero(t, n)
	require.Equal(t, "a", sink.String(), "an empty chunk does not release the withheld byte")

	n, err = writer.WriteString("c")
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, "ab", sink.String())
	writer.discardTail()
	require.Equal(t, "ab", sink.String())
}

// TestCSVFieldTailWriterReleasesThroughWriterFallback covers a sink without
// io.ByteWriter: the withheld byte still reaches the sink first, in order.
func TestCSVFieldTailWriterReleasesThroughWriterFallback(t *testing.T) {
	sink := &plainFieldSink{}
	writer := &csvFieldTailWriter{out: sink}

	n, err := writer.Write([]byte("ab"))
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, "a", sink.buf.String())

	n, err = writer.WriteString("cd")
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, "abc", sink.buf.String())

	writer.discardTail()
	require.Equal(t, "abc", sink.buf.String())
}

// TestCSVFieldTailWriterReportsSinkFailures pins the adapter's error contract: a
// sink failure reports only the contiguous accepted prefix; the final byte is
// withheld only once the preceding bytes have reached the sink.
func TestCSVFieldTailWriterReportsSinkFailures(t *testing.T) {
	t.Run("write fails", func(t *testing.T) {
		sink := &fixtureowner.ErrorWriter{Err: errCSVFieldSink}
		writer := &csvFieldTailWriter{out: sink}

		n, err := writer.Write([]byte("abc"))
		require.ErrorIs(t, err, errCSVFieldSink)
		require.Zero(t, n, "no prefix bytes were accepted")
	})

	t.Run("releasing the withheld byte fails", func(t *testing.T) {
		sink := &byteFailingFieldSink{err: errCSVFieldSink}
		writer := &csvFieldTailWriter{out: sink}

		n, err := writer.Write([]byte("ab"))
		require.NoError(t, err)
		require.Equal(t, 2, n)
		require.Equal(t, "a", sink.String())

		n, err = writer.Write([]byte("cd"))
		require.ErrorIs(t, err, errCSVFieldSink)
		require.Zero(t, n, "no byte of a new chunk is accepted when releasing the withheld byte fails")
		require.Equal(t, "a", sink.String())
	})

	t.Run("short write", func(t *testing.T) {
		sink := &shortFieldSink{limit: 1}
		writer := &csvFieldTailWriter{out: sink}

		n, err := writer.Write([]byte("abcd"))
		require.ErrorIs(t, err, io.ErrShortWrite)
		require.Equal(t, 1, n, "only the contiguous accepted prefix is consumed")
		require.Equal(t, "a", sink.buf.String())
	})
}

// largeExportValue is a property value of about size bytes that needs CSV
// quoting: it holds the export's delimiter, a quote, a line break and multi-byte
// UTF-8 text, and it is far longer than the export buffers.
func largeExportValue(size int) string {
	const seed = "value,with \"quotes\"\nand ünïcode — 日本語 "
	var value strings.Builder
	value.Grow(size)
	for value.Len()+len(seed) <= size {
		value.WriteString(seed)
	}
	return value.String()
}

// readExportedRecords parses an exported CSV file with encoding/csv, an
// independent reader, and returns its records.
func readExportedRecords(t *testing.T, path string) [][]string {
	t.Helper()
	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()
	records, err := csv.NewReader(file).ReadAll()
	require.NoError(t, err)
	return records
}

// readExportedFields reports, per exported row and column, whether the exported
// cell is quoted, using the reader the import consumes the file with.
func readExportedFields(t *testing.T, path string) map[string]map[string]bool {
	t.Helper()
	source, err := openCSVSource([]string{path}, Options{}.withDefaults())
	require.NoError(t, err)
	defer source.Close()
	header, err := source.ReadHeader()
	require.NoError(t, err)
	fields := make(map[string]map[string]bool)
	for {
		record, _, _, err := source.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		row := make(map[string]bool, len(header))
		for i, name := range header {
			row[name] = record[i].Quoted
		}
		fields[record[0].Text] = row
	}
	return fields
}

// importCSVExportIntoBadger loads an exported directory into a fresh Badger
// database the way the admin import command does. Badger accepts entity values
// above the in-memory engine's value ceiling, so a large property round trips.
func importCSVExportIntoBadger(t *testing.T, outDir string) storage.Engine {
	t.Helper()
	base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })
	nodeSources, relSources, err := DiscoverNeo4jCSVSources(outDir, Options{})
	require.NoError(t, err)
	_, err = ImportFull(context.Background(), base, Options{
		DatabaseName: "roundtrip",
		NodeSources:  nodeSources,
		RelSources:   relSources,
		BuildIndexes: false,
		Now:          fixedImportTime,
	})
	require.NoError(t, err)
	return storage.NewNamespacedEngine(base, "roundtrip")
}

// TestNeo4jCSVExportImportStreamsLargeQuotedMultilineUTF8Values is the streaming
// export round trip: a large value that needs quoting, escaping and multi-byte
// UTF-8 text is exported through the streaming field adapter and imported back
// unchanged, while a present empty string and an absent property stay distinct.
func TestNeo4jCSVExportImportStreamsLargeQuotedMultilineUTF8Values(t *testing.T) {
	ctx := context.Background()
	sourceEngine, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sourceEngine.Close() })
	source := storage.NewNamespacedEngine(sourceEngine, "source")

	large := largeExportValue(1 << 20)
	properties := map[string]any{
		"uid":   "big1",
		"large": large,
		"comma": "left,right",
		"quote": `say "hello"`,
		"lines": "first line\nsecond line",
		"utf8":  "ünïcode — 日本語 ✓",
		"blank": "",
	}
	_, err = source.CreateNode(ctx, &storage.Node{ID: "big1", Labels: []string{"Large"}, Properties: properties})
	require.NoError(t, err)
	_, err = source.CreateNode(ctx, &storage.Node{ID: "sparse1", Labels: []string{"Large"}, Properties: map[string]any{"uid": "sparse1"}})
	require.NoError(t, err)

	outDir := filepath.Join(t.TempDir(), "neo4j-csv")
	require.NoError(t, ExportNeo4jCSV(source, Neo4jCSVExportOptions{OutputDir: outDir}))
	dest := importCSVExportIntoBadger(t, outDir)

	got, err := dest.GetNode(ctx, "big1")
	require.NoError(t, err)
	require.Equal(t, properties, got.Properties, "the large quoted field and its neighbors survive the round trip")
	sparse, err := dest.GetNode(ctx, "sparse1")
	require.NoError(t, err)
	require.Equal(t, map[string]any{"uid": "sparse1"}, sparse.Properties)

	path := filepath.Join(outDir, "nodes.csv")
	records := readExportedRecords(t, path)
	require.Len(t, records, 3, "one header record and one record per exported node")
	header := records[0]
	rows := make(map[string]map[string]string, len(records)-1)
	for _, record := range records[1:] {
		row := make(map[string]string, len(header))
		for i, name := range header {
			row[name] = record[i]
		}
		rows[row[":ID"]] = row
	}
	require.Contains(t, rows, "big1")
	require.Contains(t, rows, "sparse1")
	require.Equal(t, large, rows["big1"]["large:string"])
	require.Equal(t, "left,right", rows["big1"]["comma:string"])
	require.Equal(t, `say "hello"`, rows["big1"]["quote:string"])
	require.Equal(t, "first line\nsecond line", rows["big1"]["lines:string"])
	require.Equal(t, "ünïcode — 日本語 ✓", rows["big1"]["utf8:string"])
	require.Equal(t, "", rows["big1"]["blank:string"])
	require.Equal(t, "sparse1", rows["sparse1"]["uid:string"])
	require.Equal(t, "", rows["sparse1"]["large:string"], "a property the node does not hold stays a bare empty cell")

	// The record terminator stays one newline after the field adapter drops the
	// newline encoding/csv appends to the field it encodes on its own.
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.True(t, bytes.HasSuffix(raw, []byte("\n")), "the file ends with its record terminator")
	require.False(t, bytes.HasSuffix(raw, []byte("\n\n")), "the field adapter leaves no extra record terminator")

	// Quoting stays encoding/csv's: the large value is quoted, the present empty
	// string is the quoted empty cell, and an absent property stays bare.
	fields := readExportedFields(t, path)
	require.True(t, fields["big1"]["large:string"], "a value that needs quoting is quoted")
	require.True(t, fields["big1"]["blank:string"], "a present empty string is the quoted empty cell")
	require.False(t, fields["sparse1"]["blank:string"], "an absent property is a bare empty cell")
}

// BenchmarkCSVLargeField measures exporting one 1 MiB property field to
// io.Discard. The field streams through the adapter in the chunks encoding/csv
// produces, and a large string chunk is forwarded without becoming a byte slice,
// so the reported allocations do not grow with the field.
func BenchmarkCSVLargeField(b *testing.B) {
	const size = 1 << 20
	large := strings.Repeat("x", size)
	writer := newCSVExportWriter(bufio.NewWriter(io.Discard), ',')
	record := []csvExportCell{{Text: large}}
	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := writer.writeRecord(record); err != nil {
			b.Fatal(err)
		}
	}
}

func TestCSVFieldTailWriterRetriesContiguousPrefix(t *testing.T) {
	sink := &shortFieldSink{limit: 1}
	writer := &csvFieldTailWriter{out: sink}
	input := []byte("abcd\n")
	n, err := writer.Write(input)
	require.ErrorIs(t, err, io.ErrShortWrite)
	require.Equal(t, 1, n)
	require.False(t, writer.held)
	sink.limit = len(input)
	consumed, err := writer.Write(input[n:])
	require.NoError(t, err)
	require.Equal(t, len(input)-n, consumed)
	writer.discardTail()
	require.Equal(t, "abcd", sink.buf.String())
}

func TestCSVFieldTailWriterPreservesTailOnShortRelease(t *testing.T) {
	sink := &shortFieldSink{limit: 0}
	writer := &csvFieldTailWriter{out: sink}
	_, err := writer.WriteString("a")
	require.NoError(t, err)
	n, err := writer.WriteString("b")
	require.ErrorIs(t, err, io.ErrShortWrite)
	require.Zero(t, n)
	require.True(t, writer.held)
	require.Equal(t, byte('a'), writer.tail)
	sink.limit = 1
	_, err = writer.WriteString("b")
	require.NoError(t, err)
	writer.discardTail()
	require.Equal(t, "a", sink.buf.String())
}

// acceptedTailErrorSink reports the bytes it accepted even when it also returns
// an error. Retrying the new chunk must not replay a previously accepted tail.
type acceptedTailErrorSink struct {
	data    bytes.Buffer
	failure error
}

func (s *acceptedTailErrorSink) Write(p []byte) (int, error) {
	n, _ := s.data.Write(p)
	failure := s.failure
	s.failure = nil
	return n, failure
}

func TestCSVFieldTailWriterDoesNotReplayAcceptedTailAfterError(t *testing.T) {
	sink := &acceptedTailErrorSink{failure: errCSVFieldSink}
	writer := &csvFieldTailWriter{out: sink}
	_, err := writer.WriteString("a")
	require.NoError(t, err)
	n, err := writer.WriteString("b")
	require.Zero(t, n)
	require.ErrorIs(t, err, errCSVFieldSink)
	_, err = writer.WriteString("b")
	require.NoError(t, err)
	_, err = writer.WriteString("\n")
	require.NoError(t, err)
	writer.discardTail()
	require.Equal(t, "ab", sink.data.String())
}
