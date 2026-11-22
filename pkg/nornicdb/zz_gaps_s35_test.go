package nornicdb

// Report gap S35 part A (v1.5.2 verification campaign, test-side only): the
// headerless historical replication record at the 16 MiB frame limit gives a
// different upgrade outcome for the same bytes (close-out L9 run: 6 validation
// refusals, 1 conversion refusal, 1 success in 8). Same file on the pin and on
// v1.5.1.
//
// TestGapS35MapOrder:
//  1. the L9 boundary case (the same node with 16 named embeddings, padded to
//     the limit), 16 upgrades of identical bytes via prepareReplicationWAL,
//     each logged with its outcome;
//  2. which step's output length varies, 64 calls each on identical input:
//     replication.TranscodeV4WALPayload (the whole record),
//     storage.TranscodeV4EntityChange (the node change),
//     storage.EncodeNodePayload (MessagePack of the decoded node),
//     storage.EncodeCommandPayload of the bare map[string][]float32;
//  3. the bypass: the same at-limit construction with ONE named embedding
//     (no map order to vary): 16 upgrades of identical bytes.
// Diagnostic: fails only on harness errors.

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/replication"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func gapS35Node(t *testing.T, n int) ([]byte, map[string][]float32) {
	vectors := make(map[string][]float32, n)
	for i := range n {
		vectors[fmt.Sprintf("v%d", i)] = []float32{float32(i), 1}
	}
	node, err := json.Marshal(struct {
		ID              string
		NamedEmbeddings map[string][]float32
	}{"nornic:n", vectors})
	if err != nil {
		t.Fatal(err)
	}
	return node, vectors
}

func gapS35Payload(t *testing.T, node []byte, pad int) []byte {
	data, err := json.Marshal(map[string]any{
		"cmd":     map[string]any{"Type": replication.CmdCreateNode, "Data": node},
		"padding": strings.Repeat("x", pad),
	})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// gapS35Spread calls f n times and reports the distinct output lengths and
// distinct outputs.
func gapS35Spread(t *testing.T, n int, f func() ([]byte, error)) string {
	lengths := map[int]int{}
	digests := map[[32]byte]bool{}
	for i := 0; i < n; i++ {
		out, err := f()
		if err != nil {
			return "error: " + err.Error()
		}
		lengths[len(out)]++
		digests[sha256.Sum256(out)] = true
	}
	ks := make([]int, 0, len(lengths))
	for k := range lengths {
		ks = append(ks, k)
	}
	sort.Ints(ks)
	parts := make([]string, 0, len(ks))
	for _, k := range ks {
		parts = append(parts, fmt.Sprintf("%dB x%d", k, lengths[k]))
	}
	return fmt.Sprintf("distinct_lengths=%d [%s] distinct_outputs=%d", len(ks), strings.Join(parts, " "), len(digests))
}

func gapS35Boundary(t *testing.T, label string, node []byte, tries int) {
	base, err := replication.TranscodeV4WALPayload(0, "replication_command", gapS35Payload(t, node, 0))
	if err != nil {
		t.Fatal(err)
	}
	entry := storage.WALEntry{Sequence: 1, Timestamp: time.Unix(1794000000, 0).UTC(), Operation: "replication_command", Checksum: 999999999}
	header, err := json.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	pad := (16 << 20) - 8 - len(header) - len(base)
	payload := gapS35Payload(t, node, pad)
	entry.Data = payload
	entry.Checksum = crc32.Checksum(payload, crc32.MakeTable(crc32.Castagnoli))
	raw, err := json.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	outcomes := map[string]int{}
	for attempt := 0; attempt < tries; attempt++ {
		dir := filepath.Join(t.TempDir(), "wal")
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "wal.log"), append(raw, '\n'), 0o600); err != nil {
			t.Fatal(err)
		}
		converted, cerr := replication.TranscodeV4WALPayload(0, "replication_command", payload)
		uerr := prepareReplicationWAL(dir, true, string(replication.ModeHAStandby))
		class := "upgraded"
		switch {
		case uerr == nil:
		case strings.Contains(uerr.Error(), "historical frame"):
			class = "refused-in-validation"
		case strings.Contains(uerr.Error(), "convert"):
			class = "refused-in-conversion"
		default:
			class = "other: " + uerr.Error()
		}
		outcomes[class]++
		t.Logf("GAPS35 %s attempt=%d input_sha=%x input_len=%d transcoded_len_same_call_site=%d (err %v) outcome=%s err=%v", label, attempt, sha256.Sum256(raw), len(raw), len(converted), cerr, class, uerr)
	}
	t.Logf("GAPS35 %s SUMMARY tries=%d outcomes=%v pad=%d", label, tries, outcomes, pad)
}

func TestGapS35MapOrder(t *testing.T) {
	node16, vectors16 := gapS35Node(t, 16)
	node1, _ := gapS35Node(t, 1)
	gapS35Boundary(t, "named16", node16, 16)

	payload := gapS35Payload(t, node16, 0)
	t.Logf("GAPS35 SPREAD TranscodeV4WALPayload(record) %s", gapS35Spread(t, 64, func() ([]byte, error) {
		return replication.TranscodeV4WALPayload(0, "replication_command", payload)
	}))
	t.Logf("GAPS35 SPREAD TranscodeV4EntityChange(node) %s", gapS35Spread(t, 64, func() ([]byte, error) {
		return storage.TranscodeV4EntityChange(storage.EntityCreateNode, node16)
	}))
	once, err := storage.TranscodeV4EntityChange(storage.EntityCreateNode, node16)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := storage.DecodeNodePayload(once)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("GAPS35 SPREAD EncodeNodePayload(decoded node, %d named embeddings) %s", len(decoded.NamedEmbeddings), gapS35Spread(t, 64, func() ([]byte, error) {
		return storage.EncodeNodePayload(decoded)
	}))
	t.Logf("GAPS35 SPREAD EncodeCommandPayload(map[string][]float32 x16) %s", gapS35Spread(t, 64, func() ([]byte, error) {
		return storage.EncodeCommandPayload(vectors16)
	}))
	t.Logf("GAPS35 SPREAD json.Marshal(map[string][]float32 x16) %s", gapS35Spread(t, 64, func() ([]byte, error) {
		return json.Marshal(vectors16)
	}))
	payload1 := gapS35Payload(t, node1, 0)
	t.Logf("GAPS35 SPREAD TranscodeV4WALPayload(record, 1 named embedding) %s", gapS35Spread(t, 64, func() ([]byte, error) {
		return replication.TranscodeV4WALPayload(0, "replication_command", payload1)
	}))
	gapS35Boundary(t, "named1-bypass", node1, 16)
	gapS35HeaderFit(t, "named16", node16, 32)
	gapS35HeaderFit(t, "named1", node1, 8)
}

// gapS35HeaderFit transcodes the at-limit record n times and, for each output,
// computes what the frame check (checkWALEntrySize, wal_entry_codec.go:105-118)
// measures: the JSON header of the converted entry, whose Checksum field is
// CRC32-C of the converted data (wal_v4_file_migration.go:284) written as a
// decimal number, plus the data length, against the 16 MiB frame.
func gapS35HeaderFit(t *testing.T, label string, node []byte, n int) {
	base, err := replication.TranscodeV4WALPayload(0, "replication_command", gapS35Payload(t, node, 0))
	if err != nil {
		t.Fatal(err)
	}
	probe := storage.WALEntry{Sequence: 1, Timestamp: time.Unix(1794000000, 0).UTC(), Operation: "replication_command", Checksum: 999999999}
	header, _ := json.Marshal(probe)
	pad := (16 << 20) - 8 - len(header) - len(base)
	payload := gapS35Payload(t, node, pad)
	table := crc32.MakeTable(crc32.Castagnoli)
	hist := map[string]int{}
	for i := 0; i < n; i++ {
		out, err := replication.TranscodeV4WALPayload(0, "replication_command", payload)
		if err != nil {
			t.Fatal(err)
		}
		sum := crc32.Checksum(out, table)
		h := storage.WALEntry{Sequence: 1, Timestamp: probe.Timestamp, Operation: "replication_command", Checksum: sum}
		hb, _ := json.Marshal(h)
		fits := len(hb) <= (16<<20)-8-len(out)
		hist[fmt.Sprintf("data=%dB checksum_digits=%d header=%dB fits=%t", len(out), len(fmt.Sprint(sum)), len(hb), fits)]++
	}
	keys := make([]string, 0, len(hist))
	for k := range hist {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		t.Logf("GAPS35 HEADERFIT %s %s x%d of %d", label, k, hist[k], n)
	}
}
