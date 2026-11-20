package nornicdb_test

// Earlier-issues batch 5, item L429 (untracked probe; never committed).
// Old-build (b6d8e762a) store whose active wal.log holds an embedding audit
// record over 16 MiB that the embedding worker wrote, the process then killed
// (SIGKILL). The parent then makes three copies under ei5Root:
//   intact        - as killed;
//   torn-audit    - wal.log cut in the middle of the oversized audit frame
//                   (what a crash during that write leaves), later frames dropped;
//   torn-ordinary - wal.log cut in the middle of the last, ordinary frame.
// Each copy is then reopened by the old build itself (its own restart).

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/stretchr/testify/require"
)

const ei5Root = "/work/tnrun/v152-upg/v150/torn-audit"

type ei5Provider struct {
	chunks, dims int
	calls        atomic.Int32
}

func (p *ei5Provider) Embed(context.Context, string) ([]float32, error) {
	v := make([]float32, p.dims)
	v[0] = 1
	return v, nil
}
func (p *ei5Provider) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i := range texts {
		out[i], _ = p.Embed(ctx, texts[i])
	}
	return out, nil
}
func (p *ei5Provider) ChunkText(text string, _, _ int) ([]string, error) { return []string{text}, nil }
func (p *ei5Provider) Dimensions() int                                   { return p.dims }
func (p *ei5Provider) Model() string                                     { return "ei5-stub" }
func (p *ei5Provider) Backend() string                                   { return "cpu" }
func (p *ei5Provider) EmbedDocumentChunks(context.Context, string, int, int) (*embed.DocumentChunkResult, error) {
	call := int(p.calls.Add(1))
	result := &embed.DocumentChunkResult{Chunks: make([]string, p.chunks), Embeddings: make([][]float32, p.chunks), Model: p.Model()}
	for i := range result.Chunks {
		result.Chunks[i] = "chunk"
		v := make([]float32, p.dims)
		for j := range v {
			v[j] = float32(call*1000000 + i + j)
		}
		result.Embeddings[i] = v
	}
	return result, nil
}

var _ embed.Embedder = (*ei5Provider)(nil)

func ei5Config(embedding bool) *nornicdb.Config {
	c := nornicdb.DefaultConfig()
	c.Database.AsyncWritesEnabled = false
	c.Database.StrictDurability = true
	c.Database.WALSyncMode = "immediate"
	c.Database.WALAutoCompactionEnabled = false
	c.Memory.DecayEnabled = false
	c.Memory.AutoLinksEnabled = false
	c.Memory.EmbeddingEnabled = embedding
	c.Memory.EmbeddingDimensions = 1024
	c.EmbeddingWorker.PropertiesInclude = []string{"text"}
	c.EmbeddingWorker.IncludeLabels = false
	c.EmbeddingWorker.NumWorkers = 1
	c.EmbeddingWorker.MaxRetries = 0
	c.EmbeddingWorker.BatchDelay = 0
	c.EmbeddingWorker.TriggerDebounceDelay = 0
	return c
}

type ei5Frame struct {
	start, end int64
	version    byte
	size       uint32
}

// ei5Frames walks the framed journal file: 9-byte header (magic, version,
// payload size), payload, CRC; version 2 and later add an 8-byte trailer and
// padding to 8 bytes.
func ei5Frames(path string) ([]ei5Frame, int64, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, err
	}
	var frames []ei5Frame
	off := int64(0)
	for off+9 <= int64(len(raw)) {
		if string(raw[off:off+4]) != "WALE" {
			return frames, int64(len(raw)), fmt.Errorf("no magic at %d", off)
		}
		version := raw[off+4]
		size := binary.LittleEndian.Uint32(raw[off+5 : off+9])
		length := int64(9) + int64(size) + 4
		if version >= 2 {
			length += 8
			length = (length + 7) &^ 7
		}
		frames = append(frames, ei5Frame{start: off, end: off + length, version: version, size: size})
		off += length
	}
	return frames, int64(len(raw)), nil
}

func ei5CopyDir(t testing.TB, src, dst string) {
	require.NoError(t, filepath.Walk(src, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, p)
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, info.Mode().Perm()|0o700)
		}
		in, err := os.Open(p)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.Create(target)
		if err != nil {
			return err
		}
		if _, err := io.Copy(out, in); err != nil {
			out.Close()
			return err
		}
		return out.Close()
	}))
}

func ei5Listing(dir string) string {
	var out []string
	_ = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(dir, p)
		out = append(out, fmt.Sprintf("%s %d", rel, info.Size()))
		return nil
	})
	return strings.Join(out, " | ")
}

// TestEi5TornAuditWriteChild is the killed process.
func TestEi5TornAuditWriteChild(t *testing.T) {
	if os.Getenv("EI5_CHILD") == "" {
		t.Skip("child only")
	}
	data := filepath.Join(ei5Root, "source", "data")
	db, err := nornicdb.Open(data, ei5Config(true))
	require.NoError(t, err)
	provider := &ei5Provider{chunks: 6600, dims: 1024}
	db.SetEmbedder(provider)
	err = upgEnsureSearch(db)
	require.NoError(t, err)
	_, err = db.ExecuteCypher(context.Background(), "CREATE (:Doc {key:'d', text:'a long document'})", nil)
	require.NoError(t, err)
	deadline := time.Now().Add(5 * time.Minute)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = db.WaitForEmbeddings(ctx)
		cancel()
		nodes, err := upgNodesByLabel(db.GetStorage(), "Doc")
		require.NoError(t, err)
		if len(nodes) == 1 {
			full, err := upgGetNode(db.GetStorage(), nodes[0].ID)
			require.NoError(t, err)
			if len(full.ChunkEmbeddings) == 6600 {
				fmt.Printf("EI5_CHILD embedded chunks=%d dims=%d\n", len(full.ChunkEmbeddings), len(full.ChunkEmbeddings[0]))
				break
			}
		}
		require.True(t, time.Now().Before(deadline), "not embedded")
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(time.Second)
	_, err = db.ExecuteCypher(context.Background(), "CREATE (:After {k: 1})", nil)
	require.NoError(t, err)
	fmt.Println("EI5_READY")
	select {}
}

func ei5OldRestart(t *testing.T, name, data string) {
	start := time.Now()
	db, err := nornicdb.Open(data, ei5Config(false))
	if err != nil {
		fmt.Printf("EI5_B6_RESTART %s err=%v elapsed=%v\n", name, err, time.Since(start).Round(time.Millisecond))
		return
	}
	docs, _ := upgNodesByLabel(db.GetStorage(), "Doc")
	after, _ := upgNodesByLabel(db.GetStorage(), "After")
	chunks := -1
	if len(docs) == 1 {
		if full, err := upgGetNode(db.GetStorage(), docs[0].ID); err == nil {
			chunks = len(full.ChunkEmbeddings)
		}
	}
	fmt.Printf("EI5_B6_RESTART %s ok elapsed=%v docs=%d docChunks=%d after=%d\n", name, time.Since(start).Round(time.Millisecond), len(docs), chunks, len(after))
	require.NoError(t, db.Close())
}

func TestEi5TornAuditWrite(t *testing.T) {
	require.NoError(t, os.RemoveAll(ei5Root))
	require.NoError(t, os.MkdirAll(filepath.Join(ei5Root, "source"), 0o755))
	cmd := exec.Command(os.Args[0], "-test.run=^TestEi5TornAuditWriteChild$", "-test.v", "-test.timeout=30m")
	cmd.Env = append(os.Environ(), "EI5_CHILD=1")
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	ready := make(chan bool, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 1<<20), 1<<20)
		ok := false
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Println("child:", line)
			if strings.Contains(line, "EI5_READY") && !ok {
				ok = true
				ready <- true
			}
		}
		if !ok {
			ready <- false
		}
	}()
	require.True(t, <-ready, "child did not reach the stop point")
	require.NoError(t, cmd.Process.Kill())
	_ = cmd.Wait()
	source := filepath.Join(ei5Root, "source", "data")
	fmt.Printf("EI5_KILLED state=%v\nEI5_LISTING %s\n", cmd.ProcessState, ei5Listing(filepath.Join(source, "wal")))
	active := filepath.Join(source, "wal", "wal.log")
	frames, fileLen, err := ei5Frames(active)
	require.NoError(t, err)
	audit := -1
	for i, f := range frames {
		fmt.Printf("EI5_FRAME %d start=%d end=%d version=%d size=%d\n", i, f.start, f.end, f.version, f.size)
		if f.size > 16<<20 {
			audit = i
		}
	}
	fmt.Printf("EI5_ACTIVE file=%d frames=%d oversized_index=%d\n", fileLen, len(frames), audit)
	require.GreaterOrEqual(t, audit, 0, "an oversized frame is in the active wal.log")
	require.Greater(t, len(frames), audit+1, "an ordinary frame follows the audit")
	last := frames[len(frames)-1]
	cuts := []struct {
		name string
		at   int64
	}{
		{"intact", fileLen},
		{"torn-audit", frames[audit].start + 9 + int64(frames[audit].size)/2},
		{"torn-ordinary", last.start + 9 + int64(last.size)/2},
	}
	for _, cut := range cuts {
		dst := filepath.Join(ei5Root, cut.name, "data")
		ei5CopyDir(t, source, dst)
		require.NoError(t, os.Truncate(filepath.Join(dst, "wal", "wal.log"), cut.at))
		fmt.Printf("EI5_COPY %s wal.log truncated to %d of %d\n", cut.name, cut.at, fileLen)
		// The old build's own restart, on a copy of the copy.
		restart := filepath.Join(ei5Root, cut.name+"-b6restart", "data")
		ei5CopyDir(t, dst, restart)
		ei5OldRestart(t, cut.name, restart)
		require.NoError(t, os.RemoveAll(filepath.Dir(restart)))
	}
}
