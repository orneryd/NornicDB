package search

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"unicode/utf8"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const (
	docsChunkWindow        = 512
	docsChunkStride        = 256
	docsVectorDims         = 64
	minDocsChunks          = 8000
	docsExperimentPlanPath = "docs/plans/rrf-parallel-retrieval-plan.md"
)

type docsCorpusChunk struct {
	id      string
	path    string
	heading string
	text    string
	vector  []float32
}

type docsCorpusFixture struct {
	chunks      []docsCorpusChunk
	fileCount   int
	sourceBytes int64
	manifest    string
}

type docsHeading struct {
	offset int
	text   string
}

type docsBenchmarkQuery struct {
	name          string
	text          string
	embeddingText string
}

var docsBenchmarkQueries = []docsBenchmarkQuery{
	{name: "balanced", text: "configure hybrid vector search and BM25 indexes", embeddingText: "configure hybrid vector search and BM25 indexes"},
	{name: "bm25_dominant", text: "NORNICDB_BM25_PREFIX_MAX_EXPANSIONS", embeddingText: "unrelated transaction backup retention policy"},
	{name: "vector_dominant", text: "no_exact_lexical_match_42", embeddingText: "HNSW vector search candidate generation and exact scoring"},
}

func loadDocsCorpusFixture() (docsCorpusFixture, error) {
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		return docsCorpusFixture{}, fmt.Errorf("locate benchmark source file")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(sourceFile), "..", ".."))
	docsRoot := filepath.Join(repoRoot, "docs")

	var paths []string
	err := filepath.WalkDir(docsRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relativePath, relErr := filepath.Rel(repoRoot, path)
		if relErr != nil {
			return fmt.Errorf("make documentation path relative: %w", relErr)
		}
		if !entry.IsDir() && strings.EqualFold(filepath.Ext(path), ".md") && filepath.ToSlash(relativePath) != docsExperimentPlanPath {
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil {
		return docsCorpusFixture{}, fmt.Errorf("walk documentation corpus: %w", err)
	}
	sort.Strings(paths)

	fixture := docsCorpusFixture{fileCount: len(paths)}
	manifest := sha256.New()
	var floatBytes [4]byte
	for _, path := range paths {
		contentBytes, readErr := os.ReadFile(path)
		if readErr != nil {
			return docsCorpusFixture{}, fmt.Errorf("read %s: %w", path, readErr)
		}
		fixture.sourceBytes += int64(len(contentBytes))
		relativePath, relErr := filepath.Rel(repoRoot, path)
		if relErr != nil {
			return docsCorpusFixture{}, fmt.Errorf("make documentation path relative: %w", relErr)
		}
		relativePath = filepath.ToSlash(relativePath)
		chunks := chunkDocsMarkdown(relativePath, strings.ReplaceAll(string(contentBytes), "\r\n", "\n"))
		for i := range chunks {
			chunk := &chunks[i]
			chunk.vector = docsTextVector(chunk.text, docsVectorDims)
			manifest.Write([]byte(chunk.id))
			manifest.Write([]byte{0})
			manifest.Write([]byte(chunk.path))
			manifest.Write([]byte{0})
			manifest.Write([]byte(chunk.heading))
			manifest.Write([]byte{0})
			manifest.Write([]byte(chunk.text))
			manifest.Write([]byte{0})
			for _, value := range chunk.vector {
				binary.LittleEndian.PutUint32(floatBytes[:], math.Float32bits(value))
				manifest.Write(floatBytes[:])
			}
		}
		fixture.chunks = append(fixture.chunks, chunks...)
	}
	fixture.manifest = fmt.Sprintf("%x", manifest.Sum(nil))
	return fixture, nil
}

func chunkDocsMarkdown(path, content string) []docsCorpusChunk {
	headings := docsHeadings(content)
	headingIndex := 0
	currentHeading := ""
	chunks := make([]docsCorpusChunk, 0, len(content)/docsChunkStride+1)

	for start, ordinal := 0, 0; start < len(content); start, ordinal = nextDocsChunkStart(content, start), ordinal+1 {
		for headingIndex < len(headings) && headings[headingIndex].offset <= start {
			currentHeading = headings[headingIndex].text
			headingIndex++
		}
		end := docsChunkEnd(content, start)
		text := strings.TrimSpace(content[start:end])
		if text != "" {
			chunks = append(chunks, docsCorpusChunk{
				id:      fmt.Sprintf("docs:%s:%06d", path, ordinal),
				path:    path,
				heading: currentHeading,
				text:    text,
			})
		}
		if start+docsChunkStride >= len(content) {
			break
		}
	}
	return chunks
}

func nextDocsChunkStart(content string, start int) int {
	next := start + docsChunkStride
	for next < len(content) && !utf8.RuneStart(content[next]) {
		next++
	}
	return next
}

func docsChunkEnd(content string, start int) int {
	end := min(start+docsChunkWindow, len(content))
	for end > start && end < len(content) && !utf8.RuneStart(content[end]) {
		end--
	}
	if end == len(content) {
		return end
	}
	searchStart := max(start+docsChunkStride, end-64)
	if split := strings.LastIndexAny(content[searchStart:end], " \t\r\n"); split >= 0 {
		return searchStart + split
	}
	return end
}

func docsHeadings(content string) []docsHeading {
	var headings []docsHeading
	offset := 0
	for _, line := range strings.SplitAfter(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") {
			heading := strings.TrimSpace(strings.TrimLeft(trimmed, "#"))
			if heading != "" {
				headings = append(headings, docsHeading{offset: offset, text: heading})
			}
		}
		offset += len(line)
	}
	return headings
}

func docsTextVector(text string, dimensions int) []float32 {
	vector := make([]float32, dimensions)
	for _, token := range tokenize(text) {
		hash := uint64(14695981039346656037)
		for i := 0; i < len(token); i++ {
			hash ^= uint64(token[i])
			hash *= 1099511628211
		}
		weight := float32(1)
		if hash&(1<<63) != 0 {
			weight = -1
		}
		vector[int(hash%uint64(dimensions))] += weight
	}
	var sumSquares float64
	for _, value := range vector {
		sumSquares += float64(value * value)
	}
	if sumSquares == 0 {
		return vector
	}
	norm := float32(math.Sqrt(sumSquares))
	for i := range vector {
		vector[i] /= norm
	}
	return vector
}

func TestDocsCorpusFixture_DeterministicAndSufficientScale(t *testing.T) {
	first, err := loadDocsCorpusFixture()
	require.NoError(t, err)
	second, err := loadDocsCorpusFixture()
	require.NoError(t, err)

	require.GreaterOrEqual(t, len(first.chunks), minDocsChunks)
	require.Equal(t, first.fileCount, second.fileCount)
	require.Equal(t, first.sourceBytes, second.sourceBytes)
	require.Equal(t, first.manifest, second.manifest)
	require.Equal(t, first.chunks, second.chunks)
	t.Logf("docs corpus: files=%d bytes=%d chunks=%d manifest=%s", first.fileCount, first.sourceBytes, len(first.chunks), first.manifest)
}

func buildDocsSearchFixture(tb testing.TB, chunks []docsCorpusChunk) *Service {
	tb.Helper()
	base := storage.NewMemoryEngine()
	engine := storage.NewNamespacedEngine(base, "docs-benchmark")
	svc := NewServiceWithDimensions(engine, docsVectorDims)
	svc.resultCache = nil

	for _, chunk := range chunks {
		node := &storage.Node{
			ID:     storage.NodeID(chunk.id),
			Labels: []string{"Documentation"},
			Properties: map[string]any{
				"path":    chunk.path,
				"heading": chunk.heading,
				"content": chunk.text,
			},
			ChunkEmbeddings: [][]float32{chunk.vector},
		}
		_, err := engine.CreateNode(node)
		require.NoError(tb, err)
		require.NoError(tb, svc.IndexNode(node))
	}
	return svc
}

func docsCorpusBenchmarkTiers(total int) []int {
	tiers := []int{1000, 2000, 4000, 8000}
	result := make([]int, 0, len(tiers)+1)
	for _, tier := range tiers {
		if tier < total {
			result = append(result, tier)
		}
	}
	return append(result, total)
}

func BenchmarkSearchProfile_RRFHybrid_DocsCorpus(b *testing.B) {
	fixture, err := loadDocsCorpusFixture()
	if err != nil {
		b.Fatal(err)
	}
	if len(fixture.chunks) < minDocsChunks {
		b.Fatalf("documentation corpus has %d chunks; need at least %d", len(fixture.chunks), minDocsChunks)
	}

	for _, size := range docsCorpusBenchmarkTiers(len(fixture.chunks)) {
		size := size
		b.Run(fmt.Sprintf("chunks_%06d", size), func(b *testing.B) {
			svc := buildDocsSearchFixture(b, fixture.chunks[:size])
			defer svc.Close()
			opts := DefaultSearchOptions()
			opts.Limit = 20

			for _, query := range docsBenchmarkQueries {
				query := query
				queryVector := docsTextVector(query.embeddingText, docsVectorDims)
				if _, err := svc.Search(context.Background(), query.text, queryVector, opts); err != nil {
					b.Fatal(err)
				}
				b.Run(query.name, func(b *testing.B) {
					b.ReportAllocs()
					b.ReportMetric(float64(fixture.fileCount), "source_files")
					b.ReportMetric(float64(fixture.sourceBytes), "source_bytes")
					b.ReportMetric(float64(size), "indexed_chunks")
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if _, err := svc.Search(context.Background(), query.text, queryVector, opts); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}

func BenchmarkSearchProfile_RRFHybrid_DocsCorpus_ModeComparison(b *testing.B) {
	fixture, err := loadDocsCorpusFixture()
	if err != nil {
		b.Fatal(err)
	}
	if len(fixture.chunks) < minDocsChunks {
		b.Fatalf("documentation corpus has %d chunks; need at least %d", len(fixture.chunks), minDocsChunks)
	}

	for _, size := range docsCorpusBenchmarkTiers(len(fixture.chunks)) {
		size := size
		b.Run(fmt.Sprintf("chunks_%06d", size), func(b *testing.B) {
			svc := buildDocsSearchFixture(b, fixture.chunks[:size])
			defer svc.Close()
			opts := DefaultSearchOptions()
			opts.Limit = 20

			for _, query := range docsBenchmarkQueries {
				query := query
				queryVector := docsTextVector(query.embeddingText, docsVectorDims)
				if _, err := sequentialHybridReference(svc, query.text, queryVector, opts); err != nil {
					b.Fatal(err)
				}
				if _, err := svc.rrfHybridSearch(context.Background(), query.text, queryVector, opts); err != nil {
					b.Fatal(err)
				}

				b.Run(query.name+"/sequential", func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if _, err := sequentialHybridReference(svc, query.text, queryVector, opts); err != nil {
							b.Fatal(err)
						}
					}
				})
				b.Run(query.name+"/parallel", func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if _, err := svc.rrfHybridSearch(context.Background(), query.text, queryVector, opts); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}

func runDocsConcurrentBenchmark(b *testing.B, concurrency int, search func() error) {
	b.Helper()
	b.StopTimer()
	var next atomic.Uint64
	var workers sync.WaitGroup
	var firstError error
	var errorOnce sync.Once
	start := make(chan struct{})
	workers.Add(concurrency)
	for worker := 0; worker < concurrency; worker++ {
		go func() {
			defer workers.Done()
			<-start
			for {
				operation := next.Add(1) - 1
				if operation >= uint64(b.N) {
					return
				}
				if err := search(); err != nil {
					errorOnce.Do(func() { firstError = err })
					return
				}
			}
		}()
	}
	b.StartTimer()
	close(start)
	workers.Wait()
	b.StopTimer()
	if firstError != nil {
		b.Fatal(firstError)
	}
}

func BenchmarkSearchProfile_RRFHybrid_DocsCorpus_Concurrency(b *testing.B) {
	fixture, err := loadDocsCorpusFixture()
	if err != nil {
		b.Fatal(err)
	}
	query := docsBenchmarkQueries[0]
	queryVector := docsTextVector(query.embeddingText, docsVectorDims)

	for _, size := range []int{8000, len(fixture.chunks)} {
		size := size
		b.Run(fmt.Sprintf("chunks_%06d", size), func(b *testing.B) {
			svc := buildDocsSearchFixture(b, fixture.chunks[:size])
			defer svc.Close()
			opts := DefaultSearchOptions()
			opts.Limit = 20
			if _, err := sequentialHybridReference(svc, query.text, queryVector, opts); err != nil {
				b.Fatal(err)
			}
			if _, err := svc.rrfHybridSearch(context.Background(), query.text, queryVector, opts); err != nil {
				b.Fatal(err)
			}

			for _, concurrency := range []int{1, 8, 16} {
				concurrency := concurrency
				b.Run(fmt.Sprintf("concurrency_%02d/sequential", concurrency), func(b *testing.B) {
					b.ReportAllocs()
					runDocsConcurrentBenchmark(b, concurrency, func() error {
						_, err := sequentialHybridReference(svc, query.text, queryVector, opts)
						return err
					})
				})
				b.Run(fmt.Sprintf("concurrency_%02d/parallel", concurrency), func(b *testing.B) {
					b.ReportAllocs()
					runDocsConcurrentBenchmark(b, concurrency, func() error {
						_, err := svc.rrfHybridSearch(context.Background(), query.text, queryVector, opts)
						return err
					})
				})
			}
		})
	}
}
