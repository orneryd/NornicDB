// Command recall-bench creates and evaluates reproducible retrieval benchmark runs.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/eval/ir"
	"github.com/orneryd/nornicdb/pkg/localllm"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/search"
)

type ingestCheckpoint struct {
	Dataset      string `json:"dataset"`
	CorpusSHA256 string `json:"corpus_sha256"`
	Documents    int64  `json:"documents"`
}

type documentSinkFunc func(context.Context, ir.Document) error

func (fn documentSinkFunc) StoreDocument(ctx context.Context, document ir.Document) error {
	return fn(ctx, document)
}

type databaseRetriever struct {
	db           *nornicdb.DB
	mode         string
	rrfPreset    string
	rrfK         float64
	vectorWeight float64
	bm25Weight   float64
	minRRFScore  float64
	reranker     *benchmarkRerankConfig
	progress     *benchmarkProgress
}

type benchmarkRerankConfig struct {
	scorer      search.RerankScorer
	topK        int
	maxDocChars int
	timeout     time.Duration
	progress    *benchmarkProgress
}

type benchmarkProgress struct {
	started       time.Time
	lastCandidate time.Time
	interval      time.Duration
	completed     int
	total         int
}

func newBenchmarkProgress(total int, interval time.Duration) *benchmarkProgress {
	now := time.Now()
	return &benchmarkProgress{started: now, lastCandidate: now, interval: interval, total: total}
}

func (p *benchmarkProgress) candidate(done, total int) {
	if p == nil || p.interval <= 0 || time.Since(p.lastCandidate) < p.interval {
		return
	}
	p.lastCandidate = time.Now()
	fmt.Fprintf(os.Stderr, "rerank progress: query %d/%d candidate %d/%d elapsed=%s\n",
		p.completed+1, p.total, done, total, time.Since(p.started).Round(time.Second))
}

func (p *benchmarkProgress) queryComplete() {
	if p == nil {
		return
	}
	p.completed++
	elapsed := time.Since(p.started)
	remaining := time.Duration(0)
	if p.completed > 0 && p.completed < p.total {
		remaining = time.Duration(float64(elapsed) * float64(p.total-p.completed) / float64(p.completed))
	}
	fmt.Fprintf(os.Stderr, "benchmark progress: queries=%d/%d elapsed=%s eta=%s\n",
		p.completed, p.total, elapsed.Round(time.Second), remaining.Round(time.Second))
}

func (r databaseRetriever) Retrieve(ctx context.Context, query string, topK int) ([]ir.RunResult, error) {
	var results []*nornicdb.SearchResult
	var err error
	switch r.mode {
	case "bm25":
		results, err = r.db.Search(ctx, query, []string{"BEIRDocument"}, topK)
	case "rrf":
		queryEmbedding, embedErr := r.db.EmbedQuery(ctx, query)
		if embedErr != nil {
			return nil, fmt.Errorf("embed query: %w", embedErr)
		}
		if len(queryEmbedding) == 0 {
			return nil, fmt.Errorf("embed query: no embedding returned")
		}
		opts, configErr := r.rrfOptions(query)
		if configErr != nil {
			return nil, configErr
		}
		opts.Limit = topK
		opts.Types = []string{"BEIRDocument"}
		results, err = r.db.HybridSearchWithOptions(ctx, query, queryEmbedding, opts)
	default:
		return nil, fmt.Errorf("unsupported retrieval mode %q", r.mode)
	}
	if err != nil {
		return nil, err
	}
	runResults, err := benchmarkRunResults(ctx, query, results, r.reranker)
	if err == nil {
		r.progress.queryComplete()
	}
	return runResults, err
}

func benchmarkRunResults(ctx context.Context, query string, results []*nornicdb.SearchResult, reranker *benchmarkRerankConfig) ([]ir.RunResult, error) {
	type candidate struct {
		documentID string
		content    string
		score      float64
	}
	candidates := make([]candidate, 0, len(results))
	seen := make(map[string]struct{}, len(results))
	for index, result := range results {
		if result == nil || result.Node == nil {
			return nil, fmt.Errorf("search result %d has no node", index)
		}
		beirID, ok := result.Node.Properties["beir_id"].(string)
		if !ok || beirID == "" {
			return nil, fmt.Errorf("search result %d is missing beir_id", index)
		}
		if _, exists := seen[beirID]; exists {
			return nil, fmt.Errorf("search result %d contains duplicate BEIR document %q", index, beirID)
		}
		seen[beirID] = struct{}{}
		title, _ := result.Node.Properties["title"].(string)
		text, _ := result.Node.Properties["text"].(string)
		content := strings.TrimSpace(strings.TrimSpace(title) + "\n\n" + strings.TrimSpace(text))
		candidates = append(candidates, candidate{documentID: beirID, content: content, score: result.Score})
	}
	if reranker != nil {
		if reranker.scorer == nil {
			return nil, fmt.Errorf("reranker scorer is required")
		}
		limit := reranker.topK
		if limit < 1 || limit > len(candidates) {
			limit = len(candidates)
		}
		for index := 0; index < limit; index++ {
			document := candidates[index].content
			if reranker.maxDocChars > 0 && len(document) > reranker.maxDocChars {
				document = document[:reranker.maxDocChars]
			}
			scoreCtx := ctx
			cancel := func() {}
			if reranker.timeout > 0 {
				scoreCtx, cancel = context.WithTimeout(ctx, reranker.timeout)
			}
			score, err := reranker.scorer.Score(scoreCtx, query, document)
			cancel()
			if err != nil {
				return nil, fmt.Errorf("rerank document %q: %w", candidates[index].documentID, err)
			}
			candidates[index].score = float64(score)
			reranker.progress.candidate(index+1, limit)
		}
		sort.SliceStable(candidates[:limit], func(left, right int) bool {
			return candidates[left].score > candidates[right].score
		})
	}
	runResults := make([]ir.RunResult, len(candidates))
	for index, candidate := range candidates {
		score := candidate.score
		if reranker != nil {
			score = float64(len(candidates) - index)
		}
		runResults[index] = ir.RunResult{DocumentID: candidate.documentID, Score: score}
	}
	return runResults, nil
}

func (r databaseRetriever) rrfOptions(query string) (*search.SearchOptions, error) {
	var opts *search.SearchOptions
	switch r.rrfPreset {
	case "adaptive":
		opts = search.GetAdaptiveRRFConfig(query)
	case "default":
		opts = search.DefaultSearchOptions()
	default:
		return nil, fmt.Errorf("unsupported RRF preset %q", r.rrfPreset)
	}
	if r.rrfK > 0 {
		opts.RRFK = r.rrfK
	}
	if r.vectorWeight > 0 {
		opts.VectorWeight = r.vectorWeight
	}
	if r.bm25Weight > 0 {
		opts.BM25Weight = r.bm25Weight
	}
	if r.minRRFScore >= 0 {
		opts.MinRRFScore = r.minRRFScore
	}
	return opts, nil
}

func configureOllamaEmbedder(db *nornicdb.DB, provider, model, apiURL string, dimensions int) error {
	if provider == "" {
		return nil
	}
	if provider != "ollama" {
		return fmt.Errorf("unsupported embedding provider %q; benchmark supports ollama", provider)
	}
	if model == "" || dimensions < 1 {
		return fmt.Errorf("embedding model and positive dimensions are required")
	}
	config := embed.DefaultOllamaConfig()
	config.Model = model
	config.Dimensions = dimensions
	if apiURL != "" {
		config.APIURL = apiURL
	}
	embedder, err := embed.NewEmbedder(config)
	if err != nil {
		return err
	}
	db.SetEmbedder(embedder)
	return nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: recall-bench <evaluate|compare|ingest|manifest|run> [flags]")
		os.Exit(2)
	}
	if os.Args[1] == "run" {
		runRetrieval(os.Args[2:])
		return
	}
	if os.Args[1] == "ingest" {
		runIngest(os.Args[2:])
		return
	}
	if os.Args[1] == "compare" {
		runCompare(os.Args[2:])
		return
	}
	if os.Args[1] == "manifest" {
		runManifest(os.Args[2:])
		return
	}
	if os.Args[1] != "evaluate" {
		fmt.Fprintln(os.Stderr, "usage: recall-bench <evaluate|compare|ingest|manifest|run> [flags]")
		os.Exit(2)
	}
	flags := flag.NewFlagSet("evaluate", flag.ExitOnError)
	qrelsPath := flags.String("qrels", "", "Path to a TREC qrels file")
	runPath := flags.String("run", "", "Path to a six-column TREC run file")
	outputPath := flags.String("output", "", "Optional path for JSON metrics output")
	_ = flags.Parse(os.Args[2:])
	if *qrelsPath == "" || *runPath == "" {
		fmt.Fprintln(os.Stderr, "evaluate requires --qrels and --run")
		os.Exit(2)
	}
	metrics, err := evaluate(*qrelsPath, *runPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	encoded, err := json.MarshalIndent(metrics, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if *outputPath == "" {
		fmt.Println(string(encoded))
		return
	}
	if err := os.WriteFile(*outputPath, append(encoded, '\n'), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runRetrieval(args []string) {
	flags := flag.NewFlagSet("run", flag.ExitOnError)
	dataDir := flags.String("data-dir", "", "Persistent NornicDB data directory")
	queriesPath := flags.String("queries", "", "Path to official BEIR queries.jsonl")
	manifestPath := flags.String("manifest", "", "Path to the shared query manifest")
	outputPath := flags.String("output", "", "Path for six-column TREC run output")
	mode := flags.String("mode", "bm25", "Retrieval mode: bm25 or rrf")
	topK := flags.Int("top-k", 100, "Retrieval depth; Recall@100 requires 100")
	rrfPreset := flags.String("rrf-preset", "adaptive", "RRF preset: adaptive (production) or default (equal weights)")
	rrfK := flags.Float64("rrf-k", 0, "Override RRF constant; 0 keeps the selected preset")
	vectorWeight := flags.Float64("vector-weight", 0, "Override vector RRF weight; 0 keeps the selected preset")
	bm25Weight := flags.Float64("bm25-weight", 0, "Override BM25 RRF weight; 0 keeps the selected preset")
	minRRFScore := flags.Float64("min-rrf-score", -1, "Override minimum RRF score; -1 keeps the selected preset")
	tag := flags.String("tag", "nornic-bm25", "TREC run tag")
	embeddingProvider := flags.String("embedding-provider", "", "Embedding provider required for rrf; currently ollama")
	embeddingModel := flags.String("embedding-model", "bge-m3:latest", "Embedding model")
	embeddingURL := flags.String("embedding-url", "http://localhost:11434", "Embedding API URL")
	embeddingDim := flags.Int("embedding-dim", 1024, "Embedding vector dimensions")
	rerankerProvider := flags.String("reranker-provider", "none", "Reranker provider: none or local-gguf")
	rerankerModel := flags.String("reranker-model", "", "Path to a local GGUF reranker model")
	rerankerPoolingType := flags.Int("reranker-pooling-type", 4, "llama.cpp pooling type for local GGUF reranking; 4 selects the rank head")
	rerankTopK := flags.Int("rerank-top-k", 100, "Number of first-stage documents to rerank")
	rerankerMaxDocChars := flags.Int("reranker-max-doc-chars", 32000, "Maximum document characters passed to the reranker")
	rerankerTimeout := flags.Duration("reranker-timeout", 30*time.Second, "Maximum time for each reranker score")
	progressInterval := flags.Duration("progress-interval", 5*time.Second, "Progress interval during a long reranker query; 0 disables candidate progress")
	_ = flags.Parse(args)
	if *dataDir == "" || *queriesPath == "" || *manifestPath == "" || *outputPath == "" {
		fmt.Fprintln(os.Stderr, "run requires --data-dir, --queries, --manifest, and --output")
		os.Exit(2)
	}
	if *mode != "bm25" && *mode != "rrf" {
		fmt.Fprintln(os.Stderr, "run supports --mode bm25 or --mode rrf")
		os.Exit(2)
	}
	if *mode == "rrf" && *embeddingProvider == "" {
		fmt.Fprintln(os.Stderr, "run --mode rrf requires --embedding-provider ollama")
		os.Exit(2)
	}
	if *topK != 100 {
		fmt.Fprintln(os.Stderr, "run requires --top-k 100 for the Recall@100 experiment")
		os.Exit(2)
	}
	if *minRRFScore < 0 {
		if *minRRFScore != -1 {
			fmt.Fprintln(os.Stderr, "min-rrf-score must be -1 or non-negative")
			os.Exit(2)
		}
	}
	if *rrfK < 0 || *vectorWeight < 0 || *bm25Weight < 0 {
		fmt.Fprintln(os.Stderr, "RRF overrides must not be negative")
		os.Exit(2)
	}
	if *rerankerProvider != "none" && *rerankerProvider != "local-gguf" {
		fmt.Fprintln(os.Stderr, "reranker-provider must be none or local-gguf")
		os.Exit(2)
	}
	if *rerankerProvider == "local-gguf" && *rerankerModel == "" {
		fmt.Fprintln(os.Stderr, "reranker-provider local-gguf requires --reranker-model")
		os.Exit(2)
	}
	if *rerankerPoolingType < 1 || *rerankerPoolingType > 4 || *rerankTopK < 1 || *rerankTopK > *topK || *rerankerMaxDocChars < 1 || *rerankerTimeout <= 0 || *progressInterval < 0 {
		fmt.Fprintln(os.Stderr, "reranker limits and timeout must be positive, and rerank-top-k must not exceed top-k")
		os.Exit(2)
	}
	manifest, err := readManifest(*manifestPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "read manifest:", err)
		os.Exit(1)
	}
	queriesFile, err := os.Open(*queriesPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open queries:", err)
		os.Exit(1)
	}
	defer queriesFile.Close()
	queries, err := ir.ReadQueries(queriesFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "read queries:", err)
		os.Exit(1)
	}
	db, err := nornicdb.Open(*dataDir, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open database:", err)
		os.Exit(1)
	}
	defer db.Close()
	if err := configureOllamaEmbedder(db, *embeddingProvider, *embeddingModel, *embeddingURL, *embeddingDim); err != nil {
		fmt.Fprintln(os.Stderr, "configure embedder:", err)
		os.Exit(1)
	}
	var rerankConfig *benchmarkRerankConfig
	var rerankerModelCloser io.Closer
	if *rerankerProvider == "local-gguf" {
		modelOptions := localllm.DefaultRerankerOptions(*rerankerModel)
		modelOptions.Features.PoolingType = *rerankerPoolingType
		model, loadErr := localllm.LoadRerankerModel(modelOptions)
		if loadErr != nil {
			fmt.Fprintln(os.Stderr, "load reranker:", loadErr)
			os.Exit(1)
		}
		rerankerModelCloser = model
		healthCtx, cancel := context.WithTimeout(context.Background(), *rerankerTimeout)
		_, healthErr := model.Score(healthCtx, "health", "check")
		cancel()
		if healthErr != nil {
			_ = model.Close()
			fmt.Fprintln(os.Stderr, "validate reranker:", healthErr)
			os.Exit(1)
		}
		rerankConfig = &benchmarkRerankConfig{scorer: model, topK: *rerankTopK, maxDocChars: *rerankerMaxDocChars, timeout: *rerankerTimeout}
	}
	if rerankerModelCloser != nil {
		defer rerankerModelCloser.Close()
	}
	progress := newBenchmarkProgress(len(manifest.QueryIDs), *progressInterval)
	if rerankConfig != nil {
		rerankConfig.progress = progress
		fmt.Fprintf(os.Stderr, "benchmark workload: %d queries x %d candidates = %d serialized reranker scores\n",
			len(manifest.QueryIDs), *rerankTopK, len(manifest.QueryIDs)**rerankTopK)
	}
	if err := os.MkdirAll(filepath.Dir(*outputPath), 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "create run directory:", err)
		os.Exit(1)
	}
	temporary := *outputPath + ".tmp"
	output, err := os.Create(temporary)
	if err != nil {
		fmt.Fprintln(os.Stderr, "create run:", err)
		os.Exit(1)
	}
	stats, runErr := ir.RunManifest(context.Background(), manifest, queries, databaseRetriever{db: db, mode: *mode, rrfPreset: *rrfPreset, rrfK: *rrfK, vectorWeight: *vectorWeight, bm25Weight: *bm25Weight, minRRFScore: *minRRFScore, reranker: rerankConfig, progress: progress}, *topK, *tag, output)
	closeErr := output.Close()
	if runErr != nil {
		_ = os.Remove(temporary)
		fmt.Fprintln(os.Stderr, "run retrieval:", runErr)
		os.Exit(1)
	}
	if closeErr != nil {
		_ = os.Remove(temporary)
		fmt.Fprintln(os.Stderr, "close run:", closeErr)
		os.Exit(1)
	}
	if err := os.Rename(temporary, *outputPath); err != nil {
		fmt.Fprintln(os.Stderr, "publish run:", err)
		os.Exit(1)
	}
	writeJSON(struct {
		Dataset             string `json:"dataset"`
		Queries             int64  `json:"queries"`
		Mode                string `json:"mode"`
		RunPath             string `json:"run_path"`
		TopK                int    `json:"top_k"`
		Manifest            string `json:"manifest_sha256"`
		Reranker            string `json:"reranker"`
		RerankerModel       string `json:"reranker_model,omitempty"`
		RerankerPoolingType int    `json:"reranker_pooling_type,omitempty"`
		RerankTopK          int    `json:"rerank_top_k,omitempty"`
		RerankerMaxDocChars int    `json:"reranker_max_doc_chars,omitempty"`
		RerankerTimeout     string `json:"reranker_timeout,omitempty"`
	}{manifest.Dataset, stats.Queries, *mode, *outputPath, *topK, manifest.SHA256, *rerankerProvider, *rerankerModel, *rerankerPoolingType, *rerankTopK, *rerankerMaxDocChars, rerankerTimeout.String()}, "")
}

func readManifest(path string) (ir.QueryManifest, error) {
	encoded, err := os.ReadFile(path)
	if err != nil {
		return ir.QueryManifest{}, err
	}
	var manifest ir.QueryManifest
	if err := json.Unmarshal(encoded, &manifest); err != nil {
		return ir.QueryManifest{}, err
	}
	if manifest.Dataset == "" || len(manifest.QueryIDs) == 0 || manifest.SHA256 == "" {
		return ir.QueryManifest{}, fmt.Errorf("manifest is incomplete")
	}
	return manifest, nil
}

func runIngest(args []string) {
	flags := flag.NewFlagSet("ingest", flag.ExitOnError)
	dataset := flags.String("dataset", "", "BEIR dataset slug")
	corpusPath := flags.String("corpus", "", "Path to official BEIR corpus.jsonl")
	dataDir := flags.String("data-dir", "", "Persistent NornicDB data directory")
	statePath := flags.String("state", "", "Optional resumable ingest checkpoint path")
	embeddingProvider := flags.String("embedding-provider", "", "Optional embedding provider; currently ollama")
	embeddingModel := flags.String("embedding-model", "bge-m3:latest", "Embedding model")
	embeddingURL := flags.String("embedding-url", "http://localhost:11434", "Embedding API URL")
	embeddingDim := flags.Int("embedding-dim", 1024, "Embedding vector dimensions")
	embeddingChunkSize := flags.Int("embedding-chunk-size", 512, "Maximum tokens per corpus embedding chunk")
	embeddingChunkOverlap := flags.Int("embedding-chunk-overlap", 50, "Overlapping tokens between corpus embedding chunks")
	embeddingTimeout := flags.Duration("embedding-timeout", 30*time.Minute, "Maximum time to wait for corpus embeddings")
	_ = flags.Parse(args)
	if *dataset == "" || *corpusPath == "" || *dataDir == "" {
		fmt.Fprintln(os.Stderr, "ingest requires --dataset, --corpus, and --data-dir")
		os.Exit(2)
	}
	if *statePath == "" {
		*statePath = filepath.Join(*dataDir, "recall-bench-ingest.json")
	}
	if *embeddingChunkSize < 1 || *embeddingChunkOverlap < 0 || *embeddingChunkOverlap >= *embeddingChunkSize {
		fmt.Fprintln(os.Stderr, "embedding chunk size must be positive and greater than chunk overlap")
		os.Exit(2)
	}
	digest, err := fileSHA256(*corpusPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "hash corpus:", err)
		os.Exit(1)
	}
	checkpoint, err := readIngestCheckpoint(*statePath, *dataset, digest)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	config := nornicdb.DefaultConfig()
	config.EmbeddingWorker.ChunkSize = *embeddingChunkSize
	config.EmbeddingWorker.ChunkOverlap = *embeddingChunkOverlap
	db, err := nornicdb.Open(*dataDir, config)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open database:", err)
		os.Exit(1)
	}
	defer db.Close()
	if err := configureOllamaEmbedder(db, *embeddingProvider, *embeddingModel, *embeddingURL, *embeddingDim); err != nil {
		fmt.Fprintln(os.Stderr, "configure embedder:", err)
		os.Exit(1)
	}
	corpus, err := os.Open(*corpusPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open corpus:", err)
		os.Exit(1)
	}
	defer corpus.Close()
	ctx := context.Background()
	stats, err := ir.IngestDocuments(ctx, corpus, documentSinkFunc(func(ctx context.Context, document ir.Document) error {
		physicalID := benchmarkNodeID(document.ID)
		if _, err := db.GetNode(ctx, physicalID); err == nil {
			checkpoint.Documents++
			return writeIngestCheckpoint(*statePath, checkpoint)
		} else if !errors.Is(err, nornicdb.ErrNotFound) {
			return fmt.Errorf("lookup document %q: %w", document.ID, err)
		}
		_, err := db.CreateNodeWithID(ctx, physicalID, []string{"BEIRDocument"}, map[string]interface{}{
			"beir_id": document.ID,
			"title":   document.Title,
			"text":    document.Text,
		})
		if err != nil {
			return err
		}
		checkpoint.Documents++
		return writeIngestCheckpoint(*statePath, checkpoint)
	}))
	if err != nil {
		fmt.Fprintln(os.Stderr, "ingest corpus:", err)
		os.Exit(1)
	}
	checkpoint.Documents = stats.Documents
	if err := writeIngestCheckpoint(*statePath, checkpoint); err != nil {
		fmt.Fprintln(os.Stderr, "write ingest checkpoint:", err)
		os.Exit(1)
	}
	if *embeddingProvider != "" {
		if _, err := db.EmbedExisting(ctx); err != nil {
			fmt.Fprintln(os.Stderr, "trigger embeddings:", err)
			os.Exit(1)
		}
		waitCtx, cancel := context.WithTimeout(ctx, *embeddingTimeout)
		err := db.WaitForEmbeddings(waitCtx)
		cancel()
		if err != nil {
			fmt.Fprintln(os.Stderr, "wait for embeddings:", err)
			os.Exit(1)
		}
	}
	if err := db.BuildSearchIndexes(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "build search indexes:", err)
		os.Exit(1)
	}
	writeJSON(struct {
		Dataset      string `json:"dataset"`
		CorpusSHA256 string `json:"corpus_sha256"`
		Documents    int64  `json:"documents"`
		StatePath    string `json:"state_path"`
	}{*dataset, digest, stats.Documents, *statePath}, "")
}

func benchmarkNodeID(beirID string) string {
	digest := sha256.Sum256([]byte(beirID))
	return fmt.Sprintf("beir-%x", digest[:])
}

func fileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}

func readIngestCheckpoint(path, dataset, digest string) (ingestCheckpoint, error) {
	checkpoint := ingestCheckpoint{Dataset: dataset, CorpusSHA256: digest}
	encoded, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return checkpoint, nil
	}
	if err != nil {
		return ingestCheckpoint{}, fmt.Errorf("read ingest checkpoint: %w", err)
	}
	if err := json.Unmarshal(encoded, &checkpoint); err != nil {
		return ingestCheckpoint{}, fmt.Errorf("parse ingest checkpoint: %w", err)
	}
	if checkpoint.Dataset != dataset || checkpoint.CorpusSHA256 != digest {
		return ingestCheckpoint{}, fmt.Errorf("ingest checkpoint does not match dataset and corpus fingerprint")
	}
	return checkpoint, nil
}

func writeIngestCheckpoint(path string, checkpoint ingestCheckpoint) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(checkpoint, "", "  ")
	if err != nil {
		return err
	}
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, append(encoded, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

func runManifest(args []string) {
	flags := flag.NewFlagSet("manifest", flag.ExitOnError)
	dataset := flags.String("dataset", "", "BEIR dataset slug")
	split := flags.String("split", "test", "BEIR query split")
	queriesPath := flags.String("queries", "", "Path to official BEIR queries.jsonl")
	qrelsPath := flags.String("qrels", "", "Path to BEIR qrels TSV")
	limit := flags.Int("limit", 1000, "Maximum number of queries")
	seed := flags.Uint64("seed", 20260810, "Deterministic sampling seed")
	outputPath := flags.String("output", "", "Path for the JSON query manifest")
	_ = flags.Parse(args)
	if *dataset == "" || *queriesPath == "" || *qrelsPath == "" || *outputPath == "" {
		fmt.Fprintln(os.Stderr, "manifest requires --dataset, --queries, --qrels, and --output")
		os.Exit(2)
	}
	queriesFile, err := os.Open(*queriesPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open queries:", err)
		os.Exit(1)
	}
	defer queriesFile.Close()
	queries, err := ir.ReadQueries(queriesFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "read queries:", err)
		os.Exit(1)
	}
	qrelsFile, err := os.Open(*qrelsPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open qrels:", err)
		os.Exit(1)
	}
	defer qrelsFile.Close()
	qrels, err := ir.ReadQrels(qrelsFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "read qrels:", err)
		os.Exit(1)
	}
	queries = ir.FilterQueriesWithQrels(queries, qrels)
	manifest, err := ir.NewQueryManifest(*dataset, *split, queries, *limit, *seed)
	if err != nil {
		fmt.Fprintln(os.Stderr, "create manifest:", err)
		os.Exit(1)
	}
	writeJSON(manifest, *outputPath)
}

func runCompare(args []string) {
	flags := flag.NewFlagSet("compare", flag.ExitOnError)
	qrelsPath := flags.String("qrels", "", "Path to a TREC qrels file")
	baselinePath := flags.String("baseline", "", "Path to the baseline TREC run")
	candidatePath := flags.String("candidate", "", "Path to the candidate TREC run")
	seed := flags.Uint64("seed", 20260810, "Bootstrap random seed")
	resamples := flags.Int("resamples", 10000, "Number of paired bootstrap resamples")
	outputPath := flags.String("output", "", "Optional path for JSON comparison output")
	_ = flags.Parse(args)
	if *qrelsPath == "" || *baselinePath == "" || *candidatePath == "" {
		fmt.Fprintln(os.Stderr, "compare requires --qrels, --baseline, and --candidate")
		os.Exit(2)
	}
	qrels, baseline, err := loadRun(*qrelsPath, *baselinePath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	_, candidate, err := loadRun(*qrelsPath, *candidatePath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	comparison, err := ir.Compare(qrels, baseline, candidate, *seed, *resamples)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	writeJSON(comparison, *outputPath)
}

func evaluate(qrelsPath, runPath string) (ir.Metrics, error) {
	qrels, run, err := loadRun(qrelsPath, runPath)
	if err != nil {
		return ir.Metrics{}, err
	}
	return ir.Evaluate(qrels, run), nil
}

func loadRun(qrelsPath, runPath string) (ir.Qrels, map[string][]string, error) {
	qrelsFile, err := os.Open(qrelsPath)
	if err != nil {
		return nil, nil, fmt.Errorf("open qrels: %w", err)
	}
	defer qrelsFile.Close()
	qrels, err := ir.ReadQrels(qrelsFile)
	if err != nil {
		return nil, nil, fmt.Errorf("read qrels: %w", err)
	}
	runFile, err := os.Open(runPath)
	if err != nil {
		return nil, nil, fmt.Errorf("open run: %w", err)
	}
	defer runFile.Close()
	run, err := ir.ReadRun(runFile)
	if err != nil {
		return nil, nil, fmt.Errorf("read run: %w", err)
	}
	return qrels, run, nil
}

func writeJSON(value any, outputPath string) {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if outputPath == "" {
		fmt.Println(string(encoded))
		return
	}
	if err := os.WriteFile(outputPath, append(encoded, '\n'), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
