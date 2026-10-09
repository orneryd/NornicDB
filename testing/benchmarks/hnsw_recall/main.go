// Command hnsw_recall compares HNSW results with exact cosine search on a
// generated uniform and clustered corpus.
package main

import (
	"context"
	"encoding/binary"
	"fmt"
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"os"
	"sort"
	"time"

	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/search"
)

const (
	dimensions = 128
	k          = 10
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: go run ./testing/benchmarks/hnsw_recall DATA_DIRECTORY")
		os.Exit(2)
	}
	directory := os.Args[1]
	runVectorOnly("uniform", loadVectors(directory+"/uniform.f32"), loadVectors(directory+"/uniform_q.f32"))
	runClusteredStages(loadVectors(directory+"/clustered.f32"), loadVectors(directory+"/clustered_q.f32"))
}

func runVectorOnly(name string, vectors, queries [][]float32) {
	ids := make([]string, len(vectors))
	for i := range ids {
		ids[i] = fmt.Sprintf("v%d", i)
	}
	index, duration := buildIndex(vectors, ids, nil)
	fmt.Printf("build,vector_only,%s,%d,%s\n", name, len(vectors), duration)
	reportRecall("vector_only", name, index, vectors, ids, queries, nil)
}

func runClusteredStages(vectors, queries [][]float32) {
	ids, hints, queryEntries := clusteredLexicalMetadata(len(vectors), len(queries))
	vectorOnly, vectorBuild := buildIndex(vectors, ids, nil)
	fmt.Printf("build,vector_only,clustered,%d,%s\n", len(vectors), vectorBuild)
	reportRecall("vector_only", "clustered", vectorOnly, vectors, ids, queries, nil)
	reportRecall("query_lexical_entries", "clustered", vectorOnly, vectors, ids, queries, queryEntries)

	lexicalBuild, lexicalBuildDuration := buildIndex(vectors, ids, hints)
	fmt.Printf("build,lexical_metadata,clustered,%d,%s\n", len(vectors), lexicalBuildDuration)
	reportRecall("build_lexical_metadata", "clustered", lexicalBuild, vectors, ids, queries, nil)
	reportRecall("combined", "clustered", lexicalBuild, vectors, ids, queries, queryEntries)
}

func buildIndex(vectors [][]float32, ids []string, hints []search.LexicalSeedHint) (*search.HNSWIndex, time.Duration) {
	config := search.DefaultHNSWConfig()
	config.M = 16
	config.EfConstruction = 100
	config.EfSearch = 50
	config.UseGPUBuild = false
	index := search.NewHNSWIndex(dimensions, config)
	index.SetBuildLexicalHints(hints)
	started := time.Now()
	for i, value := range vectors {
		if err := index.Add(ids[i], value); err != nil {
			panic(err)
		}
	}
	return index, time.Since(started)
}

func reportRecall(stage, name string, index *search.HNSWIndex, vectors [][]float32, ids []string, queries [][]float32, queryEntries [][]string) {
	truth := exactTopK(vectors, ids, queries)
	for _, ef := range []int{50, 200, 800, 3200} {
		hits := 0
		zeroHitQueries := 0
		started := time.Now()
		for queryIndex, query := range queries {
			var entries []string
			if queryIndex < len(queryEntries) {
				entries = queryEntries[queryIndex]
			}
			results, err := index.SearchWithEfFromEntries(context.Background(), query, k, -1, ef, entries)
			if err != nil {
				panic(err)
			}
			queryHits := 0
			for _, result := range results {
				if truth[queryIndex][result.ID] {
					queryHits++
				}
			}
			hits += queryHits
			if queryHits == 0 {
				zeroHitQueries++
			}
		}
		fmt.Printf("recall,%s,%s,%d,%.4f,%d,%d,%s\n", stage, name, ef,
			float64(hits)/float64(k*len(queries)), zeroHitQueries, len(queries), time.Since(started))
	}
}

func exactTopK(vectors [][]float32, ids []string, queries [][]float32) []map[string]bool {
	type scoredVector struct {
		index int
		score float64
	}
	truth := make([]map[string]bool, len(queries))
	for queryIndex, query := range queries {
		scores := make([]scoredVector, len(vectors))
		for vectorIndex, value := range vectors {
			scores[vectorIndex] = scoredVector{vectorIndex, vector.DotProduct(query, value)}
		}
		sort.Slice(scores, func(i, j int) bool { return scores[i].score > scores[j].score })
		truth[queryIndex] = make(map[string]bool, k)
		for _, candidate := range scores[:k] {
			truth[queryIndex][ids[candidate.index]] = true
		}
	}
	return truth
}

func clusteredLexicalMetadata(vectorCount, queryCount int) ([]string, []search.LexicalSeedHint, [][]string) {
	const (
		topics            = 40
		documentsPerTopic = 50
		chunksPerDocument = 50
	)
	ids := make([]string, vectorCount)
	fulltext := search.NewFulltextIndexV2()
	for topic := 0; topic < topics; topic++ {
		for document := 0; document < documentsPerTopic; document++ {
			base := topic*documentsPerTopic*chunksPerDocument + document*chunksPerDocument
			documentID := fmt.Sprintf("doc-%02d-%02d", topic, document)
			fulltext.Index(documentID, fmt.Sprintf("topic_%02d document_%02d_%02d", topic, topic, document))
			for chunk := 0; chunk < chunksPerDocument && base+chunk < vectorCount; chunk++ {
				ids[base+chunk] = documentID
				if chunk > 0 {
					ids[base+chunk] = fmt.Sprintf("%s-chunk-%d", documentID, chunk)
				}
			}
		}
	}

	hints := fulltext.LexicalSeedHints(16*16, 16/2)
	entries := make([][]string, queryCount)
	for queryIndex := 0; queryIndex < queryCount; queryIndex++ {
		topic := queryIndex / 5
		document := (queryIndex % 5) * 10
		results := fulltext.Search(fmt.Sprintf("topic_%02d document_%02d_%02d", topic, topic, document), 8)
		entries[queryIndex] = make([]string, len(results))
		for i := range results {
			entries[queryIndex][i] = results[i].ID
		}
	}
	return ids, hints, entries
}

func loadVectors(path string) [][]float32 {
	data, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	count := len(data) / 4 / dimensions
	vectors := make([][]float32, count)
	for i := range vectors {
		vectors[i] = make([]float32, dimensions)
		for j := range vectors[i] {
			offset := (i*dimensions + j) * 4
			vectors[i][j] = math.Float32frombits(binary.LittleEndian.Uint32(data[offset:]))
		}
	}
	return vectors
}
