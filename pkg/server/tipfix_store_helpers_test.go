package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

var hsStart = time.Now()

func hsLog(format string, args ...interface{}) {
	fmt.Printf("HS t=%7.2f "+format+"\n", append([]interface{}{time.Since(hsStart).Seconds()}, args...)...)
}

func hsEnvS(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

type hsEmbedder struct {
	dims       int
	calls      atomic.Int64
	texts      atomic.Int64
	chars      atomic.Int64
	chunkCalls atomic.Int64
	chunkChars atomic.Int64
	maxChunkIn atomic.Int64
}

func (e *hsEmbedder) vec(text string) []float32 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(text[:min(len(text), 256)]))
	r := rand.New(rand.NewSource(int64(h.Sum64())))
	v := make([]float32, e.dims)
	for i := range v {
		v[i] = r.Float32() - 0.5
	}
	return v
}

func (e *hsEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	e.calls.Add(1)
	e.texts.Add(1)
	e.chars.Add(int64(len(text)))
	return e.vec(text), nil
}

func (e *hsEmbedder) EmbedBatch(_ context.Context, texts []string) ([][]float32, error) {
	e.calls.Add(1)
	out := make([][]float32, len(texts))
	for i, text := range texts {
		e.texts.Add(1)
		e.chars.Add(int64(len(text)))
		out[i] = e.vec(text)
	}
	return out, nil
}

func (e *hsEmbedder) ChunkText(text string, maxTokens, _ int) ([]string, error) {
	e.chunkCalls.Add(1)
	e.chunkChars.Add(int64(len(text)))
	for cur := e.maxChunkIn.Load(); int64(len(text)) > cur && !e.maxChunkIn.CompareAndSwap(cur, int64(len(text))); cur = e.maxChunkIn.Load() {
	}
	size := maxTokens * 4
	if size <= 0 {
		size = 32768
	}
	if len(text) <= size {
		return []string{text}, nil
	}
	var chunks []string
	for start := 0; start < len(text); start += size {
		chunks = append(chunks, text[start:min(len(text), start+size)])
	}
	return chunks, nil
}

func (e *hsEmbedder) Dimensions() int { return e.dims }
func (e *hsEmbedder) Model() string   { return "deterministic-2048" }
func (e *hsEmbedder) Backend() string { return "cpu" }
func (e *hsEmbedder) Close() error    { return nil }

type hsWords struct{ words []string }

func hsVocab(r *rand.Rand, n int) *hsWords {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	words := make([]string, n)
	for i := range words {
		word := make([]byte, 3+r.Intn(8))
		for j := range word {
			word[j] = letters[r.Intn(len(letters))]
		}
		words[i] = string(word)
	}
	return &hsWords{words: words}
}

func (v *hsWords) text(r *rand.Rand, n int) string {
	var b strings.Builder
	b.Grow(n + 16)
	for b.Len() < n {
		f := r.Float64()
		b.WriteString(v.words[int(f*f*float64(len(v.words)))])
		b.WriteByte(' ')
	}
	return b.String()[:n]
}

func hs2Strs(prefix string, n int, r *rand.Rand) []interface{} {
	out := make([]interface{}, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s%07d", prefix, r.Intn(10000000))
	}
	return out
}

func hs2Floats(n int, r *rand.Rand) []interface{} {
	out := make([]interface{}, n)
	for i := range out {
		out[i] = r.Float64()
	}
	return out
}

type hsClient struct {
	h     http.Handler
	token string
	db    string
}

type hsResp struct {
	Commit string            `json:"commit"`
	Errors []json.RawMessage `json:"errors"`
}

func (c *hsClient) do(method, path string, body interface{}) (int, []byte, time.Duration, error) {
	var raw []byte
	if body != nil {
		var err error
		raw, err = json.Marshal(body)
		if err != nil {
			return 0, nil, 0, err
		}
	}
	req := httptest.NewRequest(method, path, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", c.token)
	recorder := httptest.NewRecorder()
	started := time.Now()
	c.h.ServeHTTP(recorder, req)
	return recorder.Code, recorder.Body.Bytes(), time.Since(started), nil
}

func hsStmts(query string, params map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{"statements": []map[string]interface{}{{"statement": query, "parameters": params}}}
}

func (c *hsClient) check(code int, body []byte) (*hsResp, error) {
	var response hsResp
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("status %d decode: %v: %.300s", code, err, body)
	}
	if code >= 300 || len(response.Errors) > 0 {
		return &response, fmt.Errorf("status %d errors: %.500s", code, body)
	}
	return &response, nil
}
