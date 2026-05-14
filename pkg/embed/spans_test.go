package embed

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// stubEmbedder records each method call and returns canned results so
// the TracedEmbedder wrapper can be exercised without spinning up a
// real model. Each helper records its inputs onto a tape we can
// assert on after the wrapped call returns.
type stubEmbedder struct {
	embedCalls       int
	embedBatchCalls  int
	chunkTextCalls   int
	lastText         string
	lastTexts        []string
	lastChunkText    string
	lastChunkMax     int
	lastChunkOverlap int
	embedErr         error
	embedBatchErr    error
	chunkErr         error
	embedResult      []float32
	embedBatchResult [][]float32
	chunkResult      []string
}

func (s *stubEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	s.embedCalls++
	s.lastText = text
	return s.embedResult, s.embedErr
}

func (s *stubEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	s.embedBatchCalls++
	s.lastTexts = texts
	return s.embedBatchResult, s.embedBatchErr
}

func (s *stubEmbedder) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	s.chunkTextCalls++
	s.lastChunkText = text
	s.lastChunkMax = maxTokens
	s.lastChunkOverlap = overlap
	return s.chunkResult, s.chunkErr
}

func (s *stubEmbedder) Dimensions() int { return 4 }
func (s *stubEmbedder) Model() string   { return "test-model" }
func (s *stubEmbedder) Backend() string { return "cpu" }

func TestTracedEmbedder_EmbedDelegatesAndPropagatesResult(t *testing.T) {
	stub := &stubEmbedder{embedResult: []float32{0.1, 0.2, 0.3, 0.4}}
	traced := NewTracedEmbedder(stub)

	got, err := traced.Embed(context.Background(), "hello world")
	require.NoError(t, err)
	require.Equal(t, []float32{0.1, 0.2, 0.3, 0.4}, got)
	require.Equal(t, 1, stub.embedCalls)
	require.Equal(t, "hello world", stub.lastText)
}

func TestTracedEmbedder_EmbedRecordsErrorOnSpan(t *testing.T) {
	stub := &stubEmbedder{embedErr: errors.New("provider down")}
	traced := NewTracedEmbedder(stub)

	got, err := traced.Embed(context.Background(), "x")
	require.Error(t, err)
	require.EqualError(t, err, "provider down")
	require.Nil(t, got)
}

func TestTracedEmbedder_EmbedBatchDelegates(t *testing.T) {
	stub := &stubEmbedder{
		embedBatchResult: [][]float32{
			{0.1, 0.2, 0.3, 0.4},
			{0.5, 0.6, 0.7, 0.8},
		},
	}
	traced := NewTracedEmbedder(stub)

	got, err := traced.EmbedBatch(context.Background(), []string{"a", "b"})
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, 1, stub.embedBatchCalls)
	require.Equal(t, []string{"a", "b"}, stub.lastTexts)
}

func TestTracedEmbedder_EmbedBatchPropagatesError(t *testing.T) {
	stub := &stubEmbedder{embedBatchErr: errors.New("batch failed")}
	traced := NewTracedEmbedder(stub)

	got, err := traced.EmbedBatch(context.Background(), []string{"x"})
	require.Error(t, err)
	require.Nil(t, got)
}

func TestTracedEmbedder_ChunkTextDelegatesUntraced(t *testing.T) {
	stub := &stubEmbedder{chunkResult: []string{"chunk-1", "chunk-2"}}
	traced := NewTracedEmbedder(stub)

	got, err := traced.ChunkText("hello world", 100, 20)
	require.NoError(t, err)
	require.Equal(t, []string{"chunk-1", "chunk-2"}, got)
	require.Equal(t, 1, stub.chunkTextCalls)
	require.Equal(t, "hello world", stub.lastChunkText)
	require.Equal(t, 100, stub.lastChunkMax)
	require.Equal(t, 20, stub.lastChunkOverlap)
}

func TestTracedEmbedder_PassthroughGetters(t *testing.T) {
	traced := NewTracedEmbedder(&stubEmbedder{})
	require.Equal(t, 4, traced.Dimensions())
	require.Equal(t, "test-model", traced.Model())
	require.Equal(t, "cpu", traced.Backend())
}
