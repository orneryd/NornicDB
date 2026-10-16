package antlr

import (
	"strings"
	"testing"
)

// Replay samples at page boundaries and inside a long operation, rather than
// spending an input-sized amount of work on every possible sample position.
func replayRuntimeCancellation(t *testing.T, run func(func())) {
	t.Helper()
	calls := 0
	run(func() { calls++ })
	if calls == 0 {
		t.Fatal("fixture did not enter a work-aware operation")
	}
	for _, stop := range []int{1, 2, 63, 64, 65, 127, 128, calls / 2, calls - 1, calls} {
		if stop <= 0 || stop > calls {
			continue
		}
		count := 0
		expectWorkStop(t, func() {
			run(func() {
				count++
				if count == stop {
					panic(cancellationTestStop{})
				}
			})
		})
	}
}

func TestCancellationUnicodeAndTextReaders(t *testing.T) {
	text := strings.Repeat("α😀x\n\r\t", 1024)
	plain := NewInputStream(text)
	for _, check := range []func(){nil, func() {}} {
		stream := NewInputStreamWithWork(text, check)
		if stream.String() != text || stream.GetText(2, 400) != plain.GetText(2, 400) {
			t.Fatal("Unicode text parity changed")
		}
		for i := 0; i < 20; i++ {
			if stream.LA(1) != int(plain.data[i]) {
				t.Fatal("lookahead changed")
			}
			stream.Consume()
		}
		if stream.GetText(stream.Size()+1, stream.Size()+2) != "" {
			t.Fatal("out of range text changed")
		}
	}
	replayRuntimeCancellation(t, func(check func()) { NewInputStreamWithWork(text, check) })
	replayRuntimeCancellation(t, func(check func()) {
		stream := NewInputStreamWithWork(text, nil)
		stream.characters = &workSequence[rune]{}
		for _, r := range text {
			stream.characters.Append(r, nil)
		}
		stream.cancellationCheck = check
		_ = stream.String()
	})
	for _, value := range []string{"", "plain", strings.Repeat("x", 16384), text} {
		if concatTextWithWork(func() {}, "prefix", value, "suffix") != "prefix"+value+"suffix" {
			t.Fatal("concatenation changed")
		}
		if escapeTextWithWork(value, func() {}) != escapeTextWithWork(value, nil) {
			t.Fatal("escaping changed")
		}
	}
	if ConcatTextWithWork(nil, "a", "b") != "ab" {
		t.Fatal("nil concatenation changed")
	}
	replayRuntimeCancellation(t, func(check func()) { concatTextWithWork(check, text, text) })
	replayRuntimeCancellation(t, func(check func()) { escapeTextWithWork(text, check) })
}

func TestCancellationAltDictionaryAndContextMergeTails(t *testing.T) {
	key := strings.Repeat("key", 4096)
	dictionary := NewAltDict()
	dictionary.put(key, 1)
	dictionary.put(key, 2)
	if dictionary.Get(key) != 2 || dictionary.Get("absent") != nil || len(dictionary.values()) != 1 {
		t.Fatal("dictionary update/parity changed")
	}
	replayRuntimeCancellation(t, func(check func()) { dictionary.getWithWork(key, check) })
	replayRuntimeCancellation(t, func(check func()) { copy := NewAltDict(); copy.putWithWork(key, 1, check) })
	for _, pair := range [][2]string{{"a", "aa"}, {"a", "b"}, {key, key}, {key, key[:len(key)-1] + "x"}} {
		operation := altStringOperation(func() {})
		if operation.Equals(pair[0], pair[1], func() {}) != (pair[0] == pair[1]) {
			t.Fatal("dictionary key equality changed")
		}
	}
	parents := make([]*PredictionContext, 256)
	states := make([]int, 256)
	for i := range states {
		parents[i] = BasePredictionContextEMPTY
		states[i] = i + 2
	}
	array := NewArrayPredictionContext(parents, states)
	small := NewArrayPredictionContext([]*PredictionContext{BasePredictionContextEMPTY}, []int{1})
	for _, pair := range [][2]*PredictionContext{{array, small}, {small, array}} {
		healthy := mergeArrays(pair[0], pair[1], false, nil)
		if healthy.length() != 257 {
			t.Fatal("merge tail lost states")
		}
		replayRuntimeCancellation(t, func(check func()) {
			merged := mergeArraysWithWork(pair[0], pair[1], false, NewJPCMap(MergeCacheCollection, "test"), check)
			if !merged.EqualsWithWork(healthy, check) {
				t.Fatal("checked merge changed values")
			}
		})
	}
}
