package cypher

import (
	"iter"
	"unicode/utf8"
)

// The cancellation owner's iterators. A pass over a population, a list, a
// map or the text of a value ranges over one of them: each step makes the
// owner's check first (walk.check), so the observation dominates every
// element the pass reaches, whatever the loop body then does with it
// (continue, an empty inner loop, a rejected or duplicate element). A nil
// walk (no statement) never stops.

// observedItems is range list, observing the statement at each element.
func observedItems[T any](walk *materializedRowCheckpoint, list []T) iter.Seq2[int, T] {
	return func(yield func(int, T) bool) {
		for index, item := range list {
			walk.Check()
			if !yield(index, item) {
				return
			}
		}
	}
}

// observedEntries is range entries, observing the statement at each entry.
func observedEntries[K comparable, V any](walk *materializedRowCheckpoint, entries map[K]V) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		for key, value := range entries {
			walk.Check()
			if !yield(key, value) {
				return
			}
		}
	}
}

// observedRunes is range text, observing the statement at each rune.
func observedRunes(walk *materializedRowCheckpoint, text string) iter.Seq2[int, rune] {
	return func(yield func(int, rune) bool) {
		for at := 0; at < len(text); {
			walk.Check()
			r, size := utf8.DecodeRuneInString(text[at:])
			if !yield(at, r) {
				return
			}
			at += size
		}
	}
}

// observedBytes is the byte offsets of text from start, observing the
// statement at each: for a scan that steps through bytes itself.
func observedBytes(walk *materializedRowCheckpoint, text string, start int) iter.Seq[int] {
	return func(yield func(int) bool) {
		for at := start; at < len(text); at++ {
			walk.Check()
			if !yield(at) {
				return
			}
		}
	}
}

// observedCount is 0 .. n-1, observing the statement at each: a pass whose
// length is a data-sized count (range(1, n), a scalar-driven output).
func observedCount(walk *materializedRowCheckpoint, n int) iter.Seq[int] {
	return func(yield func(int) bool) {
		for index := 0; index < n; index++ {
			walk.Check()
			if !yield(index) {
				return
			}
		}
	}
}
