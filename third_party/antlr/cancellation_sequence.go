// Copyright (c) 2026 NornicDB contributors.
// Use of this file is governed by the BSD 3-clause license in LICENSE.

package antlr

// workSequence keeps the first small page contiguous. Later growth prepares a
// fixed page and directory link; it never recopies the accumulated prefix.
// The runtime instantiates it with pointers, interfaces and integer values.
type workSequence[T any] struct {
	first  []T
	pages  scalarDirectory[sequencePage[T]]
	length int
}
type sequencePage[T any] struct{ values [64]T }

func sequenceFromSlice[T any](values []T) *workSequence[T] {
	return &workSequence[T]{first: values, length: len(values)}
}

func (sequence *workSequence[T]) Len() int { return sequence.length }

// Only context-free public slice APIs materialize. Prediction uses At/Append.
func (sequence *workSequence[T]) materialize() []T {
	return sequence.materializeWithWork(nil)
}
func (sequence *workSequence[T]) materializeWithWork(check func()) []T {
	chargeWork(check)
	values := make([]T, sequence.length)
	for i := range values {
		values[i] = sequence.At(i, check)
	}
	return values
}
func (sequence *workSequence[T]) At(index int, check func()) T {
	chargeWork(check)
	if index < 0 || index >= sequence.length {
		panic("sequence index out of range")
	}
	if index < len(sequence.first) {
		return sequence.first[index]
	}
	return sequence.pages.get(index/64, check).values[index%64]
}
func (sequence *workSequence[T]) Set(index int, value T, check func()) {
	chargeWork(check)
	if index < 0 || index >= sequence.length {
		panic("sequence index out of range")
	}
	if index < len(sequence.first) {
		sequence.first[index] = value
		return
	}
	page := sequence.pages.get(index/64, check)
	chargeWork(check)
	page.values[index%64] = value
}
func (sequence *workSequence[T]) Append(value T, check func()) {
	chargeWork(check)
	index := sequence.length
	if index < 64 {
		if sequence.first == nil {
			sequence.first = make([]T, 0, 64)
		}
		chargeWork(check)
		sequence.first = append(sequence.first, value)
		sequence.length++
		return
	}
	if index%64 == 0 {
		page := &sequencePage[T]{}
		page.values[0] = value
		sequence.pages.put(index/64, page, check)
		sequence.length++
		return
	}
	page := sequence.pages.get(index/64, check)
	chargeWork(check)
	page.values[index%64] = value
	sequence.length++
}
func (sequence *workSequence[T]) Pop(check func()) T {
	value := sequence.At(sequence.length-1, check)
	var empty T
	sequence.Set(sequence.length-1, empty, check)
	sequence.length--
	if sequence.length < 64 {
		sequence.first = sequence.first[:sequence.length]
	}
	return value
}
