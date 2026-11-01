package cypher

// Cancellable in-place stable sorting.
//
// The standard library's stable sort (sort.SliceStable) is an insertion sort of
// small blocks followed by symmetric merges with no auxiliary row buffer,
// but it cannot be interrupted. The materialized ORDER BY owner needs a stable sort
// that (a) observes its caller's cancellation at a bounded number of
// comparisons and moves and (b) keeps no second buffer of keyed entries, so
// ordering n rows adds no n-sized scratch beyond the keys the statement already
// charges.
//
// This file adapts the standard library's algorithm to those needs. Adapted
// from the Go standard library, src/sort/zsortfunc.go (stable_func,
// symMerge_func, rotate_func, swapRange_func and insertionSort_func):
//
// Copyright 2022 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the repository root NOTICES.md (Go stable-sort adaptation).
//
// Changes from the library: the sorter works through comparison, move and
// caller-check callbacks over caller-owned entries instead of a reflect
// swapper, and every comparison and move is charged against a fixed budget.
// When the budget runs out the sorter asks the caller whether to continue, and
// stops the sort as soon as the caller has stopped (stable reports false).
// The symmetric-merge algorithm, its bounds and the order of equal entries are
// unchanged.

// inPlaceStableSort stably sorts caller-owned entries through indexing
// callbacks, observing its caller every pipelineMaterializedCancelCheck
// comparisons or moves. It uses no auxiliary row buffer; symmetric merging
// has a logarithmic recursion stack.
type inPlaceStableSort struct {
	less  func(left, right int) bool
	swap  func(left, right int)
	check func() bool // false once the caller has stopped; the check records why
	// budget is the comparisons and moves left before the next caller
	// observation; stopped reports that the caller stopped the sort.
	budget  int
	stopped bool
}

// stable sorts n entries and reports false when the caller stopped it. A sort
// that returns false may have permuted entries part way; the caller owns the
// entries and decides what a stopped sort leaves visible.
func (s *inPlaceStableSort) stable(n int) bool {
	const blockSize = 20 // must be > 0
	a, b := 0, blockSize
	for b <= n {
		s.insertionSort(a, b)
		if s.stopped {
			return false
		}
		a, b = b, b+blockSize
	}
	s.insertionSort(a, n)
	for size := blockSize; size < n && !s.stopped; size *= 2 {
		a, b := 0, 2*size
		for b <= n && !s.stopped {
			s.symMerge(a, a+size, b)
			a, b = b, b+2*size
		}
		if middle := a + size; middle < n && !s.stopped {
			s.symMerge(a, middle, n)
		}
	}
	return !s.stopped
}

// step charges one comparison or move against the budget and asks the caller
// once the budget is spent: false when the caller has stopped the sort.
func (s *inPlaceStableSort) step() bool {
	if s.stopped {
		return false
	}
	s.budget--
	if s.budget > 0 {
		return true
	}
	s.budget = pipelineMaterializedCancelCheck
	if !s.check() {
		s.stopped = true
		return false
	}
	return true
}

// compare reports whether entry left sorts before entry right, and false when
// the caller stopped the sort.
func (s *inPlaceStableSort) compare(left, right int) bool {
	return s.step() && s.less(left, right)
}

// move swaps entries from and to, and reports false when the caller stopped
// the sort before the swap.
func (s *inPlaceStableSort) move(from, to int) bool {
	if !s.step() {
		return false
	}
	s.swap(from, to)
	return true
}

func (s *inPlaceStableSort) insertionSort(a, b int) {
	for i := a + 1; i < b && !s.stopped; i++ {
		for j := i; j > a && s.compare(j, j-1); j-- {
			if !s.move(j, j-1) {
				return
			}
		}
	}
}

// symMerge merges the two sorted subsequences data[a:m] and data[m:b] using
// the SymMerge algorithm from Pok-Son Kim and Arne Kutzner, "Stable Minimum
// Storage Merging by Symmetric Comparisons", in Susanne Albers and Tomasz
// Radzik, editors, Algorithms - ESA 2004, volume 3221 of Lecture Notes in
// Computer Science, pages 714-723. Springer, 2004.
//
// Let M = m-a and N = b-m. Wolog M < N.
// The recursion depth is bound by ceil(log(N+M)).
// The algorithm needs O(M*log(N/M + 1)) calls to less.
// The algorithm needs O((M+N)*log(M)) calls to swap.
//
// symMerge assumes non-degenerate arguments: a < m && m < b.
// Having the caller check this condition eliminates many leaf recursion calls,
// which improves performance.
func (s *inPlaceStableSort) symMerge(a, m, b int) {
	if s.stopped {
		return
	}
	// Avoid unnecessary recursions of symMerge
	// by direct insertion of entry a into data[m:b]
	// if data[a:m] only contains one element.
	if m-a == 1 {
		// Use binary search to find the lowest index i
		// such that data[i] >= data[a] for m <= i < b.
		// Exit the search loop with i == b in case no such index exists.
		i := m
		j := b
		for i < j {
			h := int(uint(i+j) >> 1)
			if s.compare(h, a) {
				i = h + 1
			} else {
				j = h
			}
			if s.stopped {
				return
			}
		}
		// Swap entries until data[a] reaches the position before i.
		for k := a; k < i-1; k++ {
			if !s.move(k, k+1) {
				return
			}
		}
		return
	}

	// Avoid unnecessary recursions of symMerge
	// by direct insertion of entry m into data[a:m]
	// if data[m:b] only contains one element.
	if b-m == 1 {
		// Use binary search to find the lowest index i
		// such that data[i] > data[m] for a <= i < m.
		// Exit the search loop with i == m in case no such index exists.
		i := a
		j := m
		for i < j {
			h := int(uint(i+j) >> 1)
			if !s.compare(m, h) {
				i = h + 1
			} else {
				j = h
			}
			if s.stopped {
				return
			}
		}
		// Swap entries until data[m] reaches the position i.
		for k := m; k > i; k-- {
			if !s.move(k, k-1) {
				return
			}
		}
		return
	}

	mid := int(uint(a+b) >> 1)
	n := mid + m
	var start, r int
	if m > mid {
		start = n - b
		r = mid
	} else {
		start = a
		r = m
	}
	p := n - 1

	for start < r {
		c := int(uint(start+r) >> 1)
		if !s.compare(p-c, c) {
			start = c + 1
		} else {
			r = c
		}
		if s.stopped {
			return
		}
	}

	end := n - start
	if start < m && m < end {
		if !s.rotate(start, m, end) {
			return
		}
	}
	if a < start && start < mid {
		s.symMerge(a, start, mid)
	}
	if mid < end && end < b {
		s.symMerge(mid, end, b)
	}
}

// rotate rotates two consecutive blocks u = data[a:m] and v = data[m:b] in
// data: data of the form 'x u v y' is changed to 'x v u y'. It performs at most
// b-a swaps, and it assumes non-degenerate arguments: a < m && m < b.
func (s *inPlaceStableSort) rotate(a, m, b int) bool {
	i := m - a
	j := b - m

	for i != j {
		if i > j {
			if !s.swapRange(m-i, m, j) {
				return false
			}
			i -= j
		} else {
			if !s.swapRange(m-i, m+j-i, i) {
				return false
			}
			j -= i
		}
	}
	// i == j
	return s.swapRange(m-i, m, i)
}

func (s *inPlaceStableSort) swapRange(a, b, n int) bool {
	for k := 0; k < n; k++ {
		if !s.move(a+k, b+k) {
			return false
		}
	}
	return true
}
