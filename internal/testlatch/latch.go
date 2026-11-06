// Package testlatch provides a leaf, dependency-free one-shot latch for test
// gates. It sits below pkg/storage so in-package storage tests can share it.
package testlatch

import "sync"

// Latch is a channel that is closed at most once. Release is idempotent, so a
// test may release explicitly while its cleanup also releases.
type Latch struct {
	ch   chan struct{}
	once sync.Once
}

// New returns an unreleased latch.
func New() *Latch { return &Latch{ch: make(chan struct{})} }

// Release closes the latch. Repeated and concurrent calls are no-ops.
func (l *Latch) Release() { l.once.Do(func() { close(l.ch) }) }

// Done is closed once the latch has been released.
func (l *Latch) Done() <-chan struct{} { return l.ch }

// Wait blocks until the latch is released.
func (l *Latch) Wait() { <-l.ch }
