//go:build !cancellationprobe

package testutil

const probeBuilt = false

const probeInterval = 0

func probeArm(int, func()) {}

func probeMost() int { return 0 }
