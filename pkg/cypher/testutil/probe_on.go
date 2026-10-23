//go:build cancellationprobe

package testutil

import "github.com/orneryd/nornicdb/pkg/cypher/bulk"

// probeBuilt reports the cancellation probe build: the sweep cancels a call
// in the middle of a pass and bounds the owner's checks after it.
const probeBuilt = true

const probeInterval = bulk.ProbeInterval

func probeArm(after int, fire func()) { bulk.ProbeArm(after, fire) }

func probeMost() int { return bulk.ProbeMost() }
