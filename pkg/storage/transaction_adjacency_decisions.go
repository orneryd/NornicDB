package storage

import (
	"context"
	"encoding/binary"
	"hash/maphash"
	"slices"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// observeEmptyAdjacencyLocked retains the exact empty range a current mutation
// read observed. Own its type slice: a Go caller may reuse the request after the
// read returns. Repeated reads of the same range need only one observation.
func (tx *BadgerTransaction) observeEmptyAdjacencyLocked(ctx context.Context, request AdjacentEdgeRead) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if tx.entityLocks.absenceIndex == nil && len(tx.entityLocks.absent) < 8 {
		for _, previous := range tx.entityLocks.absent {
			if sameAdjacentDecisionRange(previous, request) {
				return nil
			}
		}
	}
	var fingerprint uint64
	collision := false
	if tx.entityLocks.absenceIndex != nil || len(tx.entityLocks.absent) >= 8 {
		if tx.entityLocks.absenceIndex == nil {
			index := make(map[uint64]int, 16)
			var collisions map[uint64][]int
			for at, previous := range tx.entityLocks.absent {
				key, err := adjacencyDecisionFingerprint(ctx, previous)
				if err != nil {
					return err
				}
				if _, exists := index[key]; exists {
					if collisions == nil {
						collisions = make(map[uint64][]int)
					}
					collisions[key] = append(collisions[key], at)
				} else {
					index[key] = at
				}
			}
			tx.entityLocks.absenceIndex = index
			tx.entityLocks.absenceCollisions = collisions
		}
		var err error
		fingerprint, err = adjacencyDecisionFingerprint(ctx, request)
		if err != nil {
			return err
		}
		if at, exists := tx.entityLocks.absenceIndex[fingerprint]; exists {
			if sameAdjacentDecisionRange(tx.entityLocks.absent[at], request) {
				return nil
			}
			for _, at := range tx.entityLocks.absenceCollisions[fingerprint] {
				if sameAdjacentDecisionRange(tx.entityLocks.absent[at], request) {
					return nil
				}
			}
			collision = true
		}
	}
	if len(tx.entityLocks.absent) != 0 && slices.Equal(tx.entityLocks.absent[0].Types, request.Types) {
		request.Types = tx.entityLocks.absent[0].Types
	} else {
		request.Types = slices.Clone(request.Types)
	}
	request.Headers = true
	tx.entityLocks.absent = appendDoubled(tx.entityLocks.absent, request)
	if tx.entityLocks.absenceIndex != nil {
		at := len(tx.entityLocks.absent) - 1
		if collision {
			if tx.entityLocks.absenceCollisions == nil {
				tx.entityLocks.absenceCollisions = make(map[uint64][]int)
			}
			tx.entityLocks.absenceCollisions[fingerprint] = append(tx.entityLocks.absenceCollisions[fingerprint], at)
		} else {
			tx.entityLocks.absenceIndex[fingerprint] = at
		}
	}
	return nil
}

func sameAdjacentDecisionRange(a, b AdjacentEdgeRead) bool {
	return a.Node == b.Node && a.Other == b.Other && a.Direction == b.Direction && slices.Equal(a.Types, b.Types)
}

// Fingerprints index candidate ranges only. Exact comparisons above retain
// correctness even when hashes collide; the sparse collision lists allocate
// only for an actual collision. Seeded standard-library hashing avoids a full
// string tuple allocation on each recorded decision.
var adjacencyDecisionHashSeed = maphash.MakeSeed()

func adjacencyDecisionFingerprint(ctx context.Context, request AdjacentEdgeRead) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	var h maphash.Hash
	h.SetSeed(adjacencyDecisionHashSeed)
	h.WriteByte(byte(request.Direction))
	var length [8]byte
	binary.LittleEndian.PutUint64(length[:], uint64(len(request.Types)))
	h.Write(length[:])
	for _, part := range []string{string(request.Node), string(request.Other)} {
		binary.LittleEndian.PutUint64(length[:], uint64(len(part)))
		h.Write(length[:])
		h.WriteString(part)
	}
	for _, part := range request.Types {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		binary.LittleEndian.PutUint64(length[:], uint64(len(part)))
		h.Write(length[:])
		h.WriteString(part)
	}
	return h.Sum64(), nil
}

// ownsAdjacencyDecisionLocked distinguishes an explicit write of the range's
// anchor from a relationship create that merely references it. The latter must
// not acquire a blanket endpoint exclusion or turn read-committed into serializable.
func (tx *BadgerTransaction) ownsAdjacencyDecisionLocked(node NodeID) bool {
	return tx.entityLocks.find(entityWriteLockKey(bufferedNodeEntity, string(node))) != nil ||
		tx.ownsEntityLocked(bufferedNodeEntity, string(node))
}

// validateEmptyAdjacencyLocked rechecks only write-owned empty ranges. Lock
// acquisition calls it after waits, and the commit owner calls it again while
// holding the canonical identity authority, before durable acceptance. A changed
// decision is a real optimistic conflict, not a fabricated deadlock.
func (tx *BadgerTransaction) validateEmptyAdjacencyLocked(ctx context.Context) error {
	for _, request := range tx.entityLocks.absent {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !tx.ownsAdjacencyDecisionLocked(request.Node) {
			continue
		}
		edges, err := tx.engine.ReadAdjacentEdges(ctx, request)
		if err != nil {
			return err
		}
		if len(edges) != 0 {
			return localizedError(localization.StorageTransactionAdjacentEdgeChanged(string(request.Node), string(edges[0].ID)), ErrConflict)
		}
	}
	return nil
}
