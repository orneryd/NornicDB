package search

import (
	"math"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/math/vector"
)

const (
	ivfpqBundleFormatVersion = 3 // 3: vector-entry keys (vectorEntryKeyFormat)
)

// IVFPQProfile is the concrete runtime profile used to build/query compressed ANN.
type IVFPQProfile struct {
	Dimensions          int
	IVFLists            int
	PQSegments          int
	PQBits              int
	NProbe              int
	RerankTopK          int
	TrainingSampleMax   int
	KMeansMaxIterations int
	OverflowMax         int
}

// IVFPQBuildStats captures build observability for acceptance gates.
type IVFPQBuildStats struct {
	VectorCount         int
	TrainingSampleCount int
	ListCount           int
	AvgListSize         float64
	MaxListSize         int
	BytesPerVector      float64
	BuildDuration       time.Duration
}

type ivfpqCodebook struct {
	SubDim   int
	Codeword [][]float32
}

type ivfpqList struct {
	IDs      []string
	CodeSize int
	Codes    []byte
}

type ivfpqOverflowVector struct {
	ID     string
	Vector []float32
}

func (l *ivfpqList) appendCode(code []byte) {
	if l == nil || len(code) == 0 {
		return
	}
	if l.CodeSize <= 0 {
		l.CodeSize = len(code)
	}
	l.Codes = append(l.Codes, code...)
}

func (l *ivfpqList) codeAt(idx int) ([]byte, bool) {
	if l == nil || l.CodeSize <= 0 || idx < 0 {
		return nil, false
	}
	start := idx * l.CodeSize
	end := start + l.CodeSize
	if start < 0 || end > len(l.Codes) {
		return nil, false
	}
	return l.Codes[start:end], true
}

// IVFPQIndex stores a compressed IVF/PQ ANN structure.
type IVFPQIndex struct {
	profile         IVFPQProfile
	centroids       [][]float32
	centroidNorm    [][]float32
	codebooks       []ivfpqCodebook
	lists           []ivfpqList
	overflow        []ivfpqOverflowVector
	formatVersion   int
	builtAtUnixNano int64
	scratchPool     sync.Pool
	// maxResidual is the largest error |r - PQ(r)| of an encoded residual,
	// so an exact cosine exceeds a list member's PQ score by at most it
	// (scoreBound); residualKnown is false for a bundle saved without it.
	maxResidual   float64
	residualKnown bool
	// maxCoordinateError is the maximum absolute reconstruction error in
	// each coordinate. Its projection bounds queries more tightly when the
	// largest residual errors lie outside the query direction.
	maxCoordinateError []float64
}

// scoreBound is how far a vector's exact cosine with a unit query can lie
// above the score SearchApprox gives it: a list member's score is
// q·(c + PQ(r)) for the stored unit vector c + r, so the difference is
// q·(r - PQ(r)) ≤ |r - PQ(r)| ≤ maxResidual. Overflow vectors are scored
// exactly. +Inf when the bundle didn't record it. Float32 rounding is the
// caller's slack.
func (i *IVFPQIndex) scoreBound() float64 {
	if i == nil || !i.residualKnown {
		return math.Inf(1)
	}
	return i.maxResidual
}

// queryScoreBound projects the reconstruction-error box onto the unit
// query: |q·(r-PQ(r))| <= sum_d |q_d| max_x |error(x,d)|. It uses the
// tighter of this and the L2 bound. Older bundles retain their L2 bound;
// bundles predating both bounds remain unbounded.
func (i *IVFPQIndex) queryScoreBound(query []float32) float64 {
	bound := i.scoreBound()
	if i == nil || !i.residualKnown || len(i.maxCoordinateError) == 0 {
		return bound
	}
	q := vector.Normalize(query)
	projected := 0.0
	for d, coordinateError := range i.maxCoordinateError {
		projected += math.Abs(float64(q[d])) * coordinateError
	}
	return min(bound, projected)
}

type ivfpqScratch struct {
	lut      [][]float32
	heapData []Candidate
}

func (i *IVFPQIndex) Profile() IVFPQProfile {
	if i == nil {
		return IVFPQProfile{}
	}
	return i.profile
}

func (i *IVFPQIndex) Count() int {
	if i == nil {
		return 0
	}
	total := 0
	for idx := range i.lists {
		total += len(i.lists[idx].IDs)
	}
	return total
}

func (i *IVFPQIndex) compatibleProfile(want IVFPQProfile) bool {
	if i == nil {
		return false
	}
	have := i.profile
	return have.Dimensions == want.Dimensions &&
		have.IVFLists == want.IVFLists &&
		have.PQSegments == want.PQSegments &&
		have.PQBits == want.PQBits &&
		have.TrainingSampleMax == want.TrainingSampleMax &&
		have.KMeansMaxIterations == want.KMeansMaxIterations &&
		have.OverflowMax == want.OverflowMax
}

// listCount is the number of inverted lists; probing that many searches all.
func (i *IVFPQIndex) listCount() int {
	if i == nil {
		return 0
	}
	return len(i.lists)
}

// population is every vector SearchApprox can return: the lists' vectors
// and the overflow vectors it merges into every search.
func (i *IVFPQIndex) population() int {
	if i == nil {
		return 0
	}
	return i.Count() + len(i.overflow)
}
