package search

import (
	"sync"
	"time"
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
