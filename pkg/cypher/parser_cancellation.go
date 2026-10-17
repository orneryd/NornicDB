package cypher

// CheckBoundary samples the existing statement probe at parser admission and
// completion. Runtime iterations use Continue and the same amortized counter.
func (probe *pipelineCancellation) CheckBoundary() bool { return probe.poll() }

func (probe *pipelineCancellation) compareForSort(left, right interface{}) int {
	order := compareValuesForSortWithCancellation(left, right, probe)
	if probe.err != nil {
		panic(pipelineSortCancellation{})
	}
	return order
}
