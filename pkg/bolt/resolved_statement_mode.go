package bolt

func (stream *resultStream) resolvedQueryType() string {
	if stream.result != nil {
		return boltSummaryQueryType(stream.isWrite, stream.result.Stats, stream.result.Metadata)
	}
	return boltSummaryQueryType(stream.isWrite, nil)
}

func (stream *resultStream) resolvedWrite() bool {
	switch stream.resolvedQueryType() {
	case "w", "rw", "s":
		return true
	}
	return false
}
