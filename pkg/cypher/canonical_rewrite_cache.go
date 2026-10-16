package cypher

// Cache slots hold complete immutable rewrites. Query-owned probes are only
// borrowed while hashing/comparing; they are never retained in the shared memo.
func (memo *canonicalRewriteMemo) loadWithCancellation(query string, probe *pipelineCancellation) *queryRewrite {
	if probe == nil {
		if cached := memo.slot(query).Load(); cached != nil && cached.original == query {
			return cached
		}
		return nil
	}
	hash := hashPipelineString(query, memo.seed, probe)
	if probe.err != nil {
		return nil
	}
	cached := memo.slots[hash%uint64(len(memo.slots))].Load()
	if cached != nil && comparePipelineStrings(cached.original, query, probe) == 0 && probe.err == nil {
		return cached
	}
	return nil
}

func (memo *canonicalRewriteMemo) storeWithCancellation(query string, rewrite *queryRewrite, probe *pipelineCancellation) {
	if probe == nil {
		memo.slot(query).Store(rewrite)
		return
	}
	hash := hashPipelineString(query, memo.seed, probe)
	if probe.err != nil {
		return
	}
	memo.slots[hash%uint64(len(memo.slots))].Store(rewrite)
}
