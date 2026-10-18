package cypher

import "sync"

// rewriteFreePass names a front-end rewrite pass whose "nothing to rewrite"
// outcome rewriteFreeTexts remembers.
type rewriteFreePass uint8

const (
	rewriteFreeFunctionAliases rewriteFreePass = iota + 1
	rewriteFreeCypher25Expressions
	rewriteFreeQueryStructure
)

// rewriteFreeTexts remembers the statement texts a front-end pass (GQL
// function aliases, Cypher 25 expressions, Cypher 25 composition) leaves
// unchanged, so a repeated statement skips their scans; most statements use
// none of these forms. Each outcome depends on the text alone, and the
// language version, which is part of the key (#907: the passes made a cached
// statement 21-24% slower).
var rewriteFreeTexts [boundedCacheShardCount]struct {
	mu    sync.RWMutex
	texts pipelineStringMap[uint8]
}

func rewriteFree(pass rewriteFreePass, cypher25 bool, text string) bool {
	return rewriteFreeWithCancellation(pass, cypher25, text, nil)
}

func markRewriteFree(pass rewriteFreePass, cypher25 bool, text string) {
	markRewriteFreeWithCancellation(pass, cypher25, text, nil)
}

func rewriteFreeBit(pass rewriteFreePass, cypher25 bool) uint8 {
	shift := 2 * (pass - 1)
	if cypher25 {
		shift++
	}
	return 1 << shift
}

func rewriteFreeWithCancellation(pass rewriteFreePass, cypher25 bool, text string, probe *pipelineCancellation) bool {
	shardIndex := boundedCacheStringShardIndexWithCancellation(text, probe)
	if probe != nil && probe.err != nil {
		return false
	}
	shard := &rewriteFreeTexts[shardIndex]
	bits, found := getQueryTextCache(&shard.texts, &shard.mu, text, probe)
	return found && bits&rewriteFreeBit(pass, cypher25) != 0
}

func markRewriteFreeWithCancellation(pass rewriteFreePass, cypher25 bool, text string, probe *pipelineCancellation) {
	shardIndex := boundedCacheStringShardIndexWithCancellation(text, probe)
	if probe != nil && probe.err != nil {
		return
	}
	shard := &rewriteFreeTexts[shardIndex]
	if !lockQueryTextCache(&shard.mu, false, probe) {
		return
	}
	defer shard.mu.Unlock()
	bits, _ := shard.texts.GetWithCancellation(text, probe)
	if probe != nil && probe.err != nil {
		return
	}
	putQueryTextCache(&shard.texts, 4096/boundedCacheShardCount, text, bits|rewriteFreeBit(pass, cypher25), probe)
}
