package cypher

// rewriteFreePass names a front-end rewrite pass whose "nothing to rewrite"
// outcome rewriteFreeTexts remembers.
type rewriteFreePass uint8

const (
	rewriteFreeFunctionAliases rewriteFreePass = iota + 1
	rewriteFreeCypher25Expressions
	rewriteFreeQueryStructure
)

type rewriteFreeKey struct {
	pass     rewriteFreePass
	cypher25 bool
	text     string
}

// rewriteFreeTexts remembers the statement texts a front-end pass (GQL
// function aliases, Cypher 25 expressions, Cypher 25 composition) leaves
// unchanged, so a repeated statement skips their scans; most statements use
// none of these forms. Each outcome depends on the text alone, and the
// language version, which is part of the key (#907: the passes made a cached
// statement 21-24% slower).
var rewriteFreeTexts = newBoundedCache[rewriteFreeKey, struct{}](4096, func(k rewriteFreeKey) int {
	return boundedCacheStringShardIndex(k.text)
})

func rewriteFree(pass rewriteFreePass, cypher25 bool, text string) bool {
	_, ok := rewriteFreeTexts.get(rewriteFreeKey{pass: pass, cypher25: cypher25, text: text})
	return ok
}

func markRewriteFree(pass rewriteFreePass, cypher25 bool, text string) {
	rewriteFreeTexts.put(rewriteFreeKey{pass: pass, cypher25: cypher25, text: text}, struct{}{})
}
