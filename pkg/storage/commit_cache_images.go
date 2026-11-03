package storage

// commitCacheImages retains the canonical frame until its physical writer
// publishes cache bodies. The common single-namespace frame is inline; other
// namespaces keep their own immutable pairs, without another last-writer fold.
type commitCacheImages struct {
	first commitStates
	rest  []commitStates
}

func (images *commitCacheImages) acceptStates(states commitStates) {
	if images.first.nodes == nil && images.first.edges == nil {
		images.first = states
		return
	}
	images.rest = append(images.rest, states)
}

func (cw *commitWriter) publishCacheImages() {
	publish := func(states commitStates) {
		for _, pair := range states.nodes {
			if pair.after != nil {
				cw.engine.cacheStoreWrittenNode(pair.after, cw.durableVersion)
			}
		}
		for _, pair := range states.edges {
			if pair.after != nil {
				cw.engine.cacheStoreEdge(pair.after, cw.durableVersion)
			}
		}
	}
	publish(cw.cacheImages.first)
	for _, states := range cw.cacheImages.rest {
		publish(states)
	}
}
