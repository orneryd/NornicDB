package cypher

// Set membership and insertion positions share the encoded-string map owner.
type pipelineStringSet struct {
	probe *pipelineCancellation
	data  pipelineStringMap[int]
	next  int
}

func (set *pipelineStringSet) add(key string) bool { _, exists := set.position(key); return exists }
func (set *pipelineStringSet) contains(key string) bool {
	set.data.probe = set.probe
	_, exists := set.data.Get(key)
	return exists
}
func (set *pipelineStringSet) position(key string) (int, bool) {
	set.data.probe = set.probe
	position, exists := set.data.Get(key)
	if exists || set.probe != nil && set.probe.err != nil {
		return position, exists
	}
	position = set.next
	if set.data.Set(key, position) {
		set.next++
	}
	return position, false
}
