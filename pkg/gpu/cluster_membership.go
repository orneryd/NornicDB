package gpu

import (
	"sync"
	"time"
)

// clusterMembership is cluster membership keyed by node ID. Keying by ID
// (not by embedding position) keeps membership correct when the embedding
// index swap-deletes a removed entry.
type clusterMembership struct {
	idToCluster map[string]int
	members     map[int][]string
	pos         map[string]int // index of an id within members[its cluster]
}

func newClusterMembership(capacity int) clusterMembership {
	return clusterMembership{
		idToCluster: make(map[string]int, capacity),
		members:     make(map[int][]string),
		pos:         make(map[string]int, capacity),
	}
}

// set places id in cluster, moving it if it was a member elsewhere.
func (m *clusterMembership) set(id string, cluster int) (int, bool) {
	previous, wasMember := m.remove(id)
	m.idToCluster[id] = cluster
	m.pos[id] = len(m.members[cluster])
	m.members[cluster] = append(m.members[cluster], id)
	return previous, wasMember
}

// remove drops id and returns the cluster it was in.
func (m *clusterMembership) remove(id string) (int, bool) {
	cluster, ok := m.idToCluster[id]
	if !ok {
		return 0, false
	}
	members := m.members[cluster]
	i, last := m.pos[id], len(members)-1
	if i != last {
		members[i] = members[last]
		m.pos[members[i]] = i
	}
	m.members[cluster] = members[:last]
	delete(m.idToCluster, id)
	delete(m.pos, id)
	return cluster, true
}

func (m *clusterMembership) clusterOf(id string) (int, bool) {
	cluster, ok := m.idToCluster[id]
	return cluster, ok
}

func (m *clusterMembership) ids(cluster int) []string {
	return append([]string(nil), m.members[cluster]...)
}

// Clustering is one cluster generation's routing: centroids and membership
// by node ID. It is built unpublished (ComputeClustering, AssignedClustering),
// kept current with AssignToClustering and Remove, and published with
// PublishClustering; the index then routes through it until the next
// publication. A caller holding a Clustering keeps routing on it after a
// newer one is published: publication replaces the index's pointer, never
// the Clustering it held.
//
// Membership is kept by node ID, so a caller can reconcile embeddings added
// or removed after the snapshot into the generation and the state it
// derives before publishing.
type Clustering struct {
	centroids  [][]float32
	mu         sync.RWMutex // guards membership; innermost lock
	membership clusterMembership
	computed   bool // k-means ran; restored state keeps the previous run's stats
	iterations int
	start      time.Time
}

func newClustering(centroids [][]float32, nodeIDs []string, assignments []int) *Clustering {
	c := &Clustering{centroids: centroids, membership: newClusterMembership(len(nodeIDs))}
	for i, id := range nodeIDs {
		c.membership.set(id, assignments[i])
	}
	return c
}

// NumClusters returns the number of clusters (centroids) in the generation.
func (c *Clustering) NumClusters() int {
	return len(c.centroids)
}

// MemberIDs returns the node IDs assigned to clusterID in this generation.
func (c *Clustering) MemberIDs(clusterID int) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.membership.ids(clusterID)
}

// MemberIDsOf returns the node IDs assigned to the given clusters.
func (c *Clustering) MemberIDsOf(clusterIDs []int) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var out []string
	for _, cid := range clusterIDs {
		out = append(out, c.membership.members[cid]...)
	}
	return out
}

// ClusterOf returns the cluster id belongs to in this generation.
func (c *Clustering) ClusterOf(id string) (int, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.membership.clusterOf(id)
}

// Remove drops id from the generation and returns the cluster it was in.
func (c *Clustering) Remove(id string) (int, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.membership.remove(id)
}

// NearestClusters returns the k populated clusters whose centroids are
// nearest embedding, nearest first.
func (c *Clustering) NearestClusters(embedding []float32, k int) []int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return nearestPopulatedClusters(embedding, c.centroids, c.membership.members, k)
}

// nearestPopulatedClusters is the one nearest-clusters rule (index and
// generation routing): the k populated clusters nearest embedding. Duplicate
// inputs can leave centroids empty, and routing to those clusters would
// return no candidates.
func nearestPopulatedClusters(embedding []float32, centroids [][]float32, members map[int][]string, k int) []int {
	if len(centroids) == 0 {
		return nil
	}
	type distIdx struct {
		dist float64
		idx  int
	}
	distances := make([]distIdx, 0, len(centroids))
	for c, centroid := range centroids {
		if len(members[c]) == 0 {
			continue
		}
		distances = append(distances, distIdx{dist: squaredEuclidean(embedding, centroid), idx: c})
	}
	if k > len(distances) {
		k = len(distances)
	}
	if k <= 0 {
		return nil
	}
	// Partial sort to find k nearest
	for i := 0; i < k; i++ {
		minIdx := i
		for j := i + 1; j < len(distances); j++ {
			if distances[j].dist < distances[minIdx].dist {
				minIdx = j
			}
		}
		distances[i], distances[minIdx] = distances[minIdx], distances[i]
	}
	result := make([]int, k)
	for i := 0; i < k; i++ {
		result[i] = distances[i].idx
	}
	return result
}

// nearestCentroidForIDLocked finds the centroid nearest id's current
// embedding; callers hold ci.mu.
func (ci *ClusterIndex) nearestCentroidForIDLocked(id string, centroids [][]float32) (int, bool) {
	idx, ok := ci.idToIndex[id]
	if !ok || len(centroids) == 0 || (idx+1)*ci.dimensions > len(ci.cpuVectors) {
		return 0, false
	}
	return nearestCentroidFromList(ci.cpuVectors[idx*ci.dimensions:(idx+1)*ci.dimensions], centroids), true
}

// AssignToClustering places id in the generation c, published or not, at the
// centroid nearest its current embedding in the index, moving it if it was
// already a member. It returns the previous cluster (if any) and the new one,
// and ok=false when the index has no embedding for id.
func (ci *ClusterIndex) AssignToClustering(c *Clustering, id string) (previous int, wasMember bool, cluster int, ok bool) {
	ci.mu.RLock()
	cluster, ok = ci.nearestCentroidForIDLocked(id, c.centroids)
	ci.mu.RUnlock()
	if !ok {
		return 0, false, 0, false
	}
	c.mu.Lock()
	previous, wasMember = c.membership.set(id, cluster)
	c.mu.Unlock()
	return previous, wasMember, cluster, true
}

// PublishedClustering returns the generation the index routes through, or
// nil when it is not clustered.
func (ci *ClusterIndex) PublishedClustering() *Clustering {
	ci.clusterMu.RLock()
	defer ci.clusterMu.RUnlock()
	return ci.routing
}

// ClusterOf returns the published cluster of id.
func (ci *ClusterIndex) ClusterOf(id string) (int, bool) {
	if c := ci.PublishedClustering(); c != nil {
		return c.ClusterOf(id)
	}
	return 0, false
}

// Remove deletes id's embedding and its published membership together.
func (ci *ClusterIndex) Remove(id string) bool {
	if c := ci.PublishedClustering(); c != nil {
		c.Remove(id)
	}
	return ci.EmbeddingIndex.Remove(id)
}

// NodeIDs returns a copy of the IDs of every embedding in the index.
func (ci *ClusterIndex) NodeIDs() []string {
	ci.mu.RLock()
	defer ci.mu.RUnlock()
	return append([]string(nil), ci.nodeIDs...)
}
