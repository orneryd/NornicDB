package gpu

import "time"

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

// Clustering is a computed cluster generation that is not yet published.
// Callers that derive state from its membership (for example per-cluster
// graphs) build that state first, then publish both together with
// PublishClustering, so readers never see routing without its derived state.
//
// Membership is kept by node ID, so a caller can reconcile embeddings added
// or removed after the snapshot (AssignToClustering, Remove) into the
// generation and the state it derives before publishing.
type Clustering struct {
	centroids  [][]float32
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
	return c.membership.ids(clusterID)
}

// ClusterOf returns the cluster id belongs to in this generation.
func (c *Clustering) ClusterOf(id string) (int, bool) {
	return c.membership.clusterOf(id)
}

// Remove drops id from the generation and returns the cluster it was in.
func (c *Clustering) Remove(id string) (int, bool) {
	return c.membership.remove(id)
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

// AssignToClustering places id in the unpublished generation at the centroid
// nearest its current embedding in the index, moving it if it was already a
// member. It returns the previous cluster (if any) and the new one, and
// ok=false when the index has no embedding for id.
func (ci *ClusterIndex) AssignToClustering(c *Clustering, id string) (previous int, wasMember bool, cluster int, ok bool) {
	ci.mu.RLock()
	cluster, ok = ci.nearestCentroidForIDLocked(id, c.centroids)
	ci.mu.RUnlock()
	if !ok {
		return 0, false, 0, false
	}
	previous, wasMember = c.membership.set(id, cluster)
	return previous, wasMember, cluster, true
}

// AssignLive places id in the published clustering at the centroid nearest
// its current embedding, moving it if it was already a member. It returns
// the previous cluster (if any) and the new one; ok=false when the index is
// not clustered or has no embedding for id.
func (ci *ClusterIndex) AssignLive(id string) (previous int, wasMember bool, cluster int, ok bool) {
	ci.clusterMu.Lock()
	defer ci.clusterMu.Unlock()
	if !ci.clustered {
		return 0, false, 0, false
	}
	ci.mu.RLock()
	cluster, ok = ci.nearestCentroidForIDLocked(id, ci.centroids)
	ci.mu.RUnlock()
	if !ok {
		return 0, false, 0, false
	}
	previous, wasMember = ci.membership.set(id, cluster)
	return previous, wasMember, cluster, true
}

// Unassign removes id from the published clustering's membership and
// returns the cluster it was in. The embedding itself stays (see Remove).
func (ci *ClusterIndex) Unassign(id string) (int, bool) {
	ci.clusterMu.Lock()
	defer ci.clusterMu.Unlock()
	return ci.membership.remove(id)
}

// ClusterOf returns the published cluster of id.
func (ci *ClusterIndex) ClusterOf(id string) (int, bool) {
	ci.clusterMu.RLock()
	defer ci.clusterMu.RUnlock()
	return ci.membership.clusterOf(id)
}

// Remove deletes id's embedding and its published membership together.
func (ci *ClusterIndex) Remove(id string) bool {
	ci.clusterMu.Lock()
	ci.membership.remove(id)
	ci.clusterMu.Unlock()
	return ci.EmbeddingIndex.Remove(id)
}

// NodeIDs returns a copy of the IDs of every embedding in the index.
func (ci *ClusterIndex) NodeIDs() []string {
	ci.mu.RLock()
	defer ci.mu.RUnlock()
	return append([]string(nil), ci.nodeIDs...)
}
