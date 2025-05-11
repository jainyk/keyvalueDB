package partition

import (
	"crypto/md5"
	"encoding/binary"
	"sort"
	"sync"
)

// ConsistentHash implements consistent hashing for distributing keys
type ConsistentHash struct {
	replicas     int               // Number of virtual nodes per physical node
	circle       map[uint32]string // Hash ring
	sortedHashes []uint32          // Sorted list of hashes for faster lookup
	nodes        map[string]bool   // Set of nodes
	mutex        sync.RWMutex
}

// NewConsistentHash creates a new consistent hash partitioner
func NewConsistentHash(replicas int) *ConsistentHash {
	return &ConsistentHash{
		replicas: replicas,
		circle:   make(map[uint32]string),
		nodes:    make(map[string]bool),
	}
}

// Add adds a node to the hash ring
func (c *ConsistentHash) Add(node string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if _, exists := c.nodes[node]; exists {
		return
	}

	c.nodes[node] = true

	// Add virtual nodes
	for i := 0; i < c.replicas; i++ {
		hash := c.hashKey(node + string(i))
		c.circle[hash] = node
	}

	c.updateSortedHashes()
}

// Remove removes a node from the hash ring
func (c *ConsistentHash) Remove(node string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if _, exists := c.nodes[node]; !exists {
		return
	}

	delete(c.nodes, node)

	// Remove virtual nodes
	for i := 0; i < c.replicas; i++ {
		hash := c.hashKey(node + string(i))
		delete(c.circle, hash)
	}

	c.updateSortedHashes()
}

// Get returns the node responsible for the given key
func (c *ConsistentHash) Get(key string) string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if len(c.circle) == 0 {
		return ""
	}

	hash := c.hashKey(key)

	// Find the first point on the circle that is >= hash
	idx := sort.Search(len(c.sortedHashes), func(i int) bool {
		return c.sortedHashes[i] >= hash
	})

	// Wrap around to the first node if we go past the end
	if idx == len(c.sortedHashes) {
		idx = 0
	}

	return c.circle[c.sortedHashes[idx]]
}

// GetN returns N nodes responsible for the given key, for replication
func (c *ConsistentHash) GetN(key string, n int) []string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if len(c.nodes) == 0 {
		return []string{}
	}

	if n > len(c.nodes) {
		n = len(c.nodes)
	}

	hash := c.hashKey(key)

	// Find the first point on the circle that is >= hash
	idx := sort.Search(len(c.sortedHashes), func(i int) bool {
		return c.sortedHashes[i] >= hash
	})

	// Wrap around to the first node if we go past the end
	if idx == len(c.sortedHashes) {
		idx = 0
	}

	result := make([]string, 0, n)
	seen := make(map[string]bool)

	for len(result) < n && len(seen) < len(c.nodes) {
		node := c.circle[c.sortedHashes[idx]]
		if !seen[node] {
			seen[node] = true
			result = append(result, node)
		}

		idx = (idx + 1) % len(c.sortedHashes)
	}

	return result
}

// GetNodes returns all nodes in the hash ring
func (c *ConsistentHash) GetNodes() []string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	nodes := make([]string, 0, len(c.nodes))
	for node := range c.nodes {
		nodes = append(nodes, node)
	}

	return nodes
}

// hashKey creates a hash from a key
func (c *ConsistentHash) hashKey(key string) uint32 {
	hash := md5.Sum([]byte(key))
	return binary.BigEndian.Uint32(hash[:4])
}

// updateSortedHashes updates the sorted list of hashes
func (c *ConsistentHash) updateSortedHashes() {
	c.sortedHashes = make([]uint32, 0, len(c.circle))
	for k := range c.circle {
		c.sortedHashes = append(c.sortedHashes, k)
	}
	sort.Slice(c.sortedHashes, func(i, j int) bool {
		return c.sortedHashes[i] < c.sortedHashes[j]
	})
}
