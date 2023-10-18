package consistenthash

import (
	"errors"
	"sort"
	"strconv"
	"sync"
)

// Default constants
const (
	topWeight   = 100
	minReplicas = 100
)

type (
	// Type definition of a hash function
	Func func(data []byte) uint64

	// Struct of Consistent Hash
	ConsistentHash struct {
		hashFunc Func
		replicas int
		keys     []uint64
		ring     map[uint64]string
		nodes    map[string]*Server
		lock     sync.RWMutex
	}
)

// Initialize a ConsistentHash with custom replica count and hash function
func getNewCustomConsistentHash(replicas int, hashFn Func) *ConsistentHash {
	if replicas < minReplicas {
		replicas = minReplicas
	}

	if hashFn == nil {
		hashFn = Hash
	}

	return &ConsistentHash{
		hashFunc: hashFn,
		replicas: replicas,
		ring:     make(map[uint64]string),
		nodes:    make(map[string]*Server),
	}
}

// Initialize a default Consistent Hash
func GetNewConsistentHash() *ConsistentHash {
	return getNewCustomConsistentHash(minReplicas, Hash)
}

// Add a server with weight i.e. decide replicas based on performance
func (ch *ConsistentHash) AddServerWithWeight(id string, weight int) {
	replicas := ch.replicas * weight / topWeight
	ch.AddServerWithReplicas(id, replicas)
}

// Add a server with given replica count
func (ch *ConsistentHash) AddServerWithReplicas(id string, replicas int) (error) {
	ch.lock.Lock()
	defer ch.lock.Unlock()

	ch.nodes[id] = getNewServer(id)

	for i := 0; i < replicas; i++ {
		hash := ch.hashFunc([]byte(id + strconv.Itoa(i)))
		ch.keys = append(ch.keys, hash)
		if val, ok := ch.ring[hash]; !ok || val == "" {
			ch.ring[hash] = id
		} else {
			return errors.New("Hash collision")
		}

		// Handle data migration
	}

	sort.Slice(ch.keys, func(i, j int) bool {
		return ch.keys[i] < ch.keys[j]
	})

	return nil
}

// Remove a given server
func (ch *ConsistentHash) RemoveServer(id string) {
	ch.lock.Lock()
	defer ch.lock.Unlock()

	ch.nodes[id] = nil

	for i := 0; i < ch.replicas; i++ {
		hash := ch.hashFunc([]byte(id + strconv.Itoa(i)))
		ch.ring[hash] = ""
		index := sort.Search(len(ch.keys), func (i int) bool {
			return ch.keys[i] == hash
		})
		ch.keys = append(ch.keys[:index], ch.keys[index+1:]...)

		// Handle data migration
	}
}

// Add a key value pair to the Consistent Hash
func (ch *ConsistentHash) AddKey(key, value string) error {
	ch.lock.Lock()
	defer ch.lock.Unlock()

	hash := ch.hashFunc([]byte(key))
	index := sort.Search(len(ch.keys), func (i int) bool {
		return ch.keys[i] >= hash
	}) % len(ch.keys)

	id := ch.ring[ch.keys[index]]

	err := ch.nodes[id].put(key, value)
	if err != nil {
		return err
	}
	return nil
}

// Get the value of a given key from the Consistent Hash
func (ch *ConsistentHash) GetKey(key string) (string, error) {
	ch.lock.RLock()
	defer ch.lock.RUnlock()

	hash := ch.hashFunc([]byte(key))
	index := sort.Search(len(ch.keys), func (i int) bool {
		return ch.keys[i] >= hash
	}) % len(ch.keys)

	id := ch.ring[ch.keys[index]]

	value, err := ch.nodes[id].get(key)
	if err != nil {
		return "", err
	}
	return value, nil
}