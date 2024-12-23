package consistence_hashing

import (
	"crypto/md5"
	"fmt"
	"sort"
)

type ConsistentHashing struct {
	hashRing     map[int]string
	serverToKeys map[string][]string
	virtualNodes int
}

func NewConsistentHashing(virtualNodes int) *ConsistentHashing {
	return &ConsistentHashing{
		hashRing:     make(map[int]string),
		serverToKeys: make(map[string][]string),
		virtualNodes: virtualNodes,
	}
}

func (ch *ConsistentHashing) hash(key string) int {
	hash := md5.Sum([]byte(key))
	// Use the first 4 bytes as the hash integer
	return int(hash[0]<<4) | (int(hash[1]) << 16) | (int(hash[2]) << 8) | int(hash[3])
}

// Add a server to the hash ring (including virtual nodes)
func (ch *ConsistentHashing) AddServer(server string) {
	ch.serverToKeys[server] = []string{}
	for i := 0; i < ch.virtualNodes; i++ {
		virtualNodeName := fmt.Sprintf("%s_VN_%d", server, i)
		hash := ch.hash(virtualNodeName)
		ch.hashRing[hash] = server
		fmt.Printf("Added virtual node: %s with hash: %d\n", virtualNodeName, hash)
	}

	// Reassign keys impacted by the addition of this server
	fmt.Printf("Reassigning keys for new server %s...\n", server)
	ch.reassignKeysAfterServerAddition(server)
}

// Reassign keys impacted by the addition of a new server
func (ch *ConsistentHashing) reassignKeysAfterServerAddition(newServer string) {
	// Iterate over the virtual nodes of the new server
	for i := 0; i < ch.virtualNodes; i++ {
		virtualNodeName := fmt.Sprintf("%s_VN_%d", newServer, i)
		virtualNodeHash := ch.hash(virtualNodeName)

		// Find the next server in the hash ring
		keys := make([]int, 0, len(ch.hashRing))
		for key := range ch.hashRing {
			keys = append(keys, key)
		}
		sort.Ints(keys)

		var nextServerHash int
		for _, h := range keys {
			if h > virtualNodeHash {
				nextServerHash = h
				break
			}
		}
		if nextServerHash == 0 {
			// Wrap around to the first server if no server exists after this hash
			nextServerHash = keys[0]
		}

		nextServer := ch.hashRing[nextServerHash]

		if nextServer != newServer {
			// Get keys assigned to the next server
			nextServerKeys := ch.serverToKeys[nextServer]

			// Reassign keys now belonging to the new server
			for i := 0; i < len(nextServerKeys); i++ {
				key := nextServerKeys[i]
				keyHash := ch.hash(key)

				// Check if the key now belongs to the new server
				if keyHash <= virtualNodeHash || (nextServerHash != 0 && virtualNodeHash < nextServerHash) {
					ch.serverToKeys[newServer] = append(ch.serverToKeys[newServer], key)
					nextServerKeys = append(nextServerKeys[:i], nextServerKeys[i+1:]...)
					fmt.Printf("Key %s reassigned from %s to %s\n", key, nextServer, newServer)
				}
			}
			ch.serverToKeys[newServer] = nextServerKeys
		}
	}
}

// Remove a server from the hash ring
func (ch *ConsistentHashing) RemoveServer(server string) {
	// Remove virtual nodes for the server
	for i := 0; i < ch.virtualNodes; i++ {
		virtualNodeName := fmt.Sprintf("%s_VN_%d", server, i)
		hash := ch.hash(virtualNodeName)
		delete(ch.hashRing, hash)
		fmt.Printf("Removed virtual node: %s with hash: %d\n", virtualNodeName, hash)
	}

	// Reassign keys impacted by the removal of this server
	fmt.Printf("Reassigning keys from %s...\n", server)
	ch.reassignKeysAfterServerRemoval(server)
}

// Reassign keys impacted by the removal of a server
func (ch *ConsistentHashing) reassignKeysAfterServerRemoval(server string) {
	// Retrieve keys mapped to the removed server
	keysToReassign := ch.serverToKeys[server]
	if len(keysToReassign) == 0 {
		fmt.Printf("No keys were assigned to %s\n", server)
		return
	}
	delete(ch.hashRing, ch.hash(server))

	// Reassign each key to the new responsible server
	for _, key := range keysToReassign {
		newServer := ch.GetServer(key)                                       // Find the new server for the key
		ch.serverToKeys[newServer] = append(ch.serverToKeys[newServer], key) // Assign key to the new server
		fmt.Printf("Key %s reassigned from %s to %s\n", key, server, newServer)
	}
}

// Get the server responsible for a key
func (ch *ConsistentHashing) GetServer(key string) string {
	hash := ch.hash(key)

	// Find the closest server using the sorted keys in hashRing
	keys := make([]int, 0, len(ch.hashRing))
	for key := range ch.hashRing {
		keys = append(keys, key)
	}
	sort.Ints(keys)

	// Find the server responsible for the key by ceiling
	for _, h := range keys {
		if h >= hash {
			return ch.hashRing[h]
		}
	}

	// Wrap around if no server is found (i.e., return the first server)
	return ch.hashRing[keys[0]]
}

// Assign a key to a server and store it in the server-to-keys map
func (ch *ConsistentHashing) AssignKey(key string) {
	server := ch.GetServer(key)
	ch.serverToKeys[server] = append(ch.serverToKeys[server], key)
	fmt.Printf("Assigned key %s to server %s\n", key, server)
}

// Display all keys stored on each server
func (ch *ConsistentHashing) DisplayServerKeys() {
	for server, keys := range ch.serverToKeys {
		fmt.Printf("Server: %s, Keys: %v\n", server, keys)
	}
}
