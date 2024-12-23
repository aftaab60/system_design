package main

import (
	"fmt"
	"github.com/aftaab60/system_design/GO/consistence_hashing"
)

func main() {
	// Initialize consistent hashing with 3 virtual nodes per server
	ch := consistence_hashing.NewConsistentHashing(3)

	// Add servers
	ch.AddServer("Server1")
	ch.AddServer("Server2")
	ch.AddServer("Server3")

	// Assign keys to servers
	keys := []string{"Key1", "Key2", "Key3", "Key4", "Key5"}
	for _, key := range keys {
		ch.AssignKey(key)
	}

	// Display keys stored in each server
	fmt.Println("\nInitial Key Distribution:")
	ch.DisplayServerKeys()

	// Add a new server
	fmt.Println("\nAdding Server4...\n")
	ch.AddServer("Server4")

	// Display updated keys stored in each server
	fmt.Println("\nUpdated Key Distribution:")
	ch.DisplayServerKeys()

	// Remove a server
	fmt.Println("\nRemoving Server2...\n")
	ch.RemoveServer("Server2")

	// Display final keys stored in each server
	fmt.Println("\nFinal Key Distribution:")
	ch.DisplayServerKeys()
}
