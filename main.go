package main

import (
	"fmt"

	"github.com/Srinidhi444/Distributed-KV-Store/internal/store"
)

func main() {
	db := store.NewStore()
	db.Set("name", "Srinidhi")
	db.Set("language", "Go")

	value, exists := db.Get("name")
	fmt.Println("NAME:", value, "Exists:", exists)

	db.Set("language", "Rust")

	value, exists = db.Get("language")
	fmt.Println("Language:", value, "Exists:", exists)

	// 4. Delete a key
	db.Delete("name")

	value, exists = db.Get("name")
	fmt.Println("After deletion:", value, "Exists:", exists)
}
