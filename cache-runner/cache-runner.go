package main

import (
	"github.com/rohannunu/pebble-cs598rap/cache"
)

func main() {
	cache := cache.CreateCacheAndPebble(10)
	defer cache.Close()

	// Example usage
	key := []byte("exampleKey")
	value := []byte("exampleValue")

	success, err := cache.Set(key, value, true, false)
	if err != nil {
		panic(err)
	}
	if success {
		println("Value set in cache")
	} else {
		println("Value set in database")
	}

	retrievedValue, found, err := cache.Get(key, false)

	if err != nil {
		panic(err)
	}
	if found {
		println("Value retrieved from cache:", string(retrievedValue))
	} else {
		println("Value not found")
	}
}
