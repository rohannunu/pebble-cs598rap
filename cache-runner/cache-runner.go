package main

import (
	"example.com/pebble-app/cache"
)

func main() {
	cache := cache.CreateCacheAndPebble(1024 * 1024 * 100)
	defer cache.Close()

	// Example usage
	key := []byte("exampleKey")
	value := []byte("exampleValue")

	success, err := cache.Set(key, value, true)
	if err != nil {
		panic(err)
	}
	if success {
		println("Value set in cache")
	} else {
		println("Value set in database")
	}

	retrievedValue, found, err := cache.Get(key)

	if err != nil {
		panic(err)
	}
	if found {
		println("Value retrieved from cache:", string(retrievedValue))
	} else {
		println("Value not found")
	}
}
