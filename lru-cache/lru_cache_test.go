package lrucache

import (
	"testing"
)

func TestLRUCache_SetGet(t *testing.T) {
	lru := NewLRUCache(2)
	defer lru.cache.Close()

	// Set key-value pairs
	_, err := lru.Set([]byte("key1"), []byte("value1"), true)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	_, err = lru.Set([]byte("key2"), []byte("value2"), true)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Get existing keys
	value, found, err := lru.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found || string(value) != "value1" {
		t.Fatalf("Expected value1, got %s", value)
	}

	value, found, err = lru.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found || string(value) != "value2" {
		t.Fatalf("Expected value2, got %s", value)
	}
	// Add a new key to trigger eviction
	_, err = lru.Set([]byte("key3"), []byte("value3"), true)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// key2 should still be present
	value, found, err = lru.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found || string(value) != "value2" {
		t.Fatalf("Expected value2, got %s", value)
	}

	// key1 should have been evicted (but still in pebble)

	found, err = lru.Exists([]byte("key1"))
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if found {
		t.Fatalf("Expected key1 to be evicted")
	}

	// should be able to get key1 from pebble through the cache
	value, found, err = lru.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found || string(value) != "value1" {
		t.Fatalf("Expected value1 from pebble, got %s", value)
	}

}

func TestLRUCache_GetUpdate(t *testing.T) {
	lru := NewLRUCache(2)
	defer lru.cache.Close()

	// Set key-value pairs
	_, err := lru.Set([]byte("key1"), []byte("value1"), true)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	_, err = lru.Set([]byte("key2"), []byte("value2"), true)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Access key1 to make it most recently used
	_, found, err := lru.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found {
		t.Fatalf("Expected to find key1")
	}

	// Add a new key to trigger eviction
	_, err = lru.Set([]byte("key3"), []byte("value3"), true)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// key1 should still be present
	found, err = lru.Exists([]byte("key1"))
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !found {
		t.Fatalf("Expected key1 to be present")
	}

	// key2 should have been evicted
	found, err = lru.Exists([]byte("key2"))
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if found {
		t.Fatalf("Expected key2 to be evicted")
	}

	// key2 should be retrievable from pebble through the cache
	value, found, err := lru.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found || string(value) != "value2" {
		t.Fatalf("Expected value2 from pebble, got %s", value)
	}
}

func TestLRUCache_EvictAll(t *testing.T) {
	lru := NewLRUCache(2)
	defer lru.cache.Close()

	// Set key-value pairs
	_, err := lru.Set([]byte("key1"), []byte("value1"), true)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	_, err = lru.Set([]byte("key2"), []byte("value2"), true)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// push 2 more in to evict both
	_, err = lru.Set([]byte("key3"), []byte("value3"), true)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	_, err = lru.Set([]byte("key4"), []byte("value4"), true)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// checl the last 2 exist in the LRU cache
	found, err := lru.Exists([]byte("key3"))
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !found {
		t.Fatalf("Expected key3 to be present in LRU cache")
	}

	found, err = lru.Exists([]byte("key4"))
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !found {
		t.Fatalf("Expected key4 to be present in LRU cache")
	}

	// check the first 2 are evicted from the LRU cache
	found, err = lru.Exists([]byte("key1"))
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if found {
		t.Fatalf("Expected key1 to be evicted from LRU cache")
	}

	found, err = lru.Exists([]byte("key2"))
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if found {
		t.Fatalf("Expected key2 to be evicted from LRU cache")
	}
}
