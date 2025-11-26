package densitycache

import "testing"

// global
var testing_async bool = true

// Helper: check existence in the underlying in-memory cache.
func existsInMemory(dc *DensityCache, key []byte) bool {
	return dc.cache.Exists(key)
}

func TestDensityCache_SetGet(t *testing.T) {
	dc := NewDensityCache(2)
	defer dc.Close()

	// Set key-value pairs
	if _, err := dc.Set([]byte("key1"), []byte("value1"), true, testing_async); err != nil {
		t.Fatalf("Set key1 failed: %v", err)
	}
	if _, err := dc.Set([]byte("key2"), []byte("value2"), true, testing_async); err != nil {
		t.Fatalf("Set key2 failed: %v", err)
	}

	// Get existing keys
	value, found, err := dc.Get([]byte("key1"), testing_async)
	if err != nil {
		t.Fatalf("Get key1 failed: %v", err)
	}
	if !found || string(value) != "value1" {
		t.Fatalf("Expected value1, got %s", value)
	}

	value, found, err = dc.Get([]byte("key2"), testing_async)
	if err != nil {
		t.Fatalf("Get key2 failed: %v", err)
	}
	if !found || string(value) != "value2" {
		t.Fatalf("Expected value2, got %s", value)
	}

	// Add a new key to trigger eviction (capacity=2)
	if _, err := dc.Set([]byte("key3"), []byte("value3"), true, testing_async); err != nil {
		t.Fatalf("Set key3 failed: %v", err)
	}

	// key3 should be present in memory
	if !existsInMemory(dc, []byte("key3")) {
		t.Fatalf("Expected key3 to be present in in-memory cache")
	}

	// Exactly one of key1 or key2 should have been evicted from the in-memory cache
	in1 := existsInMemory(dc, []byte("key1"))
	in2 := existsInMemory(dc, []byte("key2"))

	if in1 == in2 { // both true or both false => something is wrong
		t.Fatalf("Expected exactly one of key1/key2 to be in memory; got in1=%v, in2=%v", in1, in2)
	}

	// Whichever one was evicted should still be retrievable via Pebble through the cache
	var evictedKey, keptKey string
	if !in1 && in2 {
		evictedKey = "key1"
		keptKey = "key2"
	} else {
		evictedKey = "key2"
		keptKey = "key1"
	}

	// Kept key should still be a cache hit or at least found
	value, found, err = dc.Get([]byte(keptKey), testing_async)
	if err != nil {
		t.Fatalf("Get %s failed: %v", keptKey, err)
	}
	if !found {
		t.Fatalf("Expected to find %s", keptKey)
	}

	// Evicted key should be fetched (likely from Pebble) and have the right value
	value, found, err = dc.Get([]byte(evictedKey), testing_async)
	if err != nil {
		t.Fatalf("Get %s failed: %v", evictedKey, err)
	}
	expectedVal := map[string]string{"key1": "value1", "key2": "value2"}[evictedKey]
	if !found || string(value) != expectedVal {
		t.Fatalf("Expected %s from Pebble, got %s", expectedVal, value)
	}
}

func TestDensityCache_GetUpdate(t *testing.T) {
	dc := NewDensityCache(2)
	defer dc.Close()

	// Set key-value pairs
	if _, err := dc.Set([]byte("key1"), []byte("value1"), true, testing_async); err != nil {
		t.Fatalf("Set key1 failed: %v", err)
	}
	if _, err := dc.Set([]byte("key2"), []byte("value2"), true, testing_async); err != nil {
		t.Fatalf("Set key2 failed: %v", err)
	}

	// Access key1 to exercise the "hit" path and update metadata
	if _, found, err := dc.Get([]byte("key1"), testing_async); err != nil {
		t.Fatalf("Get key1 failed: %v", err)
	} else if !found {
		t.Fatalf("Expected to find key1")
	}

	// Update key1's value
	if _, err := dc.Set([]byte("key1"), []byte("value1-updated"), true, testing_async); err != nil {
		t.Fatalf("Update key1 failed: %v", err)
	}

	// Add a new key to trigger eviction
	if _, err := dc.Set([]byte("key3"), []byte("value3"), true, testing_async); err != nil {
		t.Fatalf("Set key3 failed: %v", err)
	}

	// Regardless of which key gets evicted, all keys should be retrievable (from cache or Pebble)

	// key1
	value, found, err := dc.Get([]byte("key1"), testing_async)
	if err != nil {
		t.Fatalf("Get key1 failed: %v", err)
	}
	if !found || string(value) != "value1-updated" {
		t.Fatalf("Expected value1-updated for key1, got %s", value)
	}

	// key2
	value, found, err = dc.Get([]byte("key2"), testing_async)
	if err != nil {
		t.Fatalf("Get key2 failed: %v", err)
	}
	if !found || string(value) != "value2" {
		t.Fatalf("Expected value2 for key2, got %s", value)
	}

	// key3
	value, found, err = dc.Get([]byte("key3"), testing_async)
	if err != nil {
		t.Fatalf("Get key3 failed: %v", err)
	}
	if !found || string(value) != "value3" {
		t.Fatalf("Expected value3 for key3, got %s", value)
	}
}

func TestDensityCache_EvictAll(t *testing.T) {
	dc := NewDensityCache(2)
	defer dc.Close()

	// Insert 4 keys with capacity=2 to force multiple evictions
	if _, err := dc.Set([]byte("key1"), []byte("value1"), true, testing_async); err != nil {
		t.Fatalf("Set key1 failed: %v", err)
	}
	if _, err := dc.Set([]byte("key2"), []byte("value2"), true, testing_async); err != nil {
		t.Fatalf("Set key2 failed: %v", err)
	}
	if _, err := dc.Set([]byte("key3"), []byte("value3"), true, testing_async); err != nil {
		t.Fatalf("Set key3 failed: %v", err)
	}
	if _, err := dc.Set([]byte("key4"), []byte("value4"), true, testing_async); err != nil {
		t.Fatalf("Set key4 failed: %v", err)
	}

	// At most 'capacity' keys should be present in the in-memory cache.
	inMem := 0
	keys := []string{"key1", "key2", "key3", "key4"}
	for _, k := range keys {
		if existsInMemory(dc, []byte(k)) {
			inMem++
		}
	}
	if inMem != 2 {
		t.Fatalf("Expected exactly 2 keys in in-memory cache, got %d", inMem)
	}

	// All keys should still be retrievable via Get (from cache or Pebble)
	expectedValues := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key4": "value4",
	}

	for _, k := range keys {
		val, found, err := dc.Get([]byte(k), testing_async)
		if err != nil {
			t.Fatalf("Get %s failed: %v", k, err)
		}
		if !found || string(val) != expectedValues[k] {
			t.Fatalf("Expected %s for %s, got %s", expectedValues[k], k, val)
		}
	}

	// Entries map should never exceed capacity
	if len(dc.entries) > dc.capacity {
		t.Fatalf("entries size %d exceeds capacity %d", len(dc.entries), dc.capacity)
	}
}
