package cache

import "testing"

// global
var testing_async bool = true

func TestBasicCacheSetAndGet(t *testing.T) {
	c := CreateCacheAndPebble(2)
	defer c.Close()

	key := []byte("foo")
	value := []byte("bar")

	// Write to cache
	ok, err := c.Set(key, value, true, testing_async)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if !ok {
		t.Fatalf("Set returned false, expected true")
	}

	// Read from cache
	got, found, err := c.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found {
		t.Fatalf("Key not found")
	}
	if string(got) != string(value) {
		t.Fatalf("Got %q, want %q", got, value)
	}
}

func TestCacheSetAndGetOverCapacity(t *testing.T) {
	c := CreateCacheAndPebble(2) // small capacity
	defer c.Close()

	// Insert multiple items to exceed capacity
	keys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	values := [][]byte{[]byte("value1"), []byte("value2"), []byte("value3")}

	for i := 0; i < len(keys); i++ {
		_, err := c.Set(keys[i], values[i], true, testing_async)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	// Check if keys are still present (since it should fetch from pebble)
	for i := 0; i < len(keys); i++ {
		got, found, err := c.Get(keys[i])
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if !found {
			t.Fatalf("Expected key%d to be found", i+1)
		}
		if string(got) != string(values[i]) {
			t.Fatalf("Got %q, want %q", got, values[i])
		}
	}
}

func TestCacheSetAndGetOverCapacityWriteToPebble(t *testing.T) {
	c := CreateCacheAndPebble(2) // small capacity
	defer c.Close()

	// Insert multiple items to exceed capacity
	keys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	values := [][]byte{[]byte("value1"), []byte("value2"), []byte("value3")}

	for i := 0; i < len(keys); i++ {
		_, err := c.Set(keys[i], values[i], true, testing_async)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	// check if 2 items are in cache and 1 in pebble
	cacheCount := 0
	pebbleCount := 0
	for i := 0; i < len(keys); i++ {
		if c.Exists(keys[i]) {
			cacheCount++
		} else {
			pebbleCount++
		}
	}

	if cacheCount != 2 {
		t.Fatalf("Expected 2 items in cache, got %d", cacheCount)
	}
	if pebbleCount != 1 {
		t.Fatalf("Expected 1 item in pebble, got %d", pebbleCount)
	}
}

func TestCacheEvictOldest(t *testing.T) {
	c := CreateCacheAndPebble(3)
	defer c.Close()

	keys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	values := [][]byte{[]byte("value1"), []byte("value2"), []byte("value3")}
	for i := 0; i < len(keys); i++ {
		_, err := c.Set(keys[i], values[i], true, testing_async)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	c.Evict(keys[0], testing_async)

	// key1 should be evicted
	if c.Exists(keys[0]) {
		t.Fatalf("Expected key1 to be evicted from cache")
	}

	// key2 and key3 should be present
	if !c.Exists(keys[1]) {
		t.Fatalf("Expected key2 to be in cache")
	}
	if !c.Exists(keys[2]) {
		t.Fatalf("Expected key3 to be in cache")
	}
}

func TestEvictAll(t *testing.T) {
	c := CreateCacheAndPebble(3)
	defer c.Close()

	keys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	values := [][]byte{[]byte("value1"), []byte("value2"), []byte("value3")}
	for i := 0; i < len(keys); i++ {
		_, err := c.Set(keys[i], values[i], true, testing_async)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	c.Evict(keys[0], testing_async)
	c.Evict(keys[1], testing_async)
	c.Evict(keys[2], testing_async)

	// All keys should be evicted
	for i := 0; i < len(keys); i++ {
		if c.Exists(keys[i]) {
			t.Fatalf("Expected key%d to be evicted from cache", i+1)
		}
	}
}

func TestEvictAllAndGet(t *testing.T) {
	c := CreateCacheAndPebble(3)
	defer c.Close()

	keys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	values := [][]byte{[]byte("value1"), []byte("value2"), []byte("value3")}
	for i := 0; i < len(keys); i++ {
		_, err := c.Set(keys[i], values[i], true, testing_async)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	c.Evict(keys[0], testing_async)
	c.Evict(keys[1], testing_async)
	c.Evict(keys[2], testing_async)

	// Prefetch all keys back into cache
	for i := 0; i < len(keys); i++ {
		_, found, err := c.Get(keys[i])
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if !found {
			t.Fatalf("Expected key%d to be found after prefetch", i+1)
		}
	}
}

func TestEvictAllAndPrefetch(t *testing.T) {
	c := CreateCacheAndPebble(3)
	defer c.Close()

	keys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	values := [][]byte{[]byte("value1"), []byte("value2"), []byte("value3")}
	for i := 0; i < len(keys); i++ {
		_, err := c.Set(keys[i], values[i], true, testing_async)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	c.Evict(keys[0], testing_async)
	c.Evict(keys[1], testing_async)
	c.Evict(keys[2], testing_async)

	// Prefetch all keys back into cache
	arr := make([][]byte, len(keys))
	for i := 0; i < len(keys); i++ {
		arr[i] = keys[i]
	}

	_, err := c.Prefetch(arr)
	if err != nil {
		t.Fatalf("Prefetch failed: %v", err)
	}

	// All keys should be back in cache
	for i := 0; i < len(keys); i++ {
		if !c.Exists(keys[i]) {
			t.Fatalf("Expected key%d to be in cache after prefetch", i+1)
		}
	}
}

func TestAddManyAndPrefetch(t *testing.T) {
	c := CreateCacheAndPebble(5)
	defer c.Close()

	keys := [][]byte{
		[]byte("key1"), []byte("key2"), []byte("key3"),
		[]byte("key4"), []byte("key5"), []byte("key6"),
	}
	values := [][]byte{
		[]byte("value1"), []byte("value2"), []byte("value3"),
		[]byte("value4"), []byte("value5"), []byte("value6"),
	}
	for i := 0; i < len(keys); i++ {
		_, err := c.Set(keys[i], values[i], true, testing_async)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	// last key should have been written to pebble
	if c.Exists(keys[5]) {
		t.Fatalf("Expected key6 to be in pebble, not cache")
	}

	// evict the first key
	c.Evict(keys[0], testing_async)

	// prefetch keys 6
	prefetchKeys := [][]byte{keys[5]}
	_, err := c.Prefetch(prefetchKeys)
	if err != nil {
		t.Fatalf("Prefetch failed: %v", err)
	}

	// 6 should be in cache
	if !c.Exists(keys[5]) {
		t.Fatalf("Expected key6 to be in cache after prefetch")
	}
}

func TestRemainingCapacity(t *testing.T) {
	c := CreateCacheAndPebble(3)
	defer c.Close()

	if c.RemainingCapacity() != 3 {
		t.Fatalf("Expected remaining capacity 3, got %d", c.RemainingCapacity())
	}
	keys := [][]byte{[]byte("key1"), []byte("key2")}
	values := [][]byte{[]byte("value1"), []byte("value2")}
	for i := 0; i < len(keys); i++ {
		_, err := c.Set(keys[i], values[i], true, testing_async)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	if c.RemainingCapacity() != 1 {
		t.Fatalf("Expected remaining capacity 1, got %d", c.RemainingCapacity())
	}

	c.Evict(keys[0], testing_async)

	if c.RemainingCapacity() != 2 {
		t.Fatalf("Expected remaining capacity 2 after eviction, got %d", c.RemainingCapacity())
	}
}
