package cache

import "testing"

func TestBasicCacheSetAndGet(t *testing.T) {
	c := CreateCacheAndPebble(2)
	defer c.Close()

	key := []byte("foo")
	value := []byte("bar")

	// Write to cache
	ok, err := c.Set(key, value, true)
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
		_, err := c.Set(keys[i], values[i], true)
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
		_, err := c.Set(keys[i], values[i], true)
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
