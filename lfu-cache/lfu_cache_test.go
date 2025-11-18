package lfucache

import "testing"

func TestLFUCache_SetGetAndEvictLeastFrequent(t *testing.T) {
	lfu := NewLFUCache(2)
	defer lfu.cache.Close()

	if _, err := lfu.Set([]byte("key1"), []byte("value1"), true); err != nil {
		t.Fatalf("Set key1 failed: %v", err)
	}
	if _, err := lfu.Set([]byte("key2"), []byte("value2"), true); err != nil {
		t.Fatalf("Set key2 failed: %v", err)
	}

	// Touch key1 so it becomes more frequent than key2.
	if _, found, err := lfu.Get([]byte("key1")); err != nil || !found {
		t.Fatalf("Get key1 failed, found=%v err=%v", found, err)
	}

	// Adding key3 should evict key2 (least frequent).
	if _, err := lfu.Set([]byte("key3"), []byte("value3"), true); err != nil {
		t.Fatalf("Set key3 failed: %v", err)
	}

	found, err := lfu.Exists([]byte("key2"))
	if err != nil {
		t.Fatalf("Exists key2 failed: %v", err)
	}
	if found {
		t.Fatalf("Expected key2 to be evicted")
	}

	found, err = lfu.Exists([]byte("key1"))
	if err != nil {
		t.Fatalf("Exists key1 failed: %v", err)
	}
	if !found {
		t.Fatalf("Expected key1 to remain in cache")
	}

	found, err = lfu.Exists([]byte("key3"))
	if err != nil {
		t.Fatalf("Exists key3 failed: %v", err)
	}
	if !found {
		t.Fatalf("Expected key3 to be cached")
	}

	// key2 should still be retrievable from Pebble and reinserted.
	val, ok, err := lfu.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("Get key2 failed: %v", err)
	}
	if !ok || string(val) != "value2" {
		t.Fatalf("Unexpected key2 fetch: %v %s", ok, val)
	}
}

func TestLFUCache_EvictOldestOnFrequencyTie(t *testing.T) {
	lfu := NewLFUCache(2)
	defer lfu.cache.Close()

	if _, err := lfu.Set([]byte("k1"), []byte("value1"), true); err != nil {
		t.Fatalf("Set k1 failed: %v", err)
	}
	if _, err := lfu.Set([]byte("k2"), []byte("value2"), true); err != nil {
		t.Fatalf("Set k2 failed: %v", err)
	}

	// Both frequencies equal; inserting k3 should evict the least recently used (k1).
	if _, err := lfu.Set([]byte("k3"), []byte("value3"), true); err != nil {
		t.Fatalf("Set k3 failed: %v", err)
	}

	found, err := lfu.Exists([]byte("k1"))
	if err != nil {
		t.Fatalf("Exists k1 failed: %v", err)
	}
	if found {
		t.Fatalf("Expected k1 to be evicted on tie")
	}

	found, err = lfu.Exists([]byte("k2"))
	if err != nil {
		t.Fatalf("Exists k2 failed: %v", err)
	}
	if !found {
		t.Fatalf("Expected k2 to remain cached")
	}

	found, err = lfu.Exists([]byte("k3"))
	if err != nil {
		t.Fatalf("Exists k3 failed: %v", err)
	}
	if !found {
		t.Fatalf("Expected k3 to be cached")
	}
}

func TestLFUCache_PromoteAfterMiss(t *testing.T) {
	lfu := NewLFUCache(1)
	defer lfu.cache.Close()

	if _, err := lfu.Set([]byte("a"), []byte("valueA"), true); err != nil {
		t.Fatalf("Set a failed: %v", err)
	}
	if _, err := lfu.Set([]byte("b"), []byte("valueB"), true); err != nil {
		t.Fatalf("Set b failed: %v", err)
	}

	// Only "b" should remain.
	found, err := lfu.Exists([]byte("a"))
	if err != nil {
		t.Fatalf("Exists a failed: %v", err)
	}
	if found {
		t.Fatalf("Expected a to be evicted")
	}

	// Fetching "a" should bring it back and evict "b".
	val, ok, err := lfu.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get a failed: %v", err)
	}
	if !ok || string(val) != "valueA" {
		t.Fatalf("Unexpected Get result for a: %v, %s", ok, val)
	}

	found, err = lfu.Exists([]byte("b"))
	if err != nil {
		t.Fatalf("Exists b failed: %v", err)
	}
	if found {
		t.Fatalf("Expected b to be evicted after promoting a")
	}
}

func TestLFUCache_StatsTracking(t *testing.T) {
	lfu := NewLFUCache(1)
	defer lfu.cache.Close()

	if _, err := lfu.Set([]byte("x"), []byte("valueX"), true); err != nil {
		t.Fatalf("Set x failed: %v", err)
	}

	// Hit.
	if _, ok, err := lfu.Get([]byte("x")); err != nil || !ok {
		t.Fatalf("Get x failed: %v %v", err, ok)
	}

	// Miss on unknown key.
	if _, ok, err := lfu.Get([]byte("unknown")); err != nil {
		t.Fatalf("Get unknown failed: %v", err)
	} else if ok {
		t.Fatalf("Expected unknown key to miss")
	}

	// Insert y to evict x.
	if _, err := lfu.Set([]byte("y"), []byte("valueY"), true); err != nil {
		t.Fatalf("Set y failed: %v", err)
	}

	// Access x again -> miss + promotion.
	if _, ok, err := lfu.Get([]byte("x")); err != nil || !ok {
		t.Fatalf("Get x after eviction failed: %v %v", err, ok)
	}

	stats := lfu.Stats()
	if stats.Hits != 1 {
		t.Fatalf("expected 1 hit, got %d", stats.Hits)
	}
	if stats.Misses != 2 {
		t.Fatalf("expected 2 misses, got %d", stats.Misses)
	}
	if stats.Promotions != 1 {
		t.Fatalf("expected 1 promotion, got %d", stats.Promotions)
	}
	if stats.Evictions != 1 {
		t.Fatalf("expected 1 eviction, got %d", stats.Evictions)
	}
}
