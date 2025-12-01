package lrucache

import (
	"container/list"
	"sync"
	"sync/atomic"

	"github.com/rohannunu/pebble-cs598rap/cache"
)

type LRUStats struct {
	Hits       uint64
	Misses     uint64
	Promotions uint64
	Evictions  uint64
}

type LRUCache struct {
	cache        *cache.Cache
	order        *list.List
	elements     map[string]*list.Element
	capacity     int
	stats        *LRUStats
	LRUMutexLock sync.RWMutex
}

func (s *LRUStats) RecordHit()     { atomic.AddUint64(&s.Hits, 1) }
func (s *LRUStats) RecordMiss()    { atomic.AddUint64(&s.Misses, 1) }
func (s *LRUStats) RecordPromote() { atomic.AddUint64(&s.Promotions, 1) }
func (s *LRUStats) RecordEvict()   { atomic.AddUint64(&s.Evictions, 1) }
func (s *LRUStats) Snapshot() LRUStats {
	return LRUStats{
		Hits:       atomic.LoadUint64(&s.Hits),
		Misses:     atomic.LoadUint64(&s.Misses),
		Promotions: atomic.LoadUint64(&s.Promotions),
		Evictions:  atomic.LoadUint64(&s.Evictions),
	}
}

func (lru *LRUCache) Stats() LRUStats {
	return lru.stats.Snapshot()
}

func NewLRUCache(capacity int) *LRUCache {
	return &LRUCache{
		cache:        cache.CreateCacheAndPebble(capacity),
		order:        list.New(),
		elements:     make(map[string]*list.Element),
		capacity:     capacity,
		stats:        &LRUStats{},
		LRUMutexLock: sync.RWMutex{},
	}
}

func (lru *LRUCache) Exists(key []byte) (bool, error) {
	lru.LRUMutexLock.RLock()
	defer lru.LRUMutexLock.RUnlock()
	return lru.elements[string(key)] != nil, nil
}

func (lru *LRUCache) updateOrder(key string) {
	// Move the accessed element to the front of the order list
	// it is now most recently used. Acquire write lock only for the
	// brief mutation of the list/map.
	lru.LRUMutexLock.Lock()
	if elem, ok := lru.elements[key]; ok {
		lru.order.MoveToFront(elem)
	}
	lru.LRUMutexLock.Unlock()
}

func (lru *LRUCache) Get(key []byte, async bool) ([]byte, bool, error) {
	// Check if the key exists in the cache
	exists, err := lru.Exists(key)
	if err != nil {
		return nil, false, err
	}
	if exists {
		//we have hit in the cache
		lru.stats.RecordHit()
		value, found, err := lru.cache.Get(key)
		if err != nil {
			return nil, false, err
		}
		if found {
			lru.updateOrder(string(key))
			return value, true, nil
		}
	} else {
		//we have a miss :(
		lru.stats.RecordMiss()

		// if its not, try to get it from pebble directly and promote it to the cache
		value, found, err := lru.cache.Get(key)
		if err != nil {
			return nil, false, err
		}
		if found {
			// promote to cache
			lru.stats.RecordPromote()
			_, err := lru.Set(key, value, true, async)
			if err != nil {
				return nil, false, err
			}
			return value, true, nil
		}
	}
	return nil, false, nil
}

func (lru *LRUCache) Set(key []byte, value []byte, to_cache bool, async bool) (bool, error) {
	kstr := string(key)

	// Fast read: is it already present? (use RLock)
	lru.LRUMutexLock.RLock()
	elem, ok := lru.elements[kstr]
	lru.LRUMutexLock.RUnlock()

	if ok {
		// Update underlying cache without holding the LRU lock.
		_, err := lru.cache.Set(key, value, true, async)
		if err != nil {
			return false, err
		}

		// Move to front with brief lock.
		lru.LRUMutexLock.Lock()
		// Verify it's still present to avoid races.
		if cur, still := lru.elements[kstr]; still && cur == elem {
			lru.order.MoveToFront(elem)
		}
		lru.LRUMutexLock.Unlock()
		return true, nil
	}

	// If the cache is at capacity and we're going to add, pick an eviction
	// candidate while holding the lock briefly, then perform I/O (evict)
	// without holding the LRU lock.
	var backElem *list.Element
	var backKey string
	if to_cache && lru.capacity > 0 {
		lru.LRUMutexLock.RLock()
		if lru.order.Len() >= lru.capacity {
			backElem = lru.order.Back()
			if backElem != nil {
				backKey = backElem.Value.(string)
			}
		}
		lru.LRUMutexLock.RUnlock()

		if backKey != "" {
			// Evict from underlying cache outside the LRU lock to avoid blocking
			evicted, err := lru.cache.Evict([]byte(backKey), async)
			if err != nil {
				return false, err
			}
			if evicted {
				// Now remove from the LRU structures only if the element is
				// still the same (no concurrent reorder/insert replaced it).
				lru.LRUMutexLock.Lock()
				if cur, exists := lru.elements[backKey]; exists && cur == backElem {
					delete(lru.elements, backKey)
					lru.order.Remove(backElem)
					lru.stats.RecordEvict()
				}
				lru.LRUMutexLock.Unlock()
			}
		}
	}

	// Add the new key-value pair to the cache and mark it as most recently used.
	// Perform the underlying cache.Set (may do I/O) without holding the LRU lock.
	cached, err := lru.cache.Set(key, value, to_cache, async)
	if err != nil {
		return false, err
	}
	if !cached {
		return false, nil
	}

	lru.LRUMutexLock.Lock()
	elem = lru.order.PushFront(kstr)
	lru.elements[kstr] = elem
	lru.LRUMutexLock.Unlock()
	return true, nil
}
