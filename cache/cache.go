package cache

import (
	"log"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
)

// This is general purpose cache that can evict keys, add keys, prefectch keys etc.
// The cache will wrap around pebble to make it easier to implement certain techniques
// The remaining caches and strategies built for pebble should use this as the interface

// Write-back Cache: Keys dont get written into pebble until they are evicted to lower latency cost

// resources

type Statistics struct {
	cache_hits     int
	cache_misses   int
	cache_accesses int
	evictions      int
	prefetches     int
	additions      int
}

func (s *Statistics) CacheHit() {
	s.cache_accesses++
	s.cache_hits++
}

func (s *Statistics) CacheMiss() {
	s.cache_accesses++
	s.cache_misses++
}

func (s *Statistics) CacheEvict() {
	s.evictions++
}

func (s *Statistics) CachePrefetch() {
	s.prefetches++
}

func (s *Statistics) CacheAdd() {
	s.additions++
}

type CacheEntry struct {
	value          []byte
	size           int
	first_inserted time.Time
	last_updated   time.Time
}

type Cache struct {
	MutexLock sync.RWMutex           // in case we do multithreaded workflows
	data      map[string]*CacheEntry // key has to be a string because slices are mutable
	capacity  int

	stats *Statistics
	db    *pebble.DB
}

func makeKey(k []byte) string {
	// turns a []byte in a string to be used as a key in the cache
	return string(append([]byte(nil), k...))
}

func CreateStatistics() *Statistics {
	return &Statistics{
		cache_hits:     0,
		cache_misses:   0,
		cache_accesses: 0,
		evictions:      0,
		prefetches:     0,
	}
}

func CreateCache(db *pebble.DB, cache_capacity int) *Cache {
	// In Go, local variables escape to the heap automatically if their
	// address (or a pointer to them) is returned or stored somewhere.
	statistics := CreateStatistics()

	return &Cache{
		data:     make(map[string]*CacheEntry),
		capacity: cache_capacity,
		db:       db,
		stats:    statistics,
	}
}

func CreateCacheAndPebble(cache_capacity int) *Cache {
	// creates the cache without a pebble instance (starts one itself)
	db, err := pebble.Open("demo", &pebble.Options{})
	if err != nil {
		log.Println(err)
		return nil
	}
	return CreateCache(db, cache_capacity)
}

func (c *Cache) Close() error {
	return c.db.Close()
}

func (c *Cache) Get(key []byte) ([]byte, bool, error) {
	k := makeKey(key)

	c.MutexLock.RLock()

	if e, ok := c.data[k]; ok {
		// if this is a hit on the cache
		copied_val := append([]byte(nil), e.value...)
		c.stats.CacheHit()
		c.MutexLock.RUnlock()
		return copied_val, true, nil
	}

	c.MutexLock.RUnlock()
	c.stats.CacheMiss()

	// gather the data from the database
	value, closer, err := c.db.Get(key)
	if err == pebble.ErrNotFound {
		// not int the DB
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	copied_val := append([]byte(nil), value...)

	// the closer is to avoid extra allocations, so after we copy the data,
	// tell the closer to stop buffering the internal memory
	if closer != nil {
		_ = closer.Close()
	}

	return copied_val, true, nil
}

func (c *Cache) Set(key, value []byte, addToCache bool) (bool, error) {
	// returns bool: true if it was placed in the cache, false if it was placed into the db instead, error
	k := makeKey(key)
	v := append([]byte(nil), value...)

	c.MutexLock.Lock()
	defer c.MutexLock.Unlock()

	if e, ok := c.data[k]; ok {
		// if the data is already in the cache, update it there
		c.stats.CacheHit()
		e.last_updated = time.Now()
		e.size = len(v)
		e.value = v
		return true, nil
	} else {
		c.stats.CacheMiss()
		if addToCache && len(c.data) < c.capacity {
			// if there is enough capacity and the addToCache bool is set, add it into the cache
			en := &CacheEntry{
				value:          value,
				size:           len(v),
				first_inserted: time.Now(),
				last_updated:   time.Now(),
			}
			c.data[k] = en
			return true, nil
		} else {
			// otherwise write to pebble
			if err := c.db.Set(key, value, pebble.Sync); err != nil {
				return false, err
			}
			return false, nil
		}
	}

}

func (c *Cache) Evict(key []byte) (bool, error) {
	// The key parameter gets evicted from the cache, and written into pebble

	// returns bool of if successfully evicted, error
	k := makeKey(key)

	c.MutexLock.Lock()

	e, ok := c.data[k]
	if !ok {
		// this was never in the cache
		c.MutexLock.Unlock()
		return false, nil
	}

	// remove from cache and write it into pebble
	delete(c.data, k)
	c.MutexLock.Unlock()

	// write to pebble (incur latency cost)
	copied_val := append([]byte(nil), e.value...)
	if err := c.db.Set([]byte(k), copied_val, pebble.Sync); err != nil {
		return false, err
	}

	c.stats.CacheEvict()

	return true, nil
}

func (c *Cache) RemainingCapacity() int {
	// good idea to call this before prefetching to see what to evict and how many things we can prefetch
	return c.capacity - len(c.data)
}

func (c *Cache) Prefetch(keys [][]byte) (int, error) {
	// prefetches keys and returns how many were succesfully prefetched and added to the cache

	successes := 0

	seen := make(map[string]struct{}, len(keys))
	stringKeys := make([]string, 0, len(keys))
	for _, kb := range keys {
		k := makeKey(kb)
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		stringKeys = append(stringKeys, k)
	}

	for _, k := range stringKeys {
		// Already present?
		c.MutexLock.RLock()
		if _, ok := c.data[k]; ok {
			c.MutexLock.RUnlock()
			successes++ // already cached counts as success
			continue
		}
		c.MutexLock.RUnlock()

		// Miss → read from Pebble (no locks held during I/O)
		val, closer, err := c.db.Get([]byte(k))
		if err == pebble.ErrNotFound {
			if closer != nil {
				_ = closer.Close()
			}
			continue // not a success; key doesn't exist
		}
		if err != nil {
			if closer != nil {
				_ = closer.Close()
			}
			// Return the first real error; partial prefetches may have succeeded.
			return successes, err
		}

		// Copy before closing the closer so we own the bytes.
		copied_val := append([]byte(nil), val...)
		if closer != nil {
			_ = closer.Close()
		}

		// Insert only if there is room (do not evict).
		c.MutexLock.Lock()
		if _, ok := c.data[k]; ok {
			// Raced with another inserter: treat as success.
			c.MutexLock.Unlock()
			successes++
			continue
		}

		if c.RemainingCapacity() == 0 {
			// No space → skip (non-evicting prefetch)
			c.MutexLock.Unlock()
			continue
		}

		// Add to cache.

		en := &CacheEntry{
			value:          copied_val,
			size:           len(copied_val),
			first_inserted: time.Now(),
			last_updated:   time.Now(),
		}

		c.data[k] = en
		c.MutexLock.Unlock()
		successes++
	}

	return successes, nil
}
