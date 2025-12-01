package densitycache

import (
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rohannunu/pebble-cs598rap/cache"
)

// -------------------- Globals & constants --------------------

var globalAccess uint64 // monotonically increasing access counter

const (
	NumAgeBuckets  = 32
	NumRefClasses  = 4
	SampleSize     = 5   // how many entries to sample when evicting
	recomputeEvery = 500 // how often to recompute HD table (in lifetime updates), this is expensive
)

// stats: hits & lifetimes per (refClass, ageBucket)
var hits [NumRefClasses][NumAgeBuckets]uint64
var lifetimes [NumRefClasses][NumAgeBuckets]uint64

// precomputed hit density per (refClass, ageBucket)
var hd [NumRefClasses][NumAgeBuckets]float64
var hdMu sync.RWMutex

var statsUpdates uint64

func init() {
	rand.Seed(time.Now().UnixNano())
}

type DensityStats struct {
	Hits       uint64 // front-cache (metadata) hits
	Misses     uint64 // front-cache misses
	Admissions uint64 // objects admitted into the front-cache
	Evictions  uint64 // objects evicted from the front-cache
}

func (s *DensityStats) RecordHit()       { atomic.AddUint64(&s.Hits, 1) }
func (s *DensityStats) RecordMiss()      { atomic.AddUint64(&s.Misses, 1) }
func (s *DensityStats) RecordAdmission() { atomic.AddUint64(&s.Admissions, 1) }
func (s *DensityStats) RecordEvict()     { atomic.AddUint64(&s.Evictions, 1) }

func (s *DensityStats) Snapshot() DensityStats {
	return DensityStats{
		Hits:       atomic.LoadUint64(&s.Hits),
		Misses:     atomic.LoadUint64(&s.Misses),
		Admissions: atomic.LoadUint64(&s.Admissions),
		Evictions:  atomic.LoadUint64(&s.Evictions),
	}
}

type Entry struct {
	Key        string
	LastAccess uint64 // last access (globalAccess)
	Refs       uint64 // how many times this was referenced
}

// DensityCache is a policy wrapper around *cache.Cache,
// similar to your LRUCache: it decides when to Evict / Set(addToCache)
// based on LHD hit-density estimates.
type DensityCache struct {
	cache    *cache.Cache      // underlying write-back cache + Pebble
	capacity int               // max number of keys we manage in-memory
	entries  map[string]*Entry // key -> metadata entry
	keys     []string          // for random sampling in eviction

	// mutex protects `entries` and `keys` for concurrent access
	Mutex sync.RWMutex

	// per-cache RNG to avoid races on math/rand's global RNG
	rng   *rand.Rand
	rngMu sync.Mutex

	stats *DensityStats
}

func NewDensityCache(capacity int) *DensityCache {
	return &DensityCache{
		cache:    cache.CreateCacheAndPebble(capacity),
		capacity: capacity,
		entries:  make(map[string]*Entry),
		keys:     make([]string, 0, capacity),
		stats:    &DensityStats{},
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (dc *DensityCache) Stats() DensityStats {
	return dc.stats.Snapshot()
}

func AgeBucket(age uint64) int {
	if age >= NumAgeBuckets-1 {
		return NumAgeBuckets - 1
	}
	return int(age)
}

func RefClass(refs uint64) int {
	if refs >= NumRefClasses-1 {
		return NumRefClasses - 1
	}
	return int(refs)
}

func recordLifetimeHit(e *Entry, now uint64) {
	age := now - e.LastAccess
	a := AgeBucket(age)
	rc := RefClass(e.Refs)

	atomic.AddUint64(&hits[rc][a], 1)
	atomic.AddUint64(&lifetimes[rc][a], 1)
	bumpStats()
}

func recordLifetimeEvict(e *Entry, now uint64) {
	age := now - e.LastAccess
	a := AgeBucket(age)
	rc := RefClass(e.Refs)

	atomic.AddUint64(&lifetimes[rc][a], 1)
	bumpStats()
}

func bumpStats() {
	if atomic.AddUint64(&statsUpdates, 1)%recomputeEvery == 0 {
		recomputeHD()
	}
}

// recomputeHD recomputes the per-(refClass, ageBucket) hit-density table.
// HD(a,c) ≈ P(hit | age>=a,c) / E[remaining_life | age>=a,c], dropping size term.
func recomputeHD() {
	hdMu.Lock()
	defer hdMu.Unlock()
	for c := 0; c < NumRefClasses; c++ {
		var hitsTail, lifeTail, lifeTimeTail uint64

		for a := NumAgeBuckets - 1; a >= 0; a-- {
			h := atomic.LoadUint64(&hits[c][a])
			l := atomic.LoadUint64(&lifetimes[c][a])

			hitsTail += h
			lifeTail += l
			// use bucket index as approximate lifetime age
			lifeTimeTail += l * uint64(a)

			if lifeTail == 0 {
				hd[c][a] = 0
				continue
			}

			hitProb := float64(hitsTail) / float64(lifeTail)
			avgLifeAge := float64(lifeTimeTail) / float64(lifeTail)
			remLife := avgLifeAge - float64(a)
			if remLife <= 0 {
				hd[c][a] = 0
				continue
			}

			// all objects same size, so ignore size
			hd[c][a] = hitProb / remLife
		}
	}
}

// -------------------- Core API: Get / Set --------------------

// Get looks up metadata in DensityCache, but always pulls bytes from dc.cache.
// dc.cache internally decides whether the bytes come from its in-memory map or Pebble.
func (dc *DensityCache) Get(key []byte, async bool) ([]byte, bool, error) {
	now := atomic.AddUint64(&globalAccess, 1)
	k := string(key)
	// Check presence under read lock.
	dc.Mutex.RLock()
	e, ok := dc.entries[k]
	dc.Mutex.RUnlock()

	if ok {
		// Logical cache hit for this key: update stats and lifetime under write lock
		// to avoid races when mutating the Entry.
		dc.Mutex.Lock()
		dc.stats.RecordHit()
		recordLifetimeHit(e, now)
		e.LastAccess = now
		e.Refs++
		dc.Mutex.Unlock()

		// Fetch the actual value without holding the metadata lock.
		val, found, err := dc.cache.Get(key)
		if err != nil {
			return nil, false, err
		}
		return val, found, nil
	}

	// Not in our metadata; ask the underlying cache (which may hit its in-memory map or Pebble).
	dc.stats.RecordMiss()
	val, found, err := dc.cache.Get(key)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}

	// We got a value; decide whether to cache it in-memory by LHD policy.
	if err := dc.admit(key, val, now, async); err != nil {
		// admission failure just means we skip caching; still return value
		return val, true, nil
	}

	return val, true, nil
}

// Set forwards to the underlying cache, and if toCache == true,
// we also maintain LHD metadata and potentially evict something.
func (dc *DensityCache) Set(key, value []byte, toCache bool, async bool) (bool, error) {
	if !toCache {
		// direct write to Pebble (no in-memory cache)
		return dc.cache.Set(key, value, false, async)
	}

	now := atomic.AddUint64(&globalAccess, 1)
	k := string(key)
	// If already tracked, treat as a "hit" lifetime.
	dc.Mutex.RLock()
	e, ok := dc.entries[k]
	dc.Mutex.RUnlock()

	if ok {
		// Update the underlying cache first (I/O outside locks), then update metadata.
		cached, err := dc.cache.Set(key, value, true, async)
		if err != nil {
			return cached, err
		}
		if cached {
			dc.Mutex.Lock()
			dc.stats.RecordHit()
			recordLifetimeHit(e, now)
			e.LastAccess = now
			e.Refs++
			dc.Mutex.Unlock()
		}
		return cached, nil
	}

	// New key: ensure room in our front-cache metadata.
	dc.stats.RecordMiss()
	dc.Mutex.RLock()
	needEvict := len(dc.entries) >= dc.capacity
	dc.Mutex.RUnlock()
	if needEvict {
		if err := dc.evictOne(now, async); err != nil {
			return false, err
		}
	}

	// Insert into underlying cache (I/O outside lock).
	cached, err := dc.cache.Set(key, value, true, async)
	if err != nil || !cached {
		// If underlying refused to cache (capacity?), we also skip metadata.
		return cached, err
	}

	// Track metadata for this key under lock.
	e = &Entry{
		Key:        k,
		LastAccess: now,
		Refs:       1,
	}
	dc.Mutex.Lock()
	dc.entries[k] = e
	dc.keys = append(dc.keys, k)
	dc.Mutex.Unlock()
	dc.stats.RecordAdmission()

	return true, nil
}

// admit decides to put (key,value) into the cache by:
// 1) evicting a victim (chosen by min HD) if needed,
// 2) calling dc.cache.Set(key, value, true),
// 3) installing metadata Entry.
func (dc *DensityCache) admit(key, value []byte, now uint64, async bool) error {
	k := string(key)

	// Already tracked (rare in Get path)? Nothing to do.
	dc.Mutex.RLock()
	_, ok := dc.entries[k]
	dc.Mutex.RUnlock()
	if ok {
		return nil
	}

	// Ensure room in metadata/front-cache.
	dc.Mutex.RLock()
	needEvict := len(dc.entries) >= dc.capacity
	dc.Mutex.RUnlock()
	if needEvict {
		if err := dc.evictOne(now, async); err != nil {
			return err
		}
	}

	// Cache bytes in underlying cache (I/O outside lock).
	cached, err := dc.cache.Set(key, value, true, async)
	if err != nil || !cached {
		return err
	}

	// Track metadata for this key under lock.
	e := &Entry{
		Key:        k,
		LastAccess: now,
		Refs:       1,
	}
	dc.Mutex.Lock()
	dc.entries[k] = e
	dc.keys = append(dc.keys, k)
	dc.Mutex.Unlock()
	dc.stats.RecordAdmission()
	return nil
}

// evictOne samples a few keys and evicts the one with minimum hit density.
func (dc *DensityCache) evictOne(now uint64, async bool) error {
	// Take a snapshot of keys and entries under read lock to avoid holding
	// the lock during sampling and I/O.
	dc.Mutex.RLock()
	n := len(dc.keys)
	if n == 0 {
		dc.Mutex.RUnlock()
		return nil
	}
	keysCopy := make([]string, n)
	copy(keysCopy, dc.keys)
	// Copy entries by value so we don't hold pointers into live Entry structs.
	entriesCopy := make(map[string]Entry, len(dc.entries))
	for kk, vv := range dc.entries {
		entriesCopy[kk] = *vv
	}
	dc.Mutex.RUnlock()

	bestIdx := -1
	bestScore := math.MaxFloat64

	samples := SampleSize
	if n < samples {
		samples = n
	}

	// Read hd under its RWMutex to avoid races with recomputeHD.
	// Use a per-cache RNG guarded by rngMu to avoid races on the
	// package-level math/rand global state.
	hdMu.RLock()
	for i := 0; i < samples; i++ {
		dc.rngMu.Lock()
		idx := dc.rng.Intn(n)
		dc.rngMu.Unlock()

		k := keysCopy[idx]
		e, ok := entriesCopy[k]
		if !ok {
			continue
		}

		age := now - e.LastAccess
		a := AgeBucket(age)
		rc := RefClass(e.Refs)
		score := hd[rc][a]

		if score < bestScore {
			bestScore = score
			bestIdx = idx
		}
	}
	hdMu.RUnlock()

	if bestIdx == -1 {
		return nil
	}

	victimKey := keysCopy[bestIdx]

	// Evict from underlying write-back cache (this writes key/value to Pebble).
	_, err := dc.cache.Evict([]byte(victimKey), async)
	if err != nil {
		return err
	}

	// Safely record eviction lifetime and remove metadata under the metadata lock.
	dc.Mutex.Lock()
	if liveE, ok := dc.entries[victimKey]; ok {
		// Use the live entry to compute lifetime (consistent values under lock).
		recordLifetimeEvict(liveE, now)

		// find index of victimKey in live keys
		idx := -1
		for i, kk := range dc.keys {
			if kk == victimKey {
				idx = i
				break
			}
		}
		if idx != -1 {
			last := len(dc.keys) - 1
			dc.keys[idx] = dc.keys[last]
			dc.keys = dc.keys[:last]
			delete(dc.entries, victimKey)
			dc.stats.RecordEvict()
		}
	}
	dc.Mutex.Unlock()

	return nil
}

// Close forwards to the underlying cache.
func (dc *DensityCache) Close() error {
	return dc.cache.Close()
}
