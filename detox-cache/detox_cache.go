package detoxcache

import (
	"math"
	"sync"
	"sync/atomic"

	"github.com/rohannunu/pebble-cs598rap/cache"
)

var globalAccess uint64

// -------------------- Stats --------------------

type DeToXStats struct {
	Hits        uint64
	Misses      uint64
	Admissions  uint64
	Evictions   uint64
	Prefetches  uint64
	Transactions uint64
}

func (s *DeToXStats) RecordHit()        { atomic.AddUint64(&s.Hits, 1) }
func (s *DeToXStats) RecordMiss()       { atomic.AddUint64(&s.Misses, 1) }
func (s *DeToXStats) RecordAdmission()  { atomic.AddUint64(&s.Admissions, 1) }
func (s *DeToXStats) RecordEviction()   { atomic.AddUint64(&s.Evictions, 1) }
func (s *DeToXStats) RecordPrefetch()   { atomic.AddUint64(&s.Prefetches, 1) }
func (s *DeToXStats) RecordTxn()        { atomic.AddUint64(&s.Transactions, 1) }

func (s *DeToXStats) Snapshot() DeToXStats {
	return DeToXStats{
		Hits:         atomic.LoadUint64(&s.Hits),
		Misses:       atomic.LoadUint64(&s.Misses),
		Admissions:   atomic.LoadUint64(&s.Admissions),
		Evictions:    atomic.LoadUint64(&s.Evictions),
		Prefetches:   atomic.LoadUint64(&s.Prefetches),
		Transactions: atomic.LoadUint64(&s.Transactions),
	}
}

// -------------------- Existing structs --------------------

type Level struct {
	Keys      []string
	Timestamp uint64
}

type Transaction struct {
	Levels    []Level
	Timestamp uint64
}

type KeyMetadata struct {
	TotalScore   float64
	Frequency    uint64
	Size         int
	LastAccess   uint64
	Transactions []string
}

type GroupScore struct {
	Keys      []string
	Score     float64
	Frequency uint64
	LengthRed int
}

type DependencySet struct {
	Keys      []string
	Frequency uint64
}

type DeToXCache struct {
	cache       *cache.Cache
	capacity    int
	metadata    map[string]*KeyMetadata
	keys        []string
	agingFactor float64
	depSets     map[string][]*DependencySet
	txnHistory  []*Transaction
	closed      bool
	stats       *DeToXStats

	// mutex protects `metadata`, `keys`, `depSets`, and `txnHistory` for concurrent access
	mu sync.RWMutex
}

func NewDeToXCache(capacity int) *DeToXCache {
	return &DeToXCache{
		cache:      cache.CreateCacheAndPebble(capacity),
		capacity:   capacity,
		metadata:   make(map[string]*KeyMetadata),
		keys:       make([]string, 0, capacity),
		depSets:    make(map[string][]*DependencySet),
		txnHistory: make([]*Transaction, 0),
		stats:      &DeToXStats{},
	}
}

// Expose stats to wrappers (e.g., YCSB creator).
func (dc *DeToXCache) Stats() DeToXStats {
	return dc.stats.Snapshot()
}

func (dc *DeToXCache) scoreGroup(keys []string, lengthReduction int) float64 {
	if len(keys) == 0 {
		return 0
	}

	dc.mu.RLock()
	minFreq := uint64(math.MaxUint64)
	totalSize := 0

	for _, k := range keys {
		if meta, ok := dc.metadata[k]; ok {
			if meta.Frequency < minFreq {
				minFreq = meta.Frequency
			}
			totalSize += meta.Size
		} else {
			minFreq = 1
			totalSize += 1
		}
	}
	dc.mu.RUnlock()

	if totalSize == 0 {
		totalSize = 1
	}

	return float64(minFreq*uint64(lengthReduction)) / float64(totalSize)
}

func (dc *DeToXCache) updateKeyScore(key string, instanceScore float64) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	meta, ok := dc.metadata[key]
	if !ok {
		meta = &KeyMetadata{
			TotalScore:   0,
			Frequency:    0,
			Size:         1,
			LastAccess:   atomic.AddUint64(&globalAccess, 1),
			Transactions: make([]string, 0),
		}
		dc.metadata[key] = meta
	}

	meta.Frequency++
	meta.TotalScore += instanceScore
	meta.LastAccess = atomic.AddUint64(&globalAccess, 1)
}

func (dc *DeToXCache) getKeyScore(key string) float64 {
	dc.mu.RLock()
	defer dc.mu.RUnlock()

	meta, ok := dc.metadata[key]
	if !ok {
		return dc.agingFactor
	}

	if meta.Frequency == 0 {
		return dc.agingFactor
	}

	avgScore := meta.TotalScore / float64(meta.Frequency)
	return avgScore + dc.agingFactor
}

func (dc *DeToXCache) scoreTransaction(levels []Level) {
	if len(levels) == 0 {
		return
	}

	criticalLength := len(levels)
	groupScores := make([]*GroupScore, 0)

	for i := 0; i < len(levels); i++ {
		keys := levels[i].Keys
		if len(keys) == 0 {
			continue
		}

		lengthReduction := 1
		if i == 0 {
			lengthReduction = 1
		}

		score := dc.scoreGroup(keys, lengthReduction)
		groupScores = append(groupScores, &GroupScore{
			Keys:      keys,
			Score:     score,
			Frequency: 0,
			LengthRed: lengthReduction,
		})
	}

	for _, group := range groupScores {
		for _, key := range group.Keys {
			dc.updateKeyScore(key, group.Score)
		}
	}

	_ = criticalLength
}

func (dc *DeToXCache) Get(key []byte, async bool) ([]byte, bool, error) {
	k := string(key)

	// Fetch the actual value without holding the metadata lock.
	val, found, err := dc.cache.Get(key)
	if err != nil {
		return nil, false, err
	}

	if found {
		// Check presence under read lock first.
		dc.mu.RLock()
		meta, ok := dc.metadata[k]
		dc.mu.RUnlock()

		if ok {
			// Logical hit in DeToX: update metadata under write lock.
			dc.mu.Lock()
			dc.stats.RecordHit()
			meta.Frequency++
			meta.LastAccess = atomic.AddUint64(&globalAccess, 1)
			dc.mu.Unlock()
		} else {
			// Value exists but DeToX has never seen this key => miss + admission candidate
			dc.mu.Lock()
			dc.stats.RecordMiss()
			dc.metadata[k] = &KeyMetadata{
				TotalScore:   0,
				Frequency:    1,
				Size:         len(val),
				LastAccess:   atomic.AddUint64(&globalAccess, 1),
				Transactions: make([]string, 0),
			}
			dc.keys = append(dc.keys, k)
			dc.stats.RecordAdmission()
			dc.mu.Unlock()
		}
		if !dc.closed {
			dc.Prefetch(key)
		}
	} else {
		// not found at all
		dc.stats.RecordMiss()
	}

	return val, found, nil
}

func (dc *DeToXCache) Set(key, value []byte, toCache bool, async bool) (bool, error) {
	if !toCache {
		// direct write-through to Pebble
		return dc.cache.Set(key, value, false, async)
	}

	k := string(key)

	// Check if already tracked under read lock.
	dc.mu.RLock()
	meta, ok := dc.metadata[k]
	dc.mu.RUnlock()

	if ok {
		// Update the underlying cache first (I/O outside locks), then update metadata.
		cached, err := dc.cache.Set(key, value, true, async)
		if err != nil {
			return cached, err
		}
		if cached {
			dc.mu.Lock()
			dc.stats.RecordHit()
			meta.Frequency++
			meta.LastAccess = atomic.AddUint64(&globalAccess, 1)
			meta.Size = len(value)
			dc.mu.Unlock()
		}
		return cached, nil
	}

	// New key: ensure room in our metadata.
	dc.mu.RLock()
	needEvict := len(dc.metadata) >= dc.capacity
	dc.mu.RUnlock()

	if needEvict {
		if err := dc.evictVictim(async); err != nil {
			return false, err
		}
	}

	// Insert into underlying cache (I/O outside lock).
	cached, err := dc.cache.Set(key, value, true, async)
	if err != nil || !cached {
		return cached, err
	}

	// Track metadata for this key under lock.
	newMeta := &KeyMetadata{
		TotalScore:   0,
		Frequency:    1,
		Size:         len(value),
		LastAccess:   atomic.AddUint64(&globalAccess, 1),
		Transactions: make([]string, 0),
	}
	dc.mu.Lock()
	dc.metadata[k] = newMeta
	dc.keys = append(dc.keys, k)
	dc.mu.Unlock()
	dc.stats.RecordAdmission()

	return true, nil
}

func (dc *DeToXCache) evictVictim(async bool) error {
	// Take a snapshot of keys under read lock to avoid holding the lock during scoring.
	dc.mu.RLock()
	n := len(dc.keys)
	if n == 0 {
		dc.mu.RUnlock()
		return nil
	}
	keysCopy := make([]string, n)
	copy(keysCopy, dc.keys)
	dc.mu.RUnlock()

	victimIdx := -1
	lowestScore := math.MaxFloat64

	for i, k := range keysCopy {
		score := dc.getKeyScore(k) // getKeyScore handles its own locking
		if score < lowestScore {
			lowestScore = score
			victimIdx = i
		}
	}

	if victimIdx == -1 {
		return nil
	}

	victimKey := keysCopy[victimIdx]

	// Evict from underlying cache (I/O outside lock).
	_, err := dc.cache.Evict([]byte(victimKey), async)
	if err != nil {
		return err
	}

	// Safely remove metadata under lock.
	dc.mu.Lock()
	if _, ok := dc.metadata[victimKey]; ok {
		// Update aging factor
		dc.agingFactor = lowestScore

		// Find index of victimKey in live keys
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
		}
		delete(dc.metadata, victimKey)

		// Eviction tracked here
		dc.stats.RecordEviction()
	}
	dc.mu.Unlock()

	return nil
}

func (dc *DeToXCache) ExecuteTransaction(levels [][][]byte, async bool) (map[string][]byte, error) {
	now := atomic.AddUint64(&globalAccess, 1)
	results := make(map[string][]byte)
	txnLevels := make([]Level, 0)

	for _, levelKeys := range levels {
		level := Level{
			Keys:      make([]string, 0),
			Timestamp: now,
		}

		for _, key := range levelKeys {
			k := string(key)
			level.Keys = append(level.Keys, k)

			val, found, err := dc.Get(key, async) // Get handles its own locking
			if err != nil {
				return nil, err
			}

			if found {
				results[k] = val
			}
		}

		if len(level.Keys) > 0 {
			txnLevels = append(txnLevels, level)
		}
	}

	dc.scoreTransaction(txnLevels) // scoreTransaction -> updateKeyScore handles locking

	txn := &Transaction{
		Levels:    txnLevels,
		Timestamp: now,
	}

	// Append to transaction history under lock.
	dc.mu.Lock()
	dc.txnHistory = append(dc.txnHistory, txn)
	dc.mu.Unlock()

	// track a transaction-level counter
	dc.stats.RecordTxn()

	return results, nil
}

func (dc *DeToXCache) recordDependency(sourceKey string, dependentKeys []string) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	if _, ok := dc.depSets[sourceKey]; !ok {
		dc.depSets[sourceKey] = make([]*DependencySet, 0)
	}

	found := false
	for _, depSet := range dc.depSets[sourceKey] {
		if len(depSet.Keys) == len(dependentKeys) {
			match := true
			for i, k := range depSet.Keys {
				if k != dependentKeys[i] {
					match = false
					break
				}
			}
			if match {
				depSet.Frequency++
				found = true
				break
			}
		}
	}

	if !found {
		dc.depSets[sourceKey] = append(dc.depSets[sourceKey], &DependencySet{
			Keys:      dependentKeys,
			Frequency: 1,
		})
	}
}

func (dc *DeToXCache) Prefetch(sourceKey []byte) {
	// record one prefetch request (not per key) for now
	dc.stats.RecordPrefetch()
	go dc.prefetchAsync(sourceKey)
}

func (dc *DeToXCache) prefetchAsync(sourceKey []byte) {
	if dc.closed {
		return
	}

	k := string(sourceKey)

	// Read depSets under lock and make a copy of the most frequent set.
	dc.mu.RLock()
	depSets, ok := dc.depSets[k]
	if !ok || len(depSets) == 0 {
		dc.mu.RUnlock()
		return
	}

	var mostFrequentKeys []string
	maxFreq := uint64(0)
	for _, depSet := range depSets {
		if depSet.Frequency > maxFreq {
			maxFreq = depSet.Frequency
			mostFrequentKeys = make([]string, len(depSet.Keys))
			copy(mostFrequentKeys, depSet.Keys)
		}
	}
	dc.mu.RUnlock()

	if mostFrequentKeys == nil {
		return
	}

	prefetchKeys := make([][]byte, 0, len(mostFrequentKeys))
	for _, key := range mostFrequentKeys {
		prefetchKeys = append(prefetchKeys, []byte(key))
	}

	if !dc.closed {
		dc.cache.Prefetch(prefetchKeys)
	}
}

func (dc *DeToXCache) Close() error {
	dc.closed = true
	return dc.cache.Close()
}
